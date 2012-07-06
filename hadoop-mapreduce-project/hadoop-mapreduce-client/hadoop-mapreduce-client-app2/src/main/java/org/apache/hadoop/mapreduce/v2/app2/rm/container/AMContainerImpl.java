package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.jobhistory.ContainerHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorStopRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorContainerDeAllocateRequestEvent;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

public class AMContainerImpl implements AMContainer {

  private static final Log LOG = LogFactory.getLog(AMContainerImpl.class);

  private final ReadLock readLock;
  private final WriteLock writeLock;
  // TODO Use ContainerId or a custom JvmId.
  private final ContainerId containerId;
  private final AppContext appContext;
  private final ContainerHeartbeatHandler chh;
  
  private final List<TaskAttemptId> completedAttempts = new LinkedList<TaskAttemptId>();

  // TODO Maybe this should be pulled from the TaskAttempt.s
  private final Map<TaskAttemptId, org.apache.hadoop.mapred.Task> remoteTaskMap 
      = new HashMap<TaskAttemptId, org.apache.hadoop.mapred.Task>();
  
  // Convert to list and hash.
  
  private TaskAttemptId pendingAttempt;
  private TaskAttemptId runningAttempt;
  private TaskAttemptId failedAttempt;
  private TaskAttemptId pullAttempt;
  
  private ContainerLaunchContext clc;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;

  private static boolean stateMachineInited = false;
  private static final StateMachineFactory
      <AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent> 
      stateMachineFactory = 
      new StateMachineFactory<AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent>(
      AMContainerState.ALLOCATED);
  
  private final StateMachine<AMContainerState, AMContainerEventType, AMContainerEvent> stateMachine;

  private void initStateMachineFactory() {
    stateMachineFactory
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.LAUNCHING, AMContainerEventType.C_START_REQUEST, createLaunchRequestTransition())
        // TODO Deal with TA_ASSIGNs - and move to an error state for the Container. Release the TA - so that it can be resheduled.
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_STOP_REQUEST, createStopRequestTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_NODE_FAILED, createStopRequestTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED)
        
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.LAUNCHING, AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.IDLE, AMContainerEventType.C_LAUNCHED, createLaunchedTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED, AMContainerEventType.C_LAUNCH_FAILED, createLaunchFailedTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtLaunchingTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED, AMContainerEventType.C_STOP_REQUEST, createStopRequestAtLaunchingTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED, AMContainerEventType.C_NODE_FAILED, createStopRequestAtLaunchingTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.LAUNCHING, AMContainerEventType.C_PULL_TA) // Is assuming the pullAttempt will be null.
        
        .addTransition(AMContainerState.IDLE, AMContainerState.IDLE, AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptTransition())
        .addTransition(AMContainerState.IDLE, EnumSet.of(AMContainerState.RUNNING, AMContainerState.IDLE), AMContainerEventType.C_PULL_TA, createPullTAAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, AMContainerEventType.C_STOP_REQUEST, createStopRequestAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, AMContainerEventType.C_TIMED_OUT, createTimedOutAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtIdleTransition())
        
        .addTransition(AMContainerState.RUNNING, AMContainerState.IDLE, AMContainerEventType.C_TA_SUCCEEDED, createTASucceededAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_STOP_REQUEST, createStopRequestAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_TIMED_OUT, createTimedOutAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtIdleTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.RUNNING, AMContainerEventType.C_PULL_TA)
        
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_ASSIGN_TA, createAssignTAAtStoppingTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_TA_SUCCEEDED, createTASucceededAtStoppingTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtStoppingTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_STOP_FAILED, createStopFailedTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_TIMED_OUT, createStopFailedTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedBaseTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, EnumSet.of(AMContainerEventType.C_PULL_TA, AMContainerEventType.C_STOP_REQUEST))
        
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, AMContainerEventType.C_ASSIGN_TA, createAssignTAAtStoppingTransition())
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, AMContainerEventType.C_NODE_FAILED, createNodeFailedBaseTransition())
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, EnumSet.of(AMContainerEventType.C_PULL_TA, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_TIMED_OUT))
 
        .installTopology();
  }
  
  @SuppressWarnings("rawtypes")
  public AMContainerImpl(ContainerId containerId,
      ContainerHeartbeatHandler chh, EventHandler eventHandler,
      AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.containerId = containerId;
    this.eventHandler = eventHandler;
    this.appContext = appContext;
    this.chh = chh;

    synchronized (stateMachineFactory) {
      if (!stateMachineInited) {
        initStateMachineFactory();
        stateMachineInited = true;
      }
    }
    this.stateMachine = stateMachineFactory.make(this);
  }
  
  @Override
  public AMContainerState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ContainerId getContainerId() {
    return this.containerId;
  }

  @Override
  public List<TaskAttemptId> getCompletedTaskAttempts() {
    readLock.lock();
    try {
      return new ArrayList<TaskAttemptId>(this.completedAttempts);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<TaskAttemptId> getQueuedTaskAttempts() {
    readLock.lock();
    try {
      return Collections.singletonList(this.pendingAttempt);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptId getRunningTaskAttempt() {
    readLock.lock();
    try {
      return this.runningAttempt;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void handle(AMContainerEvent event) {
    this.writeLock.lock();
    try {
      final AMContainerState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle event " + event.getType()
            + " at current state " + oldState + " for ContainerId "
            + this.containerId, e);
        // TODO Should this fail the job ?
      }
      if (oldState != getState()) {
        LOG.info("AMContainer " + this.containerId + " transitioned from "
            + oldState + " to " + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  // TODO This is completely broken. Temporary code for getting the sync call
  // through the state machine.
  // TODO Maybe have pullTA sned out an attemptId. TAL talks to the TaskAttempt
  // to fetch the actual RemoteTask.
  public org.apache.hadoop.mapred.Task pullTaskAttempt() {
    this.writeLock.lock();
    try {
      // Force through the state machine.
      this.handle(new AMContainerEvent(containerId,
          AMContainerEventType.C_PULL_TA));
      return pullAttempt == null ? null : remoteTaskMap.get(pullAttempt);
      // TODO Clear the remoteTask at this point.
    } finally {
      this.pullAttempt = null;
      this.writeLock.unlock();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createLaunchRequestTransition() {
    return new LaunchRequest();
  }

  protected static class LaunchRequest implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerLaunchRequestEvent event = (AMContainerLaunchRequestEvent) cEvent;
      // TODO May be possible to forget about the clc or a part of it after
      // launch. Save AM resources.
      container.clc = event.getContainerLaunchContext();
      container.sendEvent(new NMCommunicatorLaunchRequestEvent(container.clc));
      LOG.info("Sending Launch Request for Container with id: "
          + container.clc.getContainerId());
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createStopRequestTransition() {
    return new StopRequest();
  }

  // TODO Rename to de-allocate container transition. subclass
  protected static class StopRequest implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendEvent(new RMCommunicatorContainerDeAllocateRequestEvent(
          container.containerId));
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createAssignTaskAttemptTransition() {
    return new AssignTaskAttempt();
  }

  // TODO Make this the base for all assignRequests. Some more error checking in
  // that case.
  protected static class AssignTaskAttempt implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      if (container.pendingAttempt != null) {
        // TODO Error. Good case for the error handler listed in a TODO further down.
        throw new InvalidStateTransitonException(container.getState(),
            cEvent.getType());
      }
      container.pendingAttempt = event.getTaskAttemptId();
      container.remoteTaskMap.put(event.getTaskAttemptId(), event.getRemoteTask());
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createLaunchedTransition() {
    return new Launched();
  }

  protected static class Launched implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      // TODO XXX: Register with ContainerHeartbeatHandler
    }
  }

  protected MultipleArcTransition<AMContainerImpl, AMContainerEvent, 
      AMContainerState> createPullTAAtIdleTransition() {
    return new PullTAAtIdle();
  }

  protected static class PullTAAtIdle implements MultipleArcTransition<
      AMContainerImpl, AMContainerEvent, AMContainerState> {

    @Override
    public AMContainerState transition(AMContainerImpl container,
        AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        container.pullAttempt = container.pendingAttempt;
        container.runningAttempt = container.pendingAttempt;
        container.pendingAttempt = null;
        return AMContainerState.RUNNING;
      } else {
        return AMContainerState.IDLE;
      }
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createLaunchFailedTransition() {
    return new LaunchFailed();
  }

  protected static class LaunchFailed extends CompletedAtLaunching {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      // TODO XXX: Is this de-allocate required ?
      container.sendEvent(new RMCommunicatorContainerDeAllocateRequestEvent(
          container.containerId));
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtLaunchingTransition() {
    return new CompletedAtLaunching();
  }

  protected static class CompletedAtLaunching implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        container.sendEvent(new TaskAttemptEvent(container.pendingAttempt,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
      }
      // Running attempt should not be populated. Not at this point anyway.
      // TODO if re-using - checks on running attempt as well.
    }
  }
 
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createStopRequestAtLaunchingTransition() {
    return new StopRequestAtLaunching();
  }

  protected static class StopRequestAtLaunching extends StopRequest {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      // TODO Is this needed / harmful ? After the previous discussion, this
      // wasn't needed. Can't figure out why not.
      // TODO XXX -> stops need to be reviewed.
      super.transition(container, cEvent);

      container.sendEvent(new NMCommunicatorStopRequestEvent(container
          .getContainerId()));
      if (container.pendingAttempt != null) {
        container.sendEvent(new TaskAttemptEvent(container.pendingAttempt,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
      }
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtIdleTransition() {
    return new CompletedAtIdle();
  }
  
  // TODO Create a base CompletedTransition - at least contains some kind of logging.
  protected static class CompletedAtIdle implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      // TODO Maybe send out an event to the TaskAttemptCleaner ?
      LOG.info("Cotnainer with id: " + container.getContainerId()
          + " Completed." + " Previous state was: " + container.getState());
      // TODO XXX: Move all these inform TA messages out to a function. Used in lots of places. Similarly for inform NMComm. Infrom RMComm etc.
      // Create a set of functions like .... informPendingTasks -> will take care of informing running, pending tasks. informCompletedTasks()
      if (container.pendingAttempt != null) {
        container.sendEvent(new TaskAttemptEvent(container.pendingAttempt,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
      }
      // TODO XXX Unregister from CHH
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createStopRequestAtIdleTransition() {
    return new StopRequestAtIdle();
  }
  
  protected static class StopRequestAtIdle implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      // Event to RMComm not required - since a STOP_FAILED now exists - which
      // is supposed to indicate all FAILURES in the stop call.
      
      container.sendEvent(new NMCommunicatorStopRequestEvent(container
          .getContainerId()));
      
      // TODO This will be sent out in the STOPPING to COMPLETED transition. That ends up delaying TA status change. if doing async cleanup via Container transitions - this will need to got out from here.
//      if (container.pendingAttempt != null) {
//        container.sendEvent(new TaskAttemptEvent(container.pendingAttempt
//            .getTaskAttemptId(), TaskAttemptEventType.TA_CONTAINER_COMPLETED));
//      }
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createTimedOutAtIdleTransition() {
    return new TimedOutAtIdle();
  }
  
  protected static class TimedOutAtIdle extends StopRequestAtIdle {
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createTASucceededAtRunningTransition() {
    return new TASucceededAtRunning();
  }
  
  protected static class TASucceededAtRunning implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.completedAttempts.add(container.runningAttempt);
      container.runningAttempt = null;
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtRunningTransition() {
    return new CompletedAtRunning();
  }

  protected static class CompletedAtRunning implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.failedAttempt = container.runningAttempt;
      container.runningAttempt = null;

      container.sendEvent(new TaskAttemptEvent(container.failedAttempt,
          TaskAttemptEventType.TA_CONTAINER_COMPLETED));
      
      // TODO XXX: Unregister from CHH.
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createStopRequestAtRunningTransition() {
    return new StopRequestAtRunning();
  }
  
  protected static class StopRequestAtRunning extends StopRequestAtIdle {
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createTimedOutAtRunningTransition() {
    return new TimedOutAtRunning();
  }

  protected static class TimedOutAtRunning extends StopRequestAtRunning {
  }

  // TODO Rename - is re-used in COMPLETED states.
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createAssignTAAtStoppingTransition() {
    return new AssignTAAtStopping();
  }

  protected static class AssignTAAtStopping implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      container.sendEvent(new TaskAttemptEvent(event.getTaskAttemptId(),
          TaskAttemptEventType.TA_CONTAINER_COMPLETED));
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createTASucceededAtStoppingTransition() {
    return new TASucceededAtStopping();
  }

  // TODO Extend SUcceededAtRunning. Eventually get rid of it when the
  // _IN_PROGRESS states are removed.
  protected static class TASucceededAtStopping implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.completedAttempts.add(container.runningAttempt);
      container.runningAttempt = null;
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtStoppingTransition() {
    return new CompletedAtStopping();
  }
  
  protected static class CompletedAtStopping implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.runningAttempt != null) {
        container.failedAttempt = container.runningAttempt;
        container.runningAttempt = null;

        container.sendEvent(new TaskAttemptEvent(container.failedAttempt,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
      }
      // Make sure pendingAttempt is not sent out multiple times, since noone nullifies it.
      if (container.pendingAttempt != null) {
        container.sendEvent(new TaskAttemptEvent(container.pendingAttempt,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
      }

      // TODO XXX: Unregister from CHH.
    }

  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createStopFailedTransition() {
    return new StopFailed();
  }
  
  protected static class StopFailed implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendEvent(new RMCommunicatorContainerDeAllocateRequestEvent(
          container.containerId));
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createNodeFailedBaseTransition() {
    return new NodeFailedBase();
  }
  
  protected static class NodeFailedBase implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      // TODO Make sure runningAttempts etc are set to null before entering this
      // state.
      // Alternately some way to track if an event has gone out for a task. or
      // let multiple events go out and the TA should be able to handle them.
      // Kill_TA going out in this case.
      if (container.runningAttempt != null) {
        container.killTaskAttempt(container.runningAttempt);
      }
      if (container.pendingAttempt != null) {
        container.killTaskAttempt(container.pendingAttempt);
      }
      for (TaskAttemptId attemptId : container.completedAttempts) {
        if (attemptId.getTaskId().getTaskType() == TaskType.MAP) {
          container.killTaskAttempt(attemptId);
        }
      }
      
    }
  }
  
  private void killTaskAttempt(TaskAttemptId attemptId) {
    sendEvent(new TaskAttemptKillEvent(attemptId,
        "The node running the task attempt was marked as bad"));
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createNodeFailedAtIdleTransition() {
    return new NodeFailedAtIdle();
  }
  
  protected static class NodeFailedAtIdle extends NodeFailedBase {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.sendEvent(new NMCommunicatorStopRequestEvent(
          container.containerId));
    }
  }
  
  
  // TODO Create a generic ERROR state. Container tries informing relevant components in this case.
  
  // TODO Maybe send cleanup events to the TaskAttemptCleaner
  
  // TODO Unregister from CHH whenever transition to the COMPLETED state, after entering the RUNNING/IDLE state.
  
  // TODO Impact of not delaying TA_COMPLETE going out till the STOPPING->COMPLETED transition -----> a TA may completed after entering the STOP state. Extremely unlikely.
  
}
