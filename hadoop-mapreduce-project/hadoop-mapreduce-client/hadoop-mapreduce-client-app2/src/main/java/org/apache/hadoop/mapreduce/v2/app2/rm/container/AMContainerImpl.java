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
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventKillRequest;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventTerminated;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventContaienrCompleted;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorStopRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorContainerDeAllocateRequestEvent;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

//XXX Introduce state: STOPPING_NEEDS_CLEANUP.
//XXX Consider havinf RM_COMPLETED go to the SCHEDULER instead of the CONTAINER.

//XXX Rightnow -> The CONTAINER state machine is getting polluted with assumptions made in the TaskAttempt.
// IN_PROGRESS states in the TaskAttempt are not bad -> as long as the only thing going out from them is a Cleanup call. (TBD: Later| For now, baked into Container state machine)


public class AMContainerImpl implements AMContainer {

  private static final Log LOG = LogFactory.getLog(AMContainerImpl.class);

  private final ReadLock readLock;
  private final WriteLock writeLock;
  // TODO Use ContainerId or a custom JvmId.
  private final ContainerId containerId;
  // Container to be used for getters on capability, locality etc.
  private final Container container;
  private final AppContext appContext;
  private final ContainerHeartbeatHandler containerHeartbeatHandler;
  private final TaskAttemptListener taskAttemptListener;
  
  private final List<TaskAttemptId> completedAttempts = new LinkedList<TaskAttemptId>();

  // TODO Maybe this should be pulled from the TaskAttempt.s
  private final Map<TaskAttemptId, org.apache.hadoop.mapred.Task> remoteTaskMap 
      = new HashMap<TaskAttemptId, org.apache.hadoop.mapred.Task>();
  
  // Convert to list and hash.
  
  private TaskAttemptId pendingAttempt;
  private TaskAttemptId runningAttempt;
  private TaskAttemptId failedAttempt;
  private TaskAttemptId pullAttempt;
  
  private boolean inError = false;
  
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
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptAtAllocatedTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtAllocatedTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_STOP_REQUEST, createStopRequestTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtAllocatedTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, EnumSet.of(AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_PULL_TA, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_STOP_FAILED, AMContainerEventType.C_TIMED_OUT), createGenericErrorTransition())
        
        
        .addTransition(AMContainerState.LAUNCHING, EnumSet.of(AMContainerState.LAUNCHING, AMContainerState.STOPPING), AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.IDLE, AMContainerEventType.C_LAUNCHED, createLaunchedTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOPPING, AMContainerEventType.C_LAUNCH_FAILED, createLaunchFailedTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.LAUNCHING, AMContainerEventType.C_PULL_TA) // Is assuming the pullAttempt will be null.
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtLaunchingTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOPPING, AMContainerEventType.C_STOP_REQUEST, createStopRequestAtLaunchingTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtLaunchingTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOPPING, EnumSet.of(AMContainerEventType.C_START_REQUEST, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_STOP_FAILED, AMContainerEventType.C_TIMED_OUT), createGenericErrorAtLaunchingTransition())
        
        
        .addTransition(AMContainerState.IDLE, EnumSet.of(AMContainerState.IDLE, AMContainerState.STOPPING), AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptAtIdleTransition())
        .addTransition(AMContainerState.IDLE, EnumSet.of(AMContainerState.RUNNING, AMContainerState.IDLE), AMContainerEventType.C_PULL_TA, createPullTAAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, AMContainerEventType.C_STOP_REQUEST, createStopRequestAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, AMContainerEventType.C_TIMED_OUT, createTimedOutAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, EnumSet.of(AMContainerEventType.C_START_REQUEST, AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_STOP_FAILED), createGenericErrorAtIdleTransition())
        
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.RUNNING, AMContainerEventType.C_PULL_TA)
        .addTransition(AMContainerState.RUNNING, AMContainerState.IDLE, AMContainerEventType.C_TA_SUCCEEDED, createTASucceededAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_STOP_REQUEST, createStopRequestAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_TIMED_OUT, createTimedOutAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtIdleTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, EnumSet.of(AMContainerEventType.C_START_REQUEST, AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_STOP_FAILED), createGenericErrorAtRunningTransition())
        
        
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_ASSIGN_TA, createAssignTAAtStoppingTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtStoppingTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedBaseTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, EnumSet.of(AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_PULL_TA, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_STOP_FAILED, AMContainerEventType.C_TIMED_OUT))
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_START_REQUEST, createGenericErrorAtStoppingTransition())
        
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, AMContainerEventType.C_ASSIGN_TA, createAssignTAAtCompletedTransition())
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, AMContainerEventType.C_NODE_FAILED, createNodeFailedBaseTransition())
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, EnumSet.of(AMContainerEventType.C_START_REQUEST, AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_STOP_FAILED), createGenericErrorAtStoppingTransition())
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, EnumSet.of(AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_COMPLETED, AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_COMPLETED, AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_TIMED_OUT))
 
        .installTopology();
  }
  
  @SuppressWarnings("rawtypes")
  public AMContainerImpl(Container container, ContainerHeartbeatHandler chh,
      TaskAttemptListener tal, EventHandler eventHandler, AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.container = container;
    this.containerId = container.getId();
    this.eventHandler = eventHandler;
    this.appContext = appContext;
    this.containerHeartbeatHandler = chh;
    this.taskAttemptListener = tal;

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
  public Container getContainer() {
    return this.container;
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
        inError = true;
        // TODO XXX: Can't set state to COMPLETED. Add a default error state.
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
      // TODO LATER May be possible to forget about the clc or a part of it after
      // launch. Save AM resources.
      container.clc = event.getContainerLaunchContext();
      container.sendEvent(new NMCommunicatorLaunchRequestEvent(container.clc));
      LOG.info("Sending Launch Request for Container with id: "
          + container.clc.getContainerId());
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createAssignTaskAttemptAtAllocatedTransition() {
    return new AssignTaskAttemptAtAllocated();
  }

  protected static class AssignTaskAttemptAtAllocated implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      container.inError = true;
      container.sendEvent(new TaskAttemptEventTerminated(event
          .getTaskAttemptId()));
      container.sendCompletedToScheduler();
      container.deAllocate();
      LOG.warn("Unexpected TA Assignment: TAId: " + event.getTaskAttemptId()
          + "  for ContainerId: " + container.getContainerId()
          + " while in state: " + container.getState());
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtAllocatedTransition() {
    return new CompltedAtAllocated();
  }

  protected static class CompltedAtAllocated implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendCompletedToScheduler();
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
      container.sendCompletedToScheduler();
      container.deAllocate();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createNodeFailedAtAllocatedTransition() {
    return new NodeFailedAtAllocated();
  }

  // TODO Identical to StopRequestAtAllocated....
  protected static class NodeFailedAtAllocated implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendCompletedToScheduler();
      container.deAllocate();
    }
  }
  
  protected void deAllocate() {
    sendEvent(new RMCommunicatorContainerDeAllocateRequestEvent(containerId));
  }
  
  protected void sendCompletedToScheduler() {
    sendEvent(new AMSchedulerEventContaienrCompleted(containerId));
  }
  
  protected void sendCompletedToTaskAttempt(TaskAttemptId taId) {
    sendEvent(new TaskAttemptEventTerminated(taId));
  }

  protected void sendKillRequestToTaskAttempt(TaskAttemptId taId) {
    sendEvent(new TaskAttemptEventKillRequest(taId,
        "Node running the contianer failed"));
  }

  protected void sendStopRequestToNM() {
    sendEvent(new NMCommunicatorStopRequestEvent(containerId));
  }
  

  protected MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState>
      createAssignTaskAttemptTransition() {
    return new AssignTaskAttempt();
  }

  protected static class AssignTaskAttempt
      implements
      MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState> {

    @Override
    // Instead of a MultpleArcTransition - this could just be an exception in
    // case of the errror.
    public AMContainerState transition(AMContainerImpl container,
        AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      if (container.pendingAttempt != null) {
        container.inError = true;
        container.sendCompletedToTaskAttempt(event.getTaskAttemptId());
        container.deAllocate();
        return AMContainerState.STOPPING;
      }
      container.pendingAttempt = event.getTaskAttemptId();
      container.remoteTaskMap.put(event.getTaskAttemptId(),
          event.getRemoteTask());
      return container.getState();
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
      container.containerHeartbeatHandler.register(container.containerId);
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
        // This will be invoked as part of the PULL_REQUEST - so pullAttempt pullAttempt
        // should ideally only end up being populated during the duration of this call,
        // which is in a write lock. pullRequest() should move this to the running state.
        container.pullAttempt = container.pendingAttempt;
        container.runningAttempt = container.pendingAttempt;
        container.pendingAttempt = null;
        return AMContainerState.RUNNING;
        // TODO XXX: Make sure the TAL sends out a TA_STARTED_REMOTELY
      } else {
        return AMContainerState.IDLE;
      }
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createLaunchFailedTransition() {
    return new LaunchFailed();
  }

  protected static class LaunchFailed implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.deAllocate();
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
        container.sendCompletedToTaskAttempt(container.pendingAttempt);
        // TODO XXX Maybe nullify pendingAttempt.
      }
      container.sendCompletedToScheduler();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createStopRequestAtLaunchingTransition() {
    return new StopRequestAtLaunching();
  }

  protected static class StopRequestAtLaunching implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendStopRequestToNM();
      container.deAllocate();
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createNodeFailedAtLaunchingTransition() {
    return new NodeFailedAtLaunching();
  }

  protected static class NodeFailedAtLaunching implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        container.sendKillRequestToTaskAttempt(container.pendingAttempt);
      }
      container.sendStopRequestToNM();
      container.deAllocate();
    }
  }

  protected MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState>
      createAssignTaskAttemptAtIdleTransition() {
    return new AssignTaskAttemptAtIdle();
  }

  // TODO Make this the base for all assignRequests. Some more error checking in
  // that case.
  protected static class AssignTaskAttemptAtIdle
      implements
      MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState> {
    @Override
    public AMContainerState transition(AMContainerImpl container,
        AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      if (container.pendingAttempt != null) {
        container.inError = true;
        container.sendCompletedToTaskAttempt(event.getTaskAttemptId());
        container.sendStopRequestToNM();
        container.deAllocate();
        container.containerHeartbeatHandler.unregister(container.containerId);
        return AMContainerState.STOPPING;
      }
      container.pendingAttempt = event.getTaskAttemptId();
      // TODO LATER. Cleanup the remoteTaskMap.
      container.remoteTaskMap.put(event.getTaskAttemptId(),
          event.getRemoteTask());
      return AMContainerState.IDLE;
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtIdleTransition() {
    return new CompletedAtIdle();
  }
  
  // TODO Create a base CompletedTransition - at least contains some kind of logging. Informing the scheduler etc.
  protected static class CompletedAtIdle implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      LOG.info("Cotnainer with id: " + container.getContainerId()
          + " Completed." + " Previous state was: " + container.getState());
      if (container.pendingAttempt != null) {
        container.sendEvent(new TaskAttemptEvent(container.pendingAttempt,
            TaskAttemptEventType.TA_TERMINATED));
      }
      container.sendCompletedToScheduler();
      container.containerHeartbeatHandler.unregister(container.containerId);
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
      container.sendStopRequestToNM();
      container.deAllocate();
      container.containerHeartbeatHandler.unregister(container.containerId);
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

  protected static class TASucceededAtRunning implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.completedAttempts.add(container.runningAttempt);
      container.runningAttempt = null;
      // TODO XXX: Unregister from TaskAttemptListener. WTF is a jvmID here.
      //container.taskAttemptListener.unregister(attemptID, jvmID);
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
      container.sendCompletedToTaskAttempt(container.runningAttempt);
      container.sendCompletedToScheduler();
      container.containerHeartbeatHandler.unregister(container.containerId);
      //TODO XXX Unregister TAL. jvmId =? 
      //container.taskAttemptListener.unregister(container.runningAttempt, jvmID);
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createStopRequestAtRunningTransition() {
    return new StopRequestAtRunning();
  }
  
  protected static class StopRequestAtRunning extends StopRequestAtIdle {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      // TODO XXX: Unregister from TaskAttemptListener.
      //container.taskAttemptListener.unregister(container.runningAttempt, jvmID);
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createTimedOutAtRunningTransition() {
    return new TimedOutAtRunning();
  }

  protected static class TimedOutAtRunning extends StopRequestAtRunning {
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createNodeFailedAtRunningTransition() {
    return new NodeFailedAtRunning();
  }

  protected static class NodeFailedAtRunning extends NodeFailedAtIdle {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      // TODO XXX: Unregister from TAL.
      // container.taskAttemptListener.unregister(container.runningAttempt,
      // jvmID);

    }
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
      container.inError = true;
      container.sendCompletedToTaskAttempt(event.getTaskAttemptId());
    }
  }

  // TODO XXX Rename all createGenericError*s ... not really generic.
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createGenericErrorAtStoppingTransition() {
    return new GenericErrorAtStopping();
  }

  protected static class GenericErrorAtStopping implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.inError = true;
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createAssignTAAtCompletedTransition() {
    return new AssignTAAtCompleted();
  }

  protected static class AssignTAAtCompleted implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      container.sendCompletedToTaskAttempt(event.getTaskAttemptId());
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtStoppingTransition() {
    return new CompletedAtStopping();
  }
  
  protected static class CompletedAtStopping implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        container.sendCompletedToTaskAttempt(container.pendingAttempt);
      }
      if (container.runningAttempt != null) {
        container.sendCompletedToTaskAttempt(container.runningAttempt);
      }
      container.sendCompletedToScheduler();
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
        // TODO XXX: Make sure TaskAttempt knows how to handle kills to REDUCEs.
//        if (attemptId.getTaskId().getTaskType() == TaskType.MAP) {
          container.killTaskAttempt(attemptId);
//        }s
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
  
  protected static class NodeFailedAtIdle implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendStopRequestToNM();
      container.deAllocate();
      if (container.pendingAttempt != null) {
        container.sendKillRequestToTaskAttempt(container.pendingAttempt);
      }
      for (TaskAttemptId taId : container.completedAttempts) {
        container.sendKillRequestToTaskAttempt(taId);
      }
      container.containerHeartbeatHandler.unregister(container.containerId);
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createAssignTaskAttemptAtRunningTransition() {
    return new AssignTaskAttemptAtRunning();
  }

  protected static class AssignTaskAttemptAtRunning implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      container.inError = true;
      container.sendCompletedToTaskAttempt(event.getTaskAttemptId());
      container.sendStopRequestToNM();
      container.deAllocate();
      container.containerHeartbeatHandler.unregister(container.containerId);
      // TODO XXX -> Maybe unregsiter from TAL ?
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createGenericErrorTransition() {
    return new GenericError();
  }

  protected static class GenericError implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.inError = true;
      container.deAllocate();
      LOG.info("Unexpected event type: " + cEvent.getType()
          + " while in state: " + container.getState() + ". Event: " + cEvent);

    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createGenericErrorAtLaunchingTransition() {
    return new GenericErrorAtLaunching();
  }

  protected static class GenericErrorAtLaunching extends GenericError {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendStopRequestToNM();
      super.transition(container, cEvent);
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createGenericErrorAtIdleTransition() {
    return new GenericErrorAtIdle();
  }

  protected static class GenericErrorAtIdle extends GenericErrorAtLaunching {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.containerHeartbeatHandler.unregister(container.containerId);
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createGenericErrorAtRunningTransition() {
    return new GenericErrorAtRunning();
  }

  protected static class GenericErrorAtRunning extends GenericErrorAtIdle {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      // TODO XXX XXX Unregister from TaskAttemptListener. WTF is the jvmId.
//      container.taskAttemptListener.unregister(container.runningAttempt, jvmID);
    }
  }

  // TODO Create a generic ERROR state. Container tries informing relevant components in this case.
  
  // TODO Maybe send cleanup events to the TaskAttemptCleaner
  
  // TODO Unregister from CHH whenever transition to the COMPLETED state, after entering the RUNNING/IDLE state.
  
  // TODO Impact of not delaying TA_COMPLETE going out till the STOPPING->COMPLETED transition -----> a TA may completed after entering the STOP state. Extremely unlikely.
  
}
