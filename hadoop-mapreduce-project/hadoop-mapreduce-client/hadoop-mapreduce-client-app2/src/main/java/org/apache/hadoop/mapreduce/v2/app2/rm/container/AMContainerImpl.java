package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.MapReduceChildJVM2;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.ContainerHeartbeatHandler;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventKillRequest;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventTerminated;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventContaienrCompleted;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerTALaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorStopRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorContainerDeAllocateRequestEvent;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

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
  
  private static Object commonContainerSpecLock = new Object();
  private static ContainerLaunchContext commonContainerSpec = null;
  private static final Object classpathLock = new Object();
  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  private static String initialClasspath = null;
  
  private final List<TaskAttemptId> completedAttempts = new LinkedList<TaskAttemptId>();

  // TODO Maybe this should be pulled from the TaskAttempt.s
  private final Map<TaskAttemptId, org.apache.hadoop.mapred.Task> remoteTaskMap 
      = new HashMap<TaskAttemptId, org.apache.hadoop.mapred.Task>();
  
  // TODO ?? Convert to list and hash.
  
  private int shufflePort; 
  private long idleTimeBetweenTasks = 0;
  private long lastTaskFinishTime;
  
  
  private TaskAttemptId pendingAttempt;
  private TaskAttemptId runningAttempt;
  private TaskAttemptId interruptedEvent;
  private TaskAttemptId pullAttempt;
  
  private boolean inError = false;
  
  private ContainerLaunchContext clc;
  private WrappedJvmID jvmId;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;

  private static boolean stateMachineInited = false;
  private static StateMachineFactory
      <AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent> 
      stateMachineFactory = 
      new StateMachineFactory<AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent>(
      AMContainerState.ALLOCATED);
  
  private final StateMachine<AMContainerState, AMContainerEventType, AMContainerEvent> stateMachine;

  private void initStateMachineFactory() {
    stateMachineFactory = 
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
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtRunningTransition())
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
  public int getShufflePort() {
    readLock.lock();
    try {
      return this.shufflePort;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void handle(AMContainerEvent event) {
    this.writeLock.lock();
    LOG.info("XXX: Processing ContainerEvent: " + event.getContainerId() + " of type "
        + event.getType() + " while in state: " + getState());
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

  // TODO Maybe have pullTA send out an attemptId. TAL talks to the TaskAttempt
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
      AMSchedulerTALaunchRequestEvent taEvent = event.getLaunchRequestEvent();
      
      // TODO LATER May be possible to forget about the clc or a part of it after
      // launch. Save AM resources.
      
      container.jvmId = new WrappedJvmID(taEvent.getRemoteTask().getJobID(), taEvent.getRemoteTask().isMapTask(), container.containerId.getId());
      
      container.clc = createContainerLaunchContext(
          event.getApplicationAcls(), container.getContainerId(),
          container.appContext.getJob(event.getJobId()).getConf(), taEvent.getJobToken(),
          taEvent.getRemoteTask(), TypeConverter.fromYarn(event.getJobId()),
          container.getContainer().getResource(), container.jvmId,
          container.taskAttemptListener, taEvent.getCredentials());
      
      container.sendEvent(new NMCommunicatorLaunchRequestEvent(container.clc, container.container));
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
    sendEvent(new NMCommunicatorStopRequestEvent(containerId,
        container.getNodeId(), container.getContainerToken()));
  }
  
  protected void unregisterAttemptFromListener(TaskAttemptId attemptId) {
    taskAttemptListener.unregisterTaskAttempt(attemptId);
  }
  
  protected void unregisterJvmFromListener(WrappedJvmID jvmId) {
    taskAttemptListener.unregisterRunningJvm(jvmId);
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
      // TODO XXX: Consider registering with the TAL, instead of the TAL pulling.
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
      AMContainerEventLaunched event = (AMContainerEventLaunched) cEvent;
      container.shufflePort = event.getShufflePort();
      container.taskAttemptListener.registerRunningJvm(container.jvmId, container.containerId);
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
        if (container.lastTaskFinishTime != 0) {
          long idleTimeDiff = System.currentTimeMillis() - container.lastTaskFinishTime;
          LOG.info("Computing idle time for container: " + container.getContainerId() + ", lastFinishTime: " + container.lastTaskFinishTime + ", Incremented by: " + idleTimeDiff);
          container.idleTimeBetweenTasks += System.currentTimeMillis() - container.lastTaskFinishTime;
        }
        LOG.info("XXX: Assigned task + [" + container.runningAttempt + "] to container: [" + container.getContainerId() + "]");
        return AMContainerState.RUNNING;
        // TODO XXX: Make sure the TAL sends out a TA_STARTED_REMOTELY, along with the shuffle port.
      } else {
        return AMContainerState.IDLE;
      }
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createLaunchFailedTransition() {
    return new LaunchFailed();
  }

  protected static class LaunchFailed implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      // TODO XXX: Send diagnostics to pending task attempt. Update action in transition table.
      if (container.pendingAttempt != null) {
        AMContainerEventLaunchFailed event = (AMContainerEventLaunchFailed) cEvent;
        container.sendEvent(new TaskAttemptDiagnosticsUpdateEvent(
            container.pendingAttempt, event.getMessage()));
      }
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
      LOG.info("XXX: IdleTimeBetweenTasks: " + container.idleTimeBetweenTasks);
      container.sendStopRequestToNM();
      container.deAllocate();
      container.containerHeartbeatHandler.unregister(container.containerId);
      container.unregisterJvmFromListener(container.jvmId);
      // TODO XXXXXXXXX: Unregister from TAL so that the Container kills itself (via a kill task assignment)
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
      container.lastTaskFinishTime = System.currentTimeMillis();
      container.completedAttempts.add(container.runningAttempt);
      container.unregisterAttemptFromListener(container.runningAttempt);
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
      container.sendCompletedToTaskAttempt(container.runningAttempt);
      container.sendCompletedToScheduler();
      container.containerHeartbeatHandler.unregister(container.containerId);
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.unregisterJvmFromListener(container.jvmId);
      container.interruptedEvent = container.runningAttempt;
      container.runningAttempt = null;
      
      
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createStopRequestAtRunningTransition() {
    return new StopRequestAtRunning();
  }
  
  protected static class StopRequestAtRunning extends StopRequestAtIdle {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.unregisterJvmFromListener(container.jvmId);
      // TODO XXX: All running transition. verify whether runningAttempt should be null.
      container.interruptedEvent = container.runningAttempt;
      container.runningAttempt = null;
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
      container.sendKillRequestToTaskAttempt(container.runningAttempt);
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.unregisterJvmFromListener(container.jvmId);
      container.interruptedEvent = container.runningAttempt;
      container.runningAttempt = null;
      
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
      if (container.interruptedEvent != null) {
        container.sendCompletedToTaskAttempt(container.interruptedEvent);
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
    sendEvent(new TaskAttemptEventKillRequest(attemptId, "The node running the task attempt was marked as bad"));
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
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.unregisterJvmFromListener(container.jvmId);
      container.containerHeartbeatHandler.unregister(container.containerId);
      container.interruptedEvent = container.runningAttempt;
      container.runningAttempt = null;
      // TODO XXX: Is the TAL unregister required ?
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
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.unregisterJvmFromListener(container.jvmId);
      container.interruptedEvent = container.runningAttempt;
      container.runningAttempt = null;
    }
  }

  // TODO Create a generic ERROR state. Container tries informing relevant components in this case.
  
  /**
   * Create a {@link LocalResource} record with all the given parameters.
   */
  private static LocalResource createLocalResource(FileSystem fc, Path file,
      LocalResourceType type, LocalResourceVisibility visibility)
      throws IOException {
    FileStatus fstat = fc.getFileStatus(file);
    URL resourceURL = ConverterUtils.getYarnUrlFromPath(fc.resolvePath(fstat
        .getPath()));
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    return BuilderUtils.newLocalResource(resourceURL, type, visibility,
        resourceSize, resourceModificationTime);
  }

  /**
   * Lock this on initialClasspath so that there is only one fork in the AM for
   * getting the initial class-path. TODO: We already construct
   * a parent CLC and use it for all the containers, so this should go away
   * once the mr-generated-classpath stuff is gone.
   */
  private static String getInitialClasspath(Configuration conf) throws IOException {
    synchronized (classpathLock) {
      if (initialClasspathFlag.get()) {
        return initialClasspath;
      }
      Map<String, String> env = new HashMap<String, String>();
      MRApps.setClasspath(env, conf);
      initialClasspath = env.get(Environment.CLASSPATH.name());
      initialClasspathFlag.set(true);
      return initialClasspath;
    }
  }
  
  /**
   * Create the common {@link ContainerLaunchContext} for all attempts.
   * @param applicationACLs 
   */
  private static ContainerLaunchContext createCommonContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs, Configuration conf,
      Token<JobTokenIdentifier> jobToken,
      final org.apache.hadoop.mapred.JobID oldJobId,
      Credentials credentials) {

    // Application resources
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    
    // Application environment
    Map<String, String> environment = new HashMap<String, String>();

    // Service data
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();

    // Tokens
    ByteBuffer taskCredentialsBuffer = ByteBuffer.wrap(new byte[]{});
    try {
      FileSystem remoteFS = FileSystem.get(conf);

      // //////////// Set up JobJar to be localized properly on the remote NM.
      String jobJar = conf.get(MRJobConfig.JAR);
      if (jobJar != null) {
        Path remoteJobJar = (new Path(jobJar)).makeQualified(remoteFS
            .getUri(), remoteFS.getWorkingDirectory());
        localResources.put(
            MRJobConfig.JOB_JAR,
            createLocalResource(remoteFS, remoteJobJar,
                LocalResourceType.FILE, LocalResourceVisibility.APPLICATION));
        LOG.info("The job-jar file on the remote FS is "
            + remoteJobJar.toUri().toASCIIString());
      } else {
        // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
        // mapreduce jar itself which is already on the classpath.
        LOG.info("Job jar is not present. "
            + "Not adding any jar to the list of resources.");
      }
      // //////////// End of JobJar setup

      // //////////// Set up JobConf to be localized properly on the remote NM.
      Path path =
          MRApps.getStagingAreaDir(conf, UserGroupInformation
              .getCurrentUser().getShortUserName());
      Path remoteJobSubmitDir =
          new Path(path, oldJobId.toString());
      Path remoteJobConfPath = 
          new Path(remoteJobSubmitDir, MRJobConfig.JOB_CONF_FILE);
      localResources.put(
          MRJobConfig.JOB_CONF_FILE,
          createLocalResource(remoteFS, remoteJobConfPath,
              LocalResourceType.FILE, LocalResourceVisibility.APPLICATION));
      LOG.info("The job-conf file on the remote FS is "
          + remoteJobConfPath.toUri().toASCIIString());
      // //////////// End of JobConf setup

      // Setup DistributedCache
      MRApps.setupDistributedCache(conf, localResources);

      // Setup up task credentials buffer
      Credentials taskCredentials = new Credentials();

      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("Adding #" + credentials.numberOfTokens()
            + " tokens and #" + credentials.numberOfSecretKeys()
            + " secret keys for NM use for launching container");
        taskCredentials.addAll(credentials);
      }

      // LocalStorageToken is needed irrespective of whether security is enabled
      // or not.
      TokenCache.setJobToken(jobToken, taskCredentials);

      DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
      LOG.info("Size of containertokens_dob is "
          + taskCredentials.numberOfTokens());
      taskCredentials.writeTokenStorageToStream(containerTokens_dob);
      taskCredentialsBuffer =
          ByteBuffer.wrap(containerTokens_dob.getData(), 0,
              containerTokens_dob.getLength());

      // Add shuffle token
      LOG.info("Putting shuffle token in serviceData");
      serviceData.put(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID,
          ShuffleHandler.serializeServiceData(jobToken));

      Apps.addToEnvironment(
          environment,  
          Environment.CLASSPATH.name(), 
          getInitialClasspath(conf));
    } catch (IOException e) {
      throw new YarnException(e);
    }

    // Shell
    environment.put(
        Environment.SHELL.name(), 
        conf.get(
            MRJobConfig.MAPRED_ADMIN_USER_SHELL, 
            MRJobConfig.DEFAULT_SHELL)
            );

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    Apps.addToEnvironment(
        environment, 
        Environment.LD_LIBRARY_PATH.name(), 
        Environment.PWD.$());

    // Add the env variables passed by the admin
    Apps.setEnvFromInputString(
        environment, 
        conf.get(
            MRJobConfig.MAPRED_ADMIN_USER_ENV, 
            MRJobConfig.DEFAULT_MAPRED_ADMIN_USER_ENV)
        );

    // Construct the actual Container
    // The null fields are per-container and will be constructed for each
    // container separately.
    ContainerLaunchContext container = BuilderUtils
        .newContainerLaunchContext(null, conf
            .get(MRJobConfig.USER_NAME), null, localResources,
            environment, null, serviceData, taskCredentialsBuffer,
            applicationACLs);

    return container;
  }

  static ContainerLaunchContext createContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs,
      ContainerId containerID, Configuration conf,
      Token<JobTokenIdentifier> jobToken, Task remoteTask,
      final org.apache.hadoop.mapred.JobID oldJobId,
      Resource assignedCapability, WrappedJvmID jvmID,
      TaskAttemptListener taskAttemptListener,
      Credentials credentials) {

    synchronized (commonContainerSpecLock) {
      if (commonContainerSpec == null) {
        commonContainerSpec = createCommonContainerLaunchContext(
            applicationACLs, conf, jobToken, oldJobId, credentials);
      }
    }

    // Fill in the fields needed per-container that are missing in the common
    // spec.

    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);
    MapReduceChildJVM2.setVMEnv(myEnv, remoteTask);

    // Set up the launch command
    List<String> commands = MapReduceChildJVM2.getVMCommand(
        taskAttemptListener.getAddress(), remoteTask, jvmID);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();
    for (Entry<String, ByteBuffer> entry : commonContainerSpec
                .getServiceData().entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    // Construct the actual Container
    ContainerLaunchContext container = BuilderUtils.newContainerLaunchContext(
        containerID, commonContainerSpec.getUser(), assignedCapability,
        commonContainerSpec.getLocalResources(), myEnv, commands,
        myServiceData, commonContainerSpec.getContainerTokens().duplicate(),
        applicationACLs);

    return container;
  }
  
}
