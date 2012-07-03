package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.job.impl.TaskAttemptImplNew;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeImpl;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeState;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

public class AMContainerImpl implements AMContainer, EventHandler<AMContainerEvent> {

  private static final Log LOG = LogFactory.getLog(AMContainerImpl.class);

  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final ContainerId containerId;
  private final AppContext appContext;

  protected EventHandler eventHandler;

  private final List<TaskAttemptId> taskAttemptIds = new LinkedList<TaskAttemptId>();

  private static boolean stateMachineInited = false;
  private static final StateMachineFactory
      <AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent> 
      stateMachineFactory = 
      new StateMachineFactory<AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent>(
      AMContainerState.ALLOCATED);
  
  private final StateMachine<AMContainerState, AMContainerEventType, AMContainerEvent> stateMachine;

  private void initStateMachineFactory() {
    stateMachineFactory
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.LAUNCHING, AMContainerEventType.C_START_REQUEST, createGenericSingleArcTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_STOP_REQUEST, createGenericSingleArcTransition())
        
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.LAUNCHING, AMContainerEventType.C_ASSIGN_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.IDLE, AMContainerEventType.C_LAUNCHED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.LAUNCHING, AMContainerEventType.C_PULL_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED, AMContainerEventType.C_LAUNCH_FAILED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED, AMContainerEventType.C_STOP_REQUEST, createGenericSingleArcTransition())
        
        .addTransition(AMContainerState.IDLE, AMContainerState.IDLE, AMContainerEventType.C_ASSIGN_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.RUNNING, AMContainerEventType.C_PULL_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, AMContainerEventType.C_STOP_REQUEST, createGenericSingleArcTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, AMContainerEventType.C_TIMED_OUT, createGenericSingleArcTransition())
        
        .addTransition(AMContainerState.RUNNING, AMContainerState.RUNNING, AMContainerEventType.C_PULL_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.IDLE, AMContainerEventType.C_TA_SUCCEEDED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_STOP_REQUEST, createGenericSingleArcTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_TIMED_OUT, createGenericSingleArcTransition())
        
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_ASSIGN_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_PULL_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_TA_SUCCEEDED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_ASSIGN_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_STOP_FAILED, createGenericSingleArcTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, EnumSet.of(AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_TIMED_OUT))
        
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, AMContainerEventType.C_ASSIGN_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, AMContainerEventType.C_PULL_TA, createGenericSingleArcTransition())
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, EnumSet.of(AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_TIMED_OUT))
 
        .installTopology();
  }
  
  
  public AMContainerImpl(ContainerId containerId, EventHandler eventHandler, AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.containerId = containerId;
    this.eventHandler = eventHandler;
    this.appContext = appContext;
    
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ContainerId getContainerId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<TaskAttemptId> getTaskAttempts() {
    // TODO Auto-generated method stub
    return null;
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

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createGenericSingleArcTransition() {
    return null;
  }
  
  protected MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState> createGenericMultiArcTransition() {
    return null;
  }
}
