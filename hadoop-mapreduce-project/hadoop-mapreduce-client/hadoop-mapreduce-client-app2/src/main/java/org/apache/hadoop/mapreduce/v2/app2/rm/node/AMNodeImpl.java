package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

public class AMNodeImpl implements AMNode, EventHandler<AMNodeEvent> {

  private static final Log LOG = LogFactory.getLog(AMNodeImpl.class);

  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final NodeId nodeId;
  private final AppContext appContext;
  private int numFailedTAs = 0;
  private int numSuccessfulTAs = 0;

  protected EventHandler eventHandler;

  private final List<ContainerId> containers = new LinkedList<ContainerId>();

  private static boolean stateMachineInited = false;
  private static final StateMachineFactory
      <AMNodeImpl, AMNodeState, AMNodeEventType, AMNodeEvent> 
      stateMachineFactory = 
      new StateMachineFactory<AMNodeImpl, AMNodeState, AMNodeEventType, AMNodeEvent>(
      AMNodeState.ACTIVE);

  private final StateMachine<AMNodeState, AMNodeEventType, AMNodeEvent> stateMachine;

  private void initStateMachineFactory() {
    stateMachineFactory
        // Transitions from ACTIVE state.
        .addTransition(AMNodeState.ACTIVE, AMNodeState.ACTIVE,
            AMNodeEventType.N_CONTAINER_ALLOCATED,
            createContainerAddedTransition())
        .addTransition(AMNodeState.ACTIVE, AMNodeState.ACTIVE,
            AMNodeEventType.N_TA_SUCCEEDED,
            createTaskAttemptSucceededTransition())
        .addTransition(AMNodeState.ACTIVE,
            EnumSet.of(AMNodeState.ACTIVE, AMNodeState.BLACKLISTED),
            AMNodeEventType.N_TA_FAILED,
            createTaskAttemptFailedTransition())
        .addTransition(AMNodeState.ACTIVE, AMNodeState.UNHEALTHY,
            AMNodeEventType.N_TURNED_UNHEALTHY,
            createNodeTurnedUnhealthyTransition())

        // Transitions from BLACKLISTED state.
        .addTransition(AMNodeState.BLACKLISTED, AMNodeState.BLACKLISTED,
            AMNodeEventType.N_CONTAINER_ALLOCATED,
            createContainerAllocatedWhileBlacklisted())
        .addTransition(AMNodeState.BLACKLISTED,
            EnumSet.of(AMNodeState.BLACKLISTED, AMNodeState.ACTIVE),
            AMNodeEventType.N_TA_SUCCEEDED,
            new TaskAttemptSucceededWhileBlacklisted())
        .addTransition(AMNodeState.BLACKLISTED, AMNodeState.BLACKLISTED,
            AMNodeEventType.N_TA_FAILED,
            createTaskAttemptFailedWhileBlacklisted())
        .addTransition(AMNodeState.BLACKLISTED, AMNodeState.UNHEALTHY,
            AMNodeEventType.N_TURNED_UNHEALTHY,
            createNodeTurnedUnhealthyTransition())
        .addTransition(AMNodeState.BLACKLISTED, AMNodeState.ACTIVE,
            AMNodeEventType.N_UNBLACKLIST_NODE,
            createForcedUnblaclist())

        // Transitions from UNHEALTHY state.
        .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY,
            AMNodeEventType.N_CONTAINER_ALLOCATED,
            createContainerAllocatedWhileUnhealthy())
        .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY,
            AMNodeEventType.N_TA_SUCCEEDED,
            createTaskAttemptSucceededWhileUnhealthy())
        .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY,
            AMNodeEventType.N_TA_FAILED,
            createTaskAttemptFailedWhileUnhealthy())
        .addTransition(AMNodeState.UNHEALTHY, AMNodeState.ACTIVE,
            AMNodeEventType.N_TURNED_HEALTHY,
            createNodeTurnedUnhealthyTransition())

        .installTopology();
  }

  @SuppressWarnings("rawtypes")
  public AMNodeImpl(NodeId nodeId, EventHandler eventHandler,
      AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.nodeId = nodeId;
    this.appContext = appContext;
    this.eventHandler = eventHandler;
    // TODO Better way to kill the job. Node really should not care about the
    // JobId.

    synchronized (stateMachineFactory) {
      if (!stateMachineInited) {
        initStateMachineFactory();
        stateMachineInited = true;
      }
    }
    this.stateMachine = stateMachineFactory.make(this);
    // TODO Handle the case where a node is created due to the RM reporting it's
    // state as UNHEALTHY
  }

  @Override
  public NodeId getNodeId() {
    return this.nodeId;
  }

  @Override
  public AMNodeState getState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerId> getContainers() {
    this.readLock.lock();
    try {
      List<ContainerId> cIds = new LinkedList<ContainerId>(this.containers);
      return cIds;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void handle(AMNodeEvent event) {
    this.writeLock.lock();
    try {
      final AMNodeState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle event " + event.getType()
            + " at current state " + oldState + " for NodeId " + this.nodeId, e);
        // TODO Should this fail the job ?
      }
      if (oldState != getState()) {
        LOG.info("AMNode " + this.nodeId + " transitioned from " + oldState
            + " to " + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createContainerAddedTransition() {
    return new ContainerAdded();
  }
  protected static class ContainerAdded implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventContainerAdded event = (AMNodeEventContainerAdded) nEvent;
      node.containers.add(event.getContainerId());
    }
  }
  
  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createTaskAttemptSucceededTransition() {
    return new TaskAttemptSucceededTransition();
  }
  protected static class TaskAttemptSucceededTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.numSuccessfulTAs++;
    }
  }

  protected MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> createTaskAttemptFailedTransition() {
    return new TaskAttemptFailed();
  }
  protected static class TaskAttemptFailed implements
      MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> {
    @Override
    public AMNodeState transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.numFailedTAs++;
      return AMNodeState.ACTIVE;
      // TODO AMNodeManager really need to be processing the failed / successful events.
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createNodeTurnedUnhealthyTransition() {
    return new NodeTurnedUnhealthy();
  }
  protected static class NodeTurnedUnhealthy implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      // TODO Auto-generated method stub
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createContainerAllocatedWhileBlacklisted() {
    return new ContainerAllocatedWhileBlacklisted();
  }
  protected static class ContainerAllocatedWhileBlacklisted implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      // TODO Auto-generated method stub
    }
  }

  protected MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> createTaskAttemptSucceededWhileBlacklisted() {
    return new TaskAttemptSucceededWhileBlacklisted();
  }
  protected static class TaskAttemptSucceededWhileBlacklisted implements
      MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> {
    @Override
    public AMNodeState transition(AMNodeImpl node, AMNodeEvent nEvent) {
      // TODO Auto-generated method stub
      return null;
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createTaskAttemptFailedWhileBlacklisted() {
    return new TaskAttemptFailedWhileBlacklisted();
  }
  protected static class TaskAttemptFailedWhileBlacklisted implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      // TODO Auto-generated method stub
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createForcedUnblaclist() {
    return new ForcedUnblaclist();
  }
  protected static class ForcedUnblaclist implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      // TODO Auto-generated method stub
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createContainerAllocatedWhileUnhealthy() {
    return new ContainerAllocatedWhileUnhealthy();
  }
  protected static class ContainerAllocatedWhileUnhealthy implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      // TODO Auto-generated method stub
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createTaskAttemptSucceededWhileUnhealthy() {
    return new TaskAttemptSucceededWhileUnhealthy();
  }
  protected static class TaskAttemptSucceededWhileUnhealthy implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      // TODO Auto-generated method stub
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createTaskAttemptFailedWhileUnhealthy() {
    return new TaskAttemptFailedWhileUnhealthy();
  }
  protected static class TaskAttemptFailedWhileUnhealthy implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      // TODO Auto-generated method stub
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createNodeTurnedHealthyTransition() {
    return new NodeTurnedHealthyTransition();
  }
  protected static class NodeTurnedHealthyTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      // TODO Auto-generated method stub
    }
  }
}
