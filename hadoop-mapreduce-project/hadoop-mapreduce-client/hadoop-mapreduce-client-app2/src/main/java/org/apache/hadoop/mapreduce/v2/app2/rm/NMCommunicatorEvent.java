package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class NMCommunicatorEvent extends AbstractEvent<NMCommunicatorEventType> {

  private final ContainerId containerId;
  private final NodeId nodeId;
  private final ContainerToken containerToken;

  public NMCommunicatorEvent(ContainerId containerId, NodeId nodeId,
      ContainerToken containerToken, NMCommunicatorEventType type) {
    super(type);
    this.containerId = containerId;
    this.nodeId = nodeId;
    this.containerToken = containerToken;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }

  public NodeId getNodeId() {
    return this.nodeId;
  }

  public ContainerToken getContainerToken() {
    return this.containerToken;
  }
}
