package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class NMCommunicatorStopRequestEvent extends NMCommunicatorEvent {

  private final ContainerId containerId;
  
  public NMCommunicatorStopRequestEvent(ContainerId containerId) {
    super(NMCommunicatorEventType.CONTAINER_STOP_REQUEST);
    this.containerId = containerId;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }
}
