package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import org.apache.hadoop.yarn.api.records.NodeId;

public class AMNodeEventTaskAttemptEnded extends AMNodeEvent {

  private final boolean failed;
  
  public AMNodeEventTaskAttemptEnded(NodeId nodeId, boolean failed) {
    super(nodeId, AMNodeEventType.N_TA_ENDED);
    this.failed = failed;
  }

  public boolean failed() {
    return failed;
  }
  
  public boolean killed() {
    return !failed;
  }
}