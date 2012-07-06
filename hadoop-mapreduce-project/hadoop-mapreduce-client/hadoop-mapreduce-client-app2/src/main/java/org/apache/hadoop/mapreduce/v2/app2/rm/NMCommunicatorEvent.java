package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.event.AbstractEvent;

public class NMCommunicatorEvent extends AbstractEvent<NMCommunicatorEventType>{

  public NMCommunicatorEvent(NMCommunicatorEventType type) {
    super(type);
  }
}
