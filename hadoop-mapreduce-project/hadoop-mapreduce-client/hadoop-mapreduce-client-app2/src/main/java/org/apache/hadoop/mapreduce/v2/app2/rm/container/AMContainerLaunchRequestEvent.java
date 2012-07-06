package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

public class AMContainerLaunchRequestEvent extends AMContainerEvent {

  private final ContainerLaunchContext clc;
  
  public AMContainerLaunchRequestEvent(ContainerLaunchContext clc) {
    super(clc.getContainerId(), AMContainerEventType.C_START_REQUEST);
    this.clc = clc;
  }
  
  public ContainerLaunchContext getContainerLaunchContext() {
    return this.clc;
  }

}
