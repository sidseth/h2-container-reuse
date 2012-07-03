package org.apache.hadoop.mapreduce.v2.app2.rm;

public enum AMSchedulerEventType {
  //Producer: TaskAttempt
  S_TA_LAUNCH_REQUEST,
  S_TA_STOP_REQUEST,
  S_TA_SUCCEEDED,
  
  //Producer: RMCommunicator
  S_CONTAINER_ALLOCATED,
  
  //Producer: RMCommunicator. May not be needed.
  S_CONTAINER_COMPLETED,
  
  //Producer: RMComm
  S_NODE_UNHEALTHY,
  S_NODE_HEALTHY,
  
}
