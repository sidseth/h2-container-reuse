package org.apache.hadoop.mapreduce.v2.app2.rm.container;

public enum AMContainerEventType {

  //Producer: Scheduler
  C_START_REQUEST,
  C_ASSIGN_TA,
  
  //Producer: NMCommunicator
  C_LAUNCHED,
  C_LAUNCH_FAILED,
  
  //Producer: TAL: PULL_TA is a sync call.
  
  //Producer: Scheduler via TA
  C_TA_SUCCEEDED,
  
  //Producer:RMCommunicator
  C_COMPLETED,
  
  //Producer: TA-> Scheduler -> Container (in case of failure etc)
  //          Scheduler -> Container (in case of pre-emption etc)
  //          Node -> Container (in case of Node unhealthy etc)
  C_STOP_REQUEST,
  
  //Producer: NMCommunicator
  C_HALT_FAILED,
  
  //Producer: ContainerHeartbeatHandler
  C_TIMED_OUT,
}
