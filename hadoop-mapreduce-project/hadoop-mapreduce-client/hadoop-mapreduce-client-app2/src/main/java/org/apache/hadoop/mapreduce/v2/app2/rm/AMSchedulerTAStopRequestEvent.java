package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class AMSchedulerTAStopRequestEvent extends AMSchedulerEvent {

  private final TaskAttemptId attemptId;
  
  public AMSchedulerTAStopRequestEvent(TaskAttemptId attemptId) {
    super(AMSchedulerEventType.S_TA_STOP_REQUEST);
    this.attemptId = attemptId;
  }
  
  public TaskAttemptId getTaskAttemptId() {
    return this.attemptId;
  }

}
