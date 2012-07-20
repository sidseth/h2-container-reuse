package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class AMSchedulerTAStopRequestEvent extends AMSchedulerEvent {

  private final TaskAttemptId attemptId;
  private final boolean failed;

  public AMSchedulerTAStopRequestEvent(TaskAttemptId attemptId, boolean failed) {
    super(AMSchedulerEventType.S_TA_STOP_REQUEST);
    this.attemptId = attemptId;
    this.failed = failed;
  }

  public TaskAttemptId getAttemptID() {
    return this.attemptId;
  }

  public boolean failed() {
    return failed;
  }

  public boolean killed() {
    return !failed;
  }
}
