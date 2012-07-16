package org.apache.hadoop.mapreduce.v2.app2.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class TaskAttemptContainerCompletedEvent extends TaskAttemptEvent {

  public TaskAttemptContainerCompletedEvent(TaskAttemptId id) {
    super(id, TaskAttemptEventType.TA_CONTAINER_COMPLETED);
    // TODO Auto-generated constructor stub
  }

}
