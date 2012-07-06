package org.apache.hadoop.mapreduce.v2.app2.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class TaskAttemptContainerCompletedEvent extends TaskAttemptEvent {

  public TaskAttemptContainerCompletedEvent(TaskAttemptId id,
      TaskAttemptEventType type) {
    super(id, type);
    // TODO Auto-generated constructor stub
  }

}
