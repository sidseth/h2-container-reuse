package org.apache.hadoop.mapreduce.v2.app2.job.event;

import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;

public class TaskAttemptRemoteStartEvent extends TaskAttemptEvent {

  private final Container container;
  // TODO Can appAcls be handled elsewhere ?
  private final Map<ApplicationAccessType, String> applicationACLs;
  private final int shufflePort;

  public TaskAttemptRemoteStartEvent(TaskAttemptId id, Container container,
      Map<ApplicationAccessType, String> appAcls, int shufflePort) {
    super(id, TaskAttemptEventType.TA_STARTED_REMOTELY);
    this.container = container;
    this.applicationACLs = appAcls;
    this.shufflePort = shufflePort;
  }

  public Container getContainer() {
    return container;
  }

  public Map<ApplicationAccessType, String> getApplicationACLs() {
    return applicationACLs;
  }

  public int getShufflePort() {
    return shufflePort;
  }
}
