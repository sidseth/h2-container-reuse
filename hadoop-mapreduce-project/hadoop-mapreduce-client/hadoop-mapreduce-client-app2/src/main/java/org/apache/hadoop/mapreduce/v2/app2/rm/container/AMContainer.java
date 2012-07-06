package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;

public interface AMContainer {
  
  public AMContainerState getState();
  public ContainerId getContainerId();
  public List<TaskAttemptId> getTaskAttempts();
  
  // TODO Add a method to get the containers capabilities - to match taskAttempts.

}
