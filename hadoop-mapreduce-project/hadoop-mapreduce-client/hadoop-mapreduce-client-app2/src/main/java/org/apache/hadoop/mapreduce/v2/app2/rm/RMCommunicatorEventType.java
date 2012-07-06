package org.apache.hadoop.mapreduce.v2.app2.rm;

public enum RMCommunicatorEventType {
  // TODO Essentialy the same as ContainerAllocator.
  CONTAINER_REQ,
  CONTAINER_DEALLOCATE,
  CONTAINER_FAILED
}
