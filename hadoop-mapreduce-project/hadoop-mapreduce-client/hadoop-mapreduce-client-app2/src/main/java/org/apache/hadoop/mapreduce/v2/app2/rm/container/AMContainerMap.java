package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;

@SuppressWarnings("serial")
public class AMContainerMap extends
    ConcurrentHashMap<ContainerId, AMContainer> implements
    EventHandler<AMContainerEvent> {

  @Override
  public void handle(AMContainerEvent event) {
    ContainerId containerId = event.getContainerId();
    get(containerId).handle(event);
  }
}