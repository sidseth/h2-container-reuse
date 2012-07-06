package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;

// TODO Seems a little strange, extending ConcurrentHashMap like this.
@SuppressWarnings("serial")
public class AMNodeMap extends ConcurrentHashMap<NodeId, AMNode> implements
    EventHandler<AMNodeEvent> {

  public void handle(AMNodeEvent event) {
    if (event.getType() == AMNodeEventType.N_NODE_WAS_BLACKLISTED) {
      // TODO Handle blacklisting.
    } else {
      NodeId nodeId = event.getNodeId();
      get(nodeId).handle(event);
    }
  }
}
