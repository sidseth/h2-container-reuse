package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;

// TODO Seems a little strange, extending ConcurrentHashMap like this.
// TODO This needs to extend AbstractService to get a handle on the conf.
@SuppressWarnings("serial")
public class AMNodeMap extends ConcurrentHashMap<NodeId, AMNode> implements
    EventHandler<AMNodeEvent> {

  
  
  private final EventHandler eventHandler;
  private final AppContext appContext;
  
  public AMNodeMap(EventHandler eventHandler, AppContext appContext) {
    this.eventHandler = eventHandler;
    this.appContext = appContext;
    
    // TODO XXX: Get a handle of allowed failures.
  }
  
  public void nodeSeen(NodeId nodeId) {
    // TODO Replace 3 with correct value.
    putIfAbsent(nodeId, new AMNodeImpl(nodeId, 3, eventHandler, appContext));
  }
  
  public boolean isHostBlackListed(String hostname) {
    return false;
 // TODO XXX -> Maintain a map of host to NodeList (case of multiple NMs)
    // Provide functionality to say isHostBlacklisted(hostname) -> all hosts.
    // ... blacklisted means don't ask for containers on this host.
    // Same list to be used for computing forcedUnblacklisting.
  }
  
  public void handle(AMNodeEvent event) {
    if (event.getType() == AMNodeEventType.N_NODE_WAS_BLACKLISTED) {
      // TODO Handle blacklisting.
    } else {
      NodeId nodeId = event.getNodeId();
      get(nodeId).handle(event);
    }
  }
}
