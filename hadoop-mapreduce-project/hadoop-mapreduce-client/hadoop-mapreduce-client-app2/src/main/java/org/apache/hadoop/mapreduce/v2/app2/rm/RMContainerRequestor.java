/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2.app2.rm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventReleased;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeEventStateChanged;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;


/**
 * Keeps the data structures to send container requests to RM.
 */
public class RMContainerRequestor extends RMCommunicator {
  
  private static final Log LOG = LogFactory.getLog(RMContainerRequestor.class);
  static final String ANY = "*";
  
  private final Clock clock;

  private int lastResponseID;
  private Resource availableResources;
  private long retrystartTime;
  private long retryInterval;

  // TODO XXX: Maintain some statistics on containers allocated, released etc.
  
  //Key -> Priority
  //Value -> Map
  //  Key->ResourceName (e.g., hostname, rackname, *)
  //  Value->Map
  //    Key->Resource Capability
  //    Value->ResourceRequest
  private final Map<Priority, Map<String, Map<Resource, ResourceRequest>>>
  remoteRequestsTable =
      new TreeMap<Priority, Map<String, Map<Resource, ResourceRequest>>>();

  private final Set<ResourceRequest> ask = new TreeSet<ResourceRequest>();
  private final Set<ContainerId> release = new TreeSet<ContainerId>(); 

//  private boolean nodeBlacklistingEnabled;
//  private int blacklistDisablePercent;
//  private AtomicBoolean ignoreBlacklisting = new AtomicBoolean(false);
//  private int blacklistedNodeCount = 0;
  private int lastClusterNmCount = 0;
  private int clusterNmCount = 0;
//  private int maxTaskFailuresPerNode;
//  private final Map<String, Integer> nodeFailures = new HashMap<String, Integer>();
//  private final Set<String> blacklistedNodes = Collections
//      .newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  public RMContainerRequestor(ClientService clientService, AppContext context, Clock clock) {
    super(clientService, context);
    this.clock = clock;
  }
  
  public static class ContainerRequest {
    final TaskAttemptId attemptID;
    final Resource capability;
    final String[] hosts;
    final String[] racks;
    //final boolean earlierAttemptFailed;
    final Priority priority;
   
    public ContainerRequest(AMSchedulerTALaunchRequestEvent event,
        Priority priority) {
      this(event.getAttemptID(), event.getCapability(), event.getHosts(),
          event.getRacks(), priority);
    }
    
    public ContainerRequest(ContainerRequestEvent event, Priority priority) {
      this(event.getAttemptID(), event.getCapability(), event.getHosts(),
          event.getRacks(), priority);
    }
    
    public ContainerRequest(TaskAttemptId attemptID,
        Resource capability, String[] hosts, String[] racks, 
        Priority priority) {
      // TODO XXX: Should be possible to get rid of this.
      this.attemptID = attemptID;
      this.capability = capability;
      this.hosts = hosts;
      this.racks = racks;
      this.priority = priority;
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("AttemptId[").append(attemptID).append("]");
      sb.append("Capability[").append(capability).append("]");
      sb.append("Priority[").append(priority).append("]");
      return sb.toString();
    }
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
//    nodeBlacklistingEnabled = 
//      conf.getBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
//    LOG.info("nodeBlacklistingEnabled:" + nodeBlacklistingEnabled);
//    maxTaskFailuresPerNode = 
//      conf.getInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 3);
//    blacklistDisablePercent =
//        conf.getInt(
//            MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT,
//            MRJobConfig.DEFAULT_MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERCENT);
//    LOG.info("maxTaskFailuresPerNode is " + maxTaskFailuresPerNode);
//    if (blacklistDisablePercent < -1 || blacklistDisablePercent > 100) {
//      throw new YarnException("Invalid blacklistDisablePercent: "
//          + blacklistDisablePercent
//          + ". Should be an integer between 0 and 100 or -1 to disabled");
//    }
//    LOG.info("blacklistDisablePercent is " + blacklistDisablePercent);
    retrystartTime = clock.getTime();
    retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
        MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
  }

  protected AMResponse makeRemoteRequest() throws YarnRemoteException {
    AllocateRequest allocateRequest = BuilderUtils.newAllocateRequest(
        applicationAttemptId, lastResponseID, super.getApplicationProgress(),
        new ArrayList<ResourceRequest>(ask), new ArrayList<ContainerId>(
            release));
    AllocateResponse allocateResponse = scheduler.allocate(allocateRequest);
    AMResponse response = allocateResponse.getAMResponse();
    lastResponseID = response.getResponseId();
    availableResources = response.getAvailableResources();
    lastClusterNmCount = clusterNmCount;
    clusterNmCount = allocateResponse.getNumClusterNodes();

    if (ask.size() > 0 || release.size() > 0) {
      LOG.info("getResources() for " + applicationId + ":" + " ask="
          + ask.size() + " release= " + release.size() + " newContainers="
          + response.getAllocatedContainers().size() + " finishedContainers="
          + response.getCompletedContainersStatuses().size()
          + " resourcelimit=" + availableResources + " knownNMs="
          + clusterNmCount);
    }

    
    
    ask.clear();
    release.clear();
    return response;
  }
  
  

  protected Resource getAvailableResources() {
    return availableResources;
  }
  
  public void addContainerReq(ContainerRequest req) {
    // Create resource requests
    for (String host : req.hosts) {
      // Data-local
      if (!context.getAllNodes().isHostBlackListed(host)) {
        addResourceRequest(req.priority, host, req.capability);
      }      
    }

    // Nothing Rack-local for now
    for (String rack : req.racks) {
      addResourceRequest(req.priority, rack, req.capability);
    }

    // Off-switch
    addResourceRequest(req.priority, ANY, req.capability); 
  }

  public void decContainerReq(ContainerRequest req) {
    // Update resource requests
    for (String hostName : req.hosts) {
      decResourceRequest(req.priority, hostName, req.capability);
    }
    
    for (String rack : req.racks) {
      decResourceRequest(req.priority, rack, req.capability);
    }
   
    decResourceRequest(req.priority, ANY, req.capability);
  }

  private void addResourceRequest(Priority priority, String resourceName,
      Resource capability) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests =
      this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      remoteRequests = new HashMap<String, Map<Resource, ResourceRequest>>();
      this.remoteRequestsTable.put(priority, remoteRequests);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added priority=" + priority);
      }
    }
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      reqMap = new HashMap<Resource, ResourceRequest>();
      remoteRequests.put(resourceName, reqMap);
    }
    ResourceRequest remoteRequest = reqMap.get(capability);
    if (remoteRequest == null) {
      remoteRequest = Records.newRecord(ResourceRequest.class);
      remoteRequest.setPriority(priority);
      remoteRequest.setHostName(resourceName);
      remoteRequest.setCapability(capability);
      remoteRequest.setNumContainers(0);
      reqMap.put(capability, remoteRequest);
    }
    remoteRequest.setNumContainers(remoteRequest.getNumContainers() + 1);

    // Note this down for next interaction with ResourceManager
    ask.add(remoteRequest);
    if (LOG.isDebugEnabled()) {
      LOG.debug("addResourceRequest:" + " applicationId="
          + applicationId.getId() + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }
  }

  private void decResourceRequest(Priority priority, String resourceName,
      Resource capability) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests =
      this.remoteRequestsTable.get(priority);
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      // as we modify the resource requests by filtering out blacklisted hosts 
      // when they are added, this value may be null when being 
      // decremented
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not decrementing resource as " + resourceName
            + " is not present in request table");
      }
      return;
    }
    ResourceRequest remoteRequest = reqMap.get(capability);

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEFORE decResourceRequest:" + " applicationId="
          + applicationId.getId() + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }

    remoteRequest.setNumContainers(remoteRequest.getNumContainers() -1);
    if (remoteRequest.getNumContainers() == 0) {
      reqMap.remove(capability);
      if (reqMap.size() == 0) {
        remoteRequests.remove(resourceName);
      }
      if (remoteRequests.size() == 0) {
        remoteRequestsTable.remove(priority);
      }
      //remove from ask if it may have
      ask.remove(remoteRequest);
    } else {
      ask.add(remoteRequest);//this will override the request if ask doesn't
      //already have it.
    }

    if (LOG.isDebugEnabled()) {
      LOG.info("AFTER decResourceRequest:" + " applicationId="
          + applicationId.getId() + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }
  }

  protected void release(ContainerId containerId) {
    release.add(containerId);
  }
  
  
  @SuppressWarnings("unchecked")
  @Override
  protected void heartbeat() throws Exception {
    int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;//first time it would be null
    AMResponse response = errorCheckedMakeRemoteRequest();
    
    int newHeadRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
    List<Container> newContainers = response.getAllocatedContainers();
    // TODO Log newly available containers.
    List<ContainerStatus> finishedContainers = response.getCompletedContainersStatuses();
    List<NodeReport> updatedNodeReports = response.getUpdatedNodes();
 
    // Inform the Containers about completion..
    for (ContainerStatus c : finishedContainers) {
      eventHandler.handle(new AMContainerEventReleased(c));
    }

    // TODO XXX Needs to know if a node is blacklisted, to remove it from the request table.
    // Assumption is that the node never becomes healthy agian.
    
    // TODO XXX In case of a node going health / unhealthy - the request tables can
    // be leaved untouched. Relying on the RM not allocating Containers on unhealthy nodes.
    // There can however be a check to ensure the node is healthy before trying to allocate
    // a container.
    
    // Inform the scheduler about new containers.
    List<ContainerId> newContainerIds;
    if (newContainers.size() > 0) {
      newContainerIds = new ArrayList<ContainerId>(newContainers.size());
      // TODO XXX Potentially add new nodes.
      // TODO Figure out a good flow to add a new node.
      for (Container container : newContainers) {
        context.getAllContainers().addNewContainer(container);
        newContainerIds.add(container.getId());
        context.getAllNodes().nodeSeen(container.getNodeId());
      }
      eventHandler.handle(new AMSchedulerEventContainersAllocated(
          newContainerIds, (newHeadRoom - headRoom != 0)));
    }

    //Inform the nodes about sate changes.
    for (NodeReport nr : updatedNodeReports) {
      eventHandler.handle(new AMNodeEventStateChanged(nr));
      // TODO XXX Does the Allocator need to be aware of this.
      // Likely yes, so that it can clean up it's tables.
    }
    
    
    
    // TODO Reduce requests 
    
    // Handle 
      // - lost nodes.
      // - Newly allocated containers
    // - Changes in headroom
      // - Newly allocated nodes.
      // - Failed / Released containers.
      // - Completed containers.
    // - Total number of nodes in the cluster.
  
    // Local events
    // - Container failed on a specific host. Local blacklisting.
    // - Needs to be removed from the RMComm requests table.
    
    // Who's responsibility is it to keep track of failed tasks, withdrawing requests etc.
    // Ideally... requests should be stored along with Nodes ?
  }
  
  
  
  
  
  @SuppressWarnings("unchecked")
  protected AMResponse errorCheckedMakeRemoteRequest() throws Exception {
    AMResponse response = null;
    try {
      response = makeRemoteRequest();
      // Reset retry count if no exception occurred.
      retrystartTime = clock.getTime();
    } catch (Exception e) {
      // This can happen when the connection to the RM has gone down. Keep
      // re-trying until the retryInterval has expired.
      if (clock.getTime() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
        eventHandler.handle(new JobEvent(this.getJob().getID(),
                                         JobEventType.INTERNAL_ERROR));
        throw new YarnException("Could not contact RM after " +
                                retryInterval + " milliseconds.");
        // TODO XXX: Some changes to the exception handling -> e is ignored, YarnException causes an exit. 
      }
      // Throw this up to the caller, which may decide to ignore it and
      // continue to attempt to contact the RM.
      throw e;
    }
    if (response.getReboot()) {
      // This can happen if the RM has been restarted. If it is in that state,
      // this application must clean itself up.
      eventHandler.handle(new JobEvent(this.getJob().getID(),
          JobEventType.INTERNAL_ERROR));
      throw new YarnException("Resource Manager doesn't recognize AttemptId: "
          + this.getContext().getApplicationID());
    }
    return response;
  }
}
