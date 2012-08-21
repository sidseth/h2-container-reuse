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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
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
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;


/**
 * Keeps the data structures to send container requests to RM.
 */
public class RMContainerRequestor extends RMCommunicator implements EventHandler<RMCommunicatorEvent> {
  
  private static final Log LOG = LogFactory.getLog(RMContainerRequestor.class);
  static final String ANY = "*";
  
  private final Clock clock;

  private int lastResponseID;
  private Resource availableResources; // aka headroom.
  private long retrystartTime;
  private long retryInterval;
  
  private int numContainerReleaseRequests;
  private int numContainersAllocated;
  private int numFinishedContainers; // Not very useful.
  
  // TODO XXX: Lots of cleanup.
  
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
  private Set<ContainerId> release = new TreeSet<ContainerId>();
  
  private Lock releaseLock = new ReentrantLock();
  private Lock askLock = new ReentrantLock();
  private final List<ContainerId> emptyReleaseList = new ArrayList<ContainerId>(0);
  private final List<ResourceRequest> emptyAskList = new ArrayList<ResourceRequest>();
  
  // TODO XXX: May need to pass this to the NodeManager.
  private int clusterNmCount = 0;
  
  // TODO XXX Consider allowing sync comm between the requestor and allocator... 
  
  // TODO XXX: Why does the RMRequestor require the ClientService ??
  // TODO XXX: Get rid of the clock. Available from the context.
  public RMContainerRequestor(ClientService clientService, AppContext context) {
    super(clientService, context);
    this.clock = context.getClock();
  }
  
  public static class ContainerRequest {
    final Resource capability;
    final String[] hosts;
    final String[] racks;
    final Priority priority;

    public ContainerRequest(AMSchedulerTALaunchRequestEvent event,
        Priority priority) {
      this(event.getCapability(), event.getHosts(), event.getRacks(), priority);
    }

    public ContainerRequest(Resource capability, String[] hosts,
        String[] racks, Priority priority) {
      this.capability = capability;
      this.hosts = hosts;
      this.racks = racks;
      this.priority = priority;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Capability[").append(capability).append("]");
      sb.append("Priority[").append(priority).append("]");
      return sb.toString();
    }
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);

    retrystartTime = clock.getTime();
    retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
        MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
  }
  
  public void stop(Configuration conf) {
    LOG.info("NumAllocatedContainers: " + numContainersAllocated
           + "NumFinihsedContainers: " + numFinishedContainers
           + "NumReleaseRequests: " + numContainerReleaseRequests);
    super.stop();
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
    int askSize = 0;
    askLock.lock();
    try {
      ask.add(remoteRequest);
      askSize = ask.size();
    } finally {
      askLock.unlock();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("addResourceRequest:" + " applicationId="
          + applicationId.getId() + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + remoteRequest.getNumContainers() + " #asks=" + askSize);
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
          + remoteRequest.getNumContainers() + " #asks=" + getAskSize());
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
      askLock.lock();
      try {
        ask.remove(remoteRequest);
      } finally {
        askLock.unlock();
      }
    } else {
      askLock.lock();
      try {
        ask.add(remoteRequest);//this will override the request if ask doesn't
      //already have it.
      } finally {
        askLock.unlock();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.info("AFTER decResourceRequest:" + " applicationId="
          + applicationId.getId() + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + remoteRequest.getNumContainers() + " #asks=" + getAskSize());
    }
  }
  
  private int getAskSize() {
    askLock.lock();
    try {
      return ask.size();
    } finally {
      askLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void heartbeat() throws Exception {
    int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;//first time it would be null
    AMResponse response = errorCheckedMakeRemoteRequest();
    
    int newHeadRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
    List<Container> newContainers = response.getAllocatedContainers();    
    logNewContainers(newContainers);
    numContainersAllocated += newContainers.size();
    
    List<ContainerStatus> finishedContainers = response.getCompletedContainersStatuses();
    logFinishedContainers(finishedContainers);
    numFinishedContainers += finishedContainers.size();
    
    List<NodeReport> updatedNodeReports = response.getUpdatedNodes();
    logUpdatedNodes(updatedNodeReports);
 
    // Inform the Containers about completion..
    for (ContainerStatus c : finishedContainers) {
      eventHandler.handle(new AMContainerEventReleased(c));
    }
   
    // Inform the scheduler about new containers.
    List<ContainerId> newContainerIds;
    if (newContainers.size() > 0) {
      newContainerIds = new ArrayList<ContainerId>(newContainers.size());
      for (Container container : newContainers) {
        context.getAllContainers().addNewContainer(container);
        newContainerIds.add(container.getId());
        // TODO XXX: Maybe send this out as an Asynchrounous event ? | Scheduler events will also have to be async in that case, and the order is critical. 
        context.getAllNodes().nodeSeen(container.getNodeId());
      }
      eventHandler.handle(new AMSchedulerEventContainersAllocated(
          newContainerIds, (newHeadRoom - headRoom != 0)));
    }

    //Inform the nodes about sate changes.
    for (NodeReport nr : updatedNodeReports) {
      eventHandler.handle(new AMNodeEventStateChanged(nr));
      // Allocator will find out from the node, if at all.
      // Relying on the RM to not allocated containers on an unhealthy node.
    }
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
  
  
  protected AMResponse makeRemoteRequest() throws Exception {
    List<ContainerId> clonedReleaseList = cloneAndClearReleaseList();
    List<ResourceRequest> clonedAskList = cloneAndClearAskList();

    AllocateRequest allocateRequest = BuilderUtils.newAllocateRequest(
        applicationAttemptId, lastResponseID, super.getApplicationProgress(),
        clonedAskList, clonedReleaseList);
    AllocateResponse allocateResponse = null;
    try {
      allocateResponse = scheduler.allocate(allocateRequest);
    } catch (Exception e) {
      rePopulateListsOnError(clonedReleaseList, clonedAskList);
      throw e;
    }
    AMResponse response = allocateResponse.getAMResponse();
    lastResponseID = response.getResponseId();
    availableResources = response.getAvailableResources();
    clusterNmCount = allocateResponse.getNumClusterNodes();

    if (clonedAskList.size() > 0 || clonedReleaseList.size() > 0) {
      LOG.info("getResources() for " + applicationId + ":" + " ask="
          + clonedAskList.size() + " release= " + clonedReleaseList.size() 
          + " newContainers=" + response.getAllocatedContainers().size() 
          + " finishedContainers="+ response.getCompletedContainersStatuses().size()
          + " resourcelimit=" + availableResources + " knownNMs=" + clusterNmCount);
    }

    return response;
  }

  @Override
  public void handle(RMCommunicatorEvent rawEvent) {
    switch(rawEvent.getType()) {
    case CONTAINER_DEALLOCATE:
      RMCommunicatorContainerDeAllocateRequestEvent event = (RMCommunicatorContainerDeAllocateRequestEvent)rawEvent;
      releaseLock.lock();
      try {
        numContainerReleaseRequests++;
        release.add(event.getContainerId());
      } finally {
        releaseLock.unlock();
      }
      break;
    case CONTAINER_FAILED:
      LOG.warn("Unexpected CONTAINER_FAILED");
      break;
    case CONTAINER_REQ:
      LOG.warn("Unexpected CONTAINER_REQ");
      break;
    default:
      break;
    }
  }


  private List<ContainerId> cloneAndClearReleaseList() {
    ArrayList<ContainerId> clonedReleaseList;
    releaseLock.lock();
    try {
      if (release.size() == 0) {
        return emptyReleaseList;
      }
      clonedReleaseList = new ArrayList<ContainerId>(release);
      release.clear();
      return clonedReleaseList;
    } finally {
      releaseLock.unlock();
    }
  }

  private List<ResourceRequest> cloneAndClearAskList() {
    ArrayList<ResourceRequest> clonedAskList;
    askLock.lock();
    try {
      if (ask.size() == 0) {
        return emptyAskList;
      }
      clonedAskList = new ArrayList<ResourceRequest>(ask);
      ask.clear();
      return clonedAskList;
    } finally {
      askLock.unlock();
    }
  }

  private void rePopulateListsOnError(List<ContainerId> clonedReleaseList,
      List<ResourceRequest> clonedAskList) {
    releaseLock.lock();
    try {
      release.addAll(clonedReleaseList);
    } finally {
      releaseLock.unlock();
    }
    askLock.lock();
    try {
      ask.addAll(clonedAskList);
      // TODO XXX: Asks cannot be populated like this. Would be better to
      // iterate over the asks and get corresponding requests from the main
      // table.
    } finally {
      askLock.unlock();
    }
  }
  

  private void logNewContainers(List<Container> newContainers) {
    if (newContainers.size() > 0) {
      LOG.info("Got allocated " + newContainers.size() + " containers");
      for (Container c : newContainers) {
        LOG.info("AllocatedContainer: " + c);
      }
    }
  }
  
  private void logFinishedContainers(List<ContainerStatus> finishedContainers) {
    if (finishedContainers.size() > 0) {
      LOG.info(finishedContainers.size() + " finished");
      for (ContainerStatus cs : finishedContainers) {
        LOG.info("FinihsedContainer: " + cs);
      }
    }
  }
  
  private void logUpdatedNodes(List<NodeReport> nodeReports) {
    if (nodeReports.size() > 0) {
      LOG.info(nodeReports.size() + " nodes changed state");
      for (NodeReport nr : nodeReports) {
        LOG.info("UpdatedNodeReport: " + nr);
      }
    }
  }
}
