package org.apache.hadoop.mapreduce.v2.app2.rm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMScheduler2.PendingAttempts;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainer;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerMap;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNode;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.RackResolver;

public class AMScheduler extends AbstractService implements EventHandler<AMSchedulerEvent> {

  static final Log LOG = LogFactory.getLog(AMScheduler.class);

  public static final float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;

  public static final Priority PRIORITY_FAST_FAIL_MAP;
  public static final Priority PRIORITY_REDUCE;
  public static final Priority PRIORITY_MAP;

  private final RMContainerRequestor requestor;
  private final AppContext appContext;
  private final EventHandler eventHandler;
  private final JobId jobId;
  private final Clock clock;

  private int containersAllocated = 0;
  private int containersReleased = 0;
  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  
  private boolean recalculateReduceSchedule = false;
  private int mapResourceReqt;//memory
  private int reduceResourceReqt;//memory
  
  private float reduceSlowStart = 0;
  private float maxReducePreemptionLimit = 0;
  private float maxReduceRampupLimit = 0;
  private boolean reduceStarted = false;
  private long lastScheduleTime = 0l;
  
  //TODO Make Configurable.
  // Run the scheduler if it hasn't run for this interval.
  private long scheduleInterval = 1000l;
  
  Timer scheduleTimer;
  ScheduleTimerTask scheduleTimerTask;
  
  private AMContainerMap containerMap;


  // Used for constructing CLC etc. May be possible to pull this from the job / task.
  private final Map<TaskAttemptId, AMSchedulerTALaunchRequestEvent> taToLaunchRequestMap 
      = new HashMap<TaskAttemptId, AMSchedulerTALaunchRequestEvent>();
  
  private final Requests scheduledRequests = new Requests();
  private final Requests pendingRequests = new Requests();
  private final AssignedRequests assignedRequests = new AssignedRequests();

  private List<ContainerId> availableContainerIds = new LinkedList<ContainerId>();
  
  
  
  //XXXXXX Get rid of this.....
  /*
   * Structures required.
   * .... Pending map requests keyed by host / rack. 
   * .... Scheduled map requests (Sent to requester). 
   *..... Pending reduce requests. (Not sent to Requester)
   *..... Pending reduce requests. (Sent to Requester).
   *
   *..... Currently allocated -> This should be maintained in the Container itself.
   *..... Currently running -> Again, maintained in the container itself.
   */
  
  
  BlockingQueue<AMSchedulerEvent> eventQueue = new LinkedBlockingQueue<AMSchedulerEvent>();
  private Thread eventHandlingThread;
  private volatile boolean stopEventHandling;

  static {
    PRIORITY_FAST_FAIL_MAP = BuilderUtils.newPriority(5);
    PRIORITY_REDUCE = BuilderUtils.newPriority(10);
    PRIORITY_MAP = BuilderUtils.newPriority(20);
  }

  @SuppressWarnings("rawtypes")
  public AMScheduler(RMContainerRequestor requestor, AppContext context, Clock clock,
      EventHandler eventHandler) {
    super("AMScheduler");
    this.requestor = requestor;
    this.appContext = context;
    this.eventHandler = eventHandler;
    this.clock = clock;
    this.jobId = requestor.getJob().getID();
  }

  
  // TODO XXX Fix this rubbish. Is all wrong.
  /*
   * Vocabulary Used: pending -> requests which are NOT yet sent to RM scheduled
   * -> requests which are sent to RM but not yet assigned assigned -> requests
   * which are assigned to a container completed -> request corresponding to
   * which container has completed
   * 
   * Lifecycle of map scheduled->assigned->completed
   * 
   * Lifecycle of reduce pending->scheduled->assigned->completed
   * 
   * Maps are scheduled as soon as their requests are received. Reduces are
   * added to the pending and are ramped up (added to scheduled) based on
   * completed maps and current availability in the cluster.
   */

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    reduceSlowStart = conf.getFloat(
        MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART,
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);
    maxReduceRampupLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_RAMPUP_UP_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_RAMP_UP_LIMIT);
    maxReducePreemptionLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_PREEMPTION_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT);
    RackResolver.init(conf);
    containerMap = this.appContext.getAllContainers();
  }

  @Override
  public void start() {
    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {

        AMSchedulerEvent event;

        while (!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            event = AMScheduler.this.eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the ContainreAllocator", t);
            // Kill the AM
            sendError("");
            return;
          }
        }
      }
    };
    this.eventHandlingThread.start();
    
    scheduleTimer = new Timer("AMSchedulerTimer", true);
    scheduleTimerTask = new ScheduleTimerTask();
    scheduleTimer.scheduleAtFixedRate(scheduleTimerTask, scheduleInterval, scheduleInterval);
    
    super.start();
  }
  
  @Override
  public void stop() {
    this.stopEventHandling = true;
    if (scheduleTimerTask != null) {
      scheduleTimerTask.stop();
    }
    super.stop();
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    eventHandler.handle(event);
  }
  
  private void sendError(String message) {
    // TODO XXX: job error.
//    eventHandler.handle(new JobEvent(getJob().getID(),
//        JobEventType.INTERNAL_ERROR));
  }
  
  private class ScheduleTimerTask extends TimerTask {

    private volatile boolean shouldRun = true;

    @Override
    public void run() {
      // TODO XXX Figure out when this needs to be stopped.
      if (clock.getTime() - lastScheduleTime > scheduleInterval && shouldRun) {
        handle(new AMSchedulerEventContainersAllocated(
            Collections.<ContainerId> emptyList()));
      }
    }

    public void stop() {
      shouldRun = false;
      this.cancel();
    }
  }
  
  
  @Override
  public void handle(AMSchedulerEvent event) {
    int qSize = eventQueue.size();
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of RMContainerAllocator: " + remCapacity);
    }
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  protected synchronized void handleEvent(AMSchedulerEvent sEvent) {
    
    recalculateReduceSchedule = true;
    switch(sEvent.getType()) {
    case S_TA_LAUNCH_REQUEST:
      handleTaLaunchRequest((AMSchedulerTALaunchRequestEvent) sEvent);
      // Add to queue of pending tasks.
      break;
    case S_TA_STOP_REQUEST: //Effectively means a failure.
      handleTaStopRequest((AMSchedulerTAStopRequestEvent)sEvent);
      break;
    case S_TA_SUCCEEDED:
      handleTaSucceededRequest((AMSchedulerTASucceededEvent)sEvent);
      break;
    case S_CONTAINERS_ALLOCATED:
      handleContainersAllocated((AMSchedulerEventContainersAllocated) sEvent);
      break;
    case S_NODE_HEALTHY:
      // Modify the request table to include this node.
      break;
    case S_NODE_UNHEALTHY:
      // Modify the request table to exclude this node.
      
      // Check whether these two events really need to come to the scheduler.
      break;
    }
  }
  
  private void handleTaLaunchRequest(AMSchedulerTALaunchRequestEvent event) {
    // Add to queue of pending tasks.
    if (event.getTaskAttemptId().getTaskId().getTaskType() == TaskType.MAP) {
      mapResourceReqt = maybeComputeNormalizedRequestForType(event,
          TaskType.MAP, mapResourceReqt);

      event.getCapability().setMemory(mapResourceReqt);
      // XXX TODO Add to various queues.
      if (shouldScheduleMap(event)) {
        scheduledRequests.addMap(event);
      } else {
        pendingRequests.addMap(event);
      }
    } else { // Reduce
      reduceResourceReqt = maybeComputeNormalizedRequestForType(event,
          TaskType.REDUCE, reduceResourceReqt);
      event.getCapability().setMemory(reduceResourceReqt);
      if (shouldScheduleReduce(event)) {
        scheduledRequests.addReduce(event);
      } else {
        pendingRequests.add(event);
      }
    }
  }

  // TODO XXX Maybe override in case of re-use
  protected boolean shouldScheduleMap(AMSchedulerTALaunchRequestEvent event) {
    return true;
  }
  
  protected boolean shouldScheduleReduce(AMSchedulerTALaunchRequestEvent event) {
    // TODO XXX: Actual computation of whether a reduce is ready to be scheduled or not.
    return true;
  }
  
  // Equivalent to container failure.
  private void handleTaStopRequest(AMSchedulerTAStopRequestEvent event) {
    TaskAttemptId taId = event.getTaskAttemptId();
    if (pending.contains(taId)) {
      pending.remove(taId);
      // TODO XXX Send out events...
    } else if (scheduled.contains(taId)) {
      scheduled.remove(taId);
      // TODO Maybe withdraw container request.
      // In case of re-use, we may not want to withdraw the request.
    } else if (assigned.contains(taId)) {
      // TODO Send out a kill request to the container.
      //... maybe remove from the assigned table, or will that happen when the container actually exits.
      // Is there a need to check container state ?
      sendEvent(new AMContainerEvent(assigned.getContainerId(taId), AMContainerEventType.C_STOP_REQUEST));
    }
  }
  
  // Decide what needs to be done with the container.
  private void handleTaSucceededRequest(AMSchedulerTASucceededEvent event) {
    // TODO XXX
    /*
     * One part of re-use. 
     * Return the container. The allocator may decide to re-use it.
     */
    // TODO This only means the task succeeded. 
    // The task should've informed the container.
    ContainerId containerId = assignedRequests.remove(event.getTaskAttemptId());
    if (containerId != null ) {// TODO Is null really possible ? 
      handleContainerAvailable(containerId);
    }
  }
  
  // TODO Override for container re-use.
  protected void handleContainerAvailable(ContainerId containerId) {
    // For now releasing the container.
    //    allocatedContainerIds.add(containerId);
    sendEvent(new AMContainerEvent(containerId, AMContainerEventType.C_STOP_REQUEST));
  }
  
  private void handleContainersAllocated(AMSchedulerEventContainersAllocated event) {
    // TODO XXX
    /*
     * Start allocating containers. Match requests to capabilities. 
     * Send out Container_START / Container_TA_ASSIGNED events.
     */
    // TODO XXX: Logging of the assigned containerIds.
    availableContainerIds.addAll(event.getContainerIds());
    schedule();
  }
  
  /* availableContainerIds contains the currently available containers.
   * Should be cleared appropriately. 
   */
  private synchronized void schedule() {
    assignContainers();
    requestContainers();
  }
  
  
  
  

  @SuppressWarnings("unchecked")
  private int maybeComputeNormalizedRequestForType(
      AMSchedulerTALaunchRequestEvent event, TaskType taskType,
      int prevComputedSize) {
    if (prevComputedSize == 0) {
      int supportedMaxContainerCapability = requestor
          .getMaxContainerCapability().getMemory();
      prevComputedSize = event.getCapability().getMemory();
      int minSlotMemSize = requestor.getMinContainerCapability().getMemory();
      prevComputedSize = (int) Math.ceil((float) prevComputedSize
          / minSlotMemSize)
          * minSlotMemSize;
      eventHandler.handle(new JobHistoryEvent(jobId,
          new NormalizedResourceEvent(TypeConverter.fromYarn(taskType),
              prevComputedSize)));
      LOG.info(taskType + "ResourceReqt:" + prevComputedSize);
      if (prevComputedSize > supportedMaxContainerCapability) {
        String diagMsg = taskType
            + " capability required is more than the supported "
            + "max container capability in the cluster. Killing the Job. "
            + taskType + "ResourceReqt: " + prevComputedSize
            + " maxContainerCapability:" + supportedMaxContainerCapability;
        LOG.info(diagMsg);
        eventHandler.handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
        eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
      }
    }
    return prevComputedSize;
  }
  
  // TODO Allow override.
  protected synchronized void assignContainers() {
    for (ContainerId containerId : availableContainerIds) {
      AMContainer amContainer = containerMap.get(containerId);
      ContainerRequest assigned = validateAndAssignContainer(amContainer, scheduled, assigned, pending);
      // TODO XXX Have assignContainer return something so that assigned contaienrs can be removed from this array.
      if (assigned == null) {
        // Release the container for now.
        releaseContainer(containerId); 
      } else {
        // Events go out to the TaskAttempt or to the Container.
      }
      // TODO XXX Remove containerId from allocatedContainerIds.
    }
  }
  
  // TODO Allow override
  protected void requestContainers() {
    
  }

  private TaskAttemptId validateAndAssignContainer(AMContainer amContainer) {
    // TODO XXX Actual asignment.
    boolean isAssignable = true;
    
    //Check if the node is usable.
    AMNode amNode = appContext.getNode(amContainer.getContainer().getNodeId());
    if (!amNode.isUsable()) {
      // TODO XXX
      // Swap container requests. This container will be released.
      isAssignable = false;
    }
    
    ContainerRequest assigned = null;
    
    if (isAssignable) {
      Container container = amContainer.getContainer();
      Priority priority = container.getPriority();
      int allocatedMemory = container.getResource().getMemory();
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)
          || PRIORITY_MAP.equals(priority)) {
        if (allocatedMemory < mapResourceReqt || scheduled.maps.isEmpty()) {
          LOG.info("Cannot assign container " + container
              + " for a map as either "
              + " container memory less than required " + mapResourceReqt
              + " or no pending map tasks - maps.isEmpty="
              + scheduled.maps.isEmpty());
          isAssignable = false;
        }
      } else if (PRIORITY_REDUCE.equals(priority)) {
        if (allocatedMemory < reduceResourceReqt || scheduled.reduces.isEmpty()) {
          LOG.info("Cannot assign container " + container
              + " for a reduce as either "
              + " container memory less than required " + reduceResourceReqt
              + " or no pending reduce tasks - reduces.isEmpty="
              + scheduled.reduces.isEmpty());
          isAssignable = false;
        }
      }
    }
    
    if (isAssignable) {
      assignedAttemptId = assign(amContainer, scheduled, assigned, pending);
    }
       
    return assignedAttemptId;
  }
  
  private TaskAttemptId assign(AMContainer amContainer,
      ScheduledAttempts scheduled, AssignedAttempts assigned,
      PendingAttempts pending) {
    TaskAttemptId assignedAttemptId = null;
    Priority priority = amContainer.getContainer().getPriority();
    if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
      assignedAttemptId = assignToFailedMap(amContainer, scheduled, assigned, pending);
    } else if (PRIORITY_REDUCE.equals(priority)) {
      assignedAttemptId = assignToReduce(amContainer, scheduled, assigned, pending);
    } else if (PRIORITY_MAP.equals(priority)) {
      assignedAttemptId = assignToMap(amContainer, scheduled, assigned, pending);
    } else {
      LOG.warn("Got assigned a container with an unrecognized priority of : " + priority + ". Will be released");
    }
    return assignedAttemptId;
  }
    

  protected void releaseContainer(ContainerId containerId) {
    requestor.release(containerId);
  }
  
  /*
  PendingAttempts
  ContainerRequestedAttempts
  RunningAttempts
  */
  

 private class Requests {
    
    private final LinkedList<TaskAttemptId> earlierFailedMaps = 
      new LinkedList<TaskAttemptId>();
    private final LinkedList<TaskAttemptId> earlierFailedReduces = 
      new LinkedList<TaskAttemptId>();
    
    /** Maps from a host to a list of Map tasks with data on the host */
    private final Map<String, LinkedList<TaskAttemptId>> mapsHostMapping = 
      new HashMap<String, LinkedList<TaskAttemptId>>();
    private final Map<String, LinkedList<TaskAttemptId>> mapsRackMapping = 
      new HashMap<String, LinkedList<TaskAttemptId>>();
    private final Map<TaskAttemptId, ContainerRequest> maps = 
      new LinkedHashMap<TaskAttemptId, ContainerRequest>();
    
    private final LinkedHashMap<TaskAttemptId, ContainerRequest> reduces = 
      new LinkedHashMap<TaskAttemptId, ContainerRequest>();
    
    boolean remove(TaskAttemptId tId) {
      ContainerRequest req = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        req = maps.remove(tId);
      } else {
        req = reduces.remove(tId);
      }
      
      if (req == null) {
        return false;
      } else {
        requestor.decContainerReq(req);
        return true;
      }
    }
    
    ContainerRequest removeReduce() {
      Iterator<Entry<TaskAttemptId, ContainerRequest>> it = reduces.entrySet().iterator();
      if (it.hasNext()) {
        Entry<TaskAttemptId, ContainerRequest> entry = it.next();
        it.remove();
        requestor.decContainerReq(entry.getValue());
        return entry.getValue();
      }
      return null;
    }
    
    // TODO XXX: Different for scheduled versus pending. Pending should have no interaction with the ContainerRequestor.
    void addMap(AMSchedulerTALaunchRequestEvent event) {
      ContainerRequest request = null;
      
      if (event.isRescheduled()) {
        earlierFailedMaps.add(event.getTaskAttemptId());
        request = new ContainerRequest(event, PRIORITY_FAST_FAIL_MAP);
        LOG.info("Added "+event.getTaskAttempt()+" to list of failed maps");
      } else {
        for (String host : event.getHosts()) {
          LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
          if (list == null) {
            list = new LinkedList<TaskAttemptId>();
            mapsHostMapping.put(host, list);
          }
          list.add(event.getTaskAttemptId());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Added attempt req to host " + host);
          }
       }
       for (String rack: event.getRacks()) {
         LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
         if (list == null) {
           list = new LinkedList<TaskAttemptId>();
           mapsRackMapping.put(rack, list);
         }
         list.add(event.getTaskAttemptId());
         if (LOG.isDebugEnabled()) {
            LOG.debug("Added attempt req to rack " + rack);
         }
       }
       request = new ContainerRequest(event, PRIORITY_MAP);
      }
      maps.put(event.getTaskAttemptId(), request);
      requestor.addContainerReq(request);
    }
    
    
    void addReduce(AMSchedulerTALaunchRequestEvent event) {
      ContainerRequest containerRequest = new ContainerRequest(event, PRIORITY_REDUCE);
      // TODO Handle rescheduled reduces.... First in the queue.
      if (event.isRescheduled()) {
        earlierFailedReduces.add(event.getTaskAttemptId());
      }
      reduces.put(event.getTaskAttemptId(), containerRequest);
      requestor.addContainerReq(containerRequest);
    }
    
    @SuppressWarnings("unchecked")
    private void assign(List<Container> allocatedContainers) {
      Iterator<Container> it = allocatedContainers.iterator();
      LOG.info("Got allocated containers " + allocatedContainers.size());
      containersAllocated += allocatedContainers.size();
      while (it.hasNext()) {
        Container allocated = it.next();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated.getId()
              + " with priority " + allocated.getPriority() + " to NM "
              + allocated.getNodeId());
        }
        
        // check if allocated container meets memory requirements 
        // and whether we have any scheduled tasks that need 
        // a container to be assigned
        boolean isAssignable = true;
        Priority priority = allocated.getPriority();
        int allocatedMemory = allocated.getResource().getMemory();
        if (PRIORITY_FAST_FAIL_MAP.equals(priority) 
            || PRIORITY_MAP.equals(priority)) {
          if (allocatedMemory < mapResourceReqt
              || maps.isEmpty()) {
            LOG.info("Cannot assign container " + allocated 
                + " for a map as either "
                + " container memory less than required " + mapResourceReqt
                + " or no pending map tasks - maps.isEmpty=" 
                + maps.isEmpty()); 
            isAssignable = false; 
          }
        } 
        else if (PRIORITY_REDUCE.equals(priority)) {
          if (allocatedMemory < reduceResourceReqt
              || reduces.isEmpty()) {
            LOG.info("Cannot assign container " + allocated 
                + " for a reduce as either "
                + " container memory less than required " + reduceResourceReqt
                + " or no pending reduce tasks - reduces.isEmpty=" 
                + reduces.isEmpty()); 
            isAssignable = false;
          }
        }          
        
        boolean blackListed = false;         
        ContainerRequest assigned = null;
        
        if (isAssignable) {
          // do not assign if allocated container is on a  
          // blacklisted host
          String allocatedHost = allocated.getNodeId().getHost();
          blackListed = isNodeBlacklisted(allocatedHost);
          if (blackListed) {
            // we need to request for a new container 
            // and release the current one
            LOG.info("Got allocated container on a blacklisted "
                + " host "+allocatedHost
                +". Releasing container " + allocated);

            // find the request matching this allocated container 
            // and replace it with a new one 
            ContainerRequest toBeReplacedReq = 
                getContainerReqToReplace(allocated);
            if (toBeReplacedReq != null) {
              LOG.info("Placing a new container request for task attempt " 
                  + toBeReplacedReq.attemptID);
              ContainerRequest newReq = 
                  getFilteredContainerRequest(toBeReplacedReq);
              requestor.decContainerReq(toBeReplacedReq);
              if (toBeReplacedReq.attemptID.getTaskId().getTaskType() ==
                  TaskType.MAP) {
                maps.put(newReq.attemptID, newReq);
              }
              else {
                reduces.put(newReq.attemptID, newReq);
              }
              requestor.addContainerReq(newReq);
            }
            else {
              LOG.info("Could not map allocated container to a valid request."
                  + " Releasing allocated container " + allocated);
            }
          }
          else {
            assigned = assign(allocated);
            if (assigned != null) {
              // Update resource requests
              requestor.decContainerReq(assigned);

              // send the container-assigned event to task attempt
              eventHandler.handle(new TaskAttemptContainerAssignedEvent(
                  assigned.attemptID, allocated, applicationACLs));

              requestor.assignedRequests.add(allocated, assigned.attemptID);

              if (LOG.isDebugEnabled()) {
                LOG.info("Assigned container (" + allocated + ") "
                    + " to task " + assigned.attemptID + " on node "
                    + allocated.getNodeId().toString());
              }
            }
            else {
              //not assigned to any request, release the container
              LOG.info("Releasing unassigned and invalid container " 
                  + allocated + ". RM has gone crazy, someone go look!"
                  + " Hey RM, if you are so rich, go donate to non-profits!");
            }
          }
        }
        
        // release container if it was blacklisted 
        // or if we could not assign it 
        if (blackListed || assigned == null) {
          containersReleased++;
          requestor.release(allocated.getId());
        }
      }
    }
    
    private ContainerRequest assign(Container allocated) {
      ContainerRequest assigned = null;
      
      Priority priority = allocated.getPriority();
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Assigning container " + allocated + " to fast fail map");
        assigned = assignToFailedMap(allocated);
      } else if (PRIORITY_REDUCE.equals(priority)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated + " to reduce");
        }
        assigned = assignToReduce(allocated);
      } else if (PRIORITY_MAP.equals(priority)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated + " to map");
        }
        assigned = assignToMap(allocated);
      } else {
        LOG.warn("Container allocated at unwanted priority: " + priority + 
            ". Returning to RM...");
      }
        
      return assigned;
    }
    
    private ContainerRequest getContainerReqToReplace(Container allocated) {
      LOG.info("Finding containerReq for allocated container: " + allocated);
      Priority priority = allocated.getPriority();
      ContainerRequest toBeReplaced = null;
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Replacing FAST_FAIL_MAP container " + allocated.getId());
        Iterator<TaskAttemptId> iter = earlierFailedMaps.iterator();
        while (toBeReplaced == null && iter.hasNext()) {
          toBeReplaced = maps.get(iter.next());
        }
        LOG.info("Found replacement: " + toBeReplaced);
        return toBeReplaced;
      }
      else if (PRIORITY_MAP.equals(priority)) {
        LOG.info("Replacing MAP container " + allocated.getId());
        // allocated container was for a map
        String host = allocated.getNodeId().getHost();
        LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
        if (list != null && list.size() > 0) {
          TaskAttemptId tId = list.removeLast();
          if (maps.containsKey(tId)) {
            toBeReplaced = maps.remove(tId);
          }
        }
        else {
          TaskAttemptId tId = maps.keySet().iterator().next();
          toBeReplaced = maps.remove(tId);          
        }        
      }
      else if (PRIORITY_REDUCE.equals(priority)) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        toBeReplaced = reduces.remove(tId);    
      }
      LOG.info("Found replacement: " + toBeReplaced);
      return toBeReplaced;
    }
    
    
    @SuppressWarnings("unchecked")
    private ContainerRequest assignToFailedMap(Container allocated) {
      //try to assign to earlierFailedMaps if present
      ContainerRequest assigned = null;
      while (assigned == null && earlierFailedMaps.size() > 0) {
        TaskAttemptId tId = earlierFailedMaps.removeFirst();      
        if (maps.containsKey(tId)) {
          assigned = maps.remove(tId);
          JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
          eventHandler.handle(jce);
          LOG.info("Assigned from earlierFailedMaps");
          break;
        }
      }
      return assigned;
    }
    
    private ContainerRequest assignToReduce(Container allocated) {
      ContainerRequest assigned = null;
      //try to assign to reduces if present
      if (assigned == null && reduces.size() > 0) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        assigned = reduces.remove(tId);
        LOG.info("Assigned to reduce");
      }
      return assigned;
    }
    
    @SuppressWarnings("unchecked")
    private ContainerRequest assignToMap(Container allocated) {
    //try to assign to maps if present 
      //first by host, then by rack, followed by *
      ContainerRequest assigned = null;
      while (assigned == null && maps.size() > 0) {
        String host = allocated.getNodeId().getHost();
        LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
        while (list != null && list.size() > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Host matched to the request list " + host);
          }
          TaskAttemptId tId = list.removeFirst();
          if (maps.containsKey(tId)) {
            assigned = maps.remove(tId);
            JobCounterUpdateEvent jce =
              new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
            jce.addCounterUpdate(JobCounter.DATA_LOCAL_MAPS, 1);
            eventHandler.handle(jce);
            hostLocalAssigned++;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Assigned based on host match " + host);
            }
            break;
          }
        }
        if (assigned == null) {
          String rack = RackResolver.resolve(host).getNetworkLocation();
          list = mapsRackMapping.get(rack);
          while (list != null && list.size() > 0) {
            TaskAttemptId tId = list.removeFirst();
            if (maps.containsKey(tId)) {
              assigned = maps.remove(tId);
              JobCounterUpdateEvent jce =
                new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
              jce.addCounterUpdate(JobCounter.RACK_LOCAL_MAPS, 1);
              eventHandler.handle(jce);
              rackLocalAssigned++;
              if (LOG.isDebugEnabled()) {
                LOG.debug("Assigned based on rack match " + rack);
              }
              break;
            }
          }
          if (assigned == null && maps.size() > 0) {
            TaskAttemptId tId = maps.keySet().iterator().next();
            assigned = maps.remove(tId);
            JobCounterUpdateEvent jce =
              new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
            jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
            eventHandler.handle(jce);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Assigned based on * match");
            }
            break;
          }
        }
      }
      return assigned;
    }
  }

  private class AssignedRequests {
    private final Map<ContainerId, TaskAttemptId> containerToAttemptMap =
      new HashMap<ContainerId, TaskAttemptId>();
    private final LinkedHashMap<TaskAttemptId, Container> maps = 
      new LinkedHashMap<TaskAttemptId, Container>();
    private final LinkedHashMap<TaskAttemptId, Container> reduces = 
      new LinkedHashMap<TaskAttemptId, Container>();
    private final Set<TaskAttemptId> preemptionWaitingReduces = 
      new HashSet<TaskAttemptId>();
    
    void add(Container container, TaskAttemptId tId) {
      LOG.info("Assigned container " + container.getId().toString() + " to " + tId);
      containerToAttemptMap.put(container.getId(), tId);
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        maps.put(tId, container);
      } else {
        reduces.put(tId, container);
      }
    }

    @SuppressWarnings("unchecked")
    void preemptReduce(int toPreempt) {
      List<TaskAttemptId> reduceList = new ArrayList<TaskAttemptId>
        (reduces.keySet());
      //sort reduces on progress
      Collections.sort(reduceList,
          new Comparator<TaskAttemptId>() {
        @Override
        public int compare(TaskAttemptId o1, TaskAttemptId o2) {
          float p = getJob().getTask(o1.getTaskId()).getAttempt(o1).getProgress() -
              getJob().getTask(o2.getTaskId()).getAttempt(o2).getProgress();
          return p >= 0 ? 1 : -1;
        }
      });
      
      for (int i = 0; i < toPreempt && reduceList.size() > 0; i++) {
        TaskAttemptId id = reduceList.remove(0);//remove the one on top
        LOG.info("Preempting " + id);
        preemptionWaitingReduces.add(id);
        eventHandler.handle(new TaskAttemptEvent(id, TaskAttemptEventType.TA_KILL));
      }
    }
    
    boolean remove(TaskAttemptId tId) {
      ContainerId containerId = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        containerId = maps.remove(tId).getId();
      } else {
        containerId = reduces.remove(tId).getId();
        if (containerId != null) {
          boolean preempted = preemptionWaitingReduces.remove(tId);
          if (preempted) {
            LOG.info("Reduce preemption successful " + tId);
          }
        }
      }
      
      if (containerId != null) {
        containerToAttemptMap.remove(containerId);
        return true;
      }
      return false;
    }
    
    TaskAttemptId get(ContainerId cId) {
      return containerToAttemptMap.get(cId);
    }
    
    NodeId getNodeId(TaskAttemptId tId) {
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        return maps.get(tId).getNodeId();
      } else {
        return reduces.get(tId).getNodeId();
      }
    }

    ContainerId get(TaskAttemptId tId) {
      Container taskContainer;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        taskContainer = maps.get(tId);
      } else {
        taskContainer = reduces.get(tId);
      }

      if (taskContainer == null) {
        return null;
      } else {
        return taskContainer.getId();
      }
    }
  }
}