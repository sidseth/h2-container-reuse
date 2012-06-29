package org.apache.hadoop.mapreduce.v2.app2.job.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.WrappedProgressSplitsBlock;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app2.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BuilderUtils;

public class TaskAttemptImplNew implements TaskAttempt,
    EventHandler<TaskAttemptEvent> {
  
  private static final Log LOG = LogFactory.getLog(TaskAttemptImplNew.class);
  
  static final Counters EMPTY_COUNTERS = new Counters();
  private static final long MEMORY_SPLITS_RESOLUTION = 1024; //TODO Make configurable?
  private final static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  protected final JobConf conf;
  protected final Path jobFile;
  protected final int partition;
  protected EventHandler eventHandler;
  private final TaskAttemptId attemptId;
  private final Clock clock;
  private final org.apache.hadoop.mapred.JobID oldJobId;
//  private final TaskAttemptListener taskAttemptListener;
  private final OutputCommitter committer;
  private final Resource resourceCapability;
  private final String[] dataLocalHosts;
  private final List<String> diagnostics = new ArrayList<String>();
  private final Lock readLock;
  private final Lock writeLock;
  private final AppContext appContext;
  private Credentials credentials;
  private Token<JobTokenIdentifier> jobToken;
//  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
//  private static String initialClasspath = null;
//  private static Object commonContainerSpecLock = new Object();
//  private static ContainerLaunchContext commonContainerSpec = null;
//  private static final Object classpathLock = new Object();
  private long launchTime;
  private long finishTime;
  private WrappedProgressSplitsBlock progressSplitBlock;
  private int shufflePort = -1;
  private String trackerName;
  private int httpPort;

  private static final StateMachineFactory
      <TaskAttemptImplNew, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      stateMachineFactory
      = new StateMachineFactory
      <TaskAttemptImplNew, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      (TaskAttemptState.NEW);
  
  private static boolean stateMachineInited = false;
  private final StateMachine
      <TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent> stateMachine;
  
  private void initStateMachine() {
    stateMachineFactory
        .addTransition(TaskAttemptState.NEW, TaskAttemptState.START_WAIT, TaskAttemptEventType.TA_SCHEDULE, createGenericSingleArcTransition())
        .addTransition(TaskAttemptState.NEW, TaskAttemptState.NEW, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, createGenericSingleArcTransition())
        .addTransition(TaskAttemptState.NEW, TaskAttemptState.FAILED, TaskAttemptEventType.TA_FAIL_REQUEST, createGenericSingleArcTransition())
        .addTransition(TaskAttemptState.NEW, TaskAttemptState.KILLED, TaskAttemptEventType.TA_KILL_REQUEST)
        
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.RUNNING, TaskAttemptEventType.TA_STARTED_REMOTELY, createGenericSingleArcTransition())
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.START_WAIT, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, createGenericSingleArcTransition())
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAIL_REQUEST, createGenericSingleArcTransition())
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptEventType.TA_KILL_REQUEST, createGenericSingleArcTransition())
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.FAILED, TaskAttemptEventType.TA_CONTAINER_COMPLETED, createGenericSingleArcTransition())
        
        
        
        
        .installTopology();
  }
  
  public TaskAttemptImplNew(TaskId taskId, int i, EventHandler eventHandler,
      TaskAttemptListener tal, Path jobFile, int partition, JobConf conf,
      String[] dataLocalHosts, OutputCommitter committer,
      Token<JobTokenIdentifier> jobToken, Credentials credentials, Clock clock,
      AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.oldJobId = TypeConverter.fromYarn(taskId.getJobId());
    this.attemptId = MRBuilderUtils.newTaskAttemptId(taskId, i);
    this.eventHandler = eventHandler;
    //tal
    //Reported status
    this.jobFile = jobFile;
    this.partition = partition;
    this.conf = conf;
    this.dataLocalHosts = dataLocalHosts;
    this.committer = committer;
    this.jobToken = jobToken;
    this.credentials = credentials;
    this.clock = clock;
    this.appContext = appContext;
    this.resourceCapability = BuilderUtils.newResource(getMemoryRequired(conf,
        taskId.getTaskType()));
    synchronized(stateMachineFactory) {
      if (!stateMachineInited) {
        initStateMachine();
      }
    }
    this.stateMachine = stateMachineFactory.make(this);
  }
  
  
  private int getMemoryRequired(Configuration conf, TaskType taskType) {
    int memory = 1024;
    if (taskType == TaskType.MAP)  {
      memory =
          conf.getInt(MRJobConfig.MAP_MEMORY_MB,
              MRJobConfig.DEFAULT_MAP_MEMORY_MB);
    } else if (taskType == TaskType.REDUCE) {
      memory =
          conf.getInt(MRJobConfig.REDUCE_MEMORY_MB,
              MRJobConfig.DEFAULT_REDUCE_MEMORY_MB);
    }
    
    return memory;
  }

  @Override
  public TaskAttemptId getID() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TaskAttemptReport getReport() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getDiagnostics() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Counters getCounters() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public float getProgress() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public TaskAttemptState getState() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isFinished() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ContainerId getAssignedContainerID() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getAssignedContainerMgrAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NodeId getNodeId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getNodeHttpAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getNodeRackName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getLaunchTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getFinishTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getShuffleFinishTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getSortFinishTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getShufflePort() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void handle(TaskAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskAttemptID() + " of type "
          + event.getType());
    }
    writeLock.lock();
    try {
      final TaskAttemptState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for "
            + this.attemptId, e);
        eventHandler.handle(new JobDiagnosticsUpdateEvent(
            this.attemptId.getTaskId().getJobId(), "Invalid event " + event.getType() + 
            " on TaskAttempt " + this.attemptId));
        eventHandler.handle(new JobEvent(this.attemptId.getTaskId().getJobId(),
            JobEventType.INTERNAL_ERROR));
      }
      if (oldState != getState()) {
          LOG.info(attemptId + " TaskAttempt Transitioned from " 
           + oldState + " to "
           + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  protected SingleArcTransition<TaskAttemptImplNew, TaskAttemptEvent> createGenericSingleArcTransition() {
    return null;
  }
  
  protected MultipleArcTransition<TaskAttemptImplNew, TaskAttemptEvent, TaskAttemptState> createGenericMultiArcTransition() {
    return null;
  }

}
