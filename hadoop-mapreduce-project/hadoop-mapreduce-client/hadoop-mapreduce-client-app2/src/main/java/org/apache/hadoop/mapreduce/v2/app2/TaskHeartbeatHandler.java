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

package org.apache.hadoop.mapreduce.v2.app2;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.HeartbeatHandlerBase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventType;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;


/**
 * This class keeps track of tasks that have already been launched. It
 * determines if a task is alive and running or marks a task as dead if it does
 * not hear from it for a long time.
 * 
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TaskHeartbeatHandler extends HeartbeatHandlerBase<TaskAttemptId> {
  
	public TaskHeartbeatHandler(AppContext context,
			int numThreads) {
        super(context, numThreads, "TaskHeartbeatHandler");
	}

	@Override
	protected int getConfiguredTimeout(Configuration conf) {
		return conf.getInt(MRJobConfig.TASK_TIMEOUT, 5 * 60 * 1000);
	}

	@Override
	protected int getConfiguredTimeoutCheckInterval(Configuration conf) {
		return conf.getInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 30 * 1000);
	}

	@Override
	public boolean hasTimedOut(
			org.apache.hadoop.mapreduce.jobhistory.HeartbeatHandlerBase.ReportTime report,
			long currentTime) {
		return (timeOut > 0) && (currentTime > report.getLastPing() + timeOut);
	}

	@Override
	public void handleTimeOut(TaskAttemptId attemptId) {
		// TODO Auto-generated method stub
	    eventHandler.handle(new TaskAttemptEvent(attemptId,
	            TaskAttemptEventType.TA_TIMED_OUT));
	}

}
