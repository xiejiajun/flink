/*
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

package org.apache.flink.client.program;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;

/**
 * An {@link ExecutionEnvironment} that never executes a job but only extracts the {@link Pipeline}.
 */
public class OptimizerPlanEnvironment extends ExecutionEnvironment {

	private Pipeline pipeline;

	public Pipeline getPipeline() {
		return pipeline;
	}

	public OptimizerPlanEnvironment(Configuration configuration, ClassLoader userClassloader, int parallelism) {
		super(configuration, userClassloader);
		if (parallelism > 0) {
			setParallelism(parallelism);
		}
	}

	@Override
	public JobClient executeAsync(String jobName) {
		pipeline = createProgramPlan();

		// TODO 提交作业的入口在ExecutionEnvironment / ExecutionEnvironment的executeAsync方法里面
		//   这里只生成StreamGraph然后就抛出异常，保证用户定义的mainClass执行后只生成执行计划，而不真正提交作业
		// do not go on with anything now!
		throw new ProgramAbortException();
	}

	public void setAsContext() {
		// TODO 构建用于返回OptimizerPlanEnvironment的ExecutionEnvironmentFactory
		ExecutionEnvironmentFactory factory = () -> this;
		initializeContextEnvironment(factory);
	}

	public void unsetAsContext() {
		resetContextEnvironment();
	}

}
