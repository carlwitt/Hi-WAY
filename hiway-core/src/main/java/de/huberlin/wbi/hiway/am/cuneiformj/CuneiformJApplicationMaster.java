/*******************************************************************************
 * In the Hi-WAY project we propose a novel approach of executing scientific
 * workflows processing Big Data, as found in NGS applications, on distributed
 * computational infrastructures. The Hi-WAY software stack comprises the func-
 * tional workflow language Cuneiform as well as the Hi-WAY ApplicationMaster
 * for Apache Hadoop 2.x (YARN).
 *
 * List of Contributors:
 *
 * Marc Bux (HU Berlin)
 * Jörgen Brandt (HU Berlin)
 * Hannes Schuh (HU Berlin)
 * Ulf Leser (HU Berlin)
 *
 * Jörgen Brandt is funded by the European Commission through the BiobankCloud
 * project. Marc Bux is funded by the Deutsche Forschungsgemeinschaft through
 * research training group SOAMED (GRK 1651).
 *
 * Copyright 2014 Humboldt-Universität zu Berlin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.huberlin.wbi.hiway.am.cuneiformj;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.json.JSONException;

import de.huberlin.wbi.cuneiform.core.cre.BaseCreActor;
import de.huberlin.wbi.cuneiform.core.invoc.Invocation;
import de.huberlin.wbi.cuneiform.core.repl.BaseRepl;
import de.huberlin.wbi.cuneiform.core.semanticmodel.NotDerivableException;
import de.huberlin.wbi.cuneiform.core.ticketsrc.TicketFailedMsg;
import de.huberlin.wbi.cuneiform.core.ticketsrc.TicketFinishedMsg;
import de.huberlin.wbi.cuneiform.core.ticketsrc.TicketSrcActor;
import de.huberlin.wbi.hiway.am.WorkflowDriver;
import de.huberlin.wbi.hiway.common.Data;
import de.huberlin.wbi.hiway.common.HiWayConfiguration;
import de.huberlin.wbi.hiway.common.TaskInstance;

public class CuneiformJApplicationMaster extends WorkflowDriver {

	public static void main(String[] args) {
		WorkflowDriver.loop(new CuneiformJApplicationMaster(), args);
	}

	private BaseCreActor creActor;
	private BaseRepl repl;

	private final TicketSrcActor ticketSrc;

	public CuneiformJApplicationMaster() {
		super();
		ExecutorService executor = Executors.newCachedThreadPool();

		creActor = new HiWayCreActor(this);
		executor.submit(creActor);

		ticketSrc = new TicketSrcActor(creActor);
		executor.submit(ticketSrc);
		executor.shutdown();
	}

	@Override
	protected Collection<String> getOutput() {
		Collection<String> outputs = new LinkedList<>();
		try {
			for (String output : repl.getAns().normalize()) {
				outputs.add(files.containsKey(output) ? files.get(output).getHdfsPath().toString() : output);
			}
		} catch (NotDerivableException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return outputs;
	}

	@Override
	public Collection<Data> getOutputFiles() {
		Collection<Data> outputFiles = new ArrayList<>();

		try {
			if (repl.hasAns()) {
				Collection<String> output = repl.getAns().normalize();
				for (String fileName : getFiles().keySet()) {
					if (output.contains(fileName)) {
						outputFiles.add(getFiles().get(fileName));
					}
				}
			}
		} catch (NotDerivableException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}

		return outputFiles;
	}

	@Override
	public UUID getRunId() {
		return (ticketSrc.getRunId());
	}

	@Override
	public Collection<TaskInstance> parseWorkflow() {
		System.out.println("Parsing Cuneiform workflow " + getWorkflowFile());
		repl = new HiWayRepl(ticketSrc, this);

		StringBuffer buf = new StringBuffer();

		try (BufferedReader reader = new BufferedReader(new FileReader(getWorkflowFile().getLocalPath().toString()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				buf.append(line).append('\n');
			}
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		repl.interpret(buf.toString());

		return new LinkedList<>();
	}

	@Override
	public void taskFailure(TaskInstance task, ContainerId containerId) {
		super.taskFailure(task, containerId);

		String line, stdOut = "", stdErr = "";
		try {
			StringBuffer buf = new StringBuffer();
			try (BufferedReader reader = new BufferedReader(new FileReader(new File(task.getId() + "_" + Invocation.STDOUT_FILENAME)))) {
				while ((line = reader.readLine()) != null)
					buf.append(line).append('\n');
			}
			stdOut = buf.toString();

			buf = new StringBuffer();
			try (BufferedReader reader = new BufferedReader(new FileReader(new File(task.getId() + "_" + Invocation.STDERR_FILENAME)))) {
				while ((line = reader.readLine()) != null)
					buf.append(line).append('\n');
			}
			stdErr = buf.toString();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}

		Invocation invocation = ((CuneiformJTaskInstance) task).getInvocation();
		if (!task.retry(getConf().getInt(HiWayConfiguration.HIWAY_AM_TASK_RETRIES, HiWayConfiguration.HIWAY_AM_TASK_RETRIES_DEFAULT))) {
			ticketSrc.sendMsg(new TicketFailedMsg(creActor, invocation.getTicket(), null, task.getCommand(), stdOut, stdErr));
		}
	}

	@Override
	public void taskSuccess(TaskInstance task, ContainerId containerId) {
		try {
			Invocation invocation = ((CuneiformJTaskInstance) task).getInvocation();
			invocation.evalReport(task.getReport());
			ticketSrc.sendMsg(new TicketFinishedMsg(creActor, invocation.getTicket(), task.getReport()));

			// set output files
			for (String outputName : invocation.getStageOutList()) {
				if (!getFiles().containsKey(outputName)) {
					Data output = new Data(outputName);
					getFiles().put(outputName, output);
				}
				Data output = getFiles().get(outputName);
				output.setContainerId(containerId.toString());

				task.addOutputData(output);
			}

		} catch (JSONException | NotDerivableException e) {
			System.out.println("Error when attempting to evaluate report of invocation " + task.toString() + ". exiting");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
	}

}
