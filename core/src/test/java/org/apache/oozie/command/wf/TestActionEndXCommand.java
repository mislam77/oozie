/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.wf;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.LauncherMapper;
import org.apache.oozie.action.hadoop.MapReduceActionExecutor;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.command.jpa.WorkflowActionGetCommand;
import org.apache.oozie.command.jpa.WorkflowActionInsertCommand;
import org.apache.oozie.command.wf.WorkflowActionXCommand.ActionExecutorContext;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestActionEndXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test : verify the PreconditionException is thrown when pending = true and action = DONE and job != RUNNING
     *
     * @throws Exception
     */
    public void testActionEndPreCondition1() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), WorkflowAction.Status.DONE);

        assertNull(inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP));
        ActionEndXCommand endCmd = new ActionEndXCommand(action.getId(), "map-reduce");
        endCmd.call();

        //precondition failed because of pending = true and action = DONE and job != RUNNING
        Long counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(endCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    /**
     * Test : verify the PreconditionException is thrown when pending = true and action = END_RETRY and job != RUNNING
     *
     * @throws Exception
     */
    public void testActionEndPreCondition2() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), WorkflowAction.Status.END_RETRY);

        assertNull(inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP));
        ActionEndXCommand endCmd = new ActionEndXCommand(action.getId(), "map-reduce");
        endCmd.call();

        //precondition failed because of pending = true and action = END_RETRY and job != RUNNING
        Long counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(endCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    /**
     * Test : verify the PreconditionException is thrown when pending = false
     *
     * @throws Exception
     */
    public void testActionEndPreCondition3() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = super.addRecordToWfActionTable(job.getId(), WorkflowAction.Status.DONE);
        assertFalse(action.getPending());

        assertNull(inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP));
        ActionEndXCommand endCmd = new ActionEndXCommand(action.getId(), "map-reduce");
        endCmd.call();

        //precondition failed because of pending = false
        Long counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(endCmd.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    @SuppressWarnings("deprecation")
    public void testActionEnd() throws Exception {
        final JPAService jpaService = Services.get().get(JPAService.class);
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), WorkflowAction.Status.PREP);
        final WorkflowActionGetCommand wfActionGetCmd = new WorkflowActionGetCommand(action.getId());

        new ActionStartXCommand(action.getId(), "map-reduce").call();
        action = jpaService.execute(wfActionGetCmd);

        ActionExecutorContext context = new WorkflowActionXCommand.ActionExecutorContext(job, action, false);
        MapReduceActionExecutor actionExecutor = new MapReduceActionExecutor();
        Configuration conf = actionExecutor.createBaseHadoopConf(context, XmlUtils.parseXml(action.getConf()));
        String user = conf.get("user.name");
        String group = conf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, group, new JobConf(conf));

        String launcherId = action.getExternalId();

        final RunningJob launcherJob = jobClient.getJob(JobID.forName(launcherId));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());
        assertTrue(LauncherMapper.hasIdSwap(launcherJob));

        new ActionCheckXCommand(action.getId()).call();
        action = jpaService.execute(wfActionGetCmd);
        String mapperId = action.getExternalId();

        assertFalse(launcherId.equals(mapperId));

        final RunningJob mrJob = jobClient.getJob(JobID.forName(mapperId));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertTrue(mrJob.isSuccessful());

        new ActionCheckXCommand(action.getId()).call();
        action = jpaService.execute(wfActionGetCmd);

        assertEquals("SUCCEEDED", action.getExternalStatus());

        new ActionEndXCommand(action.getId(), "map-reduce").call();
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowActionBean action = jpaService.execute(wfActionGetCmd);
                return action.isComplete();
            }
        });

        action = jpaService.execute(wfActionGetCmd);
        assertEquals(WorkflowAction.Status.OK, action.getStatus());
    }

    @Override
    protected WorkflowActionBean addRecordToWfActionTable(String wfId, WorkflowAction.Status status) throws Exception {
        WorkflowActionBean action = createWorkflowActionSetPending(wfId, status);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowActionInsertCommand actionInsertCmd = new WorkflowActionInsertCommand(action);
            jpaService.execute(actionInsertCmd);
        }
        catch (CommandException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf action record to table");
            throw ce;
        }
        return action;
    }

    protected WorkflowActionBean createWorkflowActionSetPending(String wfId, WorkflowAction.Status status) throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        String actionname = "testAction";
        action.setName(actionname);
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionname));
        action.setJobId(wfId);
        action.setType("map-reduce");
        action.setTransition("transition");
        action.setStatus(status);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        action.setPending();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        FileSystem fs = getFileSystem();
        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" +
        "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
        "<name-node>" + getNameNodeUri() + "</name-node>" +
        "<configuration>" +
        "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.reducer.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.input.dir</name><value>"+inputDir.toString()+"</value></property>" +
        "<property><name>mapred.output.dir</name><value>"+outputDir.toString()+"</value></property>" +
        "</configuration>" +
        "</map-reduce>";
        action.setConf(actionXml);

        return action;
    }

}