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

import java.util.Date;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.command.jpa.WorkflowActionGetCommand;
import org.apache.oozie.command.jpa.WorkflowJobGetCommand;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestSignalXCommand extends XDataTestCase {
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
     * Test : verify the PreconditionException is thrown when job=KILLED and action=NULL
     *
     * @throws Exception
     */
    public void testSignalPreCondition1() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED);

        assertNull(inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP));
        SignalXCommand signal = new SignalXCommand("signal", 1, job.getId());
        signal.call();

        //check counter for preconditionfailed
        Long counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(signal.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));

    }


    /**
     * Test : verify the PreconditionException is thrown when job=KILLED and action=OK and pending=true
     *
     * @throws Exception
     */
    public void testSignalPreCondition2() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.KILLED, WorkflowInstance.Status.KILLED);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.OK);

        assertNull(inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP));
        SignalXCommand signal = new SignalXCommand(job.getId(), action.getId());
        signal.call();

        //check counter for preconditionfailed
        Long counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(signal.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    /**
     * Test : verify the PreconditionException is thrown when job=RUNNING and action=RUNNING
     *
     * @throws Exception
     */
    public void testSignalPreCondition3() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.RUNNING);

        assertNull(inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP));
        SignalXCommand signal = new SignalXCommand(job.getId(), action.getId());
        signal.call();

        //check counter for preconditionfailed
        Long counterVal = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(signal.getName() + ".preconditionfailed").getValue();
        assertEquals(new Long(1), new Long(counterVal));
    }

    /**
     * Test : workflow job is PREP and action is null. The job ends successfully.
     *
     * @throws Exception
     */
    public void testSignalCall1() throws Exception {
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);

        new SignalXCommand("signal", 1, job.getId()).call();

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetCommand wfJobGetCmd = new WorkflowJobGetCommand(job.getId());

        job = jpaService.execute(wfJobGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.SUCCEEDED);
    }

    /**
     * Test : workflow job is PREP and action is OK. The job is failed because of invalid execution path.
     *
     * @throws Exception
     */
    public void testSignalCall2() throws Exception {
        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = this.addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.OK);

        new SignalXCommand(job.getId(), action.getId()).call();

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        WorkflowJobGetCommand wfJobGetCmd = new WorkflowJobGetCommand(job.getId());
        WorkflowActionGetCommand wfActionGetCmd = new WorkflowActionGetCommand(action.getId());

        job = jpaService.execute(wfJobGetCmd);
        action = jpaService.execute(wfActionGetCmd);
        assertEquals(job.getStatus(), WorkflowJob.Status.FAILED);
        assertEquals(action.getStatus(), WorkflowAction.Status.OK);
        assertEquals(action.getPending(), false);
    }

    @Override
    protected WorkflowActionBean createWorkflowAction(String wfId, String actionName, WorkflowAction.Status status) throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionName));
        action.setJobId(wfId);
        action.setName(actionName);
        action.setType("map-reduce");
        action.setTransition("transition");
        action.setStatus(status);
        action.setStartTime(new Date());
        action.setEndTime(new Date());
        action.setLastCheckTime(new Date());
        action.setPending();
        action.setExecutionPath("executionPath");
        action.setSignalValue("signalValue");

        String actionXml = "<map-reduce>" +
        "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
        "<name-node>" + getNameNodeUri() + "</name-node>" +
        "<configuration>" +
        "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.reducer.class</name><value>" + MapperReducerForTest.class.getName() +
        "</value></property>" +
        "<property><name>mapred.input.dir</name><value>inputDir</value></property>" +
        "<property><name>mapred.output.dir</name><value>outputDir</value></property>" +
        "</configuration>" +
        "</map-reduce>";
        action.setConf(actionXml);

        return action;
    }

}
