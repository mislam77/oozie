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
package org.apache.oozie.command.coord;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.WorkflowJobInsertCommand;
import org.apache.oozie.command.wf.ExternalIdXCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;

public class TestExternalIdXCommand extends XDataTestCase {
    Services services;

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

    public void testExternalId() throws Exception {
        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        _testGetExternalId(job.getId());
    }

    private void _testGetExternalId(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        ExternalIdXCommand extCmd = new ExternalIdXCommand("external-id");
        String ret = extCmd.call();
        assertNotNull(ret);
        assertEquals(jobId, ret);
    }

    @Override
    protected WorkflowJobBean addRecordToWfJobTable(WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                .addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        conf.set(OozieClient.APP_PATH, "testPath");
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(conf);
        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth", jobStatus, instanceStatus);
        wfBean.setExternalId("external-id");

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobInsertCommand wfInsertCmd = new WorkflowJobInsertCommand(wfBean);
            jpaService.execute(wfInsertCmd);
        }
        catch (CommandException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test wf job record to table");
            throw ce;
        }
        return wfBean;
    }

}