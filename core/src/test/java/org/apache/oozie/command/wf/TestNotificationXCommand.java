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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;

public class TestNotificationXCommand extends XDataTestCase {
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

    public void testNotification1() throws Exception {
        final String wfId = "wfid-W";
        WorkflowJobBean wfBean = createWfBean1();
        wfBean.setId(wfId);

        NotificationXCommand notifyCmd = new NotificationXCommand(wfBean);
        String url = notifyCmd.getUrl();
        assertNotNull(url);
        assertEquals("http://host/wfid-W/PREP", url);
    }

    public void testNotification2() throws Exception {
        final String wfId = "wfid-W";
        WorkflowJobBean wfBean = createWfBean2();
        wfBean.setId(wfId);
        WorkflowActionBean actionBean = createWorkflowAction(wfId, WorkflowAction.Status.OK);

        NotificationXCommand notifyCmd = new NotificationXCommand(wfBean, actionBean);
        String url = notifyCmd.getUrl();
        assertNotNull(url);
        assertEquals("http://host/wfid-W/testAction/T:transition", url);
    }

    public void testNotification3() throws Exception {
        final String wfId = "wfid-W";
        WorkflowJobBean wfBean = createWfBean2();
        wfBean.setId(wfId);
        WorkflowActionBean actionBean = createWorkflowAction(wfId, WorkflowAction.Status.RUNNING);

        NotificationXCommand notifyCmd = new NotificationXCommand(wfBean, actionBean);
        String url = notifyCmd.getUrl();
        assertNotNull(url);
        assertEquals("http://host/wfid-W/testAction/S:RUNNING", url);
    }

    private WorkflowJobBean createWfBean1() throws WorkflowException, Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end")).addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        conf.set(OozieClient.APP_PATH, "testPath");
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        conf.set(OozieClient.WORKFLOW_NOTIFICATION_URL, "http://host/$jobId/$status");
        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth", WorkflowJob.Status.PREP, WorkflowInstance.Status.PREP);
        return wfBean;
    }

    private WorkflowJobBean createWfBean2() throws WorkflowException, Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end")).addNode(new EndNodeDef("end"));
        Configuration conf = new Configuration();
        conf.set(OozieClient.APP_PATH, "testPath");
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, getTestGroup());
        conf.set(OozieClient.ACTION_NOTIFICATION_URL, "http://host/$jobId/$nodeName/$status");
        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth", WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        return wfBean;
    }

}