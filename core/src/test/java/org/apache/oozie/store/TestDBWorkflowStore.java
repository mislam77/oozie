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
package org.apache.oozie.store;

import static org.apache.oozie.store.OozieSchema.OozieColumn.ACTIONS_id;
import static org.apache.oozie.store.OozieSchema.OozieColumn.ACTIONS_wfId;
import static org.apache.oozie.store.OozieSchema.OozieColumn.WF_id;
import static org.apache.oozie.util.db.SqlStatement.getCount;
import static org.apache.oozie.util.db.SqlStatement.isEqual;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.Schema;
import org.apache.oozie.util.db.Schema.DBType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.store.OozieSchema.OozieTable;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.apache.oozie.service.DataSourceService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.db.SqlStatement;
import org.apache.oozie.util.XmlUtils;

public class TestDBWorkflowStore extends XTestCase {
    Connection conn;
    WorkflowLib wfLib;
    WorkflowStore store;
    WorkflowJobBean wfBean1;
    WorkflowJobBean wfBean2;
    String dbName;
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        cleanUpDB(services.getConf());
        services.init();
        store = Services.get().get(WorkflowStoreService.class).create();
        conn = Services.get().get(DataSourceService.class).getRawConnection();
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Override
    protected void tearDown() throws Exception {
        dropSchema(dbName, conn);
        services.destroy();
        super.tearDown();
    }

    public void testDBWorkflowStore() throws Exception {
        _testInsertWF();
        _testGetWF();
        _testUpdateWF();
        _testGetStatusCount();
        _testWaitWriteLock();
        _testGetWFIDWithExtID();
        _testSaveAction();
        _testLoadAction();
        _testUpdateAction();
        _testDeleteAction();
        _testGetActionsForWF();
        _testGetPendingActions();
        _testGetWFInfo();
        _testGetWFInfos();
        _testPurge();
    }

    private WorkflowJobBean createWorkflow(WorkflowApp app, Configuration conf, String authToken) throws Exception {
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        Configuration protoActionConf = wps.createProtoActionConf(conf, authToken);
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        WorkflowInstance wfInstance;
        wfInstance = workflowLib.createInstance(app, conf);
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setId(wfInstance.getId());
        workflow.setAppName(app.getName());
        workflow.setAppPath(conf.get(OozieClient.APP_PATH));
        workflow.setConf(XmlUtils.prettyPrint(conf).toString());
        workflow.setProtoActionConf(XmlUtils.prettyPrint(protoActionConf).toString());
        workflow.setCreatedTime(new Date());
        workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
        workflow.setStatus(WorkflowJob.Status.PREP);
        workflow.setRun(0);
        workflow.setUser(conf.get(OozieClient.USER_NAME));
        workflow.setGroup(conf.get(OozieClient.GROUP_NAME));
        workflow.setAuthToken(authToken);
        workflow.setWorkflowInstance(wfInstance);
        return workflow;
    }

    private void _testInsertWF() throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                .addNode(new EndNodeDef("end"));
        Configuration conf1 = new Configuration();

        conf1.set(OozieClient.APP_PATH, "testPath");
        conf1.set(OozieClient.LOG_TOKEN, "testToken");
        conf1.set(OozieClient.USER_NAME, "testUser1");
        conf1.set(OozieClient.GROUP_NAME, "testGroup1");
        conf1.set(WorkflowAppService.HADOOP_JT_KERBEROS_NAME, "JT");
        conf1.set(WorkflowAppService.HADOOP_NN_KERBEROS_NAME, "NN");
        wfBean1 = createWorkflow(app, conf1, "auth");

        Configuration conf2 = new Configuration();
        conf2.set(OozieClient.APP_PATH, "testPath");
        conf2.set(OozieClient.LOG_TOKEN, "testToken");
        conf2.set(OozieClient.USER_NAME, "testUser2");
        conf2.set(OozieClient.GROUP_NAME, "testGroup2");
        conf2.set(WorkflowAppService.HADOOP_JT_KERBEROS_NAME, "JT");
        conf2.set(WorkflowAppService.HADOOP_NN_KERBEROS_NAME, "NN");
        wfBean2 = createWorkflow(app, conf2, "auth");

        store.insertWorkflow(wfBean1);
        store.insertWorkflow(wfBean2);
        store.commit();

        SqlStatement s = getCount(OozieTable.WORKFLOWS);
        ResultSet rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs.close();

        s = getCount(OozieTable.WORKFLOWS).where(isEqual(WF_id, wfBean1.getId()));
        rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();

        s = getCount(OozieTable.WORKFLOWS).where(isEqual(WF_id, wfBean2.getId()));
        rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();

    }

    private void _testGetWF() throws StoreException {
        WorkflowJobBean wfBean = store.getWorkflow(wfBean1.getId(), false);
        assertEquals(wfBean.getId(), wfBean1.getId());
        assertEquals(wfBean.getStatus(), WorkflowJob.Status.PREP);
        assertEquals(wfBean.getWorkflowInstance().getId(), wfBean1.getId());
    }

    private void _testUpdateWF() throws StoreException {
        wfBean1.setStatus(WorkflowJob.Status.SUCCEEDED);
        wfBean1.getWorkflowInstance().setVar("test", "hello");
        wfBean1.setExternalId("testExtId");
        store.getWorkflow(wfBean1.getId(), true);
        store.updateWorkflow(wfBean1);
        store.commit();
        WorkflowJobBean wfBean = store.getWorkflow(wfBean1.getId(), false);
        assertEquals("hello", wfBean.getWorkflowInstance().getVar("test"));
        assertEquals(wfBean.getStatus(), WorkflowJob.Status.SUCCEEDED);
    }

    private void _testGetStatusCount() throws StoreException, InterruptedException {
        assertEquals(1, store.getWorkflowCountWithStatus(WorkflowJob.Status.PREP.name()));
        assertEquals(1, store.getWorkflowCountWithStatus(WorkflowJob.Status.SUCCEEDED.name()));
        assertEquals(1, store.getWorkflowCountWithStatusInLastNSeconds(WorkflowJob.Status.PREP.name(), 5));
        assertEquals(1, store.getWorkflowCountWithStatusInLastNSeconds(WorkflowJob.Status.SUCCEEDED.name(), 5));
        Thread.sleep(1000);
        long t1 = System.currentTimeMillis();
        WorkflowJobBean wfBean = store.getWorkflow(wfBean2.getId(), true);
        store.updateWorkflow(wfBean);
        store.commit();
        long t2 = System.currentTimeMillis();
        int s = (int)((t2 - t1)/1000);
        if(s < 1) {
            s = 1;
        }
        assertEquals(1, store.getWorkflowCountWithStatusInLastNSeconds(WorkflowJob.Status.PREP.name(), s));
        assertEquals(0, store.getWorkflowCountWithStatusInLastNSeconds(WorkflowJob.Status.SUCCEEDED.name(), s));
    }

    public class Locker implements Runnable {
        protected String id;
        private String nameIndex;
        private StringBuffer sb;
        protected long timeout;

        public Locker(String id, String nameIndex, StringBuffer buffer) {
            this.id = id;
            this.nameIndex = id + ":" + nameIndex;
            this.sb = buffer;
        }

        public void run() {
            XLog log = XLog.getLog(getClass());
            try {
                WorkflowStore store = Services.get().get(WorkflowStoreService.class).create();
                log.info("Get [{0}]", nameIndex);
                store.getWorkflow(id, true);
                log.info("Got [{0}]", nameIndex);
                sb.append(nameIndex + "-L ");
                synchronized (this) {
                    wait();
                }
                sb.append(nameIndex + "-U ");
                store.close();
                log.info("Release [{0}]", nameIndex);
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public void finish() {
            synchronized (this) {
                notify();
            }
        }
    }

    public void _testWaitWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        String id = wfBean1.getId();
        Locker l1 = new Locker(id, "1", sb);
        Locker l2 = new Locker(id, "2", sb);

        new Thread(l1).start();
        Thread.sleep(300);
        new Thread(l2).start();
        Thread.sleep(300);
        l1.finish();
        Thread.sleep(1000);
        l2.finish();
        Thread.sleep(1000);
        assertEquals(id + ":1-L " + id + ":1-U " + id + ":2-L " + id + ":2-U", sb.toString().trim());
    }

    private void _testGetWFIDWithExtID() throws StoreException {
        String id = store.getWorkflowIdForExternalId("testExtId");
        assertEquals(wfBean1.getId(), id);
    }

    private void _testSaveAction() throws StoreException, SQLException {
        WorkflowActionBean a11 = new WorkflowActionBean();
        a11.setId("11");
        a11.setJobId(wfBean1.getId());
        a11.setStatus(WorkflowAction.Status.PREP);

        WorkflowActionBean a12 = new WorkflowActionBean();
        a12.setId("12");
        a12.setJobId(wfBean1.getId());
        a12.setStatus(WorkflowAction.Status.PREP);

        WorkflowActionBean a21 = new WorkflowActionBean();
        a21.setId("21");
        a21.setJobId(wfBean2.getId());
        a21.setStatus(WorkflowAction.Status.PREP);

        WorkflowActionBean a22 = new WorkflowActionBean();
        a22.setId("22");
        a22.setJobId(wfBean2.getId());
        a22.setStatus(WorkflowAction.Status.PREP);

        store.insertAction(a11);
        store.insertAction(a12);
        store.insertAction(a21);
        store.insertAction(a22);
        store.commit();

        SqlStatement s = getCount(OozieTable.ACTIONS);
        ResultSet rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(4, rs.getInt(1));
        rs.close();

        s = getCount(OozieTable.ACTIONS).where(isEqual(ACTIONS_wfId, wfBean1.getId()));
        rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs.close();

        s = getCount(OozieTable.ACTIONS).where(isEqual(ACTIONS_wfId, wfBean2.getId()));
        rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs.close();

        s = getCount(OozieTable.ACTIONS).where(isEqual(ACTIONS_id, "11"));
        rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();

        s = getCount(OozieTable.ACTIONS).where(isEqual(ACTIONS_id, "12"));
        rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();
    }

    private void _testLoadAction() throws StoreException {
        WorkflowActionBean a11 = store.getAction("11", false);
        assertEquals(a11.getId(), "11");
        assertEquals(a11.getJobId(), wfBean1.getId());
        assertEquals(a11.getStatus(), WorkflowAction.Status.PREP);
    }

    private void _testUpdateAction() throws StoreException {
        WorkflowActionBean a11 = store.getAction("11", false);
        a11.setStatus(WorkflowAction.Status.OK);
        a11.setPending();
        a11.setPendingAge(new Date(System.currentTimeMillis() - 10000));
        store.updateAction(a11);
        store.commit();
        WorkflowActionBean a = store.getAction(a11.getId(), false);
        assertEquals(a.getId(), a11.getId());
        assertEquals(a.getStatus(), WorkflowAction.Status.OK);
    }

    private void _testDeleteAction() throws StoreException {
        store.deleteAction("12");
        store.commit();
        boolean actionDeleted = false;
        try {
            store.getAction("12", false);
        }
        catch (StoreException e) {
            if (ErrorCode.E0605.equals(e.getErrorCode())) {
                actionDeleted = true;
            }
        }
        assertEquals(true, actionDeleted);
    }

    private void _testGetActionsForWF() throws StoreException {
        List<WorkflowActionBean> actions1 = store.getActionsForWorkflow(wfBean1.getId(), false);
        assertEquals(actions1.size(), 1);
        List<WorkflowActionBean> actions2 = store.getActionsForWorkflow(wfBean2.getId(), false);
        assertEquals(actions2.size(), 2);
    }

    private void _testGetPendingActions() throws StoreException {
        List<WorkflowActionBean> pActions = store.getPendingActions(5);
        assertEquals(1, pActions.size());
        assertEquals("11", pActions.get(0).getId());
    }

    private void _testGetWFInfo() throws StoreException {
        WorkflowJobBean wfBean = store.getWorkflowInfo(wfBean1.getId());
        assertEquals(wfBean.getId(), wfBean1.getId());
        assertEquals(wfBean.getStatus(), wfBean1.getStatus());
        assertEquals(wfBean.getActions().size(), 1);
        assertEquals(wfBean.getActions().get(0).getId(), "11");
    }

    private void _testGetWFInfos() throws StoreException {
        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        WorkflowsInfo wfInfo = store.getWorkflowsInfo(filter, 1, 1);
        List<WorkflowJobBean> wfBeans = wfInfo.getWorkflows();

        assertEquals(1, wfBeans.size());

        filter = new HashMap<String, List<String>>();
        wfInfo = store.getWorkflowsInfo(filter, 1, 2);
        wfBeans = wfInfo.getWorkflows();
        assertEquals(2, wfBeans.size());

        filter = new HashMap<String, List<String>>();
        filter.put("user", Arrays.asList("testUser1"));
        wfInfo = store.getWorkflowsInfo(filter, 1, 2);
        wfBeans = wfInfo.getWorkflows();
        assertEquals(1, wfBeans.size());

        filter = new HashMap<String, List<String>>();
        filter.put("user", Arrays.asList("testUser1", "testUser2"));
        wfInfo = store.getWorkflowsInfo(filter, 1, 2);
        wfBeans = wfInfo.getWorkflows();
        assertEquals(2, wfBeans.size());

        filter = new HashMap<String, List<String>>();
        filter.put("user", Arrays.asList("testUser1"));
        filter.put("status", Arrays.asList("succeeded"));

        wfInfo = store.getWorkflowsInfo(filter, 1, 2);
        wfBeans = wfInfo.getWorkflows();
        assertEquals(1, wfBeans.size());

        filter = new HashMap<String, List<String>>();
        filter.put("user", Arrays.asList("testUser1", "testUser2"));
        filter.put("name", Arrays.asList("testApp"));
        wfInfo = store.getWorkflowsInfo(filter, 1, 2);
        wfBeans = wfInfo.getWorkflows();
        assertEquals(2, wfBeans.size());
        assertEquals(2, wfInfo.getTotal());
        assertEquals(1, wfInfo.getStart());
        assertEquals(2, wfInfo.getLen());

        filter = new HashMap<String, List<String>>();
        filter.put("user", Arrays.asList("testUser1", "testUser2"));
        filter.put("name", Arrays.asList("testApp"));
        wfInfo = store.getWorkflowsInfo(filter, 1, 1);
        wfBeans = wfInfo.getWorkflows();
        assertEquals(1, wfBeans.size());
        assertEquals(2, wfInfo.getTotal());
        assertEquals(1, wfInfo.getStart());
        assertEquals(1, wfInfo.getLen());
    }

    private void _testPurge() throws Exception {
        wfBean1.setEndTime(new Date(System.currentTimeMillis() - (31 * 24 * 60 * 60 * 1000l)));
        wfBean2.setEndTime(new Date(System.currentTimeMillis() - (31 * 24 * 60 * 60 * 1000l)));
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef("end"))
                .addNode(new EndNodeDef("end"));
        Configuration conf2 = new Configuration();
        conf2.set(OozieClient.APP_PATH, "testPath");
        conf2.set(OozieClient.LOG_TOKEN, "testToken");
        conf2.set(OozieClient.USER_NAME, "testUser2");
        conf2.set(OozieClient.GROUP_NAME, "testGroup2");
        conf2.set(WorkflowAppService.HADOOP_JT_KERBEROS_NAME, "JT");
        conf2.set(WorkflowAppService.HADOOP_NN_KERBEROS_NAME, "NN");
        WorkflowJobBean wfBean3 = createWorkflow(app, conf2, "auth");

        store.insertWorkflow(wfBean3);
        WorkflowActionBean a31 = new WorkflowActionBean();
        a31.setId("31");
        a31.setJobId(wfBean3.getId());
        a31.setStatus(WorkflowAction.Status.PREP);
        store.insertAction(a31);
        store.updateWorkflow(wfBean1);
        store.updateWorkflow(wfBean2);
        store.commit();

        SqlStatement s = getCount(OozieTable.WORKFLOWS);
        ResultSet rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(3, rs.getInt(1));
        rs.close();

        s = getCount(OozieTable.ACTIONS);
        rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(4, rs.getInt(1));
        rs.close();

        store.purge(30);
        store.commit();

        s = getCount(OozieTable.WORKFLOWS);
        rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();

        WorkflowJobBean tmp = store.getWorkflow(wfBean3.getId(), false);
        assertEquals(tmp.getId(), wfBean3.getId());

        s = getCount(OozieTable.ACTIONS);
        rs = s.prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();

        WorkflowActionBean tmpa = store.getAction("31", false);
        assertEquals("31", tmpa.getId());
    }

    private static void dropSchema(String dbName, Connection conn) {
        try {
            DBType type = DBType.MySQL;
            if (Schema.isHsqlConnection(conn)) {
                type = DBType.HSQL;
            }
            conn.prepareStatement(
                    "DROP " + (type.equals(DBType.MySQL) ? "DATABASE " : "SCHEMA ") + dbName
                            + (type.equals(DBType.HSQL) ? " CASCADE" : "")).execute();
        }
        catch (SQLException e) {

        }
    }
}