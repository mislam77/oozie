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
package org.apache.oozie.command.jpa;

import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;

public class TestCoordJPACommands extends XTestCase {
    Services services;
    CoordinatorJobBean coordBean;
    JPAService jpaService;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        cleanUpDB(services.getConf());
        services.init();
        
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            fail("jpaService can not be null");
        }
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordStore() {
        String jobId = "00000-" + new Date().getTime() + "-TestCoordinatorStore-C";
        String actionId = jobId + "_1";
        
        _testInsertJob(jobId);
        _testGetJob(jobId);
        _testUpdateCoordJob(jobId);
        _testInsertAction(jobId, actionId);
        _testGetAction(jobId, actionId);
        _testGetActionForJob(jobId, actionId);
        _testGetActionByExternalId(actionId, actionId + "_E");
        _testGetActionRunningCount(actionId);
        _testUpdateCoordAction(actionId);
    }

    private void _testUpdateCoordAction(String actionId) {
        try {
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetCommand(actionId));
            int newActNum = action.getActionNumber() + 1;
            action.setActionNumber(newActNum);
            jpaService.execute(new CoordActionUpdateCommand(action));
            action = jpaService.execute(new CoordActionGetCommand(actionId));
            assertEquals(newActNum, action.getActionNumber());
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to Update a record in Coord Action. actionId =" + actionId);
        }

    }

    private void _testGetActionRunningCount(String actionId) {
        try {
            int count = jpaService.execute(new CoordJobGetRunningActionsCountCommand(actionId));
            assertEquals(count, 0);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to GET count for action ID. actionId =" + actionId);
        }
    }

    private void _testGetActionByExternalId(String actionId, String extId) {
        try {
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetForExternalIdCommand(extId));
            assertEquals(action.getId(), actionId);
            assertEquals(action.getExternalId(), extId);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to GET a record for COORD ActionBy External ID. actionId =" + actionId + " extID =" + extId);
        }
    }


    private void _testGetActionForJob(String jobId, String actionId) {
        try {
            List<CoordinatorActionBean> actionList = jpaService.execute(new CoordJobGetActionsCommand(jobId));
            assertEquals(actionList.size(), 1);
            CoordinatorActionBean action = actionList.get(0);
            assertEquals(jobId, action.getJobId());
            assertEquals(actionId, action.getId());
            assertEquals(action.getStatus(), CoordinatorAction.Status.READY);
            assertEquals(action.getActionNumber(), 1);
            assertEquals(action.getExternalId(), actionId + "_E");
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Action_FOR_JOB. actionId =" + actionId + " jobId =" + jobId);
        }
    }

    private void _testGetAction(String jobId, String actionId) {
        try {
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetCommand(actionId));
            
            assertEquals(jobId, action.getJobId());
            assertEquals(action.getStatus(), CoordinatorAction.Status.READY);
            assertEquals(action.getActionNumber(), 1);
            assertEquals(action.getExternalId(), actionId + "_E");
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Action. actionId =" + actionId);
        }
    }

    private void _testInsertAction(String jobId, String actionId) {
        createAction(jobId, actionId);
    }

    private CoordinatorActionBean createAction(String jobId, String actionId) {
        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(jobId);
        action.setId(actionId);
        action.setActionNumber(1);
        action.setNominalTime(new Date());
        action.setStatus(Status.READY);
        action.setExternalId(actionId + "_E");
        action.setLastModifiedTime(new Date(new Date().getTime() - 1200000));
        try {
            jpaService.execute(new CoordActionInsertCommand(action));
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to insert a record into COORD Action ");
        }
        return action;
    }

    private void _testUpdateCoordJob(String jobId) {
        try {
            CoordinatorJobBean job = jpaService.execute(new CoordJobGetCommand(jobId));
            int newFreq = job.getFrequency() + 1;
            job.setFrequency(newFreq);
            jpaService.execute(new CoordJobUpdateCommand(job));
            job = jpaService.execute(new CoordJobGetCommand(jobId));
            assertEquals(newFreq, job.getFrequency());
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to UPDATE a record for COORD Job. jobId =" + jobId);
        }
    }

    private void _testGetJob(String jobId) {
        try {
            CoordinatorJobBean job = jpaService.execute(new CoordJobGetCommand(jobId));
            assertEquals(jobId, job.getId());
            assertEquals(job.getStatus(), CoordinatorJob.Status.PREP);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Job. jobId =" + jobId);
        }
    }

    private void _testInsertJob(String jobId) {
        try {
            CoordinatorJobBean job = createCoordJob(jobId);
            jpaService.execute(new CoordJobInsertCommand(job));
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Unable to insert a record into COORD Job ");
        }
    }

    private CoordinatorJobBean createCoordJob(String jobId) {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();

        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREP);
        coordJob.setCreatedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' name='NAME' frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:latest(0)}</instance>";
        appXml += "</data-in>";
        appXml += "</input-events>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:current(-1)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "<configuration>";
        appXml += "<property>";
        appXml += "<name>inputA</name>";
        appXml += "<value>${coord:dataIn('A')}</value>";
        appXml += "</property>";
        appXml += "<property>";
        appXml += "<name>inputB</name>";
        appXml += "<value>${coord:dataOut('LOCAL_A')}</value>";
        appXml += "</property>";
        appXml += "</configuration>";
        appXml += "</workflow>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        Date curr = new Date();
        coordJob.setNextMaterializedTime(curr);
        coordJob.setLastModifiedTime(curr);
        coordJob.setEndTime(new Date(curr.getTime() + 86400000));
        coordJob.setStartTime(new Date(curr.getTime() - 86400000));
        return coordJob;
    }
    
}
