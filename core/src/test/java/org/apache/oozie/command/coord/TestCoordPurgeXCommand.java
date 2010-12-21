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

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.CoordActionGetCommand;
import org.apache.oozie.command.jpa.CoordJobGetCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XmlUtils;

public class TestCoordPurgeXCommand extends XDataTestCase {
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
     * Test : purge succeeded coord job and action successfully
     *
     * @throws Exception
     */
    public void testSucCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetCommand coordJobGetCmd = new CoordJobGetCommand(job.getId());
        CoordActionGetCommand coordActionGetCmd = new CoordActionGetCommand(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.SUCCEEDED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new CoordPurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(coordJobGetCmd);
            fail("Job should be purged. Should fail.");
        }
        catch (CommandException ce) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(coordActionGetCmd);
            fail("Action should be purged. Should fail.");
        }
        catch (CommandException ce) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge failed coord job and action successfully
     *
     * @throws Exception
     */
    public void testFailCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.FAILED);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.FAILED,
                "coord-action-get.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetCommand coordJobGetCmd = new CoordJobGetCommand(job.getId());
        CoordActionGetCommand coordActionGetCmd = new CoordActionGetCommand(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.FAILED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.FAILED);

        new CoordPurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(coordJobGetCmd);
            fail("Job should be purged. Should fail.");
        }
        catch (CommandException ce) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(coordActionGetCmd);
            fail("Action should be purged. Should fail.");
        }
        catch (CommandException ce) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge killed coord job and action successfully
     *
     * @throws Exception
     */
    public void testKillCoordPurgeXCommand() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.KILLED);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.KILLED,
                "coord-action-get.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetCommand coordJobGetCmd = new CoordJobGetCommand(job.getId());
        CoordActionGetCommand coordActionGetCmd = new CoordActionGetCommand(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.KILLED);
        assertEquals(action.getStatus(), CoordinatorAction.Status.KILLED);

        new CoordPurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(coordJobGetCmd);
            fail("Job should be purged. Should fail.");
        }
        catch (CommandException ce) {
            // Job doesn't exist. Exception is expected.
        }

        try {
            jpaService.execute(coordActionGetCmd);
            fail("Action should be purged. Should fail.");
        }
        catch (CommandException ce) {
            // Job doesn't exist. Exception is expected.
        }

    }

    /**
     * Test : purge coord job and action failed
     *
     * @throws Exception
     */
    public void testCoordPurgeXCommandFailed() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING);
        CoordinatorActionBean action = addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED,
                "coord-action-get.xml");

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetCommand coordJobGetCmd = new CoordJobGetCommand(job.getId());
        CoordActionGetCommand coordActionGetCmd = new CoordActionGetCommand(action.getId());

        job = jpaService.execute(coordJobGetCmd);
        action = jpaService.execute(coordActionGetCmd);
        assertEquals(job.getStatus(), CoordinatorJob.Status.RUNNING);
        assertEquals(action.getStatus(), CoordinatorAction.Status.SUCCEEDED);

        new CoordPurgeXCommand(7, 10).call();

        try {
            job = jpaService.execute(coordJobGetCmd);
        }
        catch (CommandException ce) {
            fail("Job should not be purged. Should fail.");
        }

        try {
            jpaService.execute(coordActionGetCmd);
        }
        catch (CommandException ce) {
            fail("Action should not be purged. Should fail.");
        }

    }

    @Override
    protected CoordinatorJobBean createCoordJob(CoordinatorJob.Status status) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = writeCoordXml(appPath);

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.COORDINATOR));
        coordJob.setAppName("COORD-TEST");
        coordJob.setAppPath(appPath.toString());
        coordJob.setStatus(status);
        coordJob.setTimeZone("America/Los_Angeles");
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(DateUtils.parseDateUTC("2009-12-18T01:00Z"));
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());
        coordJob.setAuthToken("notoken");

        Configuration conf = getCoordConf(appPath);
        coordJob.setConf(XmlUtils.prettyPrint(conf).toString());
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setTimeUnit(Timeunit.DAY);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setStartTime(DateUtils.parseDateUTC("2009-12-15T01:00Z"));
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-12-17T01:00Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }
        return coordJob;
    }

}
