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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestCoordJobXCommand extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    public void testCoordJobGet() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING);
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml");
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml");
        addRecordToCoordActionTable(job.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml");
        _testGetCoordJob1(job.getId());
        addRecordToCoordActionTable(job.getId(), 4, CoordinatorAction.Status.WAITING, "coord-action-get.xml");
        _testGetCoordJob2(job.getId());
        _testGetCoordJob3(job.getId());
    }

    private void _testGetCoordJob1(String jobId) throws Exception {
        CoordJobXCommand coordJobGetCmd = new CoordJobXCommand(jobId, 1, 3);
        CoordinatorJobBean ret = coordJobGetCmd.call();
        assertNotNull(ret);
        assertEquals(jobId, ret.getId());
        assertEquals(3, ret.getActions().size());
    }

    private void _testGetCoordJob2(String jobId) throws Exception {
        CoordJobXCommand coordJobGetCmd = new CoordJobXCommand(jobId, 1, 4);
        CoordinatorJobBean ret = coordJobGetCmd.call();
        assertNotNull(ret);
        assertEquals(jobId, ret.getId());
        assertEquals(4, ret.getActions().size());
    }

    private void _testGetCoordJob3(String jobId) throws Exception {
        CoordJobXCommand coordJobGetCmd = new CoordJobXCommand(jobId, 3, 4);
        CoordinatorJobBean ret = coordJobGetCmd.call();
        assertNotNull(ret);
        assertEquals(jobId, ret.getId());
        assertEquals(2, ret.getActions().size());
    }

}
