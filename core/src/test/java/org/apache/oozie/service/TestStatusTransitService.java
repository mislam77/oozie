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
package org.apache.oozie.service;

import java.util.Date;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.BundleActionInsertCommand;
import org.apache.oozie.command.jpa.BundleJobGetCommand;
import org.apache.oozie.command.jpa.BundleJobUpdateCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestStatusTransitService extends XDataTestCase {
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
    
    private BundleActionBean addRecordToBundleActionTable(String jobId, String actionId, int pending) throws Exception {
        BundleActionBean action = createBundleAction(jobId, actionId, pending);
        
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            BundleActionInsertCommand bundleActionInsertCmd = new BundleActionInsertCommand(action);
            jpaService.execute(bundleActionInsertCmd);
        }
        catch (CommandException ex) {
            ex.printStackTrace();
            fail("Unable to insert the test bundle action record to table");
            throw ex;
        }
        
        return action;
    }

    private BundleActionBean createBundleAction(String jobId, String actionId, int pending) throws Exception {
        BundleActionBean action = new BundleActionBean();
        action.setBundleId(jobId);
        action.setBundleActionId(actionId);
        action.setPending(pending);
        action.setCoordId("1");
        action.setCoordName("abc");
        action.setStatus(Job.Status.PAUSED);        
        action.setLastModifiedTime(new Date());
        
        return action;
    }
    
    /**
     * Test : all bundle actions are done - update bundle job's pending flag to 0;
     *
     * @throws Exception
     */
    public void testStatusTransitService1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING);
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        
        job.setPending();
        jpaService.execute(new BundleJobUpdateCommand(job));
        
        final String jobId = job.getId(); 
        addRecordToBundleActionTable(jobId, "1", 0);
        addRecordToBundleActionTable(jobId, "2", 0);
        addRecordToBundleActionTable(jobId, "3", 0);
        
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetCommand(jobId));
                return job1.getPending() == 0;
            }
        });
        
        job = jpaService.execute(new BundleJobGetCommand(jobId));
        assertEquals(0, job.getPending());
    }

    /**
     * Test : none of bundle actions are done - bundle job's pending flag remains 1;
     *
     * @throws Exception
     */
    public void testStatusTransitService2() throws Exception {
        cleanUpDBTables();
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING);        
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        
        job.setPending();
        jpaService.execute(new BundleJobUpdateCommand(job));
        
        final String jobId = job.getId(); 
        addRecordToBundleActionTable(jobId, "1", 1);
        addRecordToBundleActionTable(jobId, "2", 1);
        addRecordToBundleActionTable(jobId, "3", 2);
        
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetCommand(jobId));
                return job1.getPending() == 0;
            }
        });
        
        job = jpaService.execute(new BundleJobGetCommand(jobId));
        assertEquals(job.getPending(), 1);
    }
    
    /**
     * Test : not all bundle actions are done - bundle job's pending flag remains 1;
     *
     * @throws Exception
     */
    public void testStatusTransitService3() throws Exception {
        cleanUpDBTables();
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.RUNNING);        
        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        
        job.setPending();
        jpaService.execute(new BundleJobUpdateCommand(job));
        
        final String jobId = job.getId(); 
        addRecordToBundleActionTable(jobId, "1", 0);
        addRecordToBundleActionTable(jobId, "2", 1);
        addRecordToBundleActionTable(jobId, "3", 2);
        
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                BundleJobBean job1 = jpaService.execute(new BundleJobGetCommand(jobId));
                return job1.getPending() == 0;
            }
        });
        
        job = jpaService.execute(new BundleJobGetCommand(jobId));
        assertEquals(job.getPending(), 1);
    }
}