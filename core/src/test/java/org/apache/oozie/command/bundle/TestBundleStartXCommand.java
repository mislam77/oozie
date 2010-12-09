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
package org.apache.oozie.command.bundle;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.jpa.BundleActionsGetCommand;
import org.apache.oozie.command.jpa.BundleJobGetCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XConfiguration;

public class TestBundleStartXCommand extends XDataTestCase {

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
     * Test : Start bundle job
     *
     * @throws Exception
     */
    public void testBundleStart1() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetCommand bundleJobGetCmd = new BundleJobGetCommand(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.PREP);

        new BundleStartXCommand(job.getId()).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.RUNNING);

        BundleActionsGetCommand bundleActionsGetCmd = new BundleActionsGetCommand(job.getId());
        List<BundleActionBean> actions = jpaService.execute(bundleActionsGetCmd);

        assertEquals(2, actions.size());
        assertEquals(Job.Status.PREP, actions.get(0).getStatus());
        assertEquals(true, actions.get(0).isPending());
        assertEquals(true, actions.get(0).isCritical());
        assertEquals(Job.Status.PREP, actions.get(1).getStatus());
        assertEquals(true, actions.get(1).isPending());
        assertEquals(false, actions.get(1).isCritical());
    }

    /**
     * Test : Start bundle job
     *
     * @throws Exception
     */
    public void testBundleStart2() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Configuration jobConf = null;
        try {
            jobConf = new XConfiguration(new StringReader(job.getConf()));
        }
        catch (IOException ioe) {
            log.warn("Configuration parse error. read from DB :" + job.getConf(), ioe);
            throw new CommandException(ErrorCode.E1005, ioe);
        }

        Path appPath = new Path(jobConf.get(OozieClient.BUNDLE_APP_PATH), "bundle.xml");
        jobConf.set(OozieClient.BUNDLE_APP_PATH, appPath.toString());

        BundleSubmitXCommand submitCmd = new BundleSubmitXCommand(jobConf, job.getAuthToken());
        submitCmd.call();

        BundleJobGetCommand bundleJobGetCmd = new BundleJobGetCommand(submitCmd.getJob().getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.PREP);

        new BundleStartXCommand(job.getId()).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.RUNNING);

        BundleActionsGetCommand bundleActionsGetCmd = new BundleActionsGetCommand(job.getId());
        List<BundleActionBean> actions = jpaService.execute(bundleActionsGetCmd);

        assertEquals(2, actions.size());
        assertEquals(Job.Status.PREP, actions.get(0).getStatus());
        assertEquals(true, actions.get(0).isPending());
        assertEquals(true, actions.get(0).isCritical());
        assertEquals(Job.Status.PREP, actions.get(1).getStatus());
        assertEquals(true, actions.get(1).isPending());
        assertEquals(false, actions.get(1).isCritical());
    }

    /**
     * Test : Start bundle job with dryrun
     *
     * @throws Exception
     */
    public void testBundleStartDryrun() throws Exception {
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP);

        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        BundleJobGetCommand bundleJobGetCmd = new BundleJobGetCommand(job.getId());
        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.PREP);

        new BundleStartXCommand(job.getId(), true).call();

        job = jpaService.execute(bundleJobGetCmd);
        assertEquals(job.getStatus(), Job.Status.RUNNING);

        BundleActionsGetCommand bundleActionsGetCmd = new BundleActionsGetCommand(job.getId());
        List<BundleActionBean> actions = jpaService.execute(bundleActionsGetCmd);

        assertEquals(2, actions.size());
        assertEquals(Job.Status.PREP, actions.get(0).getStatus());
        assertEquals(true, actions.get(0).isPending());
        assertEquals(true, actions.get(0).isCritical());
        assertEquals(Job.Status.PREP, actions.get(1).getStatus());
        assertEquals(true, actions.get(1).isPending());
        assertEquals(false, actions.get(1).isCritical());
    }

    /**
     * Test : Start bundle job but jobId is wrong
     *
     * @throws Exception
     */
    public void testBundleStartFailed() throws Exception {
        this.addRecordToBundleJobTable(Job.Status.PREP);

        try {
            new BundleStartXCommand("bundle-id").call();
            fail("Job doesn't exist. Should fail.");
        } catch (CommandException ce) {
            //Job doesn't exist. Exception is expected.
        }
    }
}