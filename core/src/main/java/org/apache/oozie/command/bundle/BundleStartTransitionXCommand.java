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

import java.util.List;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.StartTransitionXCommand;
import org.apache.oozie.command.jpa.BundleJobGetCommand;
import org.apache.oozie.command.jpa.CoordJobsGetForBundleCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;

public class BundleStartTransitionXCommand extends StartTransitionXCommand {
    private String jobId;
    //TODO should change to JobBean
    private BundleJobBean bundleJob;
    private List<CoordinatorJobBean> coordBeans;
    private JPAService jpaService = null;

    public BundleStartTransitionXCommand(String jobId) {
        super("bundle_start", "bundle_start", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    public BundleStartTransitionXCommand(String jobId, boolean dryrun) {
        super("bundle_start", "bundle_start", 1, dryrun);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    @Override
    public void StartChildren() {
        if (coordBeans != null) {
            for (CoordinatorJobBean cBean :coordBeans) {
                cBean.setStatus(CoordinatorJob.Status.PREP);
            }
        }
        LOG.debug("Started coord jobs for the bundle=[{0}]", jobId);
    }

    @Override
    public void transitToNext() {
        this.bundleJob.setStatus(BundleJob.Status.RUNNING);
    }

    @Override
    public void notifyParent() {

    }

    @Override
    public void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.bundleJob = jpaService.execute(new BundleJobGetCommand(jobId));
                //TODO setLogInfo(bundleJob);
                this.coordBeans = jpaService.execute(new CoordJobsGetForBundleCommand(jobId));
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected String getEntityKey() {
        return jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

}
