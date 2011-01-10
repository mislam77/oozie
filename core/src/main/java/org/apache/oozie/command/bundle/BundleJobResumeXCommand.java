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

import java.util.Date;
import java.util.List;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.ResumeTransitionXCommand;
import org.apache.oozie.command.coord.CoordResumeXCommand;
import org.apache.oozie.command.jpa.BundleActionUpdateCommand;
import org.apache.oozie.command.jpa.BundleActionsGetCommand;
import org.apache.oozie.command.jpa.BundleJobGetCommand;
import org.apache.oozie.command.jpa.BundleJobUpdateCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;

public class BundleJobResumeXCommand extends ResumeTransitionXCommand{

    private final String bundleId;
    private JPAService jpaService = null;
    private BundleJobBean bundleJob;
    private List<BundleActionBean> bundleActions;

    /**
     * Constructor to create the Bundle Resume Command.
     *
     * @param jobId : Bundle Id
     */
    public BundleJobResumeXCommand(String jobId) {
        super("bundle_submit", "bundle_submit", 1);
        this.bundleId = ParamChecker.notNull(jobId, "BundleId");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.ResumeTransitionXCommand#resume()
     */
    @Override
    public void resume() throws CommandException {
        try {
            incrJobCounter(1);
            for (BundleActionBean action : bundleActions) {
                // queue a ResumeCommand
                if (action.getCoordId() != null) {
                    queue(new CoordResumeXCommand(action.getCoordId()));
                    action.setPending(action.getPending()+1);
                    jpaService.execute(new BundleActionUpdateCommand(action));
                }
            }
            bundleJob.setSuspendedTime(new Date());
            jpaService.execute(new BundleJobUpdateCommand(bundleJob));

            return;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    public void notifyParent() throws CommandException {

    }

    @Override
    public void updateJob() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return bundleId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        try{
            this.bundleActions = jpaService.execute(new BundleActionsGetCommand(bundleId));
        }
        catch(Exception Ex){
            throw new CommandException(ErrorCode.E1311,this.bundleId);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        try{
            bundleJob = jpaService.execute(new BundleJobGetCommand(bundleId));
        }
        catch(Exception Ex){
            new CommandException(ErrorCode.E0604,bundleId);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (bundleJob.getStatus() != Job.Status.SUSPENDED) {
            throw new PreconditionException(ErrorCode.E1100, "BundleResumeCommand not Resumed - " + "job not in SUSPENDED state " + bundleId);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleJob;
    }
}