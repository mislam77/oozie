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

import java.util.List;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.jpa.WorkflowActionsDeleteForPurgeCommand;
import org.apache.oozie.command.jpa.WorkflowJobDeleteCommand;
import org.apache.oozie.command.jpa.WorkflowJobsGetForPurgeCommand;

public class PurgeXCommand extends WorkflowXCommand<Void> {
    private static XLog LOG = XLog.getLog(PurgeXCommand.class);
    private JPAService jpaService = null;
    private int olderThan;
    private int limit;
    private List<WorkflowJobBean> jobList = null;

    public PurgeXCommand(int olderThan, int limit) {
        super("purge", "purge", 0);
        this.olderThan = olderThan;
        this.limit = limit;
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED Workflow-Purge Attempting to purge Jobs older than [{0}] days.", olderThan);

        int actionDeleted = 0;
        if (jobList != null && jobList.size() != 0) {
            for (WorkflowJobBean w : jobList) {
                String wfId = w.getId();
                jpaService.execute(new WorkflowJobDeleteCommand(wfId));
                actionDeleted += jpaService.execute(new WorkflowActionsDeleteForPurgeCommand(wfId));
            }
            LOG.debug("ENDED Workflow-Purge deleted jobs :" + jobList.size() + " and actions " + actionDeleted);
        }
        else {
            LOG.debug("ENDED Workflow-Purge no workflow job to be deleted");
        }
        return null;
    }

    @Override
    protected String getEntityKey() {
        return null;
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.jobList = jpaService.execute(new WorkflowJobsGetForPurgeCommand(olderThan, limit));
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
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

}