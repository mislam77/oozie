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

import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.jpa.CoordActionsGetForJobCommand;
import org.apache.oozie.command.jpa.CoordJobGetCommand;
import org.apache.oozie.command.jpa.CoordJobUpdateCommand;
import org.apache.oozie.command.jpa.CoordActionUpdateCommand;
import org.apache.oozie.command.wf.WorkflowKillXCommand;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

import java.util.Date;
import java.util.List;

public class CoordKillXCommand extends CoordinatorXCommand<Void> {

    private String jobId;
    private final XLog LOG = XLog.getLog(CoordKillXCommand.class);
    private CoordinatorJobBean coordJob;
    private List<CoordinatorActionBean> actionList;
    private JPAService jpaService = null;

    public CoordKillXCommand(String id) {
        super("coord_kill", "coord_kill", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.coordJob = jpaService.execute(new CoordJobGetCommand(jobId));
                this.actionList = jpaService.execute(new CoordActionsGetForJobCommand(jobId));
                setLogInfo(coordJob);
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

    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED CoordKillXCommand for jobId=" + jobId);
        incrJobCounter(1);
        coordJob.setEndTime(new Date());
        coordJob.setStatus(CoordinatorJob.Status.KILLED);
        for (CoordinatorActionBean action : actionList) {
            if (action.getStatus() != CoordinatorActionBean.Status.FAILED
                    && action.getStatus() != CoordinatorActionBean.Status.TIMEDOUT
                    && action.getStatus() != CoordinatorActionBean.Status.SUCCEEDED
                    && action.getStatus() != CoordinatorActionBean.Status.KILLED) {
                // queue a WorkflowKillXCommand to delete the workflow job and actions
                if (action.getExternalId() != null) {
                    queue(new WorkflowKillXCommand(action.getExternalId()));
                }
                action.setStatus(CoordinatorActionBean.Status.KILLED);
                jpaService.execute(new CoordActionUpdateCommand(action));
            }
        }
        jpaService.execute(new CoordJobUpdateCommand(coordJob));
        LOG.info("ENDED CoordKillXCommand for jobId=" + jobId);
        return null;
    }

}
