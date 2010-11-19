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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.jpa.CoordJobGetCommand;
import org.apache.oozie.command.jpa.CoordJobUpdateCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

public class CoordRecoveryXCommand extends CoordinatorXCommand<Void> {
    private static XLog log = XLog.getLog(CoordRecoveryXCommand.class);
    private String jobId;
    private JPAService jpaService = null;
    CoordinatorJobBean coordJob = null;

    public CoordRecoveryXCommand(String id) {
        super("coord_recovery", "coord_recovery", 1);
        this.jobId = id;
    }

    @Override
    protected Void execute() throws CommandException {
        // update status of job from PREMATER to RUNNING in coordJob
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        coordJob.setLastModifiedTime(new Date());
        jpaService.execute(new CoordJobUpdateCommand(coordJob));
        log.debug("[" + jobId + "]: Recover status from PREMATER to RUNNING");
        return null;
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
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        coordJob = jpaService.execute(new CoordJobGetCommand(jobId));
        setLogInfo(coordJob);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordJob.getStatus() != CoordinatorJob.Status.PREMATER) {
            throw new PreconditionException(ErrorCode.E1100);
        }
    }

}
