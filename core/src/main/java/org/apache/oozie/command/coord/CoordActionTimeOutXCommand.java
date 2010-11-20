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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.jpa.CoordActionGetCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

public class CoordActionTimeOutXCommand extends CoordinatorXCommand<Void> {
    private CoordinatorActionBean actionBean;
    private static XLog log = XLog.getLog(CoordActionTimeOutXCommand.class);
    private JPAService jpaService = null;

    public CoordActionTimeOutXCommand(CoordinatorActionBean actionBean) {
        super("coord_action_timeout", "coord_action_timeout", 1);
        this.actionBean = actionBean;
    }

    @Override
    protected Void execute() throws CommandException {
        if (actionBean.getStatus() == CoordinatorAction.Status.WAITING) {
            actionBean.setStatus(CoordinatorAction.Status.TIMEDOUT);
            queue(new CoordActionNotificationXCommand(actionBean), 100);
            actionBean.setLastModifiedTime(new Date());
            jpaService.execute(new org.apache.oozie.command.jpa.CoordActionUpdateCommand(actionBean));
        }
        return null;
    }

    @Override
    protected String getEntityKey() {
        return actionBean.getJobId();
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
            
        actionBean = jpaService.execute(new CoordActionGetCommand(actionBean.getId()));
        setLogInfo(actionBean);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (actionBean.getStatus() != CoordinatorAction.Status.WAITING) {
            throw new PreconditionException(ErrorCode.E1100);
        }
    }
}
