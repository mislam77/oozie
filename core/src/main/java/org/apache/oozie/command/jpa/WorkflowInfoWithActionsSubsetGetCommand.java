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
package org.apache.oozie.command.jpa;

import java.util.List;

import javax.persistence.EntityManager;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;

/**
 * This JPA Command is responsible for getting the Workflow job with actions in certain range.
 */
public class WorkflowInfoWithActionsSubsetGetCommand implements JPACommand<WorkflowJobBean> {

    private String wfJobId = null;
    private WorkflowJobBean workflow;
    private final int start;
    private final int len;

    /**
     * @param wfJobId
     * @param start
     * @param len
     */
    public WorkflowInfoWithActionsSubsetGetCommand(String wfJobId, int start, int len) {
        ParamChecker.notNull(wfJobId, "wfJobId");
        this.wfJobId = wfJobId;
        this.start = start;
        this.len = len;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.jpa.JPACommand#execute(javax.persistence.EntityManager)
     */
    @Override
    public WorkflowJobBean execute(EntityManager em) throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.workflow = jpaService.execute(new WorkflowJobGetCommand(this.wfJobId));
            }
            else {
                throw new CommandException(ErrorCode.E0610, this.wfJobId);
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex);
        }

        if (this.workflow != null) {
            JPAService jpaService = Services.get().get(JPAService.class);
            List<WorkflowActionBean> actionList;
            if (jpaService != null) {
                actionList = jpaService.execute(new WorkflowActionSubsetGetCommand(this.wfJobId, start, len));
            }
            else {
                throw new CommandException(ErrorCode.E0610, this.wfJobId);
            }
            this.workflow.setActions(actionList);
        }
        else {
            throw new CommandException(ErrorCode.E0604, wfJobId);
        }

        return this.workflow;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.jpa.JPACommand#getName()
     */
    @Override
    public String getName() {
        return "WorkflowInfoWithActionsSubsetGetCommand";
    }
}
