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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Delete the list of WorkflowAction for a WorkflowJob and return the number of actions been deleted.
 */
public class WorkflowActionsDeleteForPurgeCommand implements JPACommand<Integer> {

    private String wfId = null;

    public WorkflowActionsDeleteForPurgeCommand(String wfId) {
        ParamChecker.notNull(wfId, "wfId");
        this.wfId = wfId;
    }

    @Override
    public String getName() {
        return "WorkflowActionsDeleteForPurgeCommand";
    }

    @Override
    public Integer execute(EntityManager em) throws CommandException {
        int actionsDeleted = 0;
        try {
            Query g = em.createNamedQuery("DELETE_ACTIONS_FOR_WORKFLOW");
            g.setParameter("wfId", wfId);
            actionsDeleted = g.executeUpdate();
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
        return actionsDeleted;
    }

}