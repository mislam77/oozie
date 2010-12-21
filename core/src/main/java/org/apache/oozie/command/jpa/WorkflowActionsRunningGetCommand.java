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

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.command.CommandException;

/**
 * JPA Command to get running workflow actions
 */
public class WorkflowActionsRunningGetCommand implements JPACommand<List<WorkflowActionBean>> {

    private final long checkAgeSecs;

    /**
     * @param checkAgeSecs
     */
    public WorkflowActionsRunningGetCommand(long checkAgeSecs) {
        this.checkAgeSecs = checkAgeSecs;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.jpa.JPACommand#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowActionBean> execute(EntityManager em) throws CommandException {
        List<WorkflowActionBean> actions;
        List<WorkflowActionBean> actionList = new ArrayList<WorkflowActionBean>();
        try {
            Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);
            Query q = em.createNamedQuery("GET_RUNNING_ACTIONS");
            q.setParameter("lastCheckTime", ts);
            actions = q.getResultList();
            for (WorkflowActionBean a : actions) {
                WorkflowActionBean aa = getBeanForRunningAction(a);
                actionList.add(aa);
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0605, e);
        }
        return actionList;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.jpa.JPACommand#getName()
     */
    @Override
    public String getName() {
        return "WorkflowActionsRunningGetCommand";
    }

    /**
     * @param a
     * @return
     * @throws SQLException
     */
    private WorkflowActionBean getBeanForRunningAction(WorkflowActionBean a) throws SQLException {
        if (a != null) {
            WorkflowActionBean action = new WorkflowActionBean();
            action.setId(a.getId());
            action.setConf(a.getConf());
            action.setConsoleUrl(a.getConsoleUrl());
            action.setData(a.getData());
            action.setErrorInfo(a.getErrorCode(), a.getErrorMessage());
            action.setExternalId(a.getExternalId());
            action.setExternalStatus(a.getExternalStatus());
            action.setName(a.getName());
            action.setRetries(a.getRetries());
            action.setTrackerUri(a.getTrackerUri());
            action.setTransition(a.getTransition());
            action.setType(a.getType());
            action.setEndTime(a.getEndTime());
            action.setExecutionPath(a.getExecutionPath());
            action.setLastCheckTime(a.getLastCheckTime());
            action.setLogToken(a.getLogToken());
            if (a.getPending() == true) {
                action.setPending();
            }
            action.setPendingAge(a.getPendingAge());
            action.setSignalValue(a.getSignalValue());
            action.setSlaXml(a.getSlaXml());
            action.setStartTime(a.getStartTime());
            action.setStatus(a.getStatus());
            action.setJobId(a.getWfId());
            return action;
        }
        return null;
    }
}
