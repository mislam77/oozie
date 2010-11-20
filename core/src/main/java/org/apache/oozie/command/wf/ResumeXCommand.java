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

import java.util.Date;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.jpa.WorkflowActionUpdateCommand;
import org.apache.oozie.command.jpa.WorkflowJobGetActionsCommand;
import org.apache.oozie.command.jpa.WorkflowJobGetCommand;
import org.apache.oozie.command.jpa.WorkflowJobUpdateCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;

public class ResumeXCommand extends WorkflowXCommand<Void> {

    private String id;
    private JPAService jpaService = null;
    private WorkflowJobBean workflow = null;

    public ResumeXCommand(String id) {
        super("resume", "resume", 1);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected Void execute() throws CommandException {
        try {
            if (workflow.getStatus() == WorkflowJob.Status.SUSPENDED) {
                incrJobCounter(1);
                workflow.getWorkflowInstance().resume();
                WorkflowInstance wfInstance = workflow.getWorkflowInstance();
                ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.RUNNING);
                workflow.setWorkflowInstance(wfInstance);
                workflow.setStatus(WorkflowJob.Status.RUNNING);
                
                
                //for (WorkflowActionBean action : store.getActionsForWorkflow(id, false)) {
                for (WorkflowActionBean action : jpaService.execute(new WorkflowJobGetActionsCommand(id))) {

                    // Set pending flag to true for the actions that are START_RETRY or
                    // START_MANUAL or END_RETRY or END_MANUAL
                    if (action.isRetryOrManual()) {
                        action.setPendingOnly();
                        jpaService.execute(new WorkflowActionUpdateCommand(action));
                    }

                    if (action.isPending()) {
                        if (action.getStatus() == WorkflowActionBean.Status.PREP
                                || action.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
                            queue(new ActionStartXCommand(action.getId(), action.getType()));
                        }
                        else {
                            if (action.getStatus() == WorkflowActionBean.Status.START_RETRY) {
                                Date nextRunTime = action.getPendingAge();
                                queue(new ActionStartXCommand(action.getId(), action.getType()),
                                              nextRunTime.getTime() - System.currentTimeMillis());
                            }
                            else {
                                if (action.getStatus() == WorkflowActionBean.Status.DONE
                                        || action.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
                                    queue(new ActionEndXCommand(action.getId(), action.getType()));
                                }
                                else {
                                    if (action.getStatus() == WorkflowActionBean.Status.END_RETRY) {
                                        Date nextRunTime = action.getPendingAge();
                                        queue(new ActionEndXCommand(action.getId(), action.getType()),
                                                      nextRunTime.getTime() - System.currentTimeMillis());
                                    }
                                }
                            }
                        }

                    }
                }

                jpaService.execute(new WorkflowJobUpdateCommand(workflow));
                queue(new NotificationXCommand(workflow));
            }
            return null;
        }
        catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected String getEntityKey() {
        return id;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        workflow = jpaService.execute(new WorkflowJobGetCommand(id));
        setLogInfo(workflow);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (workflow.getStatus() != WorkflowJob.Status.SUSPENDED) {
            throw new PreconditionException(ErrorCode.E1100, "workflow's status is " + workflow.getStatusStr() + " is not SUSPENDED");
        }
    }
}