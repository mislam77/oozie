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

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Persist the WorkflowAction bean.
 */
public class WorkflowActionInsertCommand implements JPACommand<String> {

    private WorkflowActionBean wfAction = null;

    public WorkflowActionInsertCommand(WorkflowActionBean wfAction) {
        ParamChecker.notNull(wfAction, "wfAction");
        this.wfAction = wfAction;
    }

    @Override
    public String getName() {
        return "WorkflowActionInsertCommand";
    }

    @Override
    public String execute(EntityManager em) throws CommandException {
        em.persist(wfAction);
        return null;
    }
}