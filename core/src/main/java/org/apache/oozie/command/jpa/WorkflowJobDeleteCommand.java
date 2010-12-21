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

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Delete workflow job
 *
 */
public class WorkflowJobDeleteCommand implements JPACommand<Void>{

    private String wfJobId = null;

    /**
     * @param wfJobId
     */
    public WorkflowJobDeleteCommand(String wfJobId){
        ParamChecker.notEmpty(wfJobId, "wfJobId");
        this.wfJobId = wfJobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.jpa.JPACommand#execute(javax.persistence.EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws CommandException {
        WorkflowJobBean job = em.find(WorkflowJobBean.class, this.wfJobId);
        if (job != null) {
            em.remove(job);
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.jpa.JPACommand#getName()
     */
    @Override
    public String getName() {
        return "WorkflowJobDeleteCommand";
    }

}
