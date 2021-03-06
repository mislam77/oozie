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
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the CoordinatorAction into a Bean and return it.
 */
public class CoordActionGetForExternalIdCommand implements JPACommand<CoordinatorActionBean> {

    private String externalId = null;

    public CoordActionGetForExternalIdCommand(String externalId) {
        ParamChecker.notNull(externalId, "externalId");
        this.externalId = externalId;
    }

    @Override
    public String getName() {
        return "CoordinatorActionGetCommand";
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoordinatorActionBean execute(EntityManager em) throws CommandException {
        try {
            CoordinatorActionBean caBean = null;
            Query q = em.createNamedQuery("GET_COORD_ACTION_FOR_EXTERNALID");
            q.setParameter("externalId", externalId);
            List<CoordinatorActionBean> actionList = q.getResultList();
            if (actionList.size() > 0) {
                caBean = actionList.get(0);
            }
            return caBean;
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
    }

}