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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Load the CoordinatorJob into a Bean and return it.
 */
public class CoordJobGetCommand implements JPACommand<CoordinatorJobBean> {

    private String coordJobId = null;

    public CoordJobGetCommand(String coordJobId) {
        ParamChecker.notNull(coordJobId, "coordJobId");
        this.coordJobId = coordJobId;
    }

    @Override
    public String getName() {
        return "CoordinatorJobGetCommand";
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoordinatorJobBean execute(EntityManager em) throws CommandException {
        List<CoordinatorJobBean> cjBeans;
        try {
            Query q = em.createNamedQuery("GET_COORD_JOB");
            q.setParameter("id", coordJobId);
            cjBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
        CoordinatorJobBean bean = null;
        if (cjBeans != null && cjBeans.size() > 0) {
            bean = cjBeans.get(0);
            bean.setStatus(bean.getStatus());
            return bean;
        }
        else {
            throw new CommandException(ErrorCode.E0604, coordJobId);
        }
    }
}