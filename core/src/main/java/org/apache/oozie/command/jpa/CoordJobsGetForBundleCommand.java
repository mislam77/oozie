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
 * Load the list of CoordinatorJb for a BundleJob and return the list.
 */
public class CoordJobsGetForBundleCommand implements JPACommand<List<CoordinatorJobBean>> {

    private String bundleJobId = null;
    private int numResults = -1;

    public CoordJobsGetForBundleCommand(String bundleJobId) {
        ParamChecker.notNull(bundleJobId, "bundleJobId");
        this.bundleJobId = bundleJobId;
    }

    public CoordJobsGetForBundleCommand(String bundleJobId, int maxResults) {
        this(bundleJobId);
        numResults = maxResults;
    }

    @Override
    public String getName() {
        return "CoordJobsGetForBundleCommand";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<CoordinatorJobBean> execute(EntityManager em) throws CommandException {
        List<CoordinatorJobBean> cjBeans = null;
        try {
            Query q = em.createNamedQuery("GET_COORD_JOBS_FOR_BUNDLE");
            q.setParameter("bundleId", bundleJobId);
            if (numResults > 0) {
                q.setMaxResults(numResults);
            }
            cjBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
        return cjBeans;
    }

}