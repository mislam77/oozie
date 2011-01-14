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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;

/**
 * A list of paused Bundle Jobs;
 */
public class BundleJobsGetNeedStartCommand implements JPACommand<List<BundleJobBean>> {
    private Date date;

    public BundleJobsGetNeedStartCommand(Date d) {
        this.date = d;
    }

    @Override
    public String getName() {
        return "BundleJobsGetNeedStartCommand";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<BundleJobBean> execute(EntityManager em) throws CommandException {
        List<BundleJobBean> bjBeans;
        List<BundleJobBean> jobList = new ArrayList<BundleJobBean>();
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_JOBS_NEED_START");
            q.setParameter("currentTime", new Timestamp(date.getTime()));
            bjBeans = q.getResultList();
            for (BundleJobBean j : bjBeans) {
                jobList.add(j);
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
        return jobList;
    }
}