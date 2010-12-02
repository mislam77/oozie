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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Delete the BundleJob
 */
public class BundleJobDeleteCommand implements JPACommand<Void> {

    private String bundleJobId = null;

    public BundleJobDeleteCommand(String bundleJobId) {
        ParamChecker.notNull(bundleJobId, "bundleJobId");
        this.bundleJobId = bundleJobId;
    }

    @Override
    public String getName() {
        return "BundleJobDeleteCommand";
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void execute(EntityManager em) throws CommandException {
        List<BundleJobBean> bdBeans;
        try {
            Query q = em.createNamedQuery("GET_BUNDLE_JOB");
            q.setParameter("id", bundleJobId);
            bdBeans = q.getResultList();
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
        BundleJobBean bean = null;
        if (bdBeans != null && bdBeans.size() > 0) {
            bean = bdBeans.get(0);
            em.remove(bean);
        }
        else {
            throw new CommandException(ErrorCode.E0604, bundleJobId);
        }
        return null;
    }
}