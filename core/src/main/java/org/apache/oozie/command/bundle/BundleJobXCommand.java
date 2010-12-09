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
package org.apache.oozie.command.bundle;

import java.util.List;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.command.jpa.BundleJobGetCommand;
import org.apache.oozie.command.jpa.BundleJobGetCoordinatorsCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

/**
 * Command for loading a coordinator job information
 */
public class BundleJobXCommand extends XCommand<BundleJobBean> {
    private String id;
    private final XLog LOG = XLog.getLog(BundleJobXCommand.class);

    /**
     * @param id bundle jobId
     */
    public BundleJobXCommand(String id) {
        super("job.info", "job.info", 1);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    @Override 
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    protected String getEntityKey() {
        return this.id;
    }

    @Override
    protected void loadState() throws CommandException {
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    protected BundleJobBean execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            BundleJobBean bundleJob = null;
            if (jpaService != null) {
                bundleJob = jpaService.execute(new BundleJobGetCommand(id));

                List<CoordinatorJobBean> coordinators = jpaService.execute(new BundleJobGetCoordinatorsCommand(id));
                bundleJob.setCoordJobs(coordinators);
            }
            else {
                LOG.error(ErrorCode.E0610);
            }
            return bundleJob;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

}
