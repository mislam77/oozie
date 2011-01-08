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
package org.apache.oozie.service;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.command.bundle.BundlePauseXCommand;
import org.apache.oozie.command.bundle.BundleUnpauseXCommand;
import org.apache.oozie.command.jpa.BundleJobsGetPausedCommand;
import org.apache.oozie.command.jpa.BundleJobsGetUnpausedCommand;
import org.apache.oozie.service.SchedulerService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.MemoryLocks;
import org.apache.oozie.util.XLog;

public class PauseUnpauseService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "PauseUnpauseService.";
    public static final String CONF_PAUSEUNPAUSE_INTERVAL = CONF_PREFIX + "PauseUnpause.interval";
    private final static XLog LOG = XLog.getLog(PauseUnpauseService.class);
    private static int runningInstanceCnt = 0;
    
    /**
     * PauseUnpauseRunnable is the runnable which is scheduled to run at the configured interval; 
     * It checks all bundles to see if they should be paused or unpaused.
     */
    static class PauseUnpauseRunnable implements Runnable {
        private JPAService jpaService = null;
        private MemoryLocks.LockToken lock;
        
        public PauseUnpauseRunnable() {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService == null) {
                LOG.error("Missing JPAService");
            }
        }

        public void run() {
            try {
                //first check if there is some other running instance from the same service;
                lock = Services.get().get(MemoryLocksService.class).getWriteLock(CONF_PREFIX, lockTimeout);                
                if (lock == null) {
                    LOG.info("This PauseUnpauseService instance will not run since there is already an instance running");
                }
                else {
                    LOG.info("Acquired lock for [{0}]", CONF_PREFIX);
                    runningInstanceCnt ++;
                    
                    Date d = new Date();

                    // pause jobs as needed;
                    List<BundleJobBean> jobList = jpaService.execute(new BundleJobsGetUnpausedCommand(-1));
                    for (BundleJobBean bundleJob : jobList) {
                        if ((bundleJob.getPauseTime() != null) &&  !bundleJob.getPauseTime().after(d)) {
                            (new BundlePauseXCommand(bundleJob)).call();
                        }
                    }

                    // unpause jobs as needed;
                    jobList = jpaService.execute(new BundleJobsGetPausedCommand(-1));
                    for (BundleJobBean bundleJob : jobList) {
                        if ((bundleJob.getPauseTime() == null || bundleJob.getPauseTime().after(d))) {
                            (new BundleUnpauseXCommand(bundleJob)).call();
                        }
                    }   
                }
            }
            catch (Exception ex) {
                LOG.warn("Exception happened when (un)pausing bundle jobs", ex);
            }
            finally {
                // release lock;
                if (lock != null) {
                    lock.release();
                    LOG.info("Released lock for [{0}]", CONF_PREFIX);
                }                
            }
        }
    }

    /**
     * Initializes the {@link PauseUnpauseService}.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        Configuration conf = services.getConf();
        Runnable pauseUnpauseRunnable = new PauseUnpauseRunnable();
        services.get(SchedulerService.class).schedule(pauseUnpauseRunnable, 10, conf.getInt(CONF_PAUSEUNPAUSE_INTERVAL, 60),
                                                      SchedulerService.Unit.SEC);
    }

    /**
     * Destroy the StateTransit Jobs Service.
     */
    @Override
    public void destroy() {
    }

    /**
     * Return the public interface for the purge jobs service.
     *
     * @return {@link PauseUnpauseService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return PauseUnpauseService.class;
    }
}