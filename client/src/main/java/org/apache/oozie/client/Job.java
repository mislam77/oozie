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
package org.apache.oozie.client;

import java.util.Date;

/**
 * Interface that represents an Oozie Job.
 */
public interface Job {
    /**
     * Defines the possible status of an Oozie JOB.
     */
    public static enum Status {
        PREP, RUNNING, SUSPENDED, SUCCEEDED, KILLED, FAILED, PAUSED, PREPPAUSED, PREPSUSPENDED, RUNNINGWITHERROR, SUSPENDEDERROR, PAUSEDWITHERROR, DONEWITHERROR
    }

    /**
     * Return the path to the Oozie application.
     *
     * @return the path to the Oozie application.
     */
    String getAppPath();

    /**
     * Return the name of the Oozie application (from the application definition).
     *
     * @return the name of the Oozie application.
     */
    String getAppName();

    /**
     * Return the JOB ID.
     *
     * @return the JOB ID.
     */
    String getId();

    /**
     * Return the JOB configuration.
     *
     * @return the JOB configuration.
     */
    String getConf();

    /**
     * Return the JOB status.
     *
     * @return the JOB status.
     */
    Status getStatus();

    /**
     * Return the JOB user owner.
     *
     * @return the JOB user owner.
     */
    String getUser();

    /**
     * Return the JOB group.
     *
     * @return the JOB group.
     */
    String getGroup();

    /**
     * Return the JOB console URL.
     *
     * @return the JOB console URL.
     */
    String getConsoleUrl();

    /**
     * Return the JOB start time.
     *
     * @return the JOB start time.
     */
    Date getStartTime();

    /**
     * Return the JOB Kickoff time.
     *
     * @return the JOB Kickoff time.
     */
    Date getKickoffTime();

    /**
     * Return the JOB end time.
     *
     * @return the JOB end time.
     */
    Date getEndTime();

    /**
     * Set the status of the job
     *
     * @param status
     */
    void setStatus(Job.Status status);

    /**
     * Set pending to true
     *
     * @param pending set pending to true
     */
    void setPending();

    /**
     * Set pending to
     *
     * @param pending set pending to false
     */
    void resetPending();

}
