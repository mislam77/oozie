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
package org.apache.oozie;

import java.io.IOException;
import java.io.Writer;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.bundle.BundleJobChangeXCommand;
import org.apache.oozie.command.bundle.BundleJobResumeXCommand;
import org.apache.oozie.command.bundle.BundleJobSuspendXCommand;
import org.apache.oozie.command.bundle.BundleJobXCommand;
import org.apache.oozie.command.bundle.BundleKillXCommand;
import org.apache.oozie.command.bundle.BundleRerunXCommand;
import org.apache.oozie.command.bundle.BundleStartXCommand;
import org.apache.oozie.command.bundle.BundleSubmitXCommand;
import org.apache.oozie.command.coord.CoordRerunCommand;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLogStreamer;

public class BundleEngine extends BaseEngine {
    /**
     * Create a system Bundle engine, with no user and no group.
     */
    public BundleEngine() {
    }

    /**
     * Create a Bundle engine to perform operations on behave of a user.
     *
     * @param user user name.
     * @param authToken the authentication token.
     */
    public BundleEngine(String user, String authToken) {
        this.user = ParamChecker.notEmpty(user, "user");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#change(java.lang.String, java.lang.String)
     */
    @Override
    public void change(String jobId, String changeValue) throws BundleEngineException {
        try {
            BundleJobChangeXCommand change = new BundleJobChangeXCommand(jobId, changeValue);
            change.call();
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#dryrunSubmit(org.apache.hadoop.conf.Configuration, boolean)
     */
    @Override
    public String dryrunSubmit(Configuration conf, boolean startJob) throws BundleEngineException {
        BundleSubmitXCommand submit = new BundleSubmitXCommand(true, conf, getAuthToken());
        try {
            String jobId = submit.call();
            return jobId;
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getCoordJob(java.lang.String)
     */
    @Override
    public CoordinatorJob getCoordJob(String jobId) throws BundleEngineException {
        throw new BundleEngineException(new XException(ErrorCode.E0301));
    }

    public BundleJob getBundleJob(String jobId) throws BundleEngineException {
        try {
            return new BundleJobXCommand(jobId).call();
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getCoordJob(java.lang.String, int, int)
     */
    @Override
    public CoordinatorJob getCoordJob(String jobId, int start, int length) throws BundleEngineException {
        throw new BundleEngineException(new XException(ErrorCode.E0301));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getDefinition(java.lang.String)
     */
    @Override
    public String getDefinition(String jobId) throws BundleEngineException {
        BundleJobBean job;
        try {
            job = new BundleJobXCommand(jobId).call();
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }
        return job.getOrigJobXml();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getJob(java.lang.String)
     */
    @Override
    public WorkflowJob getJob(String jobId) throws BundleEngineException {
        throw new BundleEngineException(new XException(ErrorCode.E0301));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getJob(java.lang.String, int, int)
     */
    @Override
    public WorkflowJob getJob(String jobId, int start, int length) throws BundleEngineException {
        throw new BundleEngineException(new XException(ErrorCode.E0301));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#getJobIdForExternalId(java.lang.String)
     */
    @Override
    public String getJobIdForExternalId(String externalId) throws BundleEngineException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#kill(java.lang.String)
     */
    @Override
    public void kill(String jobId) throws BundleEngineException {
        try {
            new BundleKillXCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new BundleEngineException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#reRun(java.lang.String, org.apache.hadoop.conf.Configuration)
     */
    @Override
    @Deprecated
    public void reRun(String jobId, Configuration conf) throws BundleEngineException {
    	throw new BundleEngineException(new XException(ErrorCode.E0301));
    }

    /**
     * Rerun Bundle actions for given rerunType
     *
     * @param jobId
     * @param rerunType
     * @param scope
     * @param refresh
     * @param noCleanup
     * @throws BaseEngineException
     */
    public BundleJobInfo reRun(String jobId, String rerunType, String scope, boolean refresh, boolean noCleanup)
            throws BaseEngineException {
        try {
            return new BundleRerunXCommand(jobId, rerunType, scope, refresh, noCleanup).call();
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#resume(java.lang.String)
     */
    @Override
    public void resume(String jobId) throws BundleEngineException {
        BundleJobResumeXCommand resume = new BundleJobResumeXCommand(jobId);
        try {
            resume.call();
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#start(java.lang.String)
     */
    @Override
    public void start(String jobId) throws BundleEngineException {
        try {
            new BundleStartXCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new BundleEngineException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#streamLog(java.lang.String, java.io.Writer)
     */
    @Override
    public void streamLog(String jobId, Writer writer) throws IOException, BundleEngineException {
        XLogStreamer.Filter filter = new XLogStreamer.Filter();
        filter.setParameter(DagXLogInfoService.JOB, jobId);

        BundleJobBean job;
        try {
            job = new BundleJobXCommand(jobId).call();
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }

        Services.get().get(XLogService.class).streamLog(filter, job.getCreatedTime(), new Date(), writer);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#submitJob(org.apache.hadoop.conf.Configuration, boolean)
     */
    @Override
    public String submitJob(Configuration conf, boolean startJob) throws BundleEngineException {
        try {
            String jobId = new BundleSubmitXCommand(conf, getAuthToken()).call();
            return jobId;
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#suspend(java.lang.String)
     */
    @Override
    public void suspend(String jobId) throws BundleEngineException {
        BundleJobSuspendXCommand suspend = new BundleJobSuspendXCommand(jobId);
        try {
            suspend.call();
        }
        catch (CommandException ex) {
            throw new BundleEngineException(ex);
        }
    }
}