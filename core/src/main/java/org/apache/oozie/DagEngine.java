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

import org.apache.oozie.util.XLogStreamer;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.wf.CompletedActionCommand;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.wf.JobCommand;
import org.apache.oozie.command.wf.JobsCommand;
import org.apache.oozie.command.wf.KillCommand;
import org.apache.oozie.command.wf.ReRunCommand;
import org.apache.oozie.command.wf.ResumeCommand;
import org.apache.oozie.command.wf.SubmitXCommand;
import org.apache.oozie.command.wf.SubmitHttpCommand;
import org.apache.oozie.command.wf.SubmitPigCommand;
import org.apache.oozie.command.wf.SubmitMRCommand;
import org.apache.oozie.command.wf.StartCommand;
import org.apache.oozie.command.wf.SuspendCommand;
import org.apache.oozie.command.wf.DefinitionCommand;
import org.apache.oozie.command.wf.ExternalIdCommand;
import org.apache.oozie.command.wf.WorkflowActionInfoCommand;
import org.apache.oozie.command.wf.WorkflowActionInfoXCommand;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

import java.io.Writer;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;

/**
 * The DagEngine bean provides all the DAG engine functionality for WS calls.
 */
public class DagEngine extends BaseEngine {

    private static final int HIGH_PRIORITY = 2;

    /**
     * Create a system Dag engine, with no user and no group.
     */
    public DagEngine() {
    }

    /**
     * Create a Dag engine to perform operations on behave of a user.
     *
     * @param user user name.
     * @param authToken the authentication token.
     */
    public DagEngine(String user, String authToken) {
        this.user = ParamChecker.notEmpty(user, "user");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    /**
     * Submit a workflow job. <p/> It validates configuration properties.
     *
     * @param conf job configuration.
     * @param startJob indicates if the job should be started or not.
     * @return the job Id.
     * @throws DagEngineException thrown if the job could not be created.
     */
    @Override
    public String submitJob(Configuration conf, boolean startJob) throws DagEngineException {
        validateSubmitConfiguration(conf);
        SubmitXCommand submit = new SubmitXCommand(conf, getAuthToken());
        try {
            String jobId = submit.call();
            if (startJob) {
                start(jobId);
            }
            return jobId;
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Submit a pig/mapreduce job through HTTP.
     * <p/>
     * It validates configuration properties.
     *
     * @param conf job configuration.
     * @param jobType job type - can be "pig" or "mapreduce".
     * @return the job Id.
     * @throws DagEngineException thrown if the job could not be created.
     */
    public String submitHttpJob(Configuration conf, String jobType) throws DagEngineException {
        validateSubmitConfiguration(conf);

        SubmitHttpCommand submit = null;
        if (jobType.equals("pig")) {
            submit = new SubmitPigCommand(conf, getAuthToken());
        }
        else if (jobType.equals("mapreduce")) {
            submit = new SubmitMRCommand(conf, getAuthToken());
        }

        try {
            String jobId = submit.call();
            start(jobId);
            return jobId;
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        // Configuration conf = new XConfiguration(IOUtils.getResourceAsReader(
        // "org/apache/oozie/coord/conf.xml", -1));

        Configuration conf = new XConfiguration();

        // String appXml =
        // IOUtils.getResourceAsString("org/apache/oozie/coord/test1.xml", -1);
        conf.set(OozieClient.APP_PATH, "file:///Users/danielwo/oozie/workflows/examples/seed/workflows/map-reduce");
        conf.set(OozieClient.USER_NAME, "danielwo");
        conf.set(OozieClient.GROUP_NAME, "other");

        conf.set("inputDir", "  blah   ");

        // System.out.println("appXml :"+ appXml + "\n conf :"+ conf);
        new Services().init();
        try {
            DagEngine de = new DagEngine("me", "TESTING_WF");
            String jobId = de.submitJob(conf, true);
            System.out.println("WF Job Id " + jobId);

            Thread.sleep(20000);
        }
        finally {
            Services.get().destroy();
        }
    }

    private void validateSubmitConfiguration(Configuration conf) throws DagEngineException {
        if (conf.get(OozieClient.APP_PATH) == null) {
            throw new DagEngineException(ErrorCode.E0401, OozieClient.APP_PATH);
        }
    }

    /**
     * Start a job.
     *
     * @param jobId job Id.
     * @throws DagEngineException thrown if the job could not be started.
     */
    @Override
    public void start(String jobId) throws DagEngineException {
        // Changing to synchronous call from asynchronous queuing to prevent the
        // loss of command if the queue is full or the queue is lost in case of
        // failure.
        try {
            new StartCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new DagEngineException(e);
        }
    }

    /**
     * Resume a job.
     *
     * @param jobId job Id.
     * @throws DagEngineException thrown if the job could not be resumed.
     */
    @Override
    public void resume(String jobId) throws DagEngineException {
        // Changing to synchronous call from asynchronous queuing to prevent the
        // loss of command if the queue is full or the queue is lost in case of
        // failure.
        try {
            new ResumeCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new DagEngineException(e);
        }
    }

    /**
     * Suspend a job.
     *
     * @param jobId job Id.
     * @throws DagEngineException thrown if the job could not be suspended.
     */
    @Override
    public void suspend(String jobId) throws DagEngineException {
        // Changing to synchronous call from asynchronous queuing to prevent the
        // loss of command if the queue is full or the queue is lost in case of
        // failure.
        try {
            new SuspendCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new DagEngineException(e);
        }
    }

    /**
     * Kill a job.
     *
     * @param jobId job Id.
     * @throws DagEngineException thrown if the job could not be killed.
     */
    @Override
    public void kill(String jobId) throws DagEngineException {
        // Changing to synchronous call from asynchronous queuing to prevent the
        // loss of command if the queue is full or the queue is lost in case of
        // failure.
        try {
            new KillCommand(jobId).call();
            XLog.getLog(getClass()).info("User " + user + " killed the WF job " + jobId);
        }
        catch (CommandException e) {
            throw new DagEngineException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#change(java.lang.String, java.lang.String)
     */
    @Override
    public void change(String jobId, String changeValue) throws DagEngineException {
        // This code should not be reached.
        throw new DagEngineException(ErrorCode.E1017);
    }

    /**
     * Rerun a job.
     *
     * @param jobId job Id to rerun.
     * @param conf configuration information for the rerun.
     * @throws DagEngineException thrown if the job could not be rerun.
     */
    @Override
    public void reRun(String jobId, Configuration conf) throws DagEngineException {
        try {
            validateReRunConfiguration(conf);
            new ReRunCommand(jobId, conf, getAuthToken()).call();
            start(jobId);
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    private void validateReRunConfiguration(Configuration conf) throws DagEngineException {
        if (conf.get(OozieClient.APP_PATH) == null) {
            throw new DagEngineException(ErrorCode.E0401, OozieClient.APP_PATH);
        }
        if (conf.get(OozieClient.RERUN_SKIP_NODES) == null) {
            throw new DagEngineException(ErrorCode.E0401, OozieClient.RERUN_SKIP_NODES);
        }
    }

    /**
     * Process an action callback.
     *
     * @param actionId the action Id.
     * @param externalStatus the action external status.
     * @param actionData the action output data, <code>null</code> if none.
     * @throws DagEngineException thrown if the callback could not be processed.
     */
    public void processCallback(String actionId, String externalStatus, Properties actionData)
            throws DagEngineException {
        XLog.Info.get().clearParameter(XLogService.GROUP);
        XLog.Info.get().clearParameter(XLogService.USER);
        Command<Void, ?> command = new CompletedActionCommand(actionId, externalStatus, actionData, HIGH_PRIORITY);
        if (!Services.get().get(CallableQueueService.class).queue(command)) {
            XLog.getLog(this.getClass()).warn(XLog.OPS, "queue is full or system is in SAFEMODE, ignoring callback");
        }
    }

    /**
     * Return the info about a job.
     *
     * @param jobId job Id.
     * @return the workflow job info.
     * @throws DagEngineException thrown if the job info could not be obtained.
     */
    @Override
    public WorkflowJob getJob(String jobId) throws DagEngineException {
        try {
            return new JobCommand(jobId).call();
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Return the info about a job with actions subset.
     *
     * @param jobId job Id
     * @param start starting from this index in the list of actions belonging to the job
     * @param length number of actions to be returned
     * @return the workflow job info.
     * @throws DagEngineException thrown if the job info could not be obtained.
     */
    @Override
    public WorkflowJob getJob(String jobId, int start, int length) throws DagEngineException {
        try {
            return new JobCommand(jobId, start, length).call();
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Return the a job definition.
     *
     * @param jobId job Id.
     * @return the job definition.
     * @throws DagEngineException thrown if the job definition could no be obtained.
     */
    @Override
    public String getDefinition(String jobId) throws DagEngineException {
        try {
            return new DefinitionCommand(jobId).call();
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Stream the log of a job.
     *
     * @param jobId job Id.
     * @param writer writer to stream the log to.
     * @throws IOException thrown if the log cannot be streamed.
     * @throws DagEngineException thrown if there is error in getting the Workflow Information for jobId.
     */
    @Override
    public void streamLog(String jobId, Writer writer) throws IOException, DagEngineException {
        XLogStreamer.Filter filter = new XLogStreamer.Filter();
        filter.setParameter(DagXLogInfoService.JOB, jobId);
        WorkflowJob job = getJob(jobId);
        Services.get().get(XLogService.class).streamLog(filter, job.getStartTime(), job.getEndTime(), writer);
    }

    private static final Set<String> FILTER_NAMES = new HashSet<String>();

    static {
        FILTER_NAMES.add(OozieClient.FILTER_USER);
        FILTER_NAMES.add(OozieClient.FILTER_NAME);
        FILTER_NAMES.add(OozieClient.FILTER_GROUP);
        FILTER_NAMES.add(OozieClient.FILTER_STATUS);
    }

    /**
     * Validate a jobs filter.
     *
     * @param filter filter to validate.
     * @return the parsed filter.
     * @throws DagEngineException thrown if the filter is invalid.
     */
    protected Map<String, List<String>> parseFilter(String filter) throws DagEngineException {
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        if (filter != null) {
            StringTokenizer st = new StringTokenizer(filter, ";");
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                if (token.contains("=")) {
                    String[] pair = token.split("=");
                    if (pair.length != 2) {
                        throw new DagEngineException(ErrorCode.E0420, filter, "elements must be name=value pairs");
                    }
                    if (!FILTER_NAMES.contains(pair[0])) {
                        throw new DagEngineException(ErrorCode.E0420, filter, XLog
                                .format("invalid name [{0}]", pair[0]));
                    }
                    if (pair[0].equals("status")) {
                        try {
                            WorkflowJob.Status.valueOf(pair[1]);
                        }
                        catch (IllegalArgumentException ex) {
                            throw new DagEngineException(ErrorCode.E0420, filter, XLog.format("invalid status [{0}]",
                                                                                              pair[1]));
                        }
                    }
                    List<String> list = map.get(pair[0]);
                    if (list == null) {
                        list = new ArrayList<String>();
                        map.put(pair[0], list);
                    }
                    list.add(pair[1]);
                }
                else {
                    throw new DagEngineException(ErrorCode.E0420, filter, "elements must be name=value pairs");
                }
            }
        }
        return map;
    }

    /**
     * Return the info about a set of jobs.
     *
     * @param filterStr job filter. Refer to the {@link org.apache.oozie.client.OozieClient} for the filter syntax.
     * @param start offset, base 1.
     * @param len number of jobs to return.
     * @return job info for all matching jobs, the jobs don't contain node action information.
     * @throws DagEngineException thrown if the jobs info could not be obtained.
     */
    @SuppressWarnings("unchecked")
    public WorkflowsInfo getJobs(String filterStr, int start, int len) throws DagEngineException {
        Map<String, List<String>> filter = parseFilter(filterStr);
        try {
            return new JobsCommand(filter, start, len).call();
        }
        catch (CommandException dce) {
            throw new DagEngineException(dce);
        }
    }

    /**
     * Return the workflow Job ID for an external ID. <p/> This is reverse lookup for recovery purposes.
     *
     * @param externalId external ID provided at job submission time.
     * @return the associated workflow job ID if any, <code>null</code> if none.
     * @throws DagEngineException thrown if the lookup could not be done.
     */
    @Override
    public String getJobIdForExternalId(String externalId) throws DagEngineException {
        try {
            return new ExternalIdCommand(externalId).call();
        }
        catch (CommandException dce) {
            throw new DagEngineException(dce);
        }
    }

    @Override
    public CoordinatorJob getCoordJob(String jobId) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    @Override
    public CoordinatorJob getCoordJob(String jobId, int start, int length) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    public WorkflowActionBean getWorkflowAction(String actionId) throws BaseEngineException {
        try {
            return new WorkflowActionInfoXCommand(actionId).call();
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    @Override
    public String dryrunSubmit(Configuration conf, boolean startJob) throws BaseEngineException {
        return null;
    }
}
