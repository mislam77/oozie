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
package org.apache.oozie.command.coord;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.FsActionExecutor;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.jpa.CoordActionGetCommand;
import org.apache.oozie.command.jpa.CoordJobGetActionForNominalTimeCommand;
import org.apache.oozie.command.jpa.CoordJobGetActionsForDatesCommand;
import org.apache.oozie.command.jpa.CoordJobGetCommand;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbOperations;
import org.jdom.Element;
import org.jdom.JDOMException;

public class CoordRerunXCommand extends CoordinatorXCommand<CoordinatorActionInfo> {

    private String jobId;
    private String rerunType;
    private String scope;
    private boolean refresh;
    private boolean noCleanup;
    private static XLog log = XLog.getLog(CoordRerunXCommand.class);
    private CoordinatorJobBean coordJob = null;
    private JPAService jpaService = null;

    public CoordRerunXCommand(String jobId, String rerunType, String scope, boolean refresh, boolean noCleanup) {
        super("coord_rerun", "coord_rerun", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.rerunType = ParamChecker.notEmpty(rerunType, "rerunType");
        this.scope = ParamChecker.notEmpty(scope, "scope");
        this.refresh = refresh;
        this.noCleanup = noCleanup;
    }

    @Override
    protected CoordinatorActionInfo execute() throws CommandException {
        try {
            CoordinatorActionInfo coordInfo = null;
            incrJobCounter(1);
            List<CoordinatorActionBean> coordActions;
            if (rerunType.equals(RestConstants.JOB_COORD_RERUN_DATE)) {
                coordActions = getCoordActionsFromDates(jobId, scope);
            }
            else if (rerunType.equals(RestConstants.JOB_COORD_RERUN_ACTION)) {
                coordActions = getCoordActionsFromIds(jobId, scope);
            }
            else {
                throw new CommandException(ErrorCode.E1018, "date or action expected.");
            }
            if (checkAllActionsRunnable(coordActions)) {
                for (CoordinatorActionBean coordAction : coordActions) {
                    String actionXml = coordAction.getActionXml();
                    if (!noCleanup) {
                        Element eAction = XmlUtils.parseXml(actionXml);
                        cleanupOutputEvents(eAction, coordJob.getUser(), coordJob.getGroup());
                    }
                    if (refresh) {
                        refreshAction(coordJob, coordAction);
                    }
                    updateAction(coordJob, coordAction, actionXml);

                    // TODO: time 100s should be configurable
                    queue(new CoordActionNotificationXCommand(coordAction), 100);
                    queue(new CoordActionInputCheckXCommand(coordAction.getId()), 100);
                }
            }
            else {
                throw new CommandException(ErrorCode.E1018, "part or all actions are not eligible to rerun!");
            }
            coordInfo = new CoordinatorActionInfo(coordActions);

            return coordInfo;
        }
        catch (XException xex) {
            throw new CommandException(xex);
        }
        catch (JDOMException jex) {
            throw new CommandException(ErrorCode.E0700, jex);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E1018, ex);
        }
    }

    /**
     * Get the list of actions for given id ranges
     *
     * @param jobId
     * @param scope
     * @param store
     * @return the list of all actions to rerun
     * @throws CommandException
     * @throws StoreException
     */
    private List<CoordinatorActionBean> getCoordActionsFromIds(String jobId, String scope)
            throws CommandException, StoreException {
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(scope, "scope");

        Set<String> actions = new HashSet<String>();
        String[] list = scope.split(",");
        for (String s : list) {
            s = s.trim();
            if (s.contains("-")) {
                String[] range = s.split("-");
                if (range.length != 2) {
                    throw new CommandException(ErrorCode.E0302, "format is wrong for action's range '" + s + "'");
                }
                int start;
                int end;
                try {
                    start = Integer.parseInt(range[0].trim());
                    end = Integer.parseInt(range[1].trim());
                    if (start > end) {
                        throw new CommandException(ErrorCode.E0302, "format is wrong for action's range '" + s + "'");
                    }
                }
                catch (NumberFormatException ne) {
                    throw new CommandException(ErrorCode.E0302, ne);
                }
                for (int i = start; i <= end; i++) {
                    actions.add(jobId + "@" + i);
                }
            }
            else {
                try {
                    Integer.parseInt(s);
                }
                catch (NumberFormatException ne) {
                    throw new CommandException(ErrorCode.E0302, "format is wrong for action id'" + s
                            + "'. Integer only.");
                }
                actions.add(jobId + "@" + s);
            }
        }

        List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();
        for (String id : actions) {
            CoordinatorActionBean coordAction = jpaService.execute(new CoordActionGetCommand(id));
            coordActions.add(coordAction);
            log.debug("Rerun coordinator for actionId='" + id + "'");
        }
        return coordActions;
    }

    /**
     * Get the list of actions for given date ranges
     *
     * @param jobId
     * @param scope
     * @param store
     * @return the list of dates to rerun
     * @throws CommandException
     * @throws StoreException
     */
    private List<CoordinatorActionBean> getCoordActionsFromDates(String jobId, String scope)
            throws CommandException, StoreException {
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(scope, "scope");

        Set<CoordinatorActionBean> actionSet = new HashSet<CoordinatorActionBean>();
        String[] list = scope.split(",");
        for (String s : list) {
            s = s.trim();
            if (s.contains("::")) {
                String[] dateRange = s.split("::");
                if (dateRange.length != 2) {
                    throw new CommandException(ErrorCode.E0302, "format is wrong for date's range '" + s + "'");
                }
                Date start;
                Date end;
                try {
                    start = DateUtils.parseDateUTC(dateRange[0].trim());
                    end = DateUtils.parseDateUTC(dateRange[1].trim());
                    if (start.after(end)) {
                        throw new CommandException(ErrorCode.E0302, "start date is older than end date: '" + s + "'");
                    }
                }
                catch (Exception e) {
                    throw new CommandException(ErrorCode.E0302, e);
                }

                List<CoordinatorActionBean> listOfActions = getActionIdsFromDateRange(jobId, start, end);
                actionSet.addAll(listOfActions);
            }
            else {
                Date date;
                try {
                    date = DateUtils.parseDateUTC(s.trim());
                }
                catch (Exception e) {
                    throw new CommandException(ErrorCode.E0302, e);
                }

                CoordinatorActionBean coordAction = jpaService.execute(new CoordJobGetActionForNominalTimeCommand(jobId, date));
                actionSet.add(coordAction);
            }
        }

        List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();
        for (CoordinatorActionBean coordAction : actionSet) {
            coordActions.add(coordAction);
            log.debug("Rerun coordinator for actionId='" + coordAction.getId() + "'");
        }
        return coordActions;
    }

    private List<CoordinatorActionBean> getActionIdsFromDateRange(String jobId, Date start, Date end) throws CommandException {
        List<CoordinatorActionBean> list = jpaService.execute(new CoordJobGetActionsForDatesCommand(jobId, start, end));
        return list;
    }

    /**
     * Check if all given actions are eligible to rerun.
     *
     * @param actions list of CoordinatorActionBean
     * @return true if all actions are eligible to rerun
     */
    private boolean checkAllActionsRunnable(List<CoordinatorActionBean> coordActions) {
        for (CoordinatorActionBean coordAction : coordActions) {
            if (!coordAction.isTerminalStatus()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cleanup output-events directories
     *
     * @param eAction
     * @param workflow
     * @param action
     */
    @SuppressWarnings("unchecked")
    private void cleanupOutputEvents(Element eAction, String user, String group) {
        Element outputList = eAction.getChild("output-events", eAction.getNamespace());
        if (outputList != null) {
            for (Element data : (List<Element>) outputList.getChildren("data-out", eAction.getNamespace())) {
                if (data.getChild("uris", data.getNamespace()) != null) {
                    String uris = data.getChild("uris", data.getNamespace()).getTextTrim();
                    if (uris != null) {
                        String[] uriArr = uris.split(CoordELFunctions.INSTANCE_SEPARATOR);
                        FsActionExecutor fsAe = new FsActionExecutor();
                        for (String uri : uriArr) {
                            Path path = new Path(uri);
                            try {
                                fsAe.delete(user, group, path);
                                log.debug("Cleanup the output dir " + path);
                            }
                            catch (ActionExecutorException ae) {
                                log.warn("Failed to cleanup the output dir " + uri, ae);
                            }
                        }
                    }

                }
            }
        }
        else {
            log.info("No output-events defined in coordinator xml. Therefore nothing to cleanup");
        }
    }

    /**
     * Refresh an Action
     *
     * @param coordJob
     * @param coordAction
     * @param store
     * @throws Exception
     */
    private void refreshAction(CoordinatorJobBean coordJob, CoordinatorActionBean coordAction)
            throws Exception {
        Configuration jobConf = null;
        try {
            jobConf = new XConfiguration(new StringReader(coordJob.getConf()));
        }
        catch (IOException ioe) {
            log.warn("Configuration parse error. read from DB :" + coordJob.getConf(), ioe);
            throw new CommandException(ErrorCode.E1005, ioe);
        }
        String jobXml = coordJob.getJobXml();
        Element eJob = XmlUtils.parseXml(jobXml);
        String actionXml = CoordCommandUtils.materializeOneInstance(jobId, dryrun, (Element) eJob.clone(), coordAction
                .getNominalTime(), coordAction.getActionNumber(), jobConf, coordAction);
        log.debug("Refresh Action actionId=" + coordAction.getId() + ", actionXml="
                + XmlUtils.prettyPrint(actionXml).toString());
        coordAction.setActionXml(actionXml);
    }

    /**
     * Update an Action into database table
     *
     * @param coordJob
     * @param coordAction
     * @param actionXml
     * @param store
     * @throws Exception
     */
    private void updateAction(CoordinatorJobBean coordJob, CoordinatorActionBean coordAction, String actionXml) throws Exception {
        log.debug("updateAction for actionId=" + coordAction.getId());
        coordAction.setStatus(CoordinatorAction.Status.WAITING);
        coordAction.setExternalId("");
        coordAction.setExternalStatus("");
        coordAction.setRerunTime(new Date());
        coordAction.setLastModifiedTime(new Date());
        jpaService.execute(new org.apache.oozie.command.jpa.CoordActionUpdateCommand(coordAction));
        writeActionRegistration(coordAction.getActionXml(), coordAction, coordJob.getUser(), coordJob.getGroup());
    }

    /**
     * Create SLA RegistrationEvent
     *
     * @param actionXml
     * @param actionBean
     * @param store
     * @param user
     * @param group
     * @throws Exception
     */
    private void writeActionRegistration(String actionXml, CoordinatorActionBean actionBean,
            String user, String group)
            throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml);
        Element eSla = eAction.getChild("action", eAction.getNamespace()).getChild("info", eAction.getNamespace("sla"));
        SLADbOperations.writeSlaRegistrationEvent(eSla, actionBean.getId(), SlaAppType.COORDINATOR_ACTION, user,
                group, log);
    }

    @Override
    protected String getEntityKey() {
        return jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        coordJob = jpaService.execute(new CoordJobGetCommand(jobId));
        setLogInfo(coordJob);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordJob.getStatus() == CoordinatorJob.Status.KILLED
                || coordJob.getStatus() == CoordinatorJob.Status.FAILED) {
            log.info("CoordRerunCommand is not able to run, job status=" + coordJob.getStatus() + ", jobid="
                    + jobId);
            throw new CommandException(ErrorCode.E1018,
                    "coordinator job is killed or failed so all actions are not eligible to rerun!");
        }
    }

}
