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

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.CoordinatorXEngine;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.StartTransitionXCommand;
import org.apache.oozie.command.jpa.BundleActionGetCommand;
import org.apache.oozie.command.jpa.BundleActionInsertCommand;
import org.apache.oozie.command.jpa.BundleActionUpdateCommand;
import org.apache.oozie.command.jpa.BundleJobGetCommand;
import org.apache.oozie.command.jpa.BundleJobUpdateCommand;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;

public class BundleStartXCommand extends StartTransitionXCommand {
    private final String jobId;
    private BundleJobBean bundleJob;
    private JPAService jpaService = null;

    public BundleStartXCommand(String jobId) {
        super("bundle_start", "bundle_start", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    public BundleStartXCommand(String jobId, boolean dryrun) {
        super("bundle_start", "bundle_start", 1, dryrun);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
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
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    public void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.bundleJob = jpaService.execute(new BundleJobGetCommand(jobId));
                setLogInfo(bundleJob);
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    public void StartChildren() throws CommandException {
        LOG.debug("Started coord jobs for the bundle=[{0}]", jobId);
        insertBundleActions();
        startCoordJobs();
        LOG.debug("Ended coord jobs for the bundle=[{0}]", jobId);
    }

    @Override
    public void notifyParent() {
    }

    @SuppressWarnings("unchecked")
    private void insertBundleActions() throws CommandException {
        if (bundleJob != null) {
            Map<String, Boolean> map = new HashMap<String, Boolean>();
            try {
                Element bAppXml = XmlUtils.parseXml(bundleJob.getJobXml());
                List<Element> coordElems = bAppXml.getChildren("coordinator", bAppXml.getNamespace());
                for (Element elem : coordElems) {
                    Attribute name = elem.getAttribute("name");
                    Attribute critical = elem.getAttribute("critical");
                    if (name != null) {
                        if (map.containsKey(name.getValue())) {
                            throw new CommandException(ErrorCode.E1304, name);
                        }
                        boolean isCritical = false;
                        if (Boolean.parseBoolean(critical.getValue())) {
                            isCritical = true;
                        }
                        map.put(name.getValue(), isCritical);
                    }
                    else {
                        throw new CommandException(ErrorCode.E1305);
                    }
                }
            }
            catch (JDOMException jex) {
                throw new CommandException(ErrorCode.E1301, jex);
            }

            for (Entry<String, Boolean> coordName : map.entrySet()) {
                BundleActionBean action = createBundleAction(jobId, coordName.getKey(), coordName.getValue());
                jpaService.execute(new BundleActionInsertCommand(action));
            }

        }
        else {
            throw new CommandException(ErrorCode.E0604, jobId);
        }
    }

    private BundleActionBean createBundleAction(String jobId, String coordName, boolean isCritical) {
        BundleActionBean action = new BundleActionBean();
        action.setBundleId(jobId);
        action.setCoordName(coordName);
        if (isCritical) {
            action.setCritical();
        } else {
            action.resetCritical();
        }
        return action;
    }

    @SuppressWarnings("unchecked")
    private void startCoordJobs() throws CommandException {
        if (bundleJob != null) {
            try {
                Element bAppXml = XmlUtils.parseXml(bundleJob.getJobXml());
                List<Element> coordElems = bAppXml.getChildren("coordinator", bAppXml.getNamespace());
                for (Element coordElem : coordElems) {
                    Attribute name = coordElem.getAttribute("name");
                    Configuration coordConf = mergeConfig(coordElem);
                    coordConf.set(OozieClient.BUNDLE_ID, jobId);
                    //TODO change to queue CoordSubmitCmd
                    CoordinatorXEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorXEngine(
                            bundleJob.getUser(), bundleJob.getAuthToken());

                    String coordId;
                    if (dryrun) {
                        coordId = coordEngine.dryrunSubmit(coordConf, true);
                    }
                    else {
                        coordId = coordEngine.submitJob(coordConf, true);
                    }
                    updateBundleAction(name.getValue(), coordId);
                }
            }
            catch (JDOMException jex) {
                throw new CommandException(ErrorCode.E1301, jex);
            }
            catch (CoordinatorEngineException ce) {
                throw new CommandException(ErrorCode.E1019, ce);
            }
        } else {
            throw new CommandException(ErrorCode.E0604, jobId);
        }
    }

    private void updateBundleAction(String coordName, String coordId) throws CommandException {
        BundleActionBean action = jpaService.execute(new BundleActionGetCommand(jobId, coordName));
        action.setCoordId(coordId);
        action.setStatus(getChildStatus());
        if(isChildPending()){
            action.setPending();
        }else{
            action.resetPending();
        }

        jpaService.execute(new BundleActionUpdateCommand(action));
    }

    /**
     * Merge Bundle job config and the configuration from the coord job to pass to Coord Engine
     *
     * @param coordElem
     * @return Configuration
     * @throws CommandException
     */
    private Configuration mergeConfig(Element coordElem) throws CommandException {
        String jobConf = bundleJob.getConf();
        // Step 1: runConf = jobConf
        Configuration runConf = null;
        try {
            runConf = new XConfiguration(new StringReader(jobConf));
        }
        catch (IOException e1) {
            LOG.warn("Configuration parse error in:" + jobConf);
            throw new CommandException(ErrorCode.E1306, e1.getMessage(), e1);
        }
        // Step 2: Merge local properties into runConf
        // extract 'property' tags under 'configuration' block in the coordElem
        // convert Element to XConfiguration
        Element localConfigElement = coordElem.getChild("configuration", coordElem.getNamespace());

        if (localConfigElement != null) {
            String strConfig = XmlUtils.prettyPrint(localConfigElement).toString();
            Configuration localConf;
            try {
                localConf = new XConfiguration(new StringReader(strConfig));
            }
            catch (IOException e1) {
                LOG.warn("Configuration parse error in:" + strConfig);
                throw new CommandException(ErrorCode.E1307, e1.getMessage(), e1);
            }

            // copy configuration properties in the coordElem to the runConf
            XConfiguration.copy(localConf, runConf);
        }

        // Step 3: Extract value of 'app-path' in coordElem, and save it as a
        // new property called 'oozie.coord.application.path'
        String appPath = coordElem.getChild("app-path", coordElem.getNamespace()).getValue();
        runConf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        return runConf;
    }

    @Override
    public Job getJob() {
        return bundleJob;
    }

    @Override
    public void setJob(Job job) {
        this.bundleJob = (BundleJobBean) job;
    }

    @Override
    public void updateJob() throws CommandException {
        jpaService.execute(new BundleJobUpdateCommand(bundleJob));
    }
}
