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
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.SubmitTransitionXCommand;
import org.apache.oozie.command.jpa.BundleJobInsertCommand;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.xml.sax.SAXException;

/**
 * This Command will submit the bundle.
 */
public class BundleSubmitXCommand extends SubmitTransitionXCommand {

    private Configuration conf;
    private final String authToken;
    public static final String CONFIG_DEFAULT = "bundle-config-default.xml";
    public static final String BUNDLE_XML_FILE = "bundle.xml";
    private final XLog log = XLog.getLog(getClass());
    private final BundleJobBean bundleBean = new BundleJobBean();
    private String jobId;
    private JPAService jpaService = null;

    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();

    static {
        String[] badUserProps = { PropertiesUtils.YEAR, PropertiesUtils.MONTH, PropertiesUtils.DAY,
                PropertiesUtils.HOUR, PropertiesUtils.MINUTE, PropertiesUtils.DAYS, PropertiesUtils.HOURS,
                PropertiesUtils.MINUTES, PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB,
                PropertiesUtils.TB, PropertiesUtils.PB, PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN,
                PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN, PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS };
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);

        String[] badDefaultProps = { PropertiesUtils.HADOOP_USER, PropertiesUtils.HADOOP_UGI,
                WorkflowAppService.HADOOP_JT_KERBEROS_NAME, WorkflowAppService.HADOOP_NN_KERBEROS_NAME };
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }

    /**
     * Constructor to create the Bundle Submit Command.
     * 
     * @param conf : Configuration for bundle job
     * @param authToken : To be used for authentication
     */
    public BundleSubmitXCommand(Configuration conf, String authToken) {
        super("coord_submit", "coord_submit", 1);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    /**
     * Constructor to create the bundle submit command.
     * 
     * @param dryrun
     * @param conf
     * @param authToken
     */
    public BundleSubmitXCommand(boolean dryrun, Configuration conf, String authToken) {
        super("coord_submit", "coord_submit", 1);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
        this.dryrun = dryrun;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.SubmitTransitionXCommand#submit()
     */
    @Override
    public String submit() throws CommandException {
        log.info("STARTED Coordinator Submit");
        try {
            incrJobCounter(1);

            XmlUtils.removeComments(this.bundleBean.getOrigJobXml().toString());
            // Resolving all variables in the job properties.
            // This ensures the Hadoop Configuration semantics is preserved.
            XConfiguration resolvedVarsConf = new XConfiguration();
            for (Map.Entry<String, String> entry : conf) {
                resolvedVarsConf.set(entry.getKey(), conf.get(entry.getKey()));
            }
            conf = resolvedVarsConf;

            this.jobId = storeToDB(bundleBean);

            if (dryrun) {
                Date startTime = bundleBean.getStartTime();
                long startTimeMilli = startTime.getTime();
                long endTimeMilli = startTimeMilli + (3600 * 1000);
                Date jobEndTime = bundleBean.getEndTime();
                Date endTime = new Date(endTimeMilli);
                if (endTime.compareTo(jobEndTime) > 0) {
                    endTime = jobEndTime;
                }
                jobId = bundleBean.getId();
                log.info("[" + jobId + "]: Update status to PREMATER");
                bundleBean.setStatus(Job.Status.PREP);
                try {
                    new XConfiguration(new StringReader(bundleBean.getConf()));
                }
                catch (IOException e1) {
                    log.warn("Configuration parse error. read from DB :" + bundleBean.getConf(), e1);
                }
                String output = bundleBean.getJobXml() + System.getProperty("line.separator");
                return output;
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E1004, "Validation ERROR :", ex.getMessage(), ex);
        }
        return this.jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        super.eagerLoadState();
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        try {
            super.eagerVerifyPrecondition();
            mergeDefaultConfig();
            String appXml = readAndValidateXml();
            this.bundleBean.setOrigJobXml(appXml);
            log.debug("jobXml after initial validation " + XmlUtils.prettyPrint(appXml).toString());
        }
        catch (BundleJobException ex) {
            log.warn("ERROR:  ", ex);
            throw new CommandException(ex);
        }
        catch (IllegalArgumentException iex) {
            log.warn("ERROR:  ", iex);
            throw new CommandException(ErrorCode.E1003, iex);
        }
        catch (Exception ex) {
            log.warn("ERROR:  ", ex);
            throw new CommandException(ErrorCode.E0803, ex);
        }
    }

    /**
     * Merge default configuration with user-defined configuration.
     * 
     * @throws CommandException
     */
    protected void mergeDefaultConfig() throws CommandException {
        Path configDefault = new Path(conf.get(OozieClient.BUNDLE_APP_PATH), CONFIG_DEFAULT);
        CoordUtils.getHadoopConf(conf);
        FileSystem fs;
        // TODO: which conf?
        try {
            String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
            String group = ParamChecker.notEmpty(conf.get(OozieClient.GROUP_NAME), OozieClient.GROUP_NAME);
            fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group, configDefault.toUri(),
                    new Configuration());
            if (fs.exists(configDefault)) {
                Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                XConfiguration.injectDefaults(defaultConf, conf);
            }
            else {
                log.info("configDefault Doesn't exist " + configDefault);
            }
            PropertiesUtils.checkDisallowedProperties(conf, DISALLOWED_USER_PROPERTIES);
        }
        catch (IOException e) {
            throw new CommandException(ErrorCode.E0702, e.getMessage() + " : Problem reading default config "
                    + configDefault, e);
        }
        catch (HadoopAccessorException e) {
            throw new CommandException(e);
        }
        log.debug("Merged CONF :" + XmlUtils.prettyPrint(conf).toString());
    }

    /**
     * Read the application XML and validate against bundle Schema
     * 
     * @return validated bundle XML
     * @throws BundleJobException
     */
    private String readAndValidateXml() throws BundleJobException {
        String appPath = ParamChecker.notEmpty(conf.get(OozieClient.BUNDLE_APP_PATH), OozieClient.BUNDLE_APP_PATH);// TODO:
        String bundleXml = readDefinition(appPath);
        validateXml(bundleXml);
        return bundleXml;
    }

    /**
     * Read bundle definition.
     * 
     * @param appPath application path.
     * @param user user name.
     * @param group group name.
     * @param autToken authentication token.
     * @return bundle definition.
     * @throws BundleJobException thrown if the definition could not be read.
     */
    protected String readDefinition(String appPath) throws BundleJobException {
        String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        String group = ParamChecker.notEmpty(conf.get(OozieClient.GROUP_NAME), OozieClient.GROUP_NAME);
        Configuration confHadoop = CoordUtils.getHadoopConf(conf);
        try {
            URI uri = new URI(appPath);
            log.debug("user =" + user + " group =" + group);
            FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group, uri,
                    new Configuration());
            Path p = new Path(uri.getPath());

            Reader reader = new InputStreamReader(fs.open(p));// TODO
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            return writer.toString();
        }
        catch (IOException ex) {
            log.warn("IOException :" + XmlUtils.prettyPrint(confHadoop), ex);
            throw new BundleJobException(ErrorCode.E1301, ex.getMessage(), ex); // TODO:
        }
        catch (URISyntaxException ex) {
            log.warn("URISyException :" + ex.getMessage());
            throw new BundleJobException(ErrorCode.E1302, appPath, ex.getMessage(), ex);// TODO:
        }
        catch (HadoopAccessorException ex) {
            throw new BundleJobException(ex);
        }
        catch (Exception ex) {
            log.warn("Exception :", ex);
            throw new BundleJobException(ErrorCode.E1301, ex.getMessage(), ex);// TODO:
        }
    }

    /**
     * Validate against Bundle XSD file
     * 
     * @param xmlContent : Input Bundle xml
     * @throws BundleJobException
     */
    private void validateXml(String xmlContent) throws BundleJobException {
        javax.xml.validation.Schema schema = Services.get().get(SchemaService.class).getSchema(SchemaName.BUNDLE);
        Validator validator = schema.newValidator();
        try {
            validator.validate(new StreamSource(new StringReader(xmlContent)));
        }
        catch (SAXException ex) {
            log.warn("SAXException :", ex);
            throw new BundleJobException(ErrorCode.E0701, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            // ex.printStackTrace();
            log.warn("IOException :", ex);
            throw new BundleJobException(ErrorCode.E0702, ex.getMessage(), ex);
        }
    }

    /**
     * Write a Bundle Job into database
     * 
     * @param : Bundle job bean
     * @return Job if.
     * @throws CommandException
     */
    private String storeToDB(BundleJobBean bundleJob) throws CommandException {
        try {
            String jobId = Services.get().get(UUIDService.class).generateId(ApplicationType.BUNDLE);
            bundleJob.setId(jobId);
            bundleJob.setAuthToken(this.authToken);
            bundleJob.setAppName(XmlUtils.parseXml(this.bundleBean.getOrigJobXml()).getAttributeValue("name"));
            bundleJob.setAppName(bundleJob.getAppName());
            bundleJob.setAppPath(conf.get(OozieClient.BUNDLE_APP_PATH));
            bundleJob.setStatus(BundleJob.Status.PREP);
            bundleJob.setCreatedTime(new Date()); // TODO: Do we need that?
            bundleJob.setUser(conf.get(OozieClient.USER_NAME));
            bundleJob.setGroup(conf.get(OozieClient.GROUP_NAME));
            bundleJob.setConf(XmlUtils.prettyPrint(conf).toString());
            bundleJob.setJobXml(XmlUtils.prettyPrint(conf).toString());
            bundleJob.setLastModifiedTime(new Date());

            if (!dryrun) {
                bundleJob.setLastModifiedTime(new Date());
                jpaService.execute(new BundleJobInsertCommand(bundleJob));
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E1301, ex.getMessage(), ex);
        }
        return jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleBean;
    }
}