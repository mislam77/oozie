package org.apache.oozie.command.bundle;

import java.util.Date;
import java.util.List;

import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.RerunTransitionXCommand;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.command.jpa.BundleActionUpdateCommand;
import org.apache.oozie.command.jpa.BundleActionsGetCommand;
import org.apache.oozie.command.jpa.BundleJobGetCommand;
import org.apache.oozie.command.jpa.BundleJobUpdateCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;

public class BundleRerunXCommand extends RerunTransitionXCommand<BundleJobInfo> {

    protected final XLog LOG = XLog.getLog(BundleRerunXCommand.class);
    private String rerunType;
    private String scope;
    private boolean refresh;
    private boolean noCleanup;
    private BundleJobBean bundleJob;
    private List<BundleActionBean> bundleActions;

    private JPAService jpaService = null;

    public BundleRerunXCommand(String jobId, String rerunType, String scope, boolean refresh, boolean noCleanup) {
        super("bundle_rerun", "bundle_rerun", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.rerunType = ParamChecker.notEmpty(rerunType, "rerunType");
        this.scope = ParamChecker.notEmpty(scope, "scope");
        this.refresh = refresh;
        this.noCleanup = noCleanup;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.bundleJob = jpaService.execute(new BundleJobGetCommand(jobId));
                this.bundleActions = jpaService.execute(new BundleActionsGetCommand(jobId));
                setLogInfo(bundleJob);
                super.setJob(bundleJob);
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
    public void RerunChildren() throws CommandException {
        if (bundleActions != null) {
            for (BundleActionBean action : bundleActions) {
                if (action.getCoordId() != null) {
                    LOG.debug("Queuing rerun for coord id :" + action.getCoordId());
                    queue(new CoordRerunXCommand(action.getCoordId(), rerunType, scope, refresh, noCleanup));
                }
                else {
                    LOG.info("Rerun for bundle=[{0}] NOT performed because there is no coordinator=[{1}]", jobId,
                            action.getBundleActionId());
                }
                updateBundleAction(action);
            }
        }
        LOG.info("Rerun coord jobs for the bundle=[{0}]", jobId);
    }

    /**
     * Update bundle action
     * 
     * @param action
     * @throws CommandException
     */
    private void updateBundleAction(BundleActionBean action) throws CommandException {
        action.incrementAndGetPending();
        action.setLastModifiedTime(new Date());
        jpaService.execute(new BundleActionUpdateCommand(action));
    }

    @Override
    public void updateJob() throws CommandException {
        jpaService.execute(new BundleJobUpdateCommand(bundleJob));

    }

    @Override
    protected String getEntityKey() {
        return jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleJob;
    }

    @Override
    public void notifyParent() throws CommandException {
        // TODO Auto-generated method stub

    }

    @Override
    public XLog getLog() {
        return LOG;
    }

}
