package org.apache.oozie.command.bundle;

import java.util.Date;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.StatusUpdateXCommand;
import org.apache.oozie.command.jpa.BundleActionGetCommand;
import org.apache.oozie.command.jpa.BundleActionUpdateCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

public class BundleStatusUpdateXCommand extends StatusUpdateXCommand {
    private final CoordinatorJobBean coordjob;
    private JPAService jpaService = null;
    private BundleActionBean bundleaction;
    private final Job.Status prevStatus;

    /**
     * @param coordjob
     * @param prevStatus
     */
    public BundleStatusUpdateXCommand(CoordinatorJobBean coordjob, CoordinatorJob.Status prevStatus) {
        super("BundleStatusUpdate", "BundleStatusUpdate", 1);
        this.coordjob = coordjob;
        this.prevStatus = convertCoordStatustoJob(prevStatus);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#setJob(org.apache.oozie.client.Job)
     */
    @Override
    public void setJob(Job job) {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#transitToNext()
     */
    @Override
    public void transitToNext() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        try {
            Job.Status coordCurrentStatus = convertCoordStatustoJob(coordjob.getStatus());
            bundleaction.setStatus(coordCurrentStatus);

            if (bundleaction.isPending()) {
                bundleaction.setPending(bundleaction.getPending() - 1);
            }
            bundleaction.setLastModifiedTime(new Date());
            bundleaction.setCoordId(coordjob.getId());
            jpaService.execute(new BundleActionUpdateCommand(bundleaction));
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E1309, bundleaction.getBundleId(), bundleaction.getCoordName());
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return coordjob.getBundleId();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
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
        try {
            super.eagerLoadState();
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.bundleaction = jpaService.execute(new BundleActionGetCommand(coordjob.getBundleId(), coordjob
                        .getAppName()));
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        super.eagerVerifyPrecondition();
        if (bundleaction.getStatusStr().compareToIgnoreCase(prevStatus.toString()) != 0) {
            // Previous status are not matched with bundle action status
            // So that's the error and we should no be updating the Bundle Action
            throw new PreconditionException(ErrorCode.E1308, bundleaction.getStatusStr(), prevStatus.toString());
        }

    }

    /**
     * @param coordStatus
     * @return
     */
    public static Job.Status convertCoordStatustoJob(CoordinatorJob.Status coordStatus) {
        if (coordStatus == CoordinatorJob.Status.PREMATER) {
            coordStatus = CoordinatorJob.Status.RUNNING;
        }

        for (Job.Status js : Job.Status.values()) {
            if (coordStatus.toString().compareToIgnoreCase(js.toString()) == 0) {
                return js;
            }
        }
        return null;
    }
}
