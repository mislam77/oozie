package org.apache.oozie.command.coord;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.jpa.CoordActionRemoveCommand;
import org.apache.oozie.command.jpa.CoordJobGetActionByActionNumberCommand;
import org.apache.oozie.command.jpa.CoordJobGetCommand;
import org.apache.oozie.command.jpa.CoordJobUpdateCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class CoordChangeXCommand extends CoordinatorXCommand<Void> {
    private final String jobId;
    private Date newEndTime = null;
    private Integer newConcurrency = null;
    private Date newPauseTime = null;
    private boolean resetPauseTime = false;
    private final XLog LOG = XLog.getLog(CoordChangeXCommand.class);
    private CoordinatorJobBean coordJob;
    private JPAService jpaService = null;
    
    private static final Set<String> ALLOWED_CHANGE_OPTIONS = new HashSet<String>();
    static {
        ALLOWED_CHANGE_OPTIONS.add("endtime");
        ALLOWED_CHANGE_OPTIONS.add("concurrency");
        ALLOWED_CHANGE_OPTIONS.add("pausetime");
    }

    /**
     * Update the coordinator job bean and update that to database.
     *
     * @param id
     * @param changeValue
     * @throws CommandException
     */
    public CoordChangeXCommand(String id, String changeValue) throws CommandException {
        super("coord_change", "coord_change", 0);
        this.jobId = ParamChecker.notEmpty(id, "id");
        ParamChecker.notEmpty(changeValue, "value");
        
        validateChangeValue(changeValue);
    }

    /**
     * @param changeValue change value.
     * @throws CommandException thrown if changeValue cannot be parsed properly.
     */
    private void validateChangeValue(String changeValue) throws CommandException {
        Map<String, String> map = JobUtils.parseChangeValue(changeValue);

        if (map.size() > ALLOWED_CHANGE_OPTIONS.size()) {
            throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
        }

        java.util.Iterator<Entry<String, String>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            String key = entry.getKey();
            String value = entry.getValue();

            if (!ALLOWED_CHANGE_OPTIONS.contains(key)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "must change endtime|concurrency|pausetime");
            }

            if (!key.equals(OozieClient.CHANGE_VALUE_PAUSETIME) && value.equalsIgnoreCase("")) {
                throw new CommandException(ErrorCode.E1015, changeValue, "value on " + key + " can not be empty");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_ENDTIME)) {
            String value = map.get(OozieClient.CHANGE_VALUE_ENDTIME);
            try {
                newEndTime = DateUtils.parseDateUTC(value);
            }
            catch (Exception ex) {
                throw new CommandException(ErrorCode.E1015, value, "must be a valid date");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_CONCURRENCY)) {
            String value = map.get(OozieClient.CHANGE_VALUE_CONCURRENCY);
            try {
                newConcurrency = Integer.parseInt(value);
            }
            catch (NumberFormatException ex) {
                throw new CommandException(ErrorCode.E1015, value, "must be a valid integer");
            }
        }

        if (map.containsKey(OozieClient.CHANGE_VALUE_PAUSETIME)) {
            String value = map.get(OozieClient.CHANGE_VALUE_PAUSETIME);
            if (value.equals("")) { // this is to reset pause time to null;
                resetPauseTime = true;
            }
            else {
                try {
                    newPauseTime = DateUtils.parseDateUTC(value);
                }
                catch (Exception ex) {
                    throw new CommandException(ErrorCode.E1015, value, "must be a valid date");
                }
            }
        }
    }

    /**
     * @param coordJob coordinator job id.
     * @param newEndTime new end time.
     * @throws CommandException thrown if new end time is not valid.
     */
    private void checkEndTime(CoordinatorJobBean coordJob, Date newEndTime) throws CommandException {
        // New endTime cannot be before coordinator job's start time.
        Date startTime = coordJob.getStartTime();
        if (newEndTime.before(startTime)) {
            throw new CommandException(ErrorCode.E1015, newEndTime, "cannot be before coordinator job's start time ["
                    + startTime + "]");
        }

        // New endTime cannot be before coordinator job's last action time.
        Date lastActionTime = coordJob.getLastActionTime();
        if (lastActionTime != null) {
            Date d = new Date(lastActionTime.getTime() - coordJob.getFrequency() * 60 * 1000);
            if (!newEndTime.after(d)) {
                throw new CommandException(ErrorCode.E1015, newEndTime,
                        "must be after coordinator job's last action time [" + d + "]");
            }
        }
    }

    /**
     * @param coordJob coordinator job id.
     * @param newPauseTime new pause time.
     * @param newEndTime new end time, can be null meaning no change on end time.
     * @throws CommandException thrown if new pause time is not valid.
     */
    private void checkPauseTime(CoordinatorJobBean coordJob, Date newPauseTime)
            throws CommandException {
        // New pauseTime has to be a non-past time.
        Date d = new Date();
        if (newPauseTime.before(d)) {
            throw new CommandException(ErrorCode.E1015, newPauseTime, "must be a non-past time");            
        }
    }
    
    /**
     * Process lookahead created actions that become invalid because of the new pause time,
     * These actions will be deleted from DB, also the coordinator job will be updated accordingly
     * 
     * @param coordJob
     * @param newPauseTime
     */
    private void processLookaheadActions(CoordinatorJobBean coordJob, Date newPauseTime) throws CommandException {
        Date lastActionTime = coordJob.getLastActionTime();
        if (lastActionTime != null) {
            // d is the real last action time.
            Date d = new Date(lastActionTime.getTime() - coordJob.getFrequency() * 60 * 1000);
            int lastActionNumber = coordJob.getLastActionNumber();
            
            boolean hasChanged = false;
            while (true) {
                if (!newPauseTime.after(d)) {
                    deleteAction(coordJob.getId(), lastActionNumber);
                    d = new Date(d.getTime() - coordJob.getFrequency() * 60 * 1000);
                    lastActionNumber = lastActionNumber - 1;
                    
                    hasChanged = true;
                }
                else {
                    break;
                }
            }
            
            if (hasChanged == true) {
                coordJob.setLastActionNumber(lastActionNumber);
                Date d1 = new Date(d.getTime() + coordJob.getFrequency() * 60 * 1000);
                coordJob.setLastActionTime(d1);
                coordJob.setNextMaterializedTime(d1);
                
                if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED) {
                    coordJob.setStatus(CoordinatorJob.Status.RUNNING);
                }
            }
        }
    }

    /**
     * delete last action for a coordinator job
     * @param coordJob
     * @param lastActionNum
     */
    private void deleteAction(String jobId, int lastActionNum) throws CommandException {
        CoordinatorActionBean actionBean = jpaService.execute(new CoordJobGetActionByActionNumberCommand(jobId, lastActionNum));
        jpaService.execute(new CoordActionRemoveCommand(actionBean.getId()));
    }

    /**
     * @param coordJob coordinator job id.
     * @param newEndTime new end time.
     * @param newConcurrency new concurrency.
     * @param newPauseTime new pause time.
     * @throws CommandException thrown if new values are not valid.
     */
    private void check(CoordinatorJobBean coordJob, Date newEndTime, Integer newConcurrency, Date newPauseTime)
            throws CommandException {
        if (coordJob.getStatus() == CoordinatorJob.Status.KILLED) {
            throw new CommandException(ErrorCode.E1016);
        }

        if (newEndTime != null) {
            checkEndTime(coordJob, newEndTime);
        }

        if (newPauseTime != null) {
            checkPauseTime(coordJob, newPauseTime);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED CoordChangeXCommand for jobId=" + jobId);
        
        try {
            setLogInfo(this.coordJob);

            if (newEndTime != null) {
                this.coordJob.setEndTime(newEndTime);
                if (this.coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED) {
                    this.coordJob.setStatus(CoordinatorJob.Status.RUNNING);
                }
            }

            if (newConcurrency != null) {
                this.coordJob.setConcurrency(newConcurrency);
            }

            if (newPauseTime != null || resetPauseTime == true) {
                this.coordJob.setPauseTime(newPauseTime);
                if (!resetPauseTime) {
                    processLookaheadActions(coordJob, newPauseTime);
                }
            }

            incrJobCounter(1);

            jpaService.execute(new CoordJobUpdateCommand(this.coordJob));

            return null;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
        finally {
            LOG.info("ENDED CoordChangeXCommand for jobId=" + jobId);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return this.jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException{
         jpaService = Services.get().get(JPAService.class);

        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);            
        }
        
        this.coordJob = jpaService.execute(new CoordJobGetCommand(jobId));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException,PreconditionException {
        check(this.coordJob, newEndTime, newConcurrency, newPauseTime);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }
}