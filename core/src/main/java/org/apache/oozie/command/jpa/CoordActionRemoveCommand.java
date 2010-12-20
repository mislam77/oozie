package org.apache.oozie.command.jpa;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Update the CoordinatorAction into a Bean and persist it.
 */
public class CoordActionRemoveCommand implements JPACommand<Void> {

    //private CoordinatorActionBean coordAction = null;
    private String coordActionId = null;

    /**
     * @param coordAction
     */
    public CoordActionRemoveCommand(String coordActionId) {
        ParamChecker.notNull(coordActionId, "coordActionId");
        this.coordActionId = coordActionId;
    }

    /*
     * (non-Javadoc)
     *
     * @seeorg.apache.oozie.command.jpa.JPACommand#execute(javax.persistence.
     * EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws CommandException {
        try {
            CoordinatorActionBean action = em.find(CoordinatorActionBean.class, coordActionId);
            if (action != null) {
                em.remove(action);
            }
            else {
                throw new CommandException(ErrorCode.E0605, coordActionId);
            }
            
            return null;
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0603, e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.jpa.JPACommand#getName()
     */
    @Override
    public String getName() {
        return "CoordActionRemoveCommand";
    }
}
