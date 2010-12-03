package org.apache.oozie.command;

import org.apache.oozie.util.XLog;

public abstract class StatusUpdateXCommand extends TransitionXCommand<Void>{

    protected final XLog LOG = XLog.getLog(SubmitTransitionXCommand.class);

    /**
     * @param name
     * @param type
     * @param priority
     */
    public StatusUpdateXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * @param name
     * @param type
     * @param priority
     * @param dryrun
     */
    public StatusUpdateXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }
}
