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
package org.apache.oozie.command;

import org.apache.oozie.client.Job;
import org.apache.oozie.util.XLog;

/**
 * Base class for submit transition command.
 */
public abstract class SubmitTransitionXCommand extends TransitionXCommand<String> {
    private String jobId;
    private Job job;

    protected final XLog LOG = XLog.getLog(SubmitTransitionXCommand.class);

    /**
     * @return
     * @throws CommandException
     */
    public abstract String submit() throws CommandException;

    /**
     * @param name
     * @param type
     * @param priority
     */
    public SubmitTransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * @param name
     * @param type
     * @param priority
     * @param dryrun
     */
    public SubmitTransitionXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#transitToNext()
     */
    @Override
    public void transitToNext() {
        job.setStatus(Job.Status.PREP);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected String execute() throws CommandException {
        loadState();
        this.jobId = submit();
        this.job = getJob();
        transitToNext();
        notifyParent();
        return this.jobId;
    }
}