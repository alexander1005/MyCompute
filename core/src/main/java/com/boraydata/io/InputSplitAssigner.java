package com.boraydata.io;

public interface InputSplitAssigner {

    /**
     * Returns the next input split that shall be consumed. The consumer's host is passed as a parameter
     * to allow localized assignments.
     *
     * @param host The host address of split requesting task.
     * @param taskId The id of the split requesting task.
     * @return the next input split to be consumed, or <code>null</code> if no more splits remain.
     */
    InputSplit getNextInputSplit(String host, int taskId);

}
