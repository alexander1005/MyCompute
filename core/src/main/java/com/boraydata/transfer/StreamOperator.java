package com.boraydata.transfer;

import com.boraydata.env.Attribute;
import com.boraydata.out.Output;

import java.io.Serializable;

public interface StreamOperator<OUT> extends Serializable {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * Initializes the operator. Sets access to the context and the output.
     */
    void setup( Output<Attribute<OUT>> output);

    /**
     * This method is called immediately before any elements are processed, it should contain the
     * operator's initialization logic.
     *
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    void open() throws Exception;

    /**
     * This method is called after all records have been added to the operators via the methods*
     * <p>The method is expected to flush all remaining buffered data. Exceptions during this
     * flushing of buffered should be propagated, in order to cause the operation to be recognized
     * as failed, because the last data items are not processed properly.
     *
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    void close() throws Exception;

    /**
     * This method is called at the very end of the operator's life, both in the case of a successful
     * completion of the operation, and in the case of a failure and canceling.
     *
     * <p>This method is expected to make a thorough effort to release all resources
     * that the operator has acquired.
     */
    void dispose() throws Exception;

    // ------------------------------------------------------------------------
    //  state snapshots
    // ------------------------------------------------------------------------


    /**
     * Called when the checkpoint with the given ID is completed and acknowledged on the JobManager.
     *
     * @param checkpointId The ID of the checkpoint that has been completed.
     *
     * @throws Exception Exceptions during checkpoint acknowledgement may be forwarded and will cause
     *                   the program to fail and enter recovery.
     */
    void notifyOfCompletedCheckpoint(long checkpointId) throws Exception;


    Output getOutput();
}
