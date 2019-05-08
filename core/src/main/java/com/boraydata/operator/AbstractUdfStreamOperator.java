package com.boraydata.operator;

import com.boraydata.conf.Configuration;
import com.boraydata.env.Attribute;
import com.boraydata.env.ExecutionConfig;
import com.boraydata.func.Function;
import com.boraydata.out.Output;
import com.boraydata.utils.ChainingStrategy;

import static java.util.Objects.requireNonNull;

public abstract class AbstractUdfStreamOperator<OUT, F extends Function> {

    private static final long serialVersionUID = 1L;

    // A sane default for most operators
    protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

    /** The user function. */
    protected final F userFunction;

    /** Flag to prevent duplicate function.close() calls in close() and dispose(). */
    private transient boolean functionsClosed = false;

    public AbstractUdfStreamOperator(F userFunction) {
        this.userFunction = requireNonNull(userFunction);
        checkUdfCheckpointingPreconditions();
    }
    protected transient Output<Attribute<OUT>> output;

    /**
     * Gets the user function executed in this operator.
     * @return The user function of this operator.
     */
    public F getUserFunction() {
        return userFunction;
    }

    // ------------------------------------------------------------------------
    //  operator life cycle
    // ------------------------------------------------------------------------


    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Since the streaming API does not implement any parametrization of functions via a
     * configuration, the config returned here is actually empty.
     *
     * @return The user function parameters (currently empty)
     */
    public Configuration getUserFunctionParameters() {
        return new Configuration();
    }

    private void checkUdfCheckpointingPreconditions() {

//        if (userFunction instanceof CheckpointedFunction
//                && userFunction instanceof ListCheckpointed) {
//
//            throw new IllegalStateException("User functions are not allowed to implement " +
//                    "CheckpointedFunction AND ListCheckpointed.");
//        }
    }
}
