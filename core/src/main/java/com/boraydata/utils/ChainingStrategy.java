package com.boraydata.utils;

public enum ChainingStrategy {

    /**
     * Operators will be eagerly chained whenever possible.
     *
     * <p>To optimize performance, it is generally a good practice to allow maximal
     * chaining and increase operator parallelism.
     */
    ALWAYS,

    /**
     * The operator will not be chained to the preceding or succeeding operators.
     */
    NEVER,

    /**
     * The operator will not be chained to the predecessor, but successors may chain to this
     * operator.
     */
    HEAD
}
