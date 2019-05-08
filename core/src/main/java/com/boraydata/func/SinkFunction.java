package com.boraydata.func;

import java.io.Serializable;

public interface SinkFunction<IN> extends Function, Serializable {

    /**
     * @deprecated Use {@link #invoke(Object, Context)}.
     */
    @Deprecated
    default void invoke(IN value) throws Exception {}

    public void open();

    /**
     * Writes the given value to the sink. This function is called for every record.
     *
     * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
     * {@code default} method for backward compatibility with the old-style method only.
     *
     * @param value The input record.
     * @param context Additional context about the input record.
     *
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
     *                   to fail and may trigger recovery.
     */
    default void invoke(IN value, Context context) throws Exception {
        invoke(value);
    }

    /**
     * Context that {@link SinkFunction SinkFunctions } can use for getting additional data about
     * an input record.
     *
     * <p>The context is only valid for the duration of a
     * {@link SinkFunction#invoke(Object, Context)} call. Do not store the context and use
     * afterwards!
     *
     * @param <T> The type of elements accepted by the sink.
     */
    interface Context<T> {

        /** Returns the current processing time. */
        long currentProcessingTime();

        /** Returns the current event-time watermark. */
        long currentWatermark();

        /**
         * Returns the timestamp of the current input record or {@code null} if the element does not
         * have an assigned timestamp.
         */
        Long timestamp();
    }
}
