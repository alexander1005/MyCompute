package com.boraydata.func;

import java.io.Serializable;


public interface SourceFunction<T> extends Function, Serializable {

    /**
     * Starts the source. Implementations can use the {@link SourceContext} emit
     * elements.
     *
     * <p>Sources that implement {@link }
     * must lock on the checkpoint lock (using a synchronized block) before updating internal
     * state and emitting elements, to make both an atomic operation:
     *
     * <pre>{@code
     *  public class ExampleSource<T> implements SourceFunction<T>, Checkpointed<Long> {
     *      private long count = 0L;
     *      private volatile boolean isRunning = true;
     *
     *      public void run(SourceContext<T> ctx) {
     *          while (isRunning && count < 1000) {
     *              synchronized (ctx.getCheckpointLock()) {
     *                  ctx.collect(count);
     *                  count++;
     *              }
     *          }
     *      }
     *
     *      public void cancel() {
     *          isRunning = false;
     *      }
     *
     *      public Long snapshotState(long checkpointId, long checkpointTimestamp) { return count; }
     *
     *      public void restoreState(Long state) { this.count = state; }
     * }
     * }</pre>
     *
     * @param ctx The context to emit elements to and for accessing locks.
     */
    void run(SourceContext<T> ctx) throws Exception;

    /**
     * Cancels the source. Most sources will have a while loop inside the
     * {@link #run(SourceContext)} method. The implementation needs to ensure that the
     * source will break out of that loop after this method is called.
     *
     * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
     * {@code false} in this method. That flag is checked in the loop condition.
     *
     * <p>When a source is canceled, the executing thread will also be interrupted
     * (via {@link Thread#interrupt()}). The interruption happens strictly after this
     * method has been called, so any interruption handler can rely on the fact that
     * this method has completed. It is good practice to make any flags altered by
     * this method "volatile", in order to guarantee the visibility of the effects of
     * this method to any interruption handler.
     */
    void cancel();

    void open();

    // ------------------------------------------------------------------------
    //  source context
    // ------------------------------------------------------------------------

    /**
     * Interface that source functions use to emit elements, and possibly watermarks.
     *
     * @param <T> The type of the elements produced by the source.
     */
    // Interface might be extended in the future with additional methods.
    interface SourceContext<T> {

        /**
         * Emits one element from the source, without attaching a timestamp. In most cases,
         * this is the default way of emitting elements.
         *
         * <p>The timestamp that the element will get assigned depends on the time characteristic of
         * the streaming program:
         * <ul>
         *         operation (like time windows).</li>
         * </ul>
         *
         * @param element The element to emit
         */
        void collect(T element);

        /**
         * Emits one element from the source, and attaches the given timestamp. This method
         * on the stream.
         *
         * <p>On certain time characteristics, this timestamp may be ignored or overwritten.
         * This allows programs to switch between the different time characteristics and behaviors
         * without changing the code of the source functions.
         * <ul>

         * </ul>
         *
         * @param element The element to emit
         * @param timestamp The timestamp in milliseconds since the Epoch
         */
        void collectWithTimestamp(T element, long timestamp);

        /**
         */
        void markAsTemporarilyIdle();

        /**
         * Returns the checkpoint lock. Please refer to the class-level comment in
         * {@link SourceFunction} for details about how to write a consistent checkpointed
         * source.
         *
         * @return The object to use as the lock
         */
        Object getCheckpointLock();

        /**
         * This method is called by the system to shut down the context.
         */
        void close();
    }
}
