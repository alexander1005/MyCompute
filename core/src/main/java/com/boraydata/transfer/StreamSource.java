package com.boraydata.transfer;


import com.boraydata.env.Attribute;
import com.boraydata.func.SourceFunction;
import com.boraydata.out.Output;


public class StreamSource<OUT, SRC extends SourceFunction<OUT>>
         implements StreamOperator<OUT> {

    private static final long serialVersionUID = 1L;

    private final SRC userFunction;
    private transient SourceFunction.SourceContext<OUT> ctx;
    private transient volatile boolean canceledOrStopped = false;
    private Output<Attribute<OUT>> output;

    public StreamSource(SRC sourceFunction) {
        this.userFunction = sourceFunction;

    }

    public void run() throws Exception {

        SourceFunction.SourceContext<OUT> ctx = new SourceFunction.SourceContext<OUT>() {

            @Override
            public void collect(OUT element) {

                if (element!=null){

                    output.collect(new Attribute<>(element));
                }
            }

            @Override
            public void collectWithTimestamp(OUT element, long timestamp) {

            }

            @Override
            public void markAsTemporarilyIdle() {

            }

            @Override
            public Object getCheckpointLock() {

                return null;
            }

            @Override
            public void close() {
                userFunction.cancel();
            }
        };
        userFunction.run(ctx);
    }



    public void cancel() {
        // important: marking the source as stopped has to happen before the function is stopped.
        // the flag that tracks this status is volatile, so the memory model also guarantees
        // the happens-before relationship
        markCanceledOrStopped();
        userFunction.cancel();

        // the context may not be initialized if the source was never running.
        if (ctx != null) {
            ctx.close();
        }
    }

    /**
     * Marks this source as canceled or stopped.
     *
     * cannot be interpreted as the result of a finite source.
     */
    protected void markCanceledOrStopped() {
        this.canceledOrStopped = true;
    }

    /**
     * Checks whether the source has been canceled or stopped.
     * @return True, if the source is canceled or stopped, false is not.
     */
    protected boolean isCanceledOrStopped() {
        return canceledOrStopped;
    }

    @Override
    public void setup(Output<Attribute<OUT>> output) {

        this.output = output;
    }

    @Override
    public void open() throws Exception {
        userFunction.open();
    }

    @Override
    public void close() throws Exception {
        userFunction.cancel();
    }

    @Override
    public void dispose() throws Exception {

    }

    @Override
    public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {

    }

    @Override
    public Output getOutput() {
        return output;
    }
}
