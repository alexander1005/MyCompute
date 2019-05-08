package com.boraydata.transfer;

import com.boraydata.func.SourceFunction;
import com.boraydata.out.Output;

public class ElementSourceContext<T>  implements SourceFunction.SourceContext<T> {

    private final Output<T> output;

    public ElementSourceContext(Output<T> output) {
        this.output = output;
    }

    @Override
    public void collect(T element) {
        output.collect(element);
    }

    @Override
    public void collectWithTimestamp(Object element, long timestamp) {

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

    }
}
