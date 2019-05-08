package com.boraydata.operator;

import com.boraydata.env.Attribute;
import com.boraydata.func.FilterFunction;
import com.boraydata.out.Output;
import com.boraydata.transfer.OneInputStreamOperator;
import com.boraydata.utils.ChainingStrategy;

public class StreamFilter<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>> implements OneInputStreamOperator<IN, IN> {

    private static final long serialVersionUID = 1L;

    public StreamFilter(FilterFunction<IN> filterFunction) {
        super(filterFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }



    @Override
    public void processElement(Attribute<IN> element) throws Exception {
        if (userFunction.filter(element.getValue())) {
            output.collect(element);
        }
    }

    @Override
    public void setup(Output<Attribute<IN>> output) {
        this.output = output;
    }

    @Override
    public void open() throws Exception {

    }

    @Override
    public void close() throws Exception {

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
