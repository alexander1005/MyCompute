package com.boraydata.operator;

import com.boraydata.env.Attribute;
import com.boraydata.func.SinkFunction;
import com.boraydata.out.Output;
import com.boraydata.transfer.OneInputStreamOperator;

public class StreamSink<IN> extends AbstractUdfStreamOperator<Object, SinkFunction<IN>>
        implements OneInputStreamOperator<IN, Object> {

    private static final long serialVersionUID = 1L;
    private Output<Attribute<Object>> output;


    public StreamSink(SinkFunction<IN> sinkFunction) {
        super(sinkFunction);
    }

    @Override
    public void setup(Output<Attribute<Object>> output) {
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

    @Override
    public void processElement(Attribute<IN> element) throws Exception {

        userFunction.invoke(element.getValue());
    }

}
