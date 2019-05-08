package com.boraydata.operator;


import com.boraydata.env.Attribute;
import com.boraydata.func.MapFunction;
import com.boraydata.transfer.OneInputStreamOperator;
import com.boraydata.out.Output;

public class StreamMap<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    private Output<Attribute<OUT>> output;

    public StreamMap(MapFunction<IN, OUT> mapper) {

        super(mapper);

    }


    @Override
    public void processElement(Attribute<IN> element) throws Exception {

        OUT va = this.userFunction.map(element.getValue());
       //
        Attribute<OUT> attribute = new Attribute<>(va);

        if (attribute!=null&&attribute.getValue()!=null)
            output.collect(attribute);

    }

    @Override
    public void setup(Output<Attribute<OUT>> output) {

        this.output  = output;
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

