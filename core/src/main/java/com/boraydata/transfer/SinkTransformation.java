package com.boraydata.transfer;

import com.boraydata.operator.StreamSink;
import com.boraydata.out.Output;

import java.util.*;

public class SinkTransformation<T> extends StreamTransformation<T> {

    private final StreamTransformation<T> input;

    private final StreamSink<T> operator;




    /**
     * Creates a new {@code SinkTransformation} from the given input {@code StreamTransformation}.
     *
     * @param input The input {@code StreamTransformation}
     * @param name The name of the {@code StreamTransformation}, this will be shown in Visualizations and the Log
     * @param operator The sink operator
     * @param parallelism The parallelism of this {@code SinkTransformation}
     */

    public SinkTransformation(
            StreamTransformation<T> input,
            String name,
            StreamSink<T> operator,
            int parallelism) {
        super(name, parallelism);
        this.input = input;
        this.operator = operator;
    }

    public StreamTransformation getInput(){
        return input;
    }

    public StreamSink getOperator(){

        return operator;
    }

    @Override
    public Collection<StreamTransformation<?>> getTransitivePredecessors() {
        List<StreamTransformation<?>> result = new ArrayList();
        result.add(this);
        result.addAll(input.getTransitivePredecessors());
        return result;
    }



    @Override
    public Output getOut() {
        return this.input.getOut();
    }

    @Override
    public void setParallelism(int parallelism) {
        parallelism = parallelism;
    }

}

