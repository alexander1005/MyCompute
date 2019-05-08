package com.boraydata.transfer;

import com.boraydata.out.Output;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OneInputTransformation<IN, OUT> extends StreamTransformation<OUT> {

    private final StreamTransformation<IN> input;

    private final OneInputStreamOperator<IN, OUT> operator;


    /**
     * Creates a new {@code OneInputTransformation} from the given input and operator.
     *
     * @param input The input {@code StreamTransformation}
     * @param name The name of the {@code StreamTransformation}, this will be shown in Visualizations and the Log
     * @param operator The {@code TwoInputStreamOperator}
     * @param parallelism The parallelism of this {@code OneInputTransformation}
     */
    public OneInputTransformation(
            StreamTransformation<IN> input,
            String name,
            OneInputStreamOperator<IN, OUT> operator,

            int parallelism) {
        super(name, parallelism);
        this.input = input;
        this.operator = operator;
    }

    /**
     * Returns the input {@code StreamTransformation} of this {@code OneInputTransformation}.
     */
    public StreamTransformation<IN> getInput() {
        return input;
    }


    /**
     * Returns the {@code TwoInputStreamOperator} of this Transformation.
     */
    public OneInputStreamOperator<IN, OUT> getOperator() {
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
        return input.getOut();
    }

    @Override
    public void setParallelism(int parallelism) {
        parallelism = parallelism;
    }

}
