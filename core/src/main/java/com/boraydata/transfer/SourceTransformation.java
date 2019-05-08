package com.boraydata.transfer;

import com.boraydata.env.Model;
import com.boraydata.out.Output;

import java.util.Collection;
import java.util.Collections;

public class SourceTransformation<T> extends StreamTransformation<T> {

    private final StreamSource<T, ?> operator;


    /**
     * Creates a new {@code SourceTransformation} from the given operator.
     *
     * @param name The name of the {@code SourceTransformation}, this will be shown in Visualizations and the Log
     * @param operator The {@code StreamSource} that is the operator of this Transformation* @param parallelism The parallelism of this {@code SourceTransformation}
     */
    public SourceTransformation(
            String name,
            StreamSource<T, ?> operator,
            int parallelism) {
        super(name,parallelism);
        this.operator = operator;
    }

    /**
     * Returns the {@code StreamSource}, the operator of this {@code SourceTransformation}.
     */
    public StreamSource<T, ?> getOperator() {
        return operator;
    }

    @Override
    public Collection<StreamTransformation<?>> getTransitivePredecessors() {
        return Collections.<StreamTransformation<?>>singleton(this);
    }

    @Override
    public Output getOut() {
        return operator.getOutput();
    }

    @Override
    public void setParallelism(int parallelism) {

    }

}
