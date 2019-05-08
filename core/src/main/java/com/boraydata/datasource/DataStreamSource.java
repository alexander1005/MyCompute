package com.boraydata.datasource;

import com.boraydata.env.ExecutionEnvironment;
import com.boraydata.operator.SingleOutputStreamOperator;
import com.boraydata.transfer.SourceTransformation;
import com.boraydata.transfer.StreamSource;

public class DataStreamSource<T> extends SingleOutputStreamOperator<T> {

    boolean isParallel;


    public DataStreamSource(SingleOutputStreamOperator<T> operator) {
        super(operator.environment, operator.getTransformation());
        this.isParallel = true;
    }


    public DataStreamSource(ExecutionEnvironment environment,
                            StreamSource<T, ?> operator,
                            boolean isParallel, String sourceName) {
        super(environment, new SourceTransformation(sourceName, operator, environment.defaultLocalParallelism));

        this.isParallel = isParallel;
        if (!isParallel) {
            setParallelism(1);
        }
    }

    @Override
    public DataStreamSource<T> setParallelism(int parallelism) {
        if (parallelism > 1 && !isParallel) {
            throw new IllegalArgumentException("Source: " + transformation.getId() + " is not a parallel source");
        } else {
            return (DataStreamSource<T>) super.setParallelism(parallelism);
        }
    }
}


