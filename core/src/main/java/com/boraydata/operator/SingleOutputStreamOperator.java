package com.boraydata.operator;


import com.boraydata.env.ExecutionEnvironment;
import com.boraydata.transfer.StreamTransformation;


public class SingleOutputStreamOperator<T> extends DataStream<T> {

    /** Indicate this is a non-parallel operator and cannot set a non-1 degree of parallelism. **/
    protected boolean nonParallel = false;
    public int parallelism;




    protected SingleOutputStreamOperator(ExecutionEnvironment environment, StreamTransformation<T> transformation) {
        super(environment, transformation);
    }



    public SingleOutputStreamOperator<T> setParallelism(int parallelism) {
//
        this.parallelism = parallelism;

        return this;
    }
}
