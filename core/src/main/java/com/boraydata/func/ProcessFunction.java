package com.boraydata.func;

import com.boraydata.collect.Collector;

public abstract class ProcessFunction<I, O> {

    private static final long serialVersionUID = 1L;



    public abstract void processElement(I value,  Collector<O> out) throws Exception;


}