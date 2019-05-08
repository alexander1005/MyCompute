package com.boraydata.env;

public class ExecutionConfig {


    public static final Integer PARALLELISM_DEFAULT =Runtime.getRuntime().availableProcessors() ;
    private int parallelism;

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism ;
    }

    public int getMaxParallelism() {
        return parallelism;
    }
}


