package com.boraydata.func;


import java.util.Arrays;
import java.util.List;

public class ElementSourceFunction<Out> implements SourceFunction {

    List<Out> outs;

    public ElementSourceFunction(Out... data) {

        outs = Arrays.asList(data);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {

        for (Out o:outs) {

            ctx.collect(o);
        }
        //outs.clear();
    }

    @Override
    public void cancel() {

    }

    @Override
    public void open() {

    }
}
