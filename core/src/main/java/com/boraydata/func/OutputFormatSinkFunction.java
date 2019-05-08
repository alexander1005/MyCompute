package com.boraydata.func;

import com.boraydata.out.OutputFormat;

public class OutputFormatSinkFunction<IN> implements SinkFunction<IN> {


    private final OutputFormat<IN> format;

    public OutputFormatSinkFunction(OutputFormat<IN> format) {

        this.format = format;
    }

    @Override
    public void invoke(IN record) throws Exception {
        try {
            //System.out.println(record);
            format.writeRecord(record);
        } catch (Exception ex) {
            throw ex;
        }
    }

    @Override
    public void open() {

    }

    public void close () throws Exception{
        format.close();
   }

}
