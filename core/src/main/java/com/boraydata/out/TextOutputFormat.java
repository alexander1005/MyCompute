package com.boraydata.out;

import com.boraydata.conf.Configuration;
import com.boraydata.fs.WriteMode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TextOutputFormat<T> extends FileOutputFormat<T>  {


    private BufferedWriter buf;

    public TextOutputFormat(String path, WriteMode writeMode) {
        super(path,writeMode);
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.buf = Files.newBufferedWriter(Paths.get(path));
    }

    @Override
    public void writeRecord(T record) throws IOException {



        System.out.println( record);

        //System.out.println(record);
       // buf.write(record.toString());
    }

    @Override
    public void close() throws IOException {
        buf.close();
    }
}
