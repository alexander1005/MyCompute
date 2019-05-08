package com.boraydata.func;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CSVSourceFunction implements SourceFunction {

    private  String delimiter;
    private  String split;
    private  String path;
    BufferedReader reader;
    public CSVSourceFunction(String path, String delimiter, String split) {

        this.path = path;
        this.delimiter = delimiter ;
        this.split = split;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {


        StringBuilder builder = new StringBuilder();

        while (reader.ready()){


            String line = reader.readLine();
            if (line.contains(delimiter)){
                int i = line.indexOf(delimiter);
                String substring = line.substring(0, i);

                substring.split(split);


                String substring1 = line.substring(i, line.length());
                if (substring1!=null&&substring1.length()>0){
                    builder.append(substring1);
                }
            }
        }

    }

    @Override
    public void cancel() {

        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open() {

        try {
            reader = Files.newBufferedReader(Paths.get(path));
        } catch (IOException e) {

            throw new NullPointerException(e.getMessage());
        }
    }
}
