package com.boraydata;

import com.boraydata.datasource.DataStreamSource;
import com.boraydata.env.ExecutionEnvironment;
import com.boraydata.env.Model;
import com.boraydata.fs.WriteMode;
import com.boraydata.func.SourceFunction;
import com.boraydata.operator.DataStream;
import com.boraydata.operator.DataStreamSink;
import com.boraydata.operator.SingleOutputStreamOperator;
import com.sun.org.apache.xpath.internal.operations.Mod;

public class App {


    public static void main(String[] args) throws Exception {


        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        String[] strings = {"1", "2", "3", "4", "5"};

        environment.setParallelism(1);
        DataStreamSource<String> source = environment.fromElements(strings);
        //environment.readCSV();

        Model model = new Model("localhost","5563","SOURCE");


        source.setMode(model);

        DataStream<String> map = source.map(t -> {
            String s = t + "dasdass";
            System.out.println(s);
            return s;
        });
        map.setParalleism(1);

        Model model1 = new Model("localhost","5563",Constants.Operator);
        map.setMode(model1);

        DataStream<String> map1 = map.map(t -> t + "ccccc");

        Model model5 = new Model("localhost","5563",Constants.Operator);

        map1.setMode(model5);

        DataStreamSink dataStreamSink = map1.writeAsText("/dasdsa/dsad.txt", new WriteMode());

        dataStreamSink.setParallelism(1);

        Model model2 = new Model("localhost","6698","SINK");
        dataStreamSink.setMode(model2);

        environment.submit("test");

        Thread.sleep(11000);
    }
}
