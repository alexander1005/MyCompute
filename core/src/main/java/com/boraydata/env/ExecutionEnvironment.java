package com.boraydata.env;

import com.boraydata.datasource.DataStreamSource;
import com.boraydata.env.factoy.ExecutionEnvironmentFactory;
import com.boraydata.func.CSVSourceFunction;
import com.boraydata.func.ElementSourceFunction;
import com.boraydata.func.SinkFunction;
import com.boraydata.func.SourceFunction;
import com.boraydata.operator.DataStreamSink;
import com.boraydata.transfer.StreamSource;
import com.boraydata.transfer.StreamTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public abstract class ExecutionEnvironment {

    protected final List<StreamTransformation<?>> transformations = new ArrayList<>();


    /** The logger used by the environment and its subclasses. */
    protected static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvironment.class);

    protected DataStreamSource dataStreamSource;

    public void addOperator(StreamTransformation<?> transformation) {
        this.transformations.add(transformation);
    }

    protected final ExecutionConfig config = new ExecutionConfig();

    protected static ExecutionEnvironmentFactory contextEnvironmentFactory;

    /** The default parallelism used when creating a local environment. */
    public static int defaultLocalParallelism = Runtime.getRuntime().availableProcessors();

    protected DataStreamSink sinkFunction;

    public static ExecutionEnvironment getExecutionEnvironment() {
        if (contextEnvironmentFactory != null) {
            return contextEnvironmentFactory.createExecutionEnvironment();
        }
        return createLocalEnvironment();

    }

    public static ExecutionEnvironment createLocalEnvironment() {
        return createLocalEnvironment(defaultLocalParallelism);
    }

    public static ExecutionEnvironment createLocalEnvironment(int parallelism) {
        LocalStreamEnvironment env = new LocalStreamEnvironment();
        env.setParallelism(parallelism);
        return env;
    }




    public final <OUT> DataStreamSource<OUT> readCSV(String path) {

        return readCSV(path,"\n");

    }

    public final <OUT> DataStreamSource<OUT> readCSV(String path,String delimiter) {

        return readCSV(path,"\n",",");
    }

    public final <OUT> DataStreamSource<OUT> readCSV(String path,String delimiter,String split) {


        if (path==null||path.length()>0)
            throw new IllegalArgumentException(" path must not null");
        SourceFunction sourceFunction = new CSVSourceFunction(path,delimiter,split);

        DataStreamSource fromElements = addSource(sourceFunction, "CSVSOURCE");
        return fromElements;
    }



    public final <OUT> DataStreamSource<OUT> fromElements(OUT... data) {

        if (data.length == 0) {
            throw new IllegalArgumentException("fromElements needs at least one element as argument");
        }

        SourceFunction sourceFunction = new ElementSourceFunction<>(data);

        DataStreamSource fromElements = addSource(sourceFunction, "Elements");

        return fromElements;
    }


//    public <OUT> DataStreamSource<OUT> fromCollection(Collection<OUT> data) {
//
//
//       // return addSource(function, "Collection Source", typeInfo).setParallelism(1);
//    }

//
//    public DataStreamSource<String> readTextFile(String filePath) {
//        return readTextFile(filePath, "UTF-8");
//    }
//
//
//    public DataStreamSource<String> readTextFile(String filePath, String charsetName) {
//
//
//    }
//
//    public DataStreamSource<String> socketTextStream(String hostname, int port, char delimiter, long maxRetry) {
//
//    }
//
//
//    public DataStreamSource<String> socketTextStream(String hostname, int port, String delimiter, long maxRetry) {
//
//    }
//


    public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function, String sourceName) {


        StreamSource<OUT, ?> sourceOperator = new StreamSource<>(function);

        DataStreamSource dataStreamSource = new DataStreamSource(this, sourceOperator, true, sourceName);

        this.transformations.add(dataStreamSource.getTransformation());

        return dataStreamSource;

    }

    private void setSource(DataStreamSource dataStreamSource) {

        this.dataStreamSource = dataStreamSource;
    }

    public abstract void submit(String jobId) throws Exception;

    public <T> void setSinkFunction(DataStreamSink sinkFunction) {
        this.sinkFunction = sinkFunction;
    }

    public ExecutionConfig getConfig() {
        return config;
    }

    public  int getParallelism(){
        return defaultLocalParallelism;
    }

    public abstract Long getBufferTimeout();

    public abstract boolean isChainingEnabled();

    public ExecutionEnvironment setParallelism(int parallelism){
        defaultLocalParallelism = parallelism;
        return this;
    }
}



