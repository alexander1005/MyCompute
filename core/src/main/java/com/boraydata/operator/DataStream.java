package com.boraydata.operator;





import com.boraydata.env.ExecutionEnvironment;
import com.boraydata.env.Model;
import com.boraydata.fs.WriteMode;
import com.boraydata.func.FilterFunction;
import com.boraydata.func.MapFunction;
import com.boraydata.func.OutputFormatSinkFunction;
import com.boraydata.out.OutputFormat;
import com.boraydata.out.TextOutputFormat;
import com.boraydata.transfer.OneInputStreamOperator;
import com.boraydata.transfer.OneInputTransformation;
import com.boraydata.transfer.StreamTransformation;

import static com.boraydata.env.ExecutionEnvironment.getExecutionEnvironment;


/**
 * A DataStream represents a stream of elements of the same type. A DataStream
 * can be transformed into another DataStream by applying a transformation as
 * for example:
 * <ul>

 * </ul>
 *
 * @param <T> The type of the elements in this stream.
 */

public class DataStream<T> {

    protected  StreamTransformation<T> transformation;
    public ExecutionEnvironment environment;
    private Model mode;

    public DataStream() {

    }
    // protected ExecutionEnvironment environment;

    public DataStream setMode(Model mode){
        this.mode = mode;
        mode.parall = transformation.getParallelism();
        transformation.model = mode;
        return this;
    }

    public StreamTransformation<T> getTransformation() {
        return transformation;
    }

    /**
     * Create a new {@link DataStream} in the given execution environment with
     * partitioning set to forward by default.
     *
     * @param environment The StreamExecutionEnvironment
     */

    public DataStream(ExecutionEnvironment environment, StreamTransformation<T> transformation) {
        this.environment = environment;
        this.transformation = transformation;
        //this.mode = new Model("localhost",null,"LOCAL");
    }

    public <R> SingleOutputStreamOperator<R> transform(String operatorName,  OneInputStreamOperator<T, R> operator) {

        // read the output type of the input Transform to coax out errors about MissingTypeInfo

        OneInputTransformation<T, R> resultTransform = new OneInputTransformation<>(
                this.transformation,
                operatorName,
                operator,
                environment.defaultLocalParallelism);

        @SuppressWarnings({"unchecked", "rawtypes"})
        SingleOutputStreamOperator<R> returnStream = new SingleOutputStreamOperator(environment, resultTransform);

        getExecutionEnvironment().addOperator(resultTransform);

        return returnStream;
    }

    public ExecutionEnvironment getExecutionEnvironment() {
        return environment;
    }

    public <R> DataStream<R> map(MapFunction<T, R> mapper) {


        return  transform("Map", new StreamMap<>(mapper));

    }


    public SingleOutputStreamOperator<T> filter(FilterFunction<T> filter) {
        return transform("Filter", new StreamFilter<>(filter));
    }



    public DataStreamSink writeAsText(String path, WriteMode writeMode) {

       return writeUsingOutputFormat(new TextOutputFormat<T>(path,writeMode));

    }


    public DataStreamSink writeUsingOutputFormat(OutputFormat<T> format) {

        OutputFormatSinkFunction<T> sinkFunction = new OutputFormatSinkFunction<>(format);
        StreamSink<T> tStreamSink = new StreamSink<>(sinkFunction);
        DataStreamSink dataStreamSink = new DataStreamSink(this, tStreamSink,environment);
        getExecutionEnvironment().addOperator( dataStreamSink.transformation);

        return dataStreamSink;
    }

    public void setParalleism(int paralleism) {

        transformation.setParallelism(paralleism);
    }
//
//
//
//
//
//
//    public void writeAsCsv(String path, WriteMode writeMode) {
//
//    }
//
//
//
//
//
//    public void writeToSocket(String hostName, int port ) {
//
//    }
//
//
//
//    public void writeUsingOutputFormat(OutputFormat<T> format) {
//
//    }

}
