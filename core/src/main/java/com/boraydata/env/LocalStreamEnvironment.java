package com.boraydata.env;


import com.boraydata.operator.StreamSink;
import com.boraydata.out.Output;
import com.boraydata.transfer.*;
import com.boraydata.utils.OpZMQUtils;
import com.boraydata.utils.SinkZMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class LocalStreamEnvironment extends ExecutionEnvironment {


    private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

    public LocalStreamEnvironment() {

    }

    /**
     * Create lazily upon first use.
     */

    @Override
    public void submit(String jobName) throws Exception {

        //  StreamGraph streamGraph = this.getStreamGraph();

        List<StreamTransformation<?>> transformations = this.transformations;

        List<Thread> sinkThread = new ArrayList<>();

        List<Thread> transferThread = new ArrayList<>();

        List<Thread> sourceThread = new ArrayList<>();

        for (int i = 0; i < transformations.size(); i++) {

            StreamTransformation streamTransformation = transformations.get(i);

            if (streamTransformation instanceof SinkTransformation) {

                SinkTransformation sink = (SinkTransformation) streamTransformation;

                StreamSink operator = sink.getOperator();


                for (int j = 0; j < sink.getParallelism(); j++) {


                    int finalJ = j;
                    int finalI2 = i;
                    Runnable runnable = () -> {

                        try {

                            Output<Attribute> output = new SinkOutput(transformations.get(finalI2 -1).model,sink.model, finalJ);
                            output.setup();
                            new SinkZMQUtils().start();
                            for(;;) {

                                Attribute next = output.getNext();
                                if (next!=null)
                                    operator.processElement(next);
                                else
                                    Thread.sleep(1000);
                            }

                        } catch (Exception e) {

                            e.printStackTrace(System.err);
                        }


                    };

                    sinkThread.add(new Thread(runnable));
                }


            } else if (streamTransformation instanceof SourceTransformation) {

                SourceTransformation source = (SourceTransformation) streamTransformation;
                StreamSource operator = source.getOperator();

                for (int j = 0; j < source.getParallelism(); j++) {

                    int finalI = i;
                    Runnable runnable = () -> {
                        try {

                            Output output = new SourceOutput(source.model, transformations.get(finalI + 1)
                                    .model);

                            output.setup();

                            operator.setup(output);
                            operator.open();
                            operator.run();
                            operator.close();
                        } catch (Exception e) {
                            e.printStackTrace(System.err);
                        }
                    };
                    sourceThread.add(new Thread(runnable));
                }

            } else {

                OneInputTransformation oitf = (OneInputTransformation) streamTransformation;

                OneInputStreamOperator operator = oitf.getOperator();

                for (int j = 0; j < oitf.getParallelism(); j++) {

                    int finalI1 = i;
                    int finalJ = j;
                    Runnable runnable = () -> {
                        try {
                            Output<Attribute> output = new NetRecviceOutput(transformations.get(finalI1-1).model,oitf.model, transformations.get(finalI1 + 1).model, finalJ);
                            output.setup();
                            operator.setup(output);
                            new OpZMQUtils().start();
                            for(;;) {

                                Attribute attribute = output.getNext();
                                if (attribute!=null)
                                    operator.processElement(attribute);
                                else
                                    Thread.sleep(1000);
                            }
                        } catch (Exception e) {
                            e.printStackTrace(System.err);
                        }
                    };
                    transferThread.add(new Thread(runnable));
                }
            }
        }

        for(int i = 0 ;i<sinkThread.size();i++){
            Thread thread = sinkThread.get(i);
            thread.setName(String.format("sink %d",i));
            thread.start();
        }
        System.out.println("已启动sink操作并行度"+sinkThread.size());
        Thread.sleep(5000);
        for(int i = 0 ;i<transferThread.size();i++){
            Thread thread = transferThread.get(i);
            thread.setName(String.format("transfer %d",i));
            thread.start();
        }
        System.out.println("已启动transfer操作并行度"+transferThread.size());
        Thread.sleep(5000);
        for(int i = 0 ;i<sourceThread.size();i++){
            Thread thread = sourceThread.get(i);
            thread.setName(String.format("source %d",i));
            thread.start();
        }
        System.out.println("已启动sourceThread操作并行度"+sourceThread.size());
    }

    @Override
    public Long getBufferTimeout() {
        return null;
    }

    @Override
    public boolean isChainingEnabled() {
        return false;
    }

//    private StreamGraph getStreamGraph() {
//
//        //new StreamGraph(this);
//
//    }


//    @Override
//    public void submit(String execute) {
//
//
//
//
//
////
////
////
////        functions.remove(0);
////
////        preStart(functions);
////
////        for (Thread t:list) {
////
////            t.start();
////            try {
////                Thread.sleep(1000);
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
////        }
//
//    }

//    private void preStart(List<DataStream> functions) {
//        for(int i =0 ;i<functions.size();i++){
//
//            DataStream dataStream = functions.get(i);
//            StreamTransformation stsf = dataStream.getTransformation();
//
//            if (i==0){
//                Thread thread = new Thread(()->{
//                    SourceTransformation sttf = (SourceTransformation) stsf;
//                    StreamSource operator = sttf.getOperator();
//                    Output output = new LocalSpsmOutput();
//                    operator.setup(output);
//                    while (true){
//                        try {
//                            operator.run();
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                });
//                list.add(thread);
//                continue;
//
//            }else if (i<functions.size()-1){
//
//                Thread thread = new Thread(() -> {
//
//                    OneInputTransformation otf = (OneInputTransformation) stsf;
//
//
//                    Output output = new LocalSpsmOutput();
//
//
//                    Output<Attribute> out = otf.getOut();
//
//                    OneInputStreamOperator operator = otf.getOperator();
//
//
//                    operator.setup(output);
//
//                    while (true){
//
//                        try {
//
//                            operator.processElement(out.getNext());
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//
//                    }
//                });
//
//                list.add(thread);
//
//            }else{
//                Thread thread = new Thread(()->{
//
//                    SinkTransformation sts = (SinkTransformation) stsf;
//
//                    SinkTransformation sts1 = sts;
//                    OneInputTransformation input = (OneInputTransformation)sts1.getInput();
//
//                    Output<Attribute> output = input.getOperator().getOutput();
//                    StreamSink operator = sts.getOperator();
//                    while (true){
//                        try {
//
//
//                            operator.processElement(output.getNext());
//
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                });
//                list.add(thread);
//            }
//
//
//        }
//

    //}


}
