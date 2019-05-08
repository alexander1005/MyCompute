package com.boraydata.utils;

import com.boraydata.env.Attribute;
import org.jctools.queues.MpscArrayQueue;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.Map;

public class SinkZMQUtils extends Thread {

    static ZContext context;

    static ZMQ.Socket subscriber;

    private static Map<String, MpscArrayQueue<Attribute>> socketMap = new HashMap<>();

    private static Map<String,Integer> resMap = new HashMap<>();

    private static volatile int i = 0;

    private static volatile int j = 0;

    private ProtoStuffSerializer protoStuffSerializer = new ProtoStuffSerializer();


    public static void registerRecTask(String taskId, String number, MpscArrayQueue<Attribute> mpscArrayQueue) {


        //System.out.println(Thread.currentThread().getName() + " SinkZMQUtils 订阅消息 " +taskId);
        socketMap.putIfAbsent(number, mpscArrayQueue);
    }

    public static synchronized void init(String url ) {

        if (j ==0){
            j++;
            context = new ZContext();
            subscriber = context.createSocket(SocketType.SUB);
            subscriber.bind(url);
            subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);
        }

    }


    @Override
    public void run() {

        if (i == 0) {

            i++;
            setName("SinkZMQUtils");
            System.out.println(Thread.currentThread().getName() + " SinkZMQUtils 开始收集信息");
            while (true) {
                // Read envelope with address
                try {



                    byte[] recv = subscriber.recv();

                    if (recv!=null){
                        Attribute attribute = protoStuffSerializer.deserializer(recv, Attribute.class);
                        socketMap.get(attribute.key).offer(attribute);
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                    //System.out.println(Thread.currentThread().getName() + " error");
                }
            }
        }
    }
}
