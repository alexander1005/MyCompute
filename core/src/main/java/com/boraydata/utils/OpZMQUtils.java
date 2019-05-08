package com.boraydata.utils;

import com.boraydata.env.Attribute;

import org.jctools.queues.MpscArrayQueue;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.Map;

public class OpZMQUtils extends Thread {

    static ZContext context;

    static ZMQ.Socket subscriber;

    private static Map<String, MpscArrayQueue<Attribute>> socketMap = new HashMap<>();
    private static volatile int i = 0;
    private ProtoStuffSerializer protoStuffSerializer = new ProtoStuffSerializer();
    private static volatile int j = 0;



    public static synchronized void registerRecTask(String taskId, String number, MpscArrayQueue<Attribute> mpscArrayQueue) {
        socketMap.putIfAbsent(number, mpscArrayQueue);
        //System.out.println(Thread.currentThread().getName() + " OpZMQUtils 订阅消息 " +taskId);

        System.out.println(String.format(" 注册id:%s",number));

    }

    public static synchronized void init(String url) {

        if (j==0){

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
            setName("OpZMQUtils");

            System.out.println(Thread.currentThread().getName() + " OpZMQUtils 开始收集信息");

            while (true) {
                // Read envelope with address
                try {
                    byte[] recv = subscriber.recv();
                    if (recv!=null && recv.length>0){

                        Attribute attribute = protoStuffSerializer.deserializer(recv, Attribute.class);

                        if (attribute!=null&& attribute.getValue()!=null && attribute.getKey()!=null){

                            System.out.println(Thread.currentThread().getName()+"|收到消息 "+attribute);

                            socketMap.get(attribute.key).offer(attribute);
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    //e.printStackTrace();
                    //System.out.println(Thread.currentThread().getName() + " error");
                }
            }
        }
    }
}
