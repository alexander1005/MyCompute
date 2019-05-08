package com.boraydata.utils;


import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.Map;

public class ZMQUtils {


    public static ZContext context = new ZContext();

    private static Map<String,ZMQ.Socket> sockets = new HashMap<>();

    private static Map<String,ZMQ.Socket> opsockets = new HashMap<>();

    public static synchronized void registerPublish(String taskId, String url) {

        sockets.computeIfAbsent(taskId,t->{
            ZMQ.Socket socket = context.createSocket(SocketType.PUB);
            boolean connect = socket.connect(url);
            return socket;
        });

    }


    private static ZMQ.Socket getSocket(String taskId){

        return sockets.get(taskId);
    }



    public static synchronized void SendRecord(String taskId,String nextId, byte[] serializer) {


        getSocket(taskId).send(serializer);

        //System.out.println(Thread.currentThread().getName() + " taskId:" +taskId +" | 发送了 "+ nextId );
        ////System.out.println(Thread.currentThread().getName() + " taskId:" +taskId +" | 发送了 "+ serializer.length );
    }

    public static void SendOpRecord(String taskId,String nextId, byte[] serializer) {

            opsockets.get(taskId).send(serializer);

        //System.out.println(Thread.currentThread().getName() + " taskId:" +taskId +" | 发送了 "+ nextId );
        ////System.out.println(Thread.currentThread().getName() + " taskId:" +taskId +" | 发送了 "+ serializer.length );
    }


    public static synchronized void registerOpPublish(String taskId,String url) {

        opsockets.computeIfAbsent(taskId,t->{
            ZMQ.Socket socket = context.createSocket(SocketType.PUB);
            socket.connect(url);
            return socket;
        });
    }



}
