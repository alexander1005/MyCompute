package com.boraydata.env;

import com.boraydata.out.Output;
import com.boraydata.utils.ProtoStuffSerializer;
import com.boraydata.utils.ZMQUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;


/**
 * zeromq 实现的网络通讯
 */
public class SourceOutput extends Output<Attribute> {


    //zeromq socket

    //序列化器
    ProtoStuffSerializer serializer = new ProtoStuffSerializer();

    /**
     *
     * @param local 当前网络信息
     * @param next  下一个网络信息
     */
    public SourceOutput(Model local, Model next) {

        this.local= local;
        this.next = next;
    }

    @Override
    public void setup() {

        if(local.host.equals(next.host)){



            ZMQUtils.registerPublish(local.taskId,String.format("ipc://%s",Math.abs(next.host.hashCode())+""));

        }else{

            ZMQUtils.registerPublish(local.taskId,String.format("tcp://%s:%s",next.host,next.post));
        }

    }

    @Override
    public void collect(Attribute record) {

        if (record ==null)
            return;

        //采用的是至多处理一次语义
        int number = Math.abs(record.getValue().hashCode() % next.parall);
        String key = next.taskId + number;
        record.setKey(key);
        //send it
        try {
            if (record!=null && record.getValue()!=null) {
                byte[] serializer = this.serializer.serializer(record);
                if (serializer!=null && serializer.length>0){

                    ZMQUtils.SendRecord(local.taskId,next.taskId,serializer);
                    System.out.println(Thread.currentThread().getName()+"| 发送了 "+record  +"指定taskId :" + key);
                }
            }
        } catch (Exception e) {
            //ZMQUtils.SendRecord(local.taskId,next.taskId,serializer.serializer(record));
           // e.printStackTrace();
        }
    }

    @Override
    public void close() {

        //关闭
    }

    @Override
    public Attribute getNext() {

        //此sink不需要getnext
        return null;
    }
}
