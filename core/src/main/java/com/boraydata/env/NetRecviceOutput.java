package com.boraydata.env;

import com.boraydata.Constants;
import com.boraydata.out.Output;
import com.boraydata.utils.OpZMQUtils;
import com.boraydata.utils.ProtoStuffSerializer;
import com.boraydata.utils.SinkZMQUtils;
import com.boraydata.utils.ZMQUtils;
import org.jctools.queues.MpscArrayQueue;


public class NetRecviceOutput<Attribute> extends Output<Attribute>{


    private final Integer number;

    MpscArrayQueue<Attribute> mpscArrayQueue = new MpscArrayQueue<>(500);

    ProtoStuffSerializer protoStuffSerializer= new ProtoStuffSerializer();

    private final String key ;

    public NetRecviceOutput(Model model, Model local, Model next, Integer integer) {

        this.last = model;
        this.next = next;
        this.local = local;
        this.number = integer;
        key = next.taskId+number;
    }

    @Override
    public void collect(Attribute record) {


        if (next.FLAG.equals(Constants.Operator)){

            com.boraydata.env.Attribute record1 = (com.boraydata.env.Attribute) record;
            record1.key = next.taskId+number;

            if (record1.getValue()!=null){

                byte[] serializer = protoStuffSerializer.serializer(record1);

                if (serializer!=null){

                    ZMQUtils.SendOpRecord(local.taskId,next.taskId, serializer);
                }
            }

        }else{

            com.boraydata.env.Attribute record1 = (com.boraydata.env.Attribute) record;
            int i =Math.abs( record1.getValue().hashCode() % next.parall);
            record1.key = next.taskId+i;
            //System.out.println(" iiiiiiiiiiiiii "+ i);
            if (record1.getValue()!=null){



                    byte[] serializer = protoStuffSerializer.serializer(record1);
                    ZMQUtils.SendOpRecord(local.taskId,next.taskId,serializer);

            }
        }
    }

    @Override
    public void close() { }

    @Override
    public Attribute getNext() {

        return mpscArrayQueue.poll();
    }

    @Override
    public void setup() {

        if (last.host.equals(local.host)){

            OpZMQUtils.init(String.format("ipc://%s",Math.abs(local.host.hashCode())+""));
        }else{

            OpZMQUtils.init(String.format("tcp://*:%s",local.post));
        }


       // O/pZMQUtils.init();


        OpZMQUtils.registerRecTask(local.taskId,local.taskId+number, (MpscArrayQueue<com.boraydata.env.Attribute>) mpscArrayQueue);

        if (last.host.equals(local.host)){

            if (next.FLAG.equals(Constants.Operator)){

                ZMQUtils.registerOpPublish(local.taskId,String.format("ipc://%s",Math.abs(local.host.hashCode())+""));
            }else{

                ZMQUtils.registerOpPublish(local.taskId,String.format("ipc://%s",next.taskId));
            }



        }else{

            ZMQUtils.registerOpPublish(local.taskId,String.format("tcp://%s:%s",next.host,next.post));
        }





    }
}
