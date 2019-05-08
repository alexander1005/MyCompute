package com.boraydata.env;

import com.boraydata.out.Output;
import com.boraydata.utils.SinkZMQUtils;
import org.jctools.queues.MpscArrayQueue;

public class SinkOutput extends Output {

    MpscArrayQueue<Attribute> mpscArrayQueue = new MpscArrayQueue(500);

    private Integer number;

    public SinkOutput(Model model, Model local, Integer number) {
        this.last = model;
        this.local = local;
        this.number = number;
    }

    @Override
    public void setup() {

        if (last.host.equals(local.host)){

            SinkZMQUtils.init(String.format("ipc://%s",local.taskId));
        }else{

            SinkZMQUtils.init(String.format("tcp://*:%s",local.post));
        }


        SinkZMQUtils.registerRecTask(local.taskId,local.taskId+number,mpscArrayQueue);
    }

    @Override
    public void collect(Object record) {

    }

    @Override
    public void close() {

    }

    @Override
    public Attribute getNext() {
        return mpscArrayQueue.poll();
    }
}
