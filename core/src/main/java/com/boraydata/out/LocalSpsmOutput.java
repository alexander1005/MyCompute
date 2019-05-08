package com.boraydata.out;

import org.jctools.queues.SpscArrayQueue;

public class LocalSpsmOutput<Attribute> extends Output<Attribute> {

    private SpscArrayQueue<Attribute> spscArrayQueue = new SpscArrayQueue<>(2^31);


    @Override
    public void collect(Attribute record) {

        spscArrayQueue.offer(record);
    }

    @Override
    public void close() {
        spscArrayQueue =null;
    }

    @Override
    public Attribute getNext() {

        while (spscArrayQueue.size()<1){

        }

        return  spscArrayQueue.poll();
    }

    @Override
    public void setup() {

    }
}
