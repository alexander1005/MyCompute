package com.boraydata.out;

import com.boraydata.collect.Collector;
import com.boraydata.env.Attribute;
import com.boraydata.env.Model;

public abstract class Output<Attribute> implements Collector<Attribute> {

    public Model local = null;

    public Model input;
    public Model next;
    public Model last;

    public abstract void setup();
}
