package com.boraydata.env;

import java.io.Serializable;
import java.util.HashMap;

import java.util.Map;
import java.util.function.BiConsumer;

public class Attribute<OUT>  implements Serializable {

    /** The actual value held by this record. */
    private OUT value;
    /**
     * Creates a new StreamRecord. The record does not have a timestamp.
     */
    public Attribute(OUT value) {
        this.value = value;
    }
    public Attribute() {

    }

    public String key ;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public OUT  getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Attribute{" +
                "value=" + value +
                '}';
    }

    public  void setValue(OUT va) {
        this.value = va;
    }
}