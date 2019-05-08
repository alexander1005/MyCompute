package com.boraydata.conf;

import java.util.HashMap;

public class Configuration {

    protected final HashMap<String, Object> confData;

    public Configuration() {
        this.confData = new HashMap<String, Object>();
    }

    public String getString(String key, String defaultValue) {
        Object o = confData.get(key);
        if (o == null) {
            return defaultValue;
        } else {
            return o.toString();
        }
    }

    public void setString(String key,String value){
        confData.put(key,value);
    }


}
