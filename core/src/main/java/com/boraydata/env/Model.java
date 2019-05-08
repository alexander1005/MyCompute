package com.boraydata.env;

import java.util.UUID;

public class Model {


    public String host;

    public String post;

    public String mode;

    public String taskId;

    public Integer parall;

    public String FLAG;

    public Model(String host, String post,String flag) {
        this.host = host;
        this.post = post;
        this.FLAG = flag;
        this.taskId = Math.abs(UUID.randomUUID().toString().replace("-","").hashCode())+"";
        System.out.println("生成model " + taskId +  "  生成 flag" + flag);
    }

    public String getHost() {
        return host;
    }

    public String getPost() {
        return post;
    }


}
