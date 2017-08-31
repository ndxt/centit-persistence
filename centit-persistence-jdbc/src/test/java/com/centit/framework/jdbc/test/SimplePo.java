package com.centit.framework.jdbc.test;

import java.io.Serializable;

/**
 * Created by codefan on 17-8-31.
 */
public class SimplePo implements Serializable {
    private String id;
    private String name;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
