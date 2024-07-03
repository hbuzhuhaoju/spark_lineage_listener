
package com.sparklistener.dev;

import java.io.Serializable;
public class TableC implements java.io.Serializable {
    private int id;
    private String detail;

    public TableC(int id, String detail) {
        this.id = id;
        this.detail = detail;
    }

    public int getId() {
        return id;
    }

    public String getDetail() {
        return detail;
    }
}