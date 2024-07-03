package com.sparklistener.dev;

import java.io.Serializable;
public  class TableA implements java.io.Serializable {
    private int id;
    private String value;

    public TableA(int id, String value) {
        this.id = id;
        this.value = value;
    }

    public int getId() {
        return id;
    }

    public String getValue() {
        return value;
    }
}