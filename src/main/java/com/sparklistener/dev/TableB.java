
package com.sparklistener.dev;

public  class TableB implements java.io.Serializable {
    private int id;
    private String description;

    public TableB(int id, String description) {
        this.id = id;
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }
}