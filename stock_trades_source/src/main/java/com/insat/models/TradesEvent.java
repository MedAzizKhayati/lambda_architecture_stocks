package com.insat.models;

import java.util.List;

public class TradesEvent {
    private String type;
    private List<Trade> data;

    public TradesEvent(String type, List<Trade> data) {
        this.type = type;
        this.data = data;
    }

    public String getType() {
        return type;
    }
    public List<Trade> getData() {
        return data;
    }
}
