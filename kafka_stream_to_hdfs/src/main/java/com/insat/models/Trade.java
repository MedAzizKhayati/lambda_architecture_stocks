package com.insat.models;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

public class Trade implements Serializable {
    @SerializedName("s")
    private String symbol;
    @SerializedName("v")
    private double volume;
    @SerializedName("p")
    private double price;
    @SerializedName("t")
    private long timestamp;

    public Trade(double price, String symbol, long timestamp, double volume) {
        this.price = price;
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.volume = volume;
    }

    public double getPrice() {
        return price;
    }

    public String getSymbol() {
        return symbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getVolume() {
        return volume;
    }

    @Override
    public String toString() {
        return "{\"p\":" + price +
                ",\"s\":\"" + symbol + "\"" +
                ",\"t\":" + timestamp +
                ",\"v\":" + volume +
                "}";
    }

    public String toCsv() {
        return symbol + "," + volume + "," + price + "," + timestamp;
    }
}
