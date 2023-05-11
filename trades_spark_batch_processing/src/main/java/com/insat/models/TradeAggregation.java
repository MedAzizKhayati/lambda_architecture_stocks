package com.insat.models;

import java.io.Serializable;

public class TradeAggregation implements Serializable {

    private final String symbol;
    private int totalAggregations = 1;
    private long beginTs;
    private long endTs;
    private long averageTs;

    private double totalVolume;
    private double minVolume;
    private double maxVolume;
    private double averageVolume;

    private double averagePrice;
    private double minPrice;
    private double maxPrice;
    private double totalPricedVolume;


    public TradeAggregation(Trade trade) {
        this.symbol = trade.getSymbol();

        this.beginTs = trade.getTimestamp();
        this.endTs = trade.getTimestamp();
        this.averageTs = trade.getTimestamp();

        this.totalVolume = trade.getVolume();
        this.minVolume = trade.getVolume();
        this.maxVolume = trade.getVolume();
        this.averageVolume = trade.getVolume();

        this.averagePrice = trade.getPrice();
        this.minPrice = trade.getPrice();
        this.maxPrice = trade.getPrice();

        this.totalPricedVolume = trade.getPrice() * trade.getVolume();
    }

    public TradeAggregation add(TradeAggregation aggregation) {
        this.totalAggregations++;

        this.totalVolume += aggregation.getTotalVolume();
        this.minVolume = Math.min(this.minVolume, aggregation.getMinVolume());
        this.maxVolume = Math.max(this.maxVolume, aggregation.getMaxVolume());
        this.totalPricedVolume += aggregation.getTotalPricedVolume();

        this.averagePrice = this.averagePrice + aggregation.getAveragePrice();
        this.minPrice = Math.min(this.minPrice, aggregation.getMinPrice());
        this.maxPrice = Math.max(this.maxPrice, aggregation.getMaxPrice());

        this.beginTs = Math.min(this.beginTs, aggregation.getBeginTs());
        this.endTs = aggregation.getEndTs();
        this.averageTs = this.averageTs + aggregation.getAverageTs();
        return this;
    }

    public void calculateAverages() {
        this.averageTs = this.averageTs / this.totalAggregations;
        this.averagePrice = this.averagePrice / this.totalAggregations;
        this.averageVolume = this.averageVolume / this.totalAggregations;
    }
    public String getSymbol() {
        return symbol;
    }

    public long getBeginTs() {
        return beginTs;
    }

    public long getEndTs() {
        return endTs;
    }

    public long getAverageTs() {
        return averageTs;
    }

    public double getTotalVolume() {
        return totalVolume;
    }

    public double getAveragePrice() {
        return averagePrice;
    }

    public double getMinPrice() {
        return minPrice;
    }

    public double getMaxPrice() {
        return maxPrice;
    }

    public double getTotalPricedVolume() {
        return totalPricedVolume;
    }

    public double getMinVolume() {
        return minVolume;
    }

    public double getMaxVolume() {
        return maxVolume;
    }

    public double getAverageVolume() {
        return averageVolume;
    }

    public int getTotalAggregations() {
        return totalAggregations;
    }

    @Override
    public String toString() {
        return "TradeAggregation{" +
                "symbol='" + symbol + '\'' +
                ", beginTs=" + beginTs +
                ", endTs=" + endTs +
                ", averageTs=" + averageTs +
                ", totalVolume=" + totalVolume +
                ", averagePrice=" + averagePrice +
                ", minPrice=" + minPrice +
                ", maxPrice=" + maxPrice +
                ", totalPricedVolume=" + totalPricedVolume +
                ", minVolume=" + minVolume +
                ", maxVolume=" + maxVolume +
                ", averageVolume=" + averageVolume +
                '}';
    }
}
