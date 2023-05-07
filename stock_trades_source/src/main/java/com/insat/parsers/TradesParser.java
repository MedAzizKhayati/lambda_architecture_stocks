package com.insat.parsers;

import com.google.gson.Gson;
import com.insat.models.Trade;
import com.insat.models.TradesEvent;
import java.util.List;

public class TradesParser {
    public static final Gson gson = new Gson();

    public static List<Trade> parseTrades(String json)   {
        TradesEvent tradesEvent = gson.fromJson(json, TradesEvent.class);
        return tradesEvent.getData();
    }
}
