package com.insat.parsers;

import com.google.gson.Gson;
import com.insat.models.Trade;

public class TradeParser {
    private static final Gson gson = new Gson();

    public static Trade parseTrade(String json)   {
        Trade trade = gson.fromJson(json, Trade.class);
        return trade;
    }
}
