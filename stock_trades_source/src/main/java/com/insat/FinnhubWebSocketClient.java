package com.insat;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.function.Consumer;

import com.insat.models.Trade;
import com.insat.parsers.TradesParser;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

public class FinnhubWebSocketClient extends WebSocketClient {

    public String[] symbols = {
            "AAPL",
            "GOOGL",
            "MSFT",
            "FB",
            "AMZN",
            "BINANCE:BTCUSDT",
            "IC MARKETS:1",
            "OANDA:EUR_USD",
            "FXCM:EURUSD",
            "OANDA:GBP_USD",
            "FXCM:GBPUSD",
            "BINANCE:ETHUSDT",
            "BINANCE:XRPUSDT",
            "BINANCE:BNBUSDT",
            "BINANCE:USDTUSDT",
            "BINANCE:ADAUSDT",
            "BINANCE:DOGEUSDT",
            "BINANCE:DOTUSDT"
    };

    private Consumer<List<Trade>> onMessageHandler = System.out::println;
    private Consumer<String> onCloseHandler = System.out::println;
    private Consumer<Exception> onErrorHandler = System.out::println;

    public FinnhubWebSocketClient(String url) throws URISyntaxException {
        super(new URI(url));
    }

    public FinnhubWebSocketClient(String url, Consumer<List<Trade>> onMessageHandler) throws URISyntaxException {
        super(new URI(url));
        this.onMessageHandler = onMessageHandler;
    }


    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        System.out.println("Connection opened");
        for (String symbol : symbols) {
            send("{\"type\":\"subscribe\",\"symbol\":\"" + symbol + "\"}");
        }
    }

    @Override
    public void onMessage(String message) {
        List<Trade> trades = TradesParser.parseTrades(message);
        if (trades != null && trades.size() > 0)
            onMessageHandler.accept(trades);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Connection closed");
        if (onCloseHandler != null) {
            onCloseHandler.accept(reason);
        }
    }

    @Override
    public void onError(Exception ex) {
        ex.printStackTrace();
        if (onErrorHandler != null) {
            onErrorHandler.accept(ex);
        }
    }

    public static void main(String[] args) throws URISyntaxException {
        String url = "wss://ws.finnhub.io?token=cgmtpi1r01qhveust8hgcgmtpi1r01qhveust8i0";
        FinnhubWebSocketClient client = new FinnhubWebSocketClient(url);
        client.connect();
    }
}
