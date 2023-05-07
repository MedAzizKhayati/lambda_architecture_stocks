package com.insat;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class StockTradesListener {

    public final static String TOPIC = "stock-trades";
    public final static String FINNHUB_API_URL = "wss://ws.finnhub.io?";
    public final static String FINNHUB_API_KEY = "cgmtpi1r01qhveust8hgcgmtpi1r01qhveust8i0";


    public static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static void main(String[] args) throws URISyntaxException {
        Properties props = getProperties();
        createTopicIfNotExists(TOPIC, props);
        Producer<String, String> producer = new KafkaProducer<>(props);

        new FinnhubWebSocketClient(
                FINNHUB_API_URL + "token=" + FINNHUB_API_KEY,
                (trades) -> {
                    trades.forEach(trade -> {
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                                TOPIC,
                                trade.getSymbol(),
                                trade.toString()
                        );
                        producer.send(record);
                    });
                    System.out.println(trades.size() + " trades sent to Kafka");
                }).connect();
    }

    public static void createTopicIfNotExists(String topic, Properties props) {
        try (AdminClient admin = AdminClient.create(props)) {
            if (!admin.listTopics().names().get().contains(topic)) {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                admin.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic " + topic + " created successfully");
            } else {
                System.out.println("Topic " + topic + " already exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
