package com.insat;


import com.insat.models.Trade;
import com.insat.parsers.TradeParser;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;


public class KafkaStreamToHdfs {
    private static final int NUM_THREADS = 1;
    private static final String ZKQUORUM = "localhost:2181";
    private static final String GROUP = "kafka-stream-to-hdfs";
    private static final String TOPICS = "stock-trades";
    private static final String APP_NAME = "KafkaStreamToHdfs";

    public static void main(String[] args) throws Exception {
        String hdfsPath = args[0];
        int duration = 10000;
        if (args.length > 1) {
            duration = Integer.parseInt(args[1]);
        }

        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(duration));

        Map<String, Integer> topicMap = Arrays.stream(TOPICS.split(","))
                .collect(Collectors.toMap(topic -> topic, v -> NUM_THREADS));

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, ZKQUORUM, GROUP, topicMap);

        JavaDStream<Trade> trades = messages
                .map(record -> TradeParser.parseTrade(record._2()));


        // spark variable
        SparkSession spark = SparkSession.builder()
                .appName("KafkaStreamToHdfs")
                .master("local[*]")
                .getOrCreate();

        // Save the data to HDFS
        trades.foreachRDD(rdd -> {
            spark.createDataFrame(rdd, Trade.class)
                    .toDF()
                    .write()
                    .format("com.databricks.spark.csv")
                    .mode("append")
                    .save(hdfsPath);
        });

        // Print the results to the console
        trades.print();

        // Start the streaming context
        jssc.start();
        jssc.awaitTermination();
    }
}
