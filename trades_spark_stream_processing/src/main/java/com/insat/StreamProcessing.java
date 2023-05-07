package com.insat;

import com.insat.models.TradeAggregation;
import com.insat.parsers.TradeParser;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import org.bson.Document;


public class StreamProcessing {

    private static final int NUM_THREADS = 1;
    private static final String ZKQUORUM = "localhost:2181";
    private static final String GROUP = "spark-streaming";
    private static final String TOPICS = "stock-trades";
    private static final String APP_NAME = "KafkaSparkStreamingMongoStockTrades";
    private static final String DATABASE_NAME = "trades";
    private static final String MONGO_URI = "mongodb+srv://admin:admin@cluster0.9qemw.mongodb.net/";
    private static final String COLLECTION_NAME = "trades_aggregations";

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        Map<String, Integer> topicMap = Arrays.stream(TOPICS.split(","))
                .collect(Collectors.toMap(topic -> topic, v -> NUM_THREADS));

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, ZKQUORUM, GROUP, topicMap);


        // Sum the volume of trades by symbol and calculate the total traded value
        JavaDStream<TradeAggregation> tradeAggregationStream = messages
                .mapToPair(record -> new Tuple2<>(record._1(), TradeParser.parseTrade(record._2())))
                .mapToPair(trade -> new Tuple2<>(trade._1(), new TradeAggregation(trade._2())))
                .reduceByKey(TradeAggregation::add)
                .map(trade -> {
                    trade._2().calculateAverages();
                    return trade._2();
                });

        // Save the results to MongoDB
        tradeAggregationStream
                .foreachRDD(rdd -> {
                    rdd.foreachPartition(partitionOfRecords -> {
                        // Create a MongoClient instance to connect to MongoDB
                        MongoClient mongoClient = MongoClients.create(
                                MONGO_URI
                        );
                        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
                        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

                        // Iterate over the records in the partition and save them to MongoDB
                        while (partitionOfRecords.hasNext()) {
                            TradeAggregation trade = partitionOfRecords.next();

                            // Create a MongoDB document with the fields to save
                            Document document = new Document("volume", new Document("total", trade.getTotalVolume())
                                    .append("min", trade.getMinVolume())
                                    .append("max", trade.getMaxVolume())
                                    .append("avg", trade.getAverageVolume()))
                                    .append("price", new Document("total", trade.getTotalPricedVolume())
                                            .append("min", trade.getMinPrice())
                                            .append("max", trade.getMaxPrice())
                                            .append("avg", trade.getAveragePrice()))
                                    .append("timestamp", new Document("last", trade.getEndTs())
                                            .append("first", trade.getBeginTs())
                                            .append("avg", trade.getAverageTs()))
                                    .append("numberOfTrades", trade.getTotalAggregations())
                                    .append("symbol", trade.getSymbol())
                                    .append("created_at", new Date().getTime());

                            // Insert the document into MongoDB
                            collection.insertOne(document);

                        }
                        // Close the MongoClient instance
                        mongoClient.close();
                    });
                });

        jssc.start();
        jssc.awaitTermination();
    }

}


