package com.insat;

import com.insat.models.Trade;
import com.insat.models.TradeAggregation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TradesBatchProcessing {
    private static final String APP_NAME = "TradesBatchProcessing";
    private static final String DATABASE_NAME = "trades";
    private static final String MONGO_URI = "mongodb+srv://admin:admin@cluster0.9qemw.mongodb.net/";
    private static final String COLLECTION_NAME = "trades_batch_aggregations";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: TradesBatchProcessing <inputDir>");
            System.out.println("Example usage: TradesBatchProcessing trades");
            System.exit(1);
        }

        String hdfsPath = args[0];
//        String aggregatedHdfsPath = args[1];

        SparkConf conf = new SparkConf().setAppName(APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(hdfsPath);

        JavaPairRDD<String, TradeAggregation> totalTradesValuePerSymbol = rdd
                .mapToPair(line -> {
                    String[] tokens = line.split(",");
                    double price = Double.parseDouble(tokens[0]);
                    String symbol = tokens[1];
                    long timestamp = Long.parseLong(tokens[2]);
                    double volume = Double.parseDouble(tokens[3]);
                    Trade trade = new Trade(
                            price,
                            symbol,
                            timestamp,
                            volume
                    );
                    return new Tuple2<>(symbol, new TradeAggregation(trade));
                })
                .reduceByKey(TradeAggregation::add);

        totalTradesValuePerSymbol.foreachPartition(
                partitionOfRecords -> {
                    // Create a MongoClient instance to connect to MongoDB
                    MongoClient mongoClient = MongoClients.create(
                            MONGO_URI
                    );
                    MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
                    MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

                    List<Document> documents = new ArrayList<>();

                    // Iterate over the records in the partition and save them to MongoDB
                    while (partitionOfRecords.hasNext()) {
                        TradeAggregation trade = partitionOfRecords.next()._2();
                        trade.calculateAverages();

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

                        documents.add(document);
                    }
                    if (documents.size() != 0) {
                        // Insert the document into MongoDB
                        collection.insertMany(documents);
                    }

                    // Close the MongoClient instance
                    mongoClient.close();
                }
        );

//        totalTradesValuePerSymbol.saveAsTextFile(aggregatedHdfsPath);

        sc.close();
    }
}
