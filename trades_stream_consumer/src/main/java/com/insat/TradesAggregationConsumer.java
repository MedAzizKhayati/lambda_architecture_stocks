package com.insat;

import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

public class TradesAggregationConsumer {
    private static final String DATABASE_NAME = "trades";
    private static final String MONGO_URI = "mongodb+srv://admin:admin@cluster0.9qemw.mongodb.net";
    private static final String COLLECTION_NAME = "trades_aggregations";
    public static void main(String[] args) {
        // Create a MongoClient instance to connect to MongoDB
        MongoClient mongoClient = MongoClients.create(
                MONGO_URI
        );
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        // Listen in real time to the collection with change streams
        for (ChangeStreamDocument<Document> documentChangeStreamDocument : collection.watch()) {
            System.out.println(documentChangeStreamDocument.getFullDocument());
        }
    }
}
