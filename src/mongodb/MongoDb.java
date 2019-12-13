/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import java.util.Arrays;
import com.mongodb.client.model.Filters;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;

/**
 *
 * @author belciñaan_sd2081
 */
public class MongoDb {

    /**
     * @param args the command line arguments
     * @return
     */
    public DBCollection mongoConn() {
        Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.SEVERE);
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        DB db = mongoClient.getDB("belcina");
        DBCollection collection = db.getCollection("bigdata");
        return collection;
    }
    // variable for columns in table
    int num1 = 0;
    int num2 = 1;
    int num3 = 2;
    int num4 = 3;
    int num5 = 4;

    public void writeWSingleOpenClose() {
        // For the start time
        System.out.println("starting time: " + java.time.LocalTime.now());
// Creating connection to database
        DBCollection coll = mongoConn();

//  for loop in in the Query 
        for (int i = 0; i < 1000; i++) {
            num1 = num1 + 1;
            num2 = num2 + 1;
            num3 = num3 + 1;
            num4 = num4 + 1;
            num5 = num5 + 1;
            // insert using BasicDBObject, an equivalent of Map
            BasicDBObject dbobj = new BasicDBObject();
            dbobj.append("firstnum", num1);
            dbobj.append("secondnum", num2);
            dbobj.append("thirdnum", num3);
            dbobj.append("fourthnum", num4);
            dbobj.append("fifthnum", num5);
            coll.insert(dbobj);
//            coll.insertOne(new Document(dbobj)); // we use this DBObject to create a Document    
        }
        //  for the end time
        long total = coll.count(); // getting the total number of documents in the collection
        System.out.println("A total of " + total + " documents now in database");
        // For the end time
        System.out.println("End time: " + java.time.LocalTime.now());

    }

    public void writeWOpenClosePerWrite() {
// For the start time
        System.out.println("starting time: " + java.time.LocalTime.now());
//  for loop in in the Query 

        for (int i = 0; i < 300; i++) {
// Creating connection to database
            DBCollection coll = mongoConn();
            num1 = num1 + 1;
            num2 = num2 + 1;
            num3 = num3 + 1;
            num4 = num4 + 1;
            num5 = num5 + 1;
            BasicDBObject dbobj = new BasicDBObject();
            dbobj.append("firstnum", num1);
            dbobj.append("secondnum", num2);
            dbobj.append("thirdnum", num3);
            dbobj.append("fourthnum", num4);
            dbobj.append("fifthnum", num5);
            coll.insert(dbobj); // we use this DBObject to create a Document    
        }
//  end of for loop
//        long total = conn.count(); // getting the total number of documents in the collection
//        System.out.println("A total of " + total + " documents now in database");
//  for the end time
        System.out.println("end time: " + java.time.LocalTime.now());
    }

    public void deleteWSingleOpenClose() {

// For the start time
        System.out.println("starting time: " + java.time.LocalTime.now());
        // Creating connection to database
        DBCollection coll = mongoConn();
//  for loop in in the Query 
        for (int i = 0; i < 1000; i++) {
            num1 = num1 + 1;
            num2 = num2 + 1;
            num3 = num3 + 1;
            num4 = num4 + 1;
            num5 = num5 + 1;
            BasicDBObject dbobj = new BasicDBObject();
            dbobj.remove("firstnum", num1);
            dbobj.remove("secondnum", num2);
            dbobj.remove("thirdnum", num3);
            dbobj.remove("fourthnum", num4);
            dbobj.remove("fifthnum", num5);
            coll.remove(dbobj);
//            conn.insertOne(new Document(dbobj)); // we use this DBObject to create a Document    
//  executing the query
        }
//  for the end time
        long total = coll.count(); // getting the total number of documents in the collection
        System.out.println("A total of " + total + " documents now in database");
        System.out.println("end time: " + java.time.LocalTime.now());
    }

    public void deleteWOpenClosePerDelete() {

// For the start time
        System.out.println("starting time: " + java.time.LocalTime.now());
//  for loop in in the Query 
        for (int i = 0; i < 300; i++) {
            // Creating connection to database
            DBCollection coll = mongoConn();
            num1 = num1 + 1;
            num2 = num2 + 1;
            num3 = num3 + 1;
            num4 = num4 + 1;
            num5 = num5 + 1;
            BasicDBObject dbobj = new BasicDBObject();
            dbobj.remove("firstnum", num1);
            dbobj.remove("secondnum", num2);
            dbobj.remove("thirdnum", num3);
            dbobj.remove("fourthnum", num4);
            dbobj.remove("fifthnum", num5);
            coll.remove(dbobj);
//            conn.insertOne(new Document(dbobj)); // we use this DBObject to create a Document    
//  executing the query
        }
//  for the end time

        System.out.println("end time: " + java.time.LocalTime.now());
    }

    public void getAvgWQueryLoop() {
// For the start time
        System.out.println("starting time: " + java.time.LocalTime.now());
// Creating connection to database
        DBCollection coll = mongoConn();
        double totalAvg1 = 0;
        double totalAvg2 = 0;
        double totalAvg3 = 0;
        double totalAvg4 = 0;
        double totalAvg5 = 0;

        for (int i = 1; i < 1001; i++) {
            BasicDBObject query = new BasicDBObject("firstnum", i);
            DBCursor c = coll.find(query);
            // Iterating through the result of the query
            while (c.hasNext()) {
                totalAvg1 += Integer.parseInt(c.next().get("firstnum").toString());
            }
            BasicDBObject query2 = new BasicDBObject("secondnum", i);
            DBCursor c2 = coll.find(query2);
            // Iterating through the result of the query
            while (c2.hasNext()) {
                totalAvg2 += Integer.parseInt(c2.next().get("secondnum").toString());
            }
            BasicDBObject query3 = new BasicDBObject("thirdnum", i);
            DBCursor c3 = coll.find(query3);
            // Iterating through the result of the query
            while (c3.hasNext()) {
                totalAvg3 += Integer.parseInt(c3.next().get("thirdnum").toString());
            }
            BasicDBObject query4 = new BasicDBObject("fourthnum", i);
            DBCursor c4 = coll.find(query4);
            // Iterating through the result of the query
            while (c4.hasNext()) {
                totalAvg4 += Integer.parseInt(c4.next().get("fourthnum").toString());
            }
            BasicDBObject query5 = new BasicDBObject("fifthnum", i);
            DBCursor c5 = coll.find(query5);
            // Iterating through the result of the query
            while (c5.hasNext()) {
                totalAvg5 += Integer.parseInt(c5.next().get("fifthnum").toString());
            }

        }
        System.out.println("Averages:");
        System.out.println("Column 1: " + totalAvg1 / 1000);
        System.out.println("Column 2: " + totalAvg2 / 1000);
        System.out.println("Column 3: " + totalAvg3 / 1000);
        System.out.println("Column 4: " + totalAvg4 / 1000);
        System.out.println("Column 5: " + totalAvg5 / 1000);
        System.out.println("end time: " + java.time.LocalTime.now());
    }
    double totalAvg1 = 0;
    double totalAvg2 = 0;
    double totalAvg3 = 0;
    double totalAvg4 = 0;
    double totalAvg5 = 0;

    public void getAverageUsingMongoAgregation() {

        // For the start time
        System.out.println("starting time: " + java.time.LocalTime.now());
        Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.SEVERE);
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("belcina");
        MongoCollection<Document> coll = database.getCollection("bigdata");
        Block<Document> addCol1Value = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                totalAvg1 += Integer.parseInt(document.get("_id").toString());
            }
        };
        Block<Document> addCol2Value = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                totalAvg2 += Integer.parseInt(document.get("_id").toString());
            }
        };
        Block<Document> addCol3Value = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                totalAvg3 += Integer.parseInt(document.get("_id").toString());
            }
        };
        Block<Document> addCol4Value = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                totalAvg4 += Integer.parseInt(document.get("_id").toString());
            }
        };
        Block<Document> addCol5Value = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                totalAvg5 += Integer.parseInt(document.get("_id").toString());
            }
        };
        for (int i = 1; i < 1001; i++) {
            coll.aggregate(
                    Arrays.asList(
                            Aggregates.match(Filters.eq("firstnum", i)),
                            Aggregates.group("$firstnum")
                    )
            ).forEach(addCol1Value);

            coll.aggregate(
                    Arrays.asList(
                            Aggregates.match(Filters.eq("secondnum", i + 1)),
                            Aggregates.group("$secondnum")
                    )
            ).forEach(addCol2Value);

            coll.aggregate(
                    Arrays.asList(
                            Aggregates.match(Filters.eq("thirdnum", i + 2)),
                            Aggregates.group("$thirdnum")
                    )
            ).forEach(addCol3Value);

            coll.aggregate(
                    Arrays.asList(
                            Aggregates.match(Filters.eq("fourthnum", i + 3)),
                            Aggregates.group("$fourthnum")
                    )
            ).forEach(addCol4Value);

            coll.aggregate(
                    Arrays.asList(
                            Aggregates.match(Filters.eq("fifthnum", i + 4)),
                            Aggregates.group("$fifthnum")
                    )
            ).forEach(addCol5Value);
        }
        System.out.println("Column 1 Average: " + totalAvg1 / 1000);
        System.out.println("Column 2 Average: " + totalAvg2 / 1000);
        System.out.println("Column 3 Average: " + totalAvg3 / 1000);
        System.out.println("Column 4 Average: " + totalAvg4 / 1000);
        System.out.println("Column 5 Average: " + totalAvg5 / 1000);
        System.out.println("end time: " + java.time.LocalTime.now());

    }

    public static void main(String[] args) {
        MongoDb md = new MongoDb();
//        md.writeWSingleOpenClose();        
//        md.deleteWSingleOpenClose();

//        md.writeWOpenClosePerWrite();
//        System.out.println("A total of " + md.mongoConn().count() + " documents now in database");
//        md.deleteWOpenClosePerDelete();
//        System.out.println("A total of " + md.mongoConn().count() + " documents now in database");
//        md.getAvgWQueryLoop();
        md.getAverageUsingMongoAgregation();
    }

}
