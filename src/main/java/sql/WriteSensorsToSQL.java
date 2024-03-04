package sql;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.swing.*;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;

public class WriteSensorsToSQL {

    String sql_table_to = "";
    String sql_database_connection_to = "";
    String sql_database_password_to = "";
    String sql_database_user_to = "";
    String mongo_url = "";
    int mongo_port;
    String mongo_database = "";
    String collection_movs = "";
    String collection_temps = "";
    Connection connTo;
    MongoClient mongoClient;
    MongoDatabase database;

    MongoCollection<Document> movsCollection;
    MongoCollection<Document> tempsCollection;

    MongoCursor<Document> cursor;

    public void run() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("MariaDBConfig.ini"));
            sql_table_to = p.getProperty("sql_table_to");
            sql_database_connection_to = p.getProperty("sql_database_connection_to");
            sql_database_password_to = p.getProperty("sql_database_password_to");
            sql_database_user_to = p.getProperty("sql_database_user_to");
            mongo_url = p.getProperty("mongo_url");
            mongo_port = Integer.parseInt(p.getProperty("mongo_port"));
            mongo_database = p.getProperty("mongo_database");
            collection_movs = p.getProperty("collection_movs");
            collection_temps = p.getProperty("collection_temps");
        } catch (Exception e) {
            System.out.println("Error reading WriteMysql.ini file " + e);
            JOptionPane.showMessageDialog(null, "The WriteMysql inifile wasn't found.", "Data Migration", JOptionPane.ERROR_MESSAGE);
        }
        connectDatabases();
        
        try {
            ReadData();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void connectDatabases() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            connTo = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to, sql_database_password_to);
            mongoClient = new MongoClient(mongo_url, mongo_port);
            database = mongoClient.getDatabase(mongo_database);
            movsCollection = database.getCollection(collection_movs);
            tempsCollection = database.getCollection(collection_temps);
        } catch (Exception e) {
            System.out.println("Mysql Server Destination down, unable to make the connection. " + e);
        }
    }


    public void ReadData() throws InterruptedException {
        
        LocalDateTime timestamp = LocalDateTime.now();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date timestamp2;
        try {
            timestamp2 = dateFormat.parse("2024-03-04T23:11:11.757332100Z");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        System.out.println(timestamp2);

        while (true) {
            String ts = timestamp.toString() + "Z";
            System.out.println("Timestamp now: " + timestamp);

            Document testQuery = Document.parse("{q: [" +
                    "{ $addFields: { timestamp: { $dateFromString: {dateString: \"$Hora\" } } } }," +
                    "{ $match: { timestamp: { $gte: ISODate('2024-03-04T23:11:11.757332100Z') } } }," +
                    "{ $project: {_id: 1, Timestamp: \"$timestamp\", SalaDestino: 1, SalaOrigem: 1 } }" +
                    "]}"
            );

            List<Document> aggregateQuery = new ArrayList<>();
            aggregateQuery.add(Document.parse("{ $addFields: { timestamp: { $dateFromString: {dateString: \"$Hora\" } } } }"));
            aggregateQuery.add(Document.parse("{ $match: { timestamp: { $gte: ISODate('2024-03-04T23:11:11.757332100Z') } } }"));
            aggregateQuery.add(Document.parse("{ $project: {_id: 1, Timestamp: \"$timestamp\", SalaDestino: 1, SalaOrigem: 1 } }"));

            List<Bson> aggregateQuery2 = new ArrayList<>();
            aggregateQuery2.add(Document.parse("{ $addFields: { timestamp: { $dateFromString: {dateString: \"$Hora\" } } } }"));
            aggregateQuery2.add(Aggregates.match(Filters.gte("timestamp", "2024-03-04T23:11:11.757332100Z")));
            aggregateQuery2.add(Document.parse("{ $project: {_id: 1, Timestamp: \"$timestamp\", SalaDestino: 1, SalaOrigem: 1 } }"));

            AggregateIterable<Document> result = movsCollection.aggregate(aggregateQuery);
            // MongoCursor<Document> cursor2 = movsCollection.find().iterator();
            for (Document doc : result) {
                System.out.println(doc.toJson());
            }
            Document doc = null;
            while (cursor.hasNext()) {
                Document doc2 = cursor.next();
                System.out.println(doc2);
            }
            if (doc != null) {
                timestamp = LocalDateTime.parse(doc.get("Timestamp").toString());
            }
            
            Thread.sleep(5000);
        }
        //        String doc = new String();
        //        int i = 0;
        //        while (i < 100) {
        //            doc = "{Name:\"Nome_" + i + "\", Location:\"Portugal\", id:" + i + "}";
        //            //WriteToMySQL(com.mongodb.util.JSON.serialize(doc));
        //            WriteToMySQL(doc);
        //            i++;
    }

    public void WriteToMySQL(String c) {
        String convertedjson = new String();
        convertedjson = c;
        String fields = new String();
        String values = new String();
        String SqlCommando = new String();
        String column_database = new String();
        fields = "";
        values = "";
        column_database = " ";
        String x = convertedjson.toString();
        String[] splitArray = x.split(",");
        for (int i = 0; i < splitArray.length; i++) {
            String[] splitArray2 = splitArray[i].split(":");
            if (i == 0) fields = splitArray2[0];
            else fields = fields + ", " + splitArray2[0];
            if (i == 0) values = splitArray2[1];
            else values = values + ", " + splitArray2[1];
        }
        fields = fields.replace("\"", "");
        SqlCommando = "Insert into " + sql_table_to + " (" + fields.substring(1, fields.length()) + ") values (" + values.substring(0, values.length() - 1) + ");";
        // System.out.println(SqlCommando);
        try {
        } catch (Exception e) {
            System.out.println(e);
        }
        try {
            Statement s = connTo.createStatement();
            int result = s.executeUpdate(SqlCommando);
            s.close();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database . " + e);
            System.out.println(SqlCommando);
        }
    }


    public static void main(String[] args) {
        // new WriteSensorsToSQL().tests();
        new WriteSensorsToSQL().run();
    }

    public void tests() {
        LocalDateTime timestamp = LocalDateTime.now();
        System.out.println(timestamp);
    }

}

