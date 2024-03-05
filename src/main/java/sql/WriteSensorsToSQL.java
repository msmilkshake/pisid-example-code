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
        
        long timestamp = System.currentTimeMillis();


        while (true) {
            System.out.println("Timestamp now: " + timestamp);

            Document movsQuery = Document.parse("{q: [" +
                    "{ $addFields: { timestamp: { $toLong: { $dateFromString: { dateString: \"$Hora\" } } } } }," +
                    "{ $match: { timestamp: { $gte: " + timestamp + " } } }," +
                    "{ $project: {_id: 1, Timestamp: \"$timestamp\", SalaDestino: 1, SalaOrigem: 1, Hora: 1 } }" +
                    "]}"
            );

            cursor = movsCollection.aggregate((List<? extends Bson>) movsQuery.get("q")).iterator();

            Document doc = null;
            while (cursor.hasNext()) {
                doc = cursor.next();
                System.out.println(doc);
            }
            if (doc != null) {
                timestamp = System.currentTimeMillis();
            }

            System.out.println("\n--- Sleeping 5 seconds... ---");
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

