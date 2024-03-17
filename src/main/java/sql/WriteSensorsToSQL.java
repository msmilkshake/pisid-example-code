package sql;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.swing.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

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
    Connection mariadbConnection;
    MongoClient mongoClient;
    MongoDatabase database;

    MongoCollection<Document> movsCollection;
    MongoCollection<Document> tempsCollection;

    MongoCursor<Document> movsCursor;
    MongoCursor<Document> tempsCursor;

    int movsFrequency = 1; // 1 seconds
    int tempsFrequency = 3; // 3 seconds

    long movsTimestamp;
    long tempsTimestamp;

    public WriteSensorsToSQL() {
        movsFrequency *= 1000;
        tempsFrequency *= 1000;
    }

    public Thread createMovsThread() {
        return new Thread(() -> {
            while (true) {
                System.out.println("[" + Thread.currentThread().getName() + "]Timestamp now: " + movsTimestamp);

                Document movsQuery = Document.parse("{q: [" +
                        "{ $addFields: { timestamp: { $toLong: { $dateFromString: { dateString: \"$Hora\" } } } } }," +
                        "{ $match: { timestamp: { $gte: " + movsTimestamp + " } } }," +
                        "{ $project: {_id: 1, Timestamp: \"$timestamp\", SalaDestino: 1, SalaOrigem: 1, Hora: 1 } }" +
                        "]}"
                );

                movsCursor = movsCollection.aggregate((List<? extends Bson>) movsQuery.get("q")).iterator();

                Document doc = null;
                while (movsCursor.hasNext()) {
                    doc = movsCursor.next();
                    System.out.println(doc);
                    persistMov(doc, System.currentTimeMillis(), 1);
                }
                if (doc != null) {
                    movsTimestamp = System.currentTimeMillis();
                }

                System.out.println("--- Sleeping " + (movsFrequency / 1000) + " seconds... ---\n");
                try {
                    Thread.sleep(movsFrequency);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, "MovsThread");
    }

    public void persistMov(Document doc, long timestamp, long experiencia) {
        int salaOrigem = doc.getInteger("SalaOrigem");
        int salaDestino = doc.getInteger("SalaDestino");
        LocalDateTime hora = LocalDateTime.parse(doc.getString("Hora").replace(" ", "T"));

        String sqlQuery = String.format("" +
                        "INSERT INTO medicoespassagens(IDExperiencia, SalaOrigem, SalaDestino, Hora, TimestampRegisto)\n" +
                        "VALUES (%d, %d, %d, '%s', FROM_UNIXTIME(%d / 1000))",
                experiencia, salaOrigem, salaDestino, hora, timestamp
        );
        try {
            Statement s = mariadbConnection.createStatement();
            int result = s.executeUpdate(sqlQuery);
            s.close();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database . " + e);
            System.out.println(sqlQuery);
        }
    }

    public Thread createTempsThread() {
        return new Thread(() -> {
            while (true) {
                System.out.println("[" + Thread.currentThread().getName() + "]Timestamp now: " + tempsTimestamp);

                Document tempsQuery = Document.parse("{q: [" +
                        "{ $addFields: { timestamp: { $toLong: { $dateFromString: { dateString: \"$Hora\" } } } } }," +
                        "{ $match: { timestamp: { $gte: " + tempsTimestamp + " } } }," +
                        "{ $project: {_id: 1, Timestamp: \"$timestamp\", Sensor: 1, Leitura: 1, Hora: 1 } }" +
                        "]}"
                );

                tempsCursor = tempsCollection.aggregate((List<? extends Bson>) tempsQuery.get("q")).iterator();

                Document doc = null;
                while (tempsCursor.hasNext()) {
                    doc = tempsCursor.next();
                    System.out.println(doc);
                    persistTemp(doc, System.currentTimeMillis(), 1);
                }
                if (doc != null) {
                    tempsTimestamp = System.currentTimeMillis();
                }

                System.out.println("--- Sleeping " + (tempsFrequency / 1000) + " seconds... ---\n");
                try {
                    Thread.sleep(tempsFrequency);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, "TempsThread");
    }

    public void persistTemp(Document doc, long timestamp, long experiencia) {
        double leitura = doc.getDouble("Leitura");
        int sensor = doc.getInteger("Sensor");
        LocalDateTime hora = LocalDateTime.parse(doc.getString("Hora").replace(" ", "T"));

        String sqlQuery = String.format(Locale.US, "" +
                        "INSERT INTO medicoestemperatura(IDExperiencia, Leitura, Sensor, Hora, TimestampRegisto)\n" +
                        "VALUES (%d, %f, %d, '%s', FROM_UNIXTIME(%d / 1000))",
                experiencia, leitura, sensor, hora, timestamp
        );
        try {
            Statement s = mariadbConnection.createStatement();
            int result = s.executeUpdate(sqlQuery);
            s.close();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database . " + e);
            System.out.println(sqlQuery);
        }
    }

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
        Thread movsThread = createMovsThread();
        Thread tempsThread = createTempsThread();

        movsTimestamp = System.currentTimeMillis();
        tempsTimestamp = System.currentTimeMillis();

        movsThread.start();
        tempsThread.start();
        try {
            movsThread.join();
            tempsThread.join();

            // ReadData();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void connectDatabases() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            mariadbConnection = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to, sql_database_password_to);
            mongoClient = new MongoClient(mongo_url, mongo_port);
            database = mongoClient.getDatabase(mongo_database);
            movsCollection = database.getCollection(collection_movs);
            tempsCollection = database.getCollection(collection_temps);
        } catch (Exception e) {
            System.out.println("Mysql Server Destination down, unable to make the connection. " + e);
        }
    }

    public static void main(String[] args) throws MalformedURLException {
//        new WriteSensorsToSQL().tests();
         new WriteSensorsToSQL().run();
    }

    public void tests() throws MalformedURLException {

    }

}

