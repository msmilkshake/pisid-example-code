package ricardotest;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.LocalDateTime;

public abstract class Sensor extends Thread {

    private MongoClient mongoClient;
    private MongoDatabase database;
    protected MongoCollection<Document> collection;
    protected Document query;
    protected MongoCursor<Document> cursor;
    protected LocalDateTime now;

    public Sensor(MongoClient mongoClient, MongoDatabase database, MongoCollection<Document> collection) {
        this.mongoClient = mongoClient;
        this.database = database;
        this.collection = collection;
    }

    public boolean checkTimeStamp(String datetime) {
        // TODO
        return false;
    }

    public boolean checkRecordFormat() {
        // TODO
        return false;
    }

    public abstract void writeToMySql(Document doc);

    public void closeConnection() {
        // Close the cursor and connection
        cursor.close();
        mongoClient.close();
    }
}
