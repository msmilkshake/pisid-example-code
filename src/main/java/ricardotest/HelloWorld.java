package ricardotest;

import com.google.gson.JsonObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HelloWorld {

    public static void main(String[] args) {
        // Connect to MongoDB server
        MongoClient mongoClient = new MongoClient("localhost", 27019);

        // Get reference to your database
        MongoDatabase database = mongoClient.getDatabase("mqtt");

        // Get reference to your collection
        MongoCollection<Document> collection = database.getCollection("movs");

        // Define the query
        //Document query = new Document("SalaDestino", new Document("$ne", null));

        Document query2 = Document.parse("{ SalaDestino: { $ne: null } }");

        // Perform the query
        MongoCursor<Document> cursor = collection.find(query2).iterator();


        // Get current date and time
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.ms");
        System.out.println(now);
        System.out.println(dateTimeFormatter.format(now));

        // Iterate over the results
        if (cursor.hasNext()) {
            Document doc = cursor.next();

            System.out.println(doc.toJson());

            System.out.println(doc.get("Hora").getClass());
            System.out.println(doc.get("SalaOrigem").getClass());
            System.out.println(doc.get("SalaDestino"));
        }

        /*
        Document testQuery = Document.parse("{q: [\n" +
                "    { $group: { _id: \"$SalaDestino\", Count: { $sum: 1 } } },\n" +
                "    { $match: { _id: 3} },\n" +
                "    { $project: { SalaDestino: \"$_id\", Count: \"$Count\", _id: 0} }\n" +
                "]}");

        // List<BsonValue> l1 = new ArrayList<>(query3);

        // Alternativa com Lista:
        List<Document> aggregateQuery = new ArrayList<>();
        aggregateQuery.add(Document.parse("{ $group: { _id: \"$SalaDestino\", Count: { $sum: 1 } } }"));
        aggregateQuery.add(Document.parse("{ $match: { _id: 3} }"));
        aggregateQuery.add(Document.parse("{ $project: { SalaDestino: \"$_id\", Count: \"$Count\", _id: 0} }"));


        // Perform the query
        cursor = collection.aggregate((List<? extends Bson>) testQuery.get("q")).iterator();

        // Iterate over the results
        while (cursor.hasNext()) {
            Document doc = cursor.next();
            System.out.println(doc.toJson());
        }

         */

        // Close the cursor and connection
        cursor.close();
        mongoClient.close();
    }
}