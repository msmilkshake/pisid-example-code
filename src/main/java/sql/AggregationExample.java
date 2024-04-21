package sql;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class AggregationExample {

    public static void main(String[] args) {
        // Connect to MongoDB server
        MongoClient mongoClient = new MongoClient("localhost", 27019);
        // Get reference to your database
        MongoDatabase database = mongoClient.getDatabase("mqtt");

        // Print the collection names
//        Iterable<String> collectionNames = database.listCollectionNames();
//        for (String collectionName : collectionNames) {
//            System.out.println(collectionName);
//        }


        // Get reference to your collection
        MongoCollection<Document> collection = database.getCollection("test");

//        long totalDocuments = collection.countDocuments(); // Count all documents
//        System.out.println("Total documents: " + totalDocuments);

        MongoCursor<Document> cursor = collection.find().iterator();
        HashMap<Integer, Integer> rooms_population = new HashMap<>();
        for (int i = 0 ; i <= 10; i++) { // initialize hash map
            rooms_population.put(i, 0);
        }

        // Iterate over the results
        while (cursor.hasNext()) {
            Document doc = cursor.next();
            int from_room = (Integer) doc.get("SalaOrigem");
            int to_room = (Integer) doc.get("SalaDestino");
            rooms_population.put(to_room, rooms_population.get(to_room) + 1);
            rooms_population.put(from_room, rooms_population.get(from_room) - 1);
//            System.out.printf("From: %d To: %d\n", from_room, to_room);
//            System.out.println(doc.toJson());
//            doc.remove("_id");
            System.out.println(doc);
        }

        System.out.println(rooms_population);

        // BsonArray query3 = BsonArray.parse("[\n" +
        //         "    { $group: { _id: \"$SalaDestino\", Count: { $sum: 1 } } },\n" +
        //         "    { $match: { _id: 3} },\n" +
        //         "    { $project: { SalaDestino: \"$_id\", Count: \"$Count\", _id: 0} }\n" +
        //         "]");
        
//        Document testQuery = Document.parse("{q: [\n" +
//                "    { $group: { _id: \"$SalaDestino\", Count: { $sum: 1 } } },\n" +
//                "    { $match: { _id: 3} },\n" +
//                "    { $project: { SalaDestino: \"$_id\", Count: \"$Count\", _id: 0} }\n" +
//                "]}");
        
        // List<BsonValue> l1 = new ArrayList<>(query3);
        
        // Alternativa com Lista:
//        List<Document> aggregateQuery = new ArrayList<>();
//        aggregateQuery.add(Document.parse("{ $group: { _id: \"$SalaDestino\", Count: { $sum: 1 } } }"));
//        aggregateQuery.add(Document.parse("{ $match: { _id: 3} }"));
//        aggregateQuery.add(Document.parse("{ $project: { SalaDestino: \"$_id\", Count: \"$Count\", _id: 0} }"));
        
        
        // Perform the query
//        cursor = collection.aggregate((List<? extends Bson>) testQuery.get("q")).iterator();

        // Iterate over the results
//        while (cursor.hasNext()) {
//            Document doc = cursor.next();
////            System.out.println(doc.toJson());
//        }

        // Close the cursor and connection
        cursor.close();
        mongoClient.close();
    }
}