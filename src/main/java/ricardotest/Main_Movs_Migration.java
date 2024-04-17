package ricardotest;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.LocalDateTime;

public class Main_Movs_Migration extends Sensor {


    public Main_Movs_Migration(MongoClient mongoClient, MongoDatabase database, MongoCollection<Document> collection) {
        super(mongoClient, database, collection);
    }

    @Override
    public void run() {
        // Definir a query ao mongo a fazer aqui, deverá ser do instante de tempo
        // Mas como por exemplo fazemos uma query com instante de agora???
        // Nunca vamos receber TimeStamps fora porque a query não retorna


        // Timestamp no mongo de quando foi introduzido o registo
        query = Document.parse("{ SalaDestino: { $ne: null } }");

        cursor = collection.find(query).iterator();

        while(cursor.hasNext()) {
            Document doc = cursor.next();


        }

        closeConnection();
    }

    public boolean checkMovement(int salaOrigem, int salaDestino) {
        // TODO
        return false;
    }

    @Override
    public void writeToMySql(Document doc) {

        //{Hora: "2024-04-16 13:51:00.644337", Leitura: 2, Sensor: 1}

        String tableName;
        String dateTime = doc.get("Hora").toString();
        int salaOrigem = (int) doc.get("SalaOrigem");
        int salaDestino = (int) doc.get("SalaDestino");

        // Criar String para insert no MySQL
        String sqlCommando = "Insert into .........";

    }




}
