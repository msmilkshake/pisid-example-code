package ricardotest;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.LocalDateTime;

public class Main_Temps_Migration extends Sensor {



    // atributo experiencia, criava-se uma class para isto???
    //

    public Main_Temps_Migration(MongoClient mongoClient, MongoDatabase database, MongoCollection<Document> collection) {
        super(mongoClient, database, collection);

    }

    @Override
    public void run() {
        // Definir a query ao mongo a fazer aqui, deverá ser do instante de tempo
        // Mas como por exemplo fazemos uma query com instante de agora???
        // Nunca vamos receber TimeStamps fora porque a query não retorna
        super.query = Document.parse("{ SalaDestino: { $ne: null } }");

        cursor = collection.find(query).iterator();

        while(cursor.hasNext()) {
            Document doc = cursor.next();

        }

        closeConnection();
    }

    public void writeToMySql(Document doc) {

        String tableName;
        String dateTime = doc.get("Hora").toString();
        double temperatura = (double) doc.get("Leitura");
        int sensor = (int) doc.get("Sensor");

        // Criar String para insert no MySQL
        String sqlCommando = "Insert into .........";

    }

    public void checkProximidadeLimite(Document doc) {
        double tempAtual = (double) doc.get("Leitura");

        //Tem de se ir buscar à experiencia
        double tempMin = 0;
        double tempMax = 0;

        double intervalo = tempMax - tempMin;

        double limiteInferior = tempMin + 0.1 * intervalo;
        double limiteSuperior = tempMin + 0.9 * intervalo;

        if (tempAtual <= limiteInferior) {
            // Lançar ALERTA??? ou função WriteMySQL???
            System.out.println("Temperatura próxima do limite inferior");
        } else if (tempAtual >= limiteSuperior) {
            // Lançar ALERTA??? ou função WriteMySQL???
            System.out.println("Temperatura próxima do limite superior");
        }
    }

    public void checkAtingiuLimite(Document doc) {
        double tempAtual = (double) doc.get("Leitura");

        //Tem de se ir buscar à experiencia
        double tempMin = 0;
        double tempMax = 0;

        if (tempAtual <= tempMin) {
            // Lançar ALERTA??? ou função WriteMySQL???
            System.out.println("Temperatura atingiu limite inferior");
        } else if (tempAtual >= tempMax) {
            // Lançar ALERTA??? ou função WriteMySQL???
            System.out.println("Temperatura atingiu limite superior");
        }

    }




}
