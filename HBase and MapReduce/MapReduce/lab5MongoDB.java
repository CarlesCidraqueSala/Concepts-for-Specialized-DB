package cristian;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Arrays.asList;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;

public class MongoDBLab {

    private MongoDatabase database;
    private MongoCollection<Document> lineitem;

    public static void main(String[] args) {
        MongoDBLab program = new MongoDBLab();
        program.createDBAndInserts();
        program.query1();
    }

    /**
     * SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
     sum(l_extendedprice) as sum_base_price,
     sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
     sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty,
     avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
     FROM lineitem
     WHERE l_shipdate <= '[date]'
     GROUP BY l_returnflag, l_linestatus
     ORDER BY l_returnflag, l_linestatus;
     */
    private void query1() {
        String l_returnflag, l_linestatus;
        int sum_qty = 0, sum_base_price = 0, sum_disc_price = 0, sum_charge = 0, avg_qty = 0,
                avg_price = 0, avg_disc = 0, count_order = 0;

        Consumer<Document> printBlock = new Consumer<Document>() {
            public void accept(final Document document) {
                System.out.println(document.toJson());
            }
        };
        lineitem.aggregate(asList(
                sort(descending(asList("RETURNFLAG", "LINESTATUS"))),
                project(fields(excludeId()))
        ))
                .forEach(printBlock);
    }

    private void createDBAndInserts() {
        // Get database connection
        MongoClient mongoClient = MongoClients.create();
        this.database = mongoClient.getDatabase("TPC-H_CBDE");
        // Get all the documents used
        this.lineitem = database.getCollection("lineitem");
        // Drop all data from previous executions
        dropAllData();
        // Generate info for the documents
        int numDocsGen = 100;

        List<Document> regionDocs = generateDocumentREGION();
        List<Document> nationDocs = generateDocumentNATION(regionDocs);
        List<Document> partDocs = generateDocumentPART(numDocsGen);
        List<Document> supplierDocs = generateDocumentSUPPLIER(numDocsGen, nationDocs);
        List<Document> partSuppDocs = generateDocumentPARTSUPP(numDocsGen, partDocs, supplierDocs);
        List<Document> customerDocs = generateDocumentCUSTOMER(numDocsGen);
        List<Document> ordersDocs = generateDocumentORDERS(numDocsGen, customerDocs);
        List<Document> lineitemDocs = generateDocumentLINEITEM(numDocsGen, ordersDocs, partSuppDocs);

        this.lineitem.insertMany(lineitemDocs);

        Document myDoc = lineitem.find().projection(excludeId()).first();
        System.out.println(myDoc.toJson());
        System.out.println(generateRandomDate());
    }

    private List<Document> generateDocumentPART(int numDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document("PARTKEY", i);
            doc.append("NAME", generateRandomString(10));
            doc.append("MFGR", generateRandomString(7));
            doc.append("BRAND", generateRandomString(8));
            doc.append("TYPE", generateRandomType());
            doc.append("SIZE", generateRandomNum(1, 10));
            doc.append("CONTAINER", generateRandomString(2));
            doc.append("RETAILPRICE", generateRandomString(3));
            doc.append("COMMENT", generateRandomString(20));
            documents.add(doc);
        }
        return documents;
    }

    private List<Document> generateDocumentPARTSUPP(int numDocs, List<Document> partDocs, List<Document> supplierDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document("PARTKEY", i);
            doc.append("SUPPKEY", generateRandomString(10));
            doc.append("AVAILQTY", generateRandomString(7));
            doc.append("BRAND", generateRandomString(8));
            doc.append("SUPPLYCOST", generateRandomString(4));
            doc.append("COMMENT", generateRandomString(20));
            doc.append("SUPPLIER", supplierDocs.get(i%supplierDocs.size()));
            doc.append("PART", partDocs.get(i%partDocs.size()));
            documents.add(doc);
        }
        return documents;
    }

    private List<Document> generateDocumentSUPPLIER(int numDocs, List<Document> nationDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document("SUPPKEY", i);
            doc.append("NAME", generateRandomString(10));
            doc.append("ADDRESS", generateRandomString(8));
            doc.append("NATIONKEY", nationDocs.get(i%nationDocs.size()));
            doc.append("PHONE", generateRandomString(9));
            doc.append("ACCTBAL", generateRandomString(10));
            doc.append("COMMENT", generateRandomString(20));
            documents.add(doc);
        }
        return documents;
    }

    private List<Document> generateDocumentNATION(List<Document> regionDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 25; i++) {
            Document doc = new Document("NATIONKEY", i);
            doc.append("NAME", generateRandomString(10));
            doc.append("REGIONKEY", regionDocs.get(i%regionDocs.size()));
            doc.append("COMMENT", generateRandomString(20));
            documents.add(doc);
        }
        return documents;
    }

    private List<Document> generateDocumentREGION() {
        List<Document> documents = new ArrayList<Document>();
        // Hasta 5, ya que solo tenemos 5 regiones.
        for (int i = 0; i < 5; i++) {
            Document doc = new Document("REGIONKEY", generateRandomRegion());
            doc.append("NAME", generateRandomString(10));
            doc.append("COMMENT", generateRandomString(20));
            documents.add(doc);
        }
        return documents;
    }

    private List<Document> generateDocumentLINEITEM(int numDocs, List<Document> ordersDocs, List<Document> partSuppDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document("ORDERKEY", ordersDocs.get(i%ordersDocs.size()));
            doc.append("PARTKEY", generateRandomString(10)); // Cambiar
            doc.append("SUPPKEY", generateRandomString(8)); // cambiar
            doc.append("LINENUMBER", generateRandomNum(1, 100));
            doc.append("QUANTITY", generateRandomNum(1, 100));
            doc.append("EXTENDEDPRICE", generateRandomNum(10, 1000));
            doc.append("DISCOUNT", generateRandomNum(0, 80));
            doc.append("TAX", generateRandomNum(0, 30));
            doc.append("RETURNFLAG", generateRandomString(20));
            doc.append("LINESTATUS", generateRandomString(20));
            doc.append("SHIPDATE", generateRandomDate());
            doc.append("COMMITDATE", generateRandomDate());
            doc.append("RECEIPTDATE", generateRandomDate());
            doc.append("SHIPINSTRUCT", generateRandomString(8));
            doc.append("SHIPMODE", generateRandomString(5));
            doc.append("COMMENT", generateRandomString(20));
            doc.append("PARTSUPP", partSuppDocs.get(i%partSuppDocs.size()));
            documents.add(doc);
        }
        return documents;
    }

    private List<Document> generateDocumentORDERS(int numDocs, List<Document> customersDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < numDocs*10; i++) {
            Document doc = new Document("ORDERKEY", i);
            doc.append("CUSTKEY", customersDocs.get(i%customersDocs.size()));
            doc.append("ORDERSTATUS", generateRandomString(5));
            doc.append("TOTALPRICE", generateRandomNum(50, 5000));
            doc.append("ORDERDATE", generateRandomDate());
            doc.append("ORDER-PRIORITY", generateRandomString(10));
            doc.append("CLERK", generateRandomString(5));
            doc.append("SHIP-PRIORITY", generateRandomString(7));
            doc.append("COMMENT", generateRandomString(20));
            documents.add(doc);
        }
        return documents;
    }

    private List<Document> generateDocumentCUSTOMER(int numDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 200; i++) {
            Document doc = new Document("CUSTKEY", i);
            doc.append("NAME", generateRandomString(10));
            doc.append("ADDRESS", generateRandomString(8));
            // ¿Index?
            doc.append("NATIONKEY", i%25);
            doc.append("PHONE", generateRandomString(9));
            doc.append("ACCTBAL", generateRandomString(11));
            doc.append("MKTSEGMENT", generateRandomString(8));
            doc.append("COMMENT", generateRandomString(20));
            documents.add(doc);
        }
        return documents;
    }

    /**
     * Genera un numero entre min y max
     * @return int
     */
    private int generateRandomNum(int min, int max) {
        Random rand = new Random();
        return min + (int) (rand.nextFloat() * (max-min+1));
    }

    /**
     * Genera un String con los Strings especificados en el array region
     * @return String
     */
    private String generateRandomRegion() {
        String[] region = new String[] { "REGION_1", "REGION_2", "REGION_3", "REGION_4", "REGION_5" };
        Random rand = new Random();
        int index = (int) (rand.nextFloat() * (region.length));
        return region[index];
    }

    /**
     * Genera una fecha entre hoy y 5 dias atras.
     * @return String
     */
    private String generateRandomDate() {
        long fiveDaysBeforeNow = 432000000;
        Date now = new Date();
        Date earlierNow = new Date(now.getTime()-fiveDaysBeforeNow);
        long initDateMillis = earlierNow.getTime();
        long endDateMillis = now.getTime();
        long randomDateInMillis = ThreadLocalRandom.current().nextLong(initDateMillis, endDateMillis);
        Date randomDate = new Date(randomDateInMillis);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        return formatter.format(randomDate);
    }

    /**
     * Genera un String con los Strings especificados en el array types
     * @return String
     */
    private String generateRandomType() {
        String[] types = new String[] { "ABCD", "CDBA", "BDCA", "ADBC", "ACDB", "BCAD" };
        Random rand = new Random();
        int index = (int) (rand.nextFloat() * (types.length));
        return types[index];
    }

    /**
     * Genera un String random con una longitud maxima determinada.
     * @param maxLength la longitud maxima del String
     * @return String
     */
    private String generateRandomString(int maxLength) {
        StringBuilder generatedString = new StringBuilder(maxLength);
        int min = 97, max = 122;
        Random rand = new Random();
        for (int i = 0; i < maxLength; i++) {
            int generatedLetter = min + (int) (rand.nextFloat() * (max-min+1));
            generatedString.append((char) generatedLetter);
        }
        return generatedString.toString();
    }

    /**
     * Borra todos los datos de la BD
     */
    private void dropAllData() {
        lineitem.drop();
    }
}