import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.min;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;
import static java.util.Arrays.asList;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

public class MongoDBLab {

    private MongoDatabase database;
    private MongoCollection<Document> lineitem;


    public static void main(String[] args) {
        MongoDBLab program = new MongoDBLab();
        program.showGUI();
    }

    private void showGUI() {
        Scanner sc = new Scanner(System.in);
        int option = 0;
        System.out.println("Quants documents vols crear? (10000 es un bon número!)");
        System.out.print("> ");
        int numDocs = sc.nextInt();
        createDBAndInserts(numDocs);
        createIndexos();
        System.out.println("Creada BD i inserts.");
        while (option != 5) {
            System.out.println("Que vols fer?");
            System.out.println("1. Executar query 1");
            System.out.println("2. Executar query 2");
            System.out.println("3. Executar query 3");
            System.out.println("4. Executar query 4");
            System.out.println("5. Sortir");
            System.out.print("> ");

            option = sc.nextInt();
            String optQ = "S";
            if (option >= 1 && option <= 4) {
                System.out.println("Vols utilitzar parametres hard-codejats (S), o utilitzar els teus propis (N)?");
                System.out.print("> ");
                optQ = sc.next();
            }
            switch (option) {
                case 1:
                    if (optQ.equals("S")) {
                        GregorianCalendar shipdate = new GregorianCalendar();
                        shipdate.add(Calendar.DATE, -2);
                        String shipdateStr = shipdate.get(Calendar.YEAR) + "-" + putZeros((shipdate.get(Calendar.MONTH) + 1)) + "-" + putZeros(shipdate.get(Calendar.DATE));
                        query1(shipdateStr);
                    } else {
                        System.out.println("Escriu una SHIPDATE (FORMAT: AAAA-MM-DD, s'han de posar els 0 si el dia/mes es menor que 10) (Ex: 2020-12-10) (RANG: [5 DIES ABANS - AVUI])");
                        System.out.print("> ");
                        String shipdate = sc.next();
                        query1(shipdate);
                    }
                    break;
                case 2:
                    if (optQ.equals("S")) {
                        String type = "TYPE_2";
                        String region = "REGION_2";
                        int size = 7;
                        query2(type, region, size);
                    } else {
                        System.out.println("Escriu un TYPE (TYPE_1, TYPE_2, TYPE_3)");
                        System.out.print("> ");
                        String type = sc.next().toUpperCase();
                        System.out.println("Escriu una REGION (REGION_1, REGION_2, REGION_3, REGION_4, REGION_5)");
                        System.out.print("> ");
                        String region = sc.next().toUpperCase();
                        System.out.println("Escriu un SIZE (Enter entre 0 y 10 inclosos)");
                        System.out.print("> ");
                        int size = sc.nextInt();
                        query2(type, region, size);
                    }
                    break;
                case 3:
                    if (optQ.equals("S")) {
                        String segment = "SEGMENT_3";
                        GregorianCalendar dateGC = new GregorianCalendar();
                        dateGC.add(Calendar.DATE, -2);
                        String date = dateGC.get(Calendar.YEAR) + "-" + putZeros((dateGC.get(Calendar.MONTH) + 1)) + "-" + putZeros(dateGC.get(Calendar.DATE));
                        query3(segment, date, date);
                    } else {
                        System.out.println("Escriu un SEGMENT (SEGMENT_1, SEGMENT_2, SEGMENT_3, SEGMENT_4, SEGMENT_5)");
                        System.out.print("> ");
                        String segment = sc.next().toUpperCase();
                        System.out.println("Escriu una ORDERDATE (FORMAT: AAAA-MM-DD, s'han de posar els 0 si el dia/mes es menor que 10) (Ex: 2020-12-10) (RANG: [5 DIES ABANS - AVUI])");
                        System.out.print("> ");
                        String orderdate = sc.next();
                        System.out.println("Escriu una SHIPDATE (FORMAT: AAAA-MM-DD, s'han de posar els 0 si el dia/mes es menor que 10) (Ex: 2020-12-10) (RANG: [5 DIES ABANS - AVUI])");
                        System.out.print("> ");
                        String shipdate = sc.next();
                        query3(segment, orderdate, shipdate);
                    }
                    break;
                case 4:
                    if (optQ.equals("S")) {
                        String region = "REGION_2";
                        GregorianCalendar dateGC = new GregorianCalendar();
                        dateGC.add(Calendar.DATE, -2);
                        String orderdate = dateGC.get(Calendar.YEAR) + "-" + putZeros((dateGC.get(Calendar.MONTH) + 1)) + "-" + putZeros(dateGC.get(Calendar.DATE));
                        query4(region, orderdate);
                    } else {
                        System.out.println("Escriu una REGION (REGION_1, REGION_2, REGION_3, REGION_4, REGION_5)");
                        System.out.print("> ");
                        String region = sc.next().toUpperCase();
                        System.out.println("Escriu una ORDERDATE (FORMAT: AAAA-MM-DD, s'han de posar els 0 si el dia/mes es menor que 10) (Ex: 2020-12-10) (RANG: [5 DIES ABANS - AVUI])");
                        System.out.print("> ");
                        String orderdate = sc.next();
                        query4(region, orderdate);
                    }
                    break;
            }
        }
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
    private void query1(String shipdate) {
        System.out.println("QUERY1, SHIPDATE = " + shipdate);
        Consumer<Document> printBlock = new Consumer<Document>() {
            public void accept(final Document document) {
                System.out.println(document.toJson());
            }
        };

        Document subtractDiscount = new Document("$subtract", Arrays.asList(1, "$DISCOUNT"));
        Document multiplyDiscount = new Document("$multiply", Arrays.asList("$EXTENDEDPRICE", subtractDiscount));
        Document sumTax = new Document("$sum", Arrays.asList(1, "$TAX"));
        Document multiplyTax = new Document("$multiply", Arrays.asList(multiplyDiscount, sumTax));
        Bson sortStage = sort(ascending("_id"));

        lineitem.aggregate(asList(
                match(lte("SHIPDATE", shipdate)),
                project(fields(include("RETURNFLAG", "LINESTATUS", "QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX"), excludeId())),
                group(asList("$RETURNFLAG", "$LINESTATUS"),
                        sum("SUM_QTY", "$QUANTITY"),
                        sum("SUM_BASE_PRICE", "$EXTENDEDPRICE"),
                        sum("SUM_DISC_PRICE", multiplyDiscount),
                        sum("SUM_CHARGE", multiplyTax),
                        avg("AVG_QTY", "$QUANTITY"),
                        avg("AVG_PRICE", "$EXTENDEDPRICE"),
                        avg("AVG_DISC", "$DISCOUNT"),
                        sum("COUNT_ORDER", 1)
                ),
                sortStage
        ))
                .forEach(printBlock);
    }

    /**
     * SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
     FROM part, supplier, partsupp, nation, region
     WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = [SIZE]
     AND p_type like '%[TYPE]' AND s_nationkey = n_nationkey
     AND n_regionkey = r_regionkey AND r_name = '[REGION]'
     AND ps_supplycost =
     (SELECT min(ps_supplycost)
     FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey
     AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
     AND n_regionkey = r_regionkey AND r_name = '[REGION]')
     ORDER BY s_acctbal desc, n_name, s_name, p_partkey;
     */
    private void query2(String type, String region, int size) {
        Consumer<Document> printBlock = new Consumer<Document>() {
            public void accept(final Document document) {
                System.out.println(document.toJson());
            }
        };
        System.out.println("QUERY2: TYPE = " + type + ", REGION = " + region + ", SIZE = " + size);

        Bson sortStageDescendent = sort(descending("PARTSUPP.SUPPLIER.ACCTBAL"));
        Bson sortStageAscendent = sort(ascending(asList("PARTSUPP.SUPPLIER.NATIONKEY.NAME", "PARTSUPP.SUPPLIER.NAME", "PARTSUPP.PART.PARTKEY")));

        lineitem.aggregate(asList(
                match(and(asList(
                        eq("PARTSUPP.PART.TYPE", type),
                        eq("PARTSUPP.SUPPLIER.NATIONKEY.REGIONKEY.NAME", region),
                        eq("PARTSUPP.PART.SIZE", size),
                        eq("PARTSUPP.SUPPLYCOST", query2Subquery(region))
                ))),
                project(fields(include("PARTSUPP.SUPPLIER.ACCCTBAL", "PARTSUPP.SUPPLIER.NAME", "PARTSUPP.SUPPLIER.NATIONKEY.NAME",
                        "PARTSUPP.PART.PARTKEY", "PARTSUPP.PART.MFGR",
                        "PARTSUPP.SUPPLIER.ADDRESS", "PARTSUPP.SUPPLIER.PHONE", "PARTSUPP.SUPPLIER.COMMENT"), excludeId())),
                sortStageDescendent,
                sortStageAscendent
        ))
                .forEach(printBlock);
    }

    /**
     * (SELECT min(ps_supplycost)
     FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey
     AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
     AND n_regionkey = r_regionkey AND r_name = '[REGION]')
     */
    private Object query2Subquery(String region) {
        AggregateIterable<Document> minSupplyCost = lineitem.aggregate(asList(
                match(eq("PARTSUPP.SUPPLIER.NATIONKEY.REGIONKEY.NAME", region)),
                project(fields(include("PARTSUPP.SUPPLYCOST"), excludeId())),
                group(null,
                        min("MINSUPPLYCOST", "$PARTSUPP.SUPPLYCOST"))

        ));
        if (minSupplyCost.cursor().hasNext()) return minSupplyCost.first().get("MINSUPPLYCOST");
        return null;
    }

    /**
     * SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority
     FROM customer, orders, lineitem
     WHERE c_mktsegment = '[SEGMENT]' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
     AND o_orderdate < '[DATE1]' AND l_shipdate > '[DATE2]'
     GROUP BY l_orderkey, o_orderdate, o_shippriority
     ORDER BY revenue desc, o_orderdate;
     * @throws ParseException
     */
    private void query3(String segment, String date1, String date2){
        Consumer<Document> printBlock = new Consumer<Document>() {
            public void accept(final Document document) {
                System.out.println(document.toJson());
            }
        };

        System.out.println("QUERY3: SEGMENT = " + segment + ", DATE1 = " + date1 +  ", DATE2 = " + date2);
        Document subtractDiscount = new Document("$subtract", Arrays.asList(1, "$DISCOUNT"));
        Document multiplyDiscount = new Document("$multiply", Arrays.asList("$EXTENDEDPRICE", subtractDiscount));
        Bson sortStage = orderBy(asList(sort(ascending("_id.1")), sort(descending("REVENUE"))));
        Bson sortStageDescendent = sort(descending("REVENUE"));
        Bson sortStageAscendent = sort(ascending("_id.1"));

        lineitem.aggregate(asList(
                match(and(asList(
                        eq("ORDERKEY.CUSTKEY.MKTSEGMENT", segment),
                        lt("ORDERKEY.ORDERDATE", date1),
                        gt("SHIPDATE", date2)
                ))),
                project(fields(include("ORDERKEY.ORDERKEY", "EXTENDEDPRICE", "DISCOUNT", "ORDERKEY.ORDERDATE", "ORDERKEY.SHIP-PRIORITY", "SHIPDATE", "ORDERKEY.CUSTKEY.MKTSEGMENT", "REVENUE"), excludeId())),
                group(asList(new Document("ORDERKEY", "$ORDERKEY.ORDERKEY"), new Document("ORDERDATE", "$ORDERKEY.ORDERDATE") , new Document("SHIP-PRIORITY", "$ORDERKEY.SHIP-PRIORITY")),
                        sum("REVENUE", multiplyDiscount)
                ),
                sortStage
        ))
                .forEach(printBlock);
    }

    /**
     * SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
     FROM customer, orders, lineitem, supplier, nation, region
     WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
     AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
     AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
     AND r_name = '[REGION]' AND o_orderdate >= date '[DATE]'
     AND o_orderdate < date '[DATE]' + interval '1' year
     GROUP BY n_name
     ORDER BY revenue desc;
     * @throws ParseException
     */
    private void query4(String region, String date) {
        Consumer<Document> printBlock = new Consumer<Document>() {
            public void accept(final Document document) {
                System.out.println(document.toJson());
            }
        };

        GregorianCalendar dateGC = new GregorianCalendar(
                Integer.parseInt(date.split("-")[0]),
                Integer.parseInt(date.split("-")[1]) - 1,
                Integer.parseInt(date.split("-")[2])
        );
        dateGC.add(Calendar.YEAR, 1);
        String date2 = dateGC.get(Calendar.YEAR) + "-" + (dateGC.get(Calendar.MONTH) + 1) + "-" + dateGC.get(Calendar.DATE);

        System.out.println("QUERY4: REGION = " + region + ", ORDERDATE = " + date);

        Document subtractDiscount = new Document("$subtract", Arrays.asList(1, "$DISCOUNT"));
        Document multiplyDiscount = new Document("$multiply", Arrays.asList("$EXTENDEDPRICE", subtractDiscount));
        Bson sortStageDescendent = sort(descending("REVENUE"));

        lineitem.aggregate(asList(
                match(and(asList(
                        eq("PARTSUPP.SUPPLIER.NATIONKEY.REGIONKEY.NAME", region),
                        gte("ORDERKEY.ORDERDATE", date),
                        lt("ORDERKEY.ORDERDATE", date2)
                ))),
                project(fields(include("PARTSUPP.SUPPLIER.NATIONKEY.NAME", "EXTENDEDPRICE", "DISCOUNT"), excludeId())),
                group("$PARTSUPP.SUPPLIER.NATIONKEY.NAME",
                        sum("REVENUE", multiplyDiscount)),
                sortStageDescendent
        ))
                .forEach(printBlock);
    }

    private void createDBAndInserts(int numDocsGen) {
        // Get database connection
        MongoClient mongoClient = MongoClients.create();
        this.database = mongoClient.getDatabase("TPC-H_CBDE");
        // Get all the documents used
        this.lineitem = database.getCollection("lineitem");
        // Drop all data from previous executions
        dropAllData();
        // Generate info for the documents
        List<Document> regionDocs = generateDocumentREGION(numDocsGen);
        List<Document> nationDocs = generateDocumentNATION(numDocsGen, regionDocs);
        List<Document> partDocs = generateDocumentPART(numDocsGen);
        List<Document> supplierDocs = generateDocumentSUPPLIER(numDocsGen, nationDocs);
        List<Document> partSuppDocs = generateDocumentPARTSUPP(numDocsGen, partDocs, supplierDocs);
        List<Document> customerDocs = generateDocumentCUSTOMER(numDocsGen);
        List<Document> ordersDocs = generateDocumentORDERS(numDocsGen, customerDocs);
        List<Document> lineitemDocs = generateDocumentLINEITEM(numDocsGen, ordersDocs, partSuppDocs);

        this.lineitem.insertMany(lineitemDocs);
    }

    private void createIndexos() {
        lineitem.createIndex(Indexes.ascending("SHIPDATE"));
        lineitem.createIndex(Indexes.ascending("PARTSUPP.SUPPLIER.NATIONKEY.REGIONKEY.NAME"));
        lineitem.createIndex(Indexes.ascending("ORDERKEY.CUSTKEY.MKTSEGMENT"));
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
            doc.append("SUPPLYCOST", generateRandomString(1));
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

    private List<Document> generateDocumentNATION(int numDocs, List<Document> regionDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document("NATIONKEY", i);
            doc.append("NAME", generateRandomNationName());
            doc.append("REGIONKEY", regionDocs.get(i%regionDocs.size()));
            doc.append("COMMENT", generateRandomString(20));
            documents.add(doc);
        }
        return documents;
    }

    private List<Document> generateDocumentREGION(int numDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document("REGIONKEY", i);
            doc.append("NAME", generateRandomRegion());
            doc.append("COMMENT", generateRandomString(20));
            documents.add(doc);
        }
        return documents;
    }

    private List<Document> generateDocumentLINEITEM(int numDocs, List<Document> ordersDocs, List<Document> partSuppDocs) {
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document("ORDERKEY", ordersDocs.get(i%ordersDocs.size()));
            doc.append("PARTKEY", generateRandomString(10));
            doc.append("SUPPKEY", generateRandomString(8));
            doc.append("LINENUMBER", generateRandomNum(1, 100));
            doc.append("QUANTITY", generateRandomNum(1, 100));
            doc.append("EXTENDEDPRICE", generateRandomNum(10, 1000));
            doc.append("DISCOUNT", generateRandomNumDecimal(0, 1));
            doc.append("TAX", generateRandomNumDecimal(0, 1));
            doc.append("RETURNFLAG", generateRandomString(1));
            doc.append("LINESTATUS", generateRandomString(1));
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
            doc.append("SHIP-PRIORITY", generateRandomShipPriority());
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
            doc.append("NATIONKEY", i%25);
            doc.append("PHONE", generateRandomString(9));
            doc.append("ACCTBAL", generateRandomString(11));
            doc.append("MKTSEGMENT", generateRandomSegment());
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
     * Genera un numero entre min y max
     * @return double
     */
    private double generateRandomNumDecimal(int min, int max) {
        Random rand = new Random();
        return min + (rand.nextFloat() * (max-min));
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

    private String generateRandomNationName() {
        String[] nations = new String[] { "NATION_1", "NATION_2", "NATION_3", "NATION_4", "NATION_5",
                "NATION_6", "NATION_7", "NATION_8", "NATION_9", "NATION_10"};
        Random rand = new Random();
        int index = (int) (rand.nextFloat() * (nations.length));
        return nations[index];
    }

    /**
     * Genera un String con los Strings especificados en el array segments
     * @return String
     */
    private String generateRandomSegment() {
        String[] segments = new String[] { "SEGMENT_1", "SEGMENT_2", "SEGMENT_3", "SEGMENT_4", "SEGMENT_5" };
        Random rand = new Random();
        int index = (int) (rand.nextFloat() * (segments.length));
        return segments[index];
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
//		return randomDate;
        return formatter.format(randomDate);
    }

    /**
     * Genera un String con los Strings especificados en el array types
     * @return String
     */
    private String generateRandomType() {
        String[] types = new String[] { "TYPE_1", "TYPE_2", "TYPE_3" };
        Random rand = new Random();
        int index = (int) (rand.nextFloat() * (types.length));
        return types[index];
    }

    private String generateRandomShipPriority() {
        String[] priorities = new String[] { "LOW", "LOW-MED", "MEDIUM", "MED-HIGH", "HIGH", "VERY HIGH" };
        Random rand = new Random();
        int index = (int) (rand.nextFloat() * (priorities.length));
        return priorities[index];
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

    private String putZeros(int number) {
        if (number < 10) return "0" + number;
        return number + "";
    }

    /**
     * Borra todos los datos de la BD
     */
    private void dropAllData() {
        lineitem.drop();
    }
}