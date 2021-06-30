
from neo4j import GraphDatabase, basic_auth
import datetime
import time

# Create the neo4j database
def create():

    neo4jdb = GraphDatabase.driver(uri="neo4j://localhost:7687", auth=("neo4j", "neo4jlab6"))
    return inserts(neo4jdb)


# Create indexes
def indexes(neo4jdb):
    session = neo4jdb.session()
    session.run("CREATE INDEX ON :LineItem(shipdate)")
    session.run("CREATE INDEX ON :Order(orderdate)")
    session.close()

    print()

# Drop all data
def drop(session):
    session.run("MATCH (n) DETACH DELETE n")

# Drop indexes created
def dropIndexes(neo4jdb):
    session = neo4jdb.session()

    session.run("DROP INDEX ON :Order(orderdate)")
    session.run("DROP INDEX ON :LineItem(shipdate)")

    session.close()

# Print toda la db para comprovar por terminal si las queries dan el resultado deseado
def print_all(session):
    print('A continuación se muestra el contenido de la db después de realizar los inserts:\n')
    for item in session.run('MATCH (n) RETURN n'):
        print(item)

# Creación node Order
def createOrder(session, identifier, orderkey, orderdate, shippriority, c_marketsegment, n_name):
    session.run("CREATE (" + identifier + ":Order {orderkey: '" + orderkey + "', orderdate: $date, shippriority:'" +
                shippriority + "', c_marketsegment: '" + c_marketsegment + "', n_name: '" + n_name + "'})",
                {"date": orderdate})


# Creación node LineItem
def createLineItem(session, identifier, orderkey, suppkey, returnflag, quantity,
                    extendedPrice, discount, tax, shipdate, linestatus):
    session.run("CREATE (" + identifier + ":LineItem {orderkey: '" + orderkey +
                "', suppkey: '" + suppkey + "', returnflag: '" + returnflag + "', quantity: " + quantity +
                ", extendedPrice: " + extendedPrice + ", discount: " + discount + ", tax: " + tax +
                ", shipdate: $date2, linestatus: '" + linestatus + "'})", {"date2": shipdate})

# Creación node Part
def createPart(session, identifier, partkey, mfgr, type, size):
    session.run("CREATE (" + identifier + ":Part {partkey:'" + partkey +
                "', mfgr:'" + mfgr + "', type: '" + type + "', size: " + size + "})")


# Creación node Supplier
def createSupplier(session, identifier, suppkey, name, accbal, adress, phone, comment, n_name, r_name):
    session.run("CREATE (" + identifier + ":Supplier {suppkey: '" + suppkey +
                "', name: '" + name + "', accbal: " + accbal + ", adress: '" + adress +
                "', phone: '" + phone + "', comment: '" + comment +
                "', n_name: '" + n_name + "', r_name: '" + r_name + "'})")

#Creación edge entre Supplier i Part
def createEdgeSupplierPart(session, supplier, suppkey, part, partkey, supplycost):
    session.run(
        "MATCH (" + supplier + ":Supplier {suppkey: '" + suppkey + "'}), (" + part + ":Part {partkey: '" + partkey +
        "'}) CREATE (" + supplier + ")-[:pssc {supplycost: $suppcost }]->(" + part + ")",
        {"suppcost": supplycost})

#Creación edge entre Order i LineItem
def createEdgeOrderLineItem(session, order, orderkey, lineitem):
    session.run("MATCH (" + order + ":Order {orderkey: '" + orderkey + "'}), (" + lineitem +
                ":LineItem {orderkey: '" + orderkey + "'}) CREATE (" + order + ")-[:tiene]->(" + lineitem + ")")

#Creación edge entre LineItem i Supplier
def createEdgeLineItemSupplier(session, lineitem, supplier, suppkey):
    session.run("MATCH (" + lineitem + ":LineItem {suppkey: '" + suppkey + "'}), (" + supplier +
                ":Supplier {suppkey: '" + suppkey + "'}) CREATE (" + lineitem + ")-[:esDe]->(" + supplier + ")")


# Data inserts into db
def inserts(neo4jdb):
    session = neo4jdb.session()

    date = datetime.datetime(2020, 12, 24)
    date2 = datetime.datetime(2020, 12, 25)
    timestamp = time.mktime(date.timetuple())
    timestamp2 = time.mktime(date2.timetuple())

    drop(session)
    #PART INSERTS
    #def createPart(session, identifier, partkey, mfgr, type, size)
    createPart(session, 'Part_1', '1', 'mfgr1', 'A', '40')
    createPart(session, 'Part_2', '2', 'mfgr2', 'B', '45')
    createPart(session, 'Part_3', '3', 'mfgr3', 'C', '35')
    createPart(session, 'Part_4', '4', 'mfgr4', 'D', '32')

    #SUPPLIER INSERTS
    #def createSupplier(session, identifier, suppkey, name, accbal, adress, phone, comment, n_name, r_name)
    createSupplier(session, 'Supp_1', 'Supp_1', 'SuppMRS_1', '1.00', 'Rue Nationale num2', '1010101', 'blah blah blah', 'France', 'Marseille')
    createSupplier(session, 'Supp_2', 'Supp_2', 'SuppMRS_2', '2.00', 'Rue Nationale num1', '9999999', 'blah', 'France', 'Marseille')
    createSupplier(session, 'Supp_3', 'Supp_3', 'SuppBCN_1', '1.00', 'Av. Roma num1', '9346189', 'blah', 'Spain', 'Barcelona')

    #ORDER INSERTS
    #def createOrder(session, identifier, orderkey, orderdate, shippriority, c_marketsegment, n_name)
    createOrder(session, 'Order_1', 'Order_1', timestamp, '0', 'mktE', 'France')
    createOrder(session, 'Order_2', 'Order_2', timestamp, '1', 'mktE', 'France')

    #LINEITEM INSERTS
    #def createLineItem(session, identifier, orderkey, suppkey, returnflag, quantity, extendedPrice, discount, tax, shipdate, linestatus)
    createLineItem(session, 'lineItem_1', 'Order_1', 'Supp_1', 'rfa', '40', '40.0', '0.3', '6.0', timestamp2, 'c')
    createLineItem(session, 'lineItem_2', 'Order_1', 'Supp_1', 'rfa', '35', '48.0', '0.7', '7.0', timestamp2, 'c')
    createLineItem(session, 'lineItem_3', 'Order_1', 'Supp_1', 'rfa', '35', '35.0', '0.25', '5.0', timestamp2, 'c')
    createLineItem(session, 'lineItem_4', 'Order_2', 'Supp_2', 'rfb', '40', '50.0', '0.50', '7.0', timestamp2, 'e')
    createLineItem(session, 'lineItem_5', 'Order_2', 'Supp_2', 'rfb', '50', '70.0', '0.7', '5.0', timestamp2, 'e')
    createLineItem(session, 'lineItem_6', 'Order_2', 'Supp_2', 'rfb', '35', '40.0', '0.4', '6.0', timestamp2, 'e')
    createLineItem(session, 'lineItem_7', 'Order_2', 'Supp_3', 'rfb', '35', '40.0', '0.4', '6.0', timestamp2, 'e')

    #CREACiÓ DE EDGES
    #def createEdgeSupplierPart(session, supplier, suppkey, part, partkey, supplycost)
    createEdgeSupplierPart(session, 'Supp_1', 'Supp_1', 'Part_1', '1', 10)
    createEdgeSupplierPart(session, 'Supp_1', 'Supp_1', 'Part_2', '2', 20)
    createEdgeSupplierPart(session, 'Supp_1', 'Supp_1', 'Part_3', '3', 30)
    createEdgeSupplierPart(session, 'Supp_1', 'Supp_1', 'Part_4', '4', 40)
    createEdgeSupplierPart(session, 'Supp_2', 'Supp_2', 'Part_1', '1', 5)
    createEdgeSupplierPart(session, 'Supp_2', 'Supp_2', 'Part_2', '2', 10)
    createEdgeSupplierPart(session, 'Supp_2', 'Supp_2', 'Part_3', '3', 15)
    createEdgeSupplierPart(session, 'Supp_2', 'Supp_2', 'Part_4', '4', 20)
    createEdgeSupplierPart(session, 'Supp_3', 'Supp_3', 'Part_4', '4', 20)

    #def createEdgeOrderLineItem(session, order, orderkey, lineitem)
    createEdgeOrderLineItem(session, 'Order_1', 'Order_1', 'lineItem_1')
    createEdgeOrderLineItem(session, 'Order_2', 'Order_2', 'lineItem_4')
    createEdgeOrderLineItem(session, 'Order_2', 'Order_2', 'lineItem_7')

    #def createEdgeLineItemSupplier(session, lineitem, supplier, suppkey)
    createEdgeLineItemSupplier(session, 'lineItem_1', 'Supp_1', 'Supp_1')
    createEdgeLineItemSupplier(session, 'lineItem_2', 'Supp_2', 'Supp_2')
    createEdgeLineItemSupplier(session, 'lineItem_7', 'Supp_3', 'Supp_3')

    print_all(session)
    print('Final del contenido de la db\n')

    session.close()
    return neo4jdb

# Query 1
def query1(neo4jdb, date):
    print('Query 1 result:')
    result = \
        neo4jdb.session().run(" MATCH " + " ( li:LineItem ) " +
                         " WHERE " + " li.shipdate <= $date " +
                         " WITH " + " li.returnflag AS l_returnflag, " +
                         " li.linestatus AS l_linestatus, " +
                         " SUM(li.quantity) AS sum_qty, " +
                         " SUM(li.extendedPrice) AS sum_base_price, " +
                         " SUM(li.extendedPrice*(1-li.discount)) AS sum_disc_price, " +
                         " SUM(li.extendedPrice*(1-li.discount)*(1+li.tax)) AS sum_charge, " +
                         " AVG(li.quantity) AS avg_qty, " +
                         " AVG(li.extendedPrice) AS avg_price, " +
                         " AVG(li.discount) AS avg_disc, " +
                         " COUNT(*) AS count_order " +
                         " RETURN " + " l_returnflag, " + " l_linestatus, " + " sum_qty, " + " sum_base_price, " + " sum_disc_price, " + " sum_charge, " + " avg_qty, " +
                         " avg_price, " + " avg_disc, " + " count_order " +
                         " ORDER BY " + " l_returnflag ASC, " + " l_linestatus ASC ",
                         {"date": time.mktime(date.timetuple())})

    i = 0
    for r in result:
        i += 1
        print(r)

    if i == 0:
        print("La Query 1 obtiene resultado vacio")
    print()


# Query 2
def query2(neo4jdb, region, type, size):
    print('Query 2 result:')
    subq2Res = \
        neo4jdb.session().run(" MATCH " + "(s: Supplier)-[res:pssc]->() " +
                         " WHERE " + "s.r_name = $region " +
                         " RETURN " + " MIN(res.supplycost) ",
                         {"region": region})

    global minSuppCost
    for item in subq2Res:
        minSuppCost = item['MIN(res.supplycost)']

    result = \
        neo4jdb.session().run(" MATCH " + " (s: Supplier)-[res:pssc]->(p1: Part) " +
                         " WHERE " + " p1.size = $size AND " + " p1.type = $type AND " + " res.supplycost = $MINsupplycost " +
                         " RETURN " +
                         " s.accbal AS s_accbal, " +
                         " s.name AS s_name, " +
                         " s.n_name AS n_name, " +
                         " p1.partkey AS p_partkey, " +
                         " p1.mfgr AS p_mfgr, " +
                         " s.adress AS s_adress, " +
                         " s.phone AS s_phone, " +
                         " s.comment AS s_comment " +
                         " ORDER BY " + " s.accbal DESC, " + " s.n_name ASC, " + " p1.partkey ASC ",
                         {"size": size,
                         "MINsupplycost": minSuppCost,
                          "type": type})
    i = 0
    for r in result:
        i += 1
        print(r)

    if i == 0:
        print("La Query 2 obtiene resultado vacio")

    print()


# Query 3
def query3(neo4jdb, date1, date2, segment):
    print('Query 3 result:')
    result = \
        neo4jdb.session().run(" MATCH " + " (o:Order)-[:tiene]->(l:LineItem) " +
                         " WHERE " +
                         " l.shipdate > $date2 AND " + " o.orderdate < $date1 AND " +
                         " o.c_marketsegment = $segment AND " + " o.orderkey = l.orderkey "
                         " WITH " +
                         " l.orderkey AS l_orderkey, " + " o.orderdate AS o_orderdate, " +
                         " o.shippriority AS o_shippriority, " + " SUM(l.extendedPrice*(1-l.discount)) AS revenue " +
                         " RETURN " +
                         " l_orderkey, " + " o_orderdate, " + " o_shippriority, " + " revenue " +
                         " ORDER BY " +
                         " revenue DESC, " +
                         " o_orderdate ASC ",
                         {"segment": segment,
                        "date1": time.mktime(date1.timetuple()),
                          "date2": time.mktime(date2.timetuple())})

    i = 0
    for r in result:
        i += 1
        print(r)

    if i == 0:
        print("La Query 3 obtiene resultado vacio")
    print()


# Query 4
def query4(neo4jdb, region, date):
    print('Query 4 result:')

    result = \
        neo4jdb.session().run(" MATCH " + "(o:Order)-[:tiene]->(l:LineItem)-[:esDe]->(s:Supplier)" +
                         " WHERE " +
                         " s.r_name = $region AND " + " s.n_name = o.n_name AND " +
                         " o.orderdate >= $date AND " + " o.orderdate < $datePlusYear "
                         " RETURN " +
                         " o.n_name AS n_name, " + " SUM(l.extendedPrice*(1-l.discount)) AS revenue " +
                         " ORDER BY " +
                         " revenue DESC ",
                         {"region": region,
                         "date": time.mktime(date.timetuple()),
                        "datePlusYear": time.mktime(date.replace(year=date.year + 1).timetuple())})
    i = 0
    for r in result:
        i += 1
        print(r)

    if i == 0:
        print("La Query 4 obtiene resultado vacio")

    print()

def run():
    neo4jdb = create()
    indexes(neo4jdb)
    print("A continuación la ejecución de las 4 queries:\n")
    query1(neo4jdb, date=datetime.datetime(2020, 12, 28))
    query2(neo4jdb, region='Marseille', type='A', size=40)
    query3(neo4jdb, date1=datetime.datetime(2020, 12, 28), date2=datetime.datetime(2020, 12, 20), segment='mktE')
    query4(neo4jdb, region="Marseille", date=datetime.datetime(2020, 11, 20))
    dropIndexes(neo4jdb)


if __name__ == '__main__':
    run()