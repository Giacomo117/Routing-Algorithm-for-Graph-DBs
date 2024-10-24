from neo4j import GraphDatabase, exceptions

# Credenziali d'accesso Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"

# Creazione del DB su Neo4j
# Connessione al database Neo4j
driver = GraphDatabase.driver(uri, auth=(username, password))

with driver.session() as session:
    print('Connessione stabilita')

    # Creazione di Constraint e Index
    try:
        session.run("create constraint agency_unique for (a:Agency) require a.id is unique;")
        session.run("create constraint route_unique for (r:Route) require r.id is unique;")
        session.run("create constraint trip_unique for (t:Trip) require t.id is unique;")
        session.run("create constraint stop_unique for (s:Stop) require s.id is unique;") 
        session.run("create constraint service_unique for (s:Service) require s.service_id is unique;")
        session.run("create constraint day_unique for (d:Day) require d.day is unique;")

        session.run("create index trip_service_index for (t:Trip) on (t.service_id);")
        session.run("create index stoptime_index for (s:Stoptime) on (s.stop_sequence);")
        session.run("create index stop_index for (s:Stop) on (s.name);")

        print('Constraint e indici creati...')
    except exceptions.ClientError:
        print("Vincoli e indici già esistenti")

    print("Inserimento del'Agenzia")
    query = """load csv with headers from  
            'file:///agency.txt' as csv  
            create (:Agency {name: csv.agency_name, url: csv.agency_url, timezone: csv.agency_timezone});"""
    session.run(query)

    print("Inserimento delle Routes")
    query = """load csv with headers from  
            'file:///routes.txt' as csv  
            match (a:Agency {name: 'aMo Modena'})  
            create (a)-[:OPERATES]->(:Route {id: csv.route_id, short_name: csv.short_name, long_name: csv.route_long_name, type: toInteger(csv.route_type)});"""
    session.run(query)

    print("Inserimento dei Trip")
    query = """load csv with headers from 
            'file:///trips.txt' as csv
            match (r:Route {id: csv.route_id})
            create (r)<-[:USES]-(:Trip {service_id: csv.service_id, id: csv.trip_id, direction_id: csv.direction_id, shape_id: csv.shape_id, headsign: csv.trip_headsign});"""
    session.run(query)

    print("Inserimento degli Stop")
    query = """load csv with headers from 
            'file:///stops.txt' as csv  
            create (:Stop {id: csv.stop_id, name: csv.stop_name, lat: toFloat(csv.stop_lat), lon: toFloat(csv.stop_lon)});"""
    session.run(query)

    print("Inserimento degli StopTimes")
    query = """CALL apoc.periodic.iterate(
            "load csv with headers from 'file:///stop_times.txt' as csv return csv",
            "match (t:Trip {id: csv.trip_id}), (s:Stop {id: csv.stop_id}) create (t)<-[:PART_OF_TRIP]-(st:Stoptime {arrival_time: time(csv.arrival_time), departure_time: time(csv.departure_time), stop_sequence: toInteger(csv.stop_sequence)})-[:LOCATED_AT]->(s)",
            {batchSize:1000, parallel:true})"""
    session.run(query)

    print("Inserimento delle relazioni tra gli StopTimes")
    query = """match (s1:Stoptime)-[:PART_OF_TRIP]->(t:Trip),  
            (s2:Stoptime)-[:PART_OF_TRIP]->(t)  
            where s2.stop_sequence=s1.stop_sequence+1  
            create (s1)-[:PRECEDES]->(s2);"""
    session.run(query)

    query="""match (s1:Stoptime)-[p:PRECEDES]->(s2:Stoptime)
            set p.waiting_time=duration.inSeconds(s1.departure_time, s2.arrival_time).seconds"""
    session.run(query)

    print("Assicurarsi di aver caricato il file new_calendar_dates.txt realizzato con lo script reshape.py")
    print("Inserimento dei services")
    query = """load csv with headers from 'file:///new_calendar_dates.txt' as csv
            merge (:Service {service_id: csv.service_id})"""
    session.run(query)

    print("Collegamento services con i trip")
    query = """MATCH (s:Service), (t:Trip) where t.service_id = s.service_id merge (t)-[:SERVICE_TYPE]->(s)"""
    session.run(query)

    print("Inserimento dei giorni in cui sono disponibili i servizi")
    query = """CALL apoc.periodic.iterate(
            "load csv with headers from 'file:///new_calendar_dates.txt' as csv return csv",
            "match (s:Service {service_id: csv.service_id}) merge (d:Day {day:date(csv.day)}) merge (s)-[:VALID_IN]->(d) SET d.exception_type = csv.exception_type",
            {batchSize:500})"""
    session.run(query)

    print("Inserimento delle relazioni tra gli Stop vicini")
    query = """MATCH (s1:Stop)
            WITH point({latitude: s1.lat, longitude: s1.lon}) AS p1, s1
            MATCH (s2:Stop)
            WITH point({latitude: s2.lat, longitude: s2.lon}) AS p2, p1, s1, s2
            WHERE point.distance(p1,p2) < 300
            MERGE (s1)-[:WALK_TO {distance: point.distance(p1,p2)}]->(s2);"""
    session.run(query)

    print('DB realizzato correttamente')