class App:
    """In this file we are going to extract from OSM crossings mapped as nodes"""

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def routing_graph_creation(self, date, speed):
        query = """CALL gds.graph.project.cypher(
                    "graph_walk",
                    "match (d:Day {day:date('%s')})<-[:VALID_IN]-(ser:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)-[:LOCATED_AT]->(s:Stop) return id(st) AS id, st.stop_sequence as stop_sequence,s.lon as lon,s.lat as lat",
                    "match (d:Day{day:date('%s')})<-[:VALID_IN]-(s:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)-[:LOCATED_AT]->(stops:Stop) match (r:Route)<-[:USES]-(t) with st as source,s as service, t.id as trip_source, stops as stops,r.id as line match (service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)-[:LOCATED_AT]->(s2:Stop)-[w:WALK_TO]->(stops) where t.id <> trip_source and source.arrival_time + duration({seconds:toInteger(w.distance/%d)}) < st.departure_time match (st)-[:PART_OF_TRIP]->(t)-[:USES]->(r:Route)  where r.id <> line with source,service,trip_source,stops,line,r.id as other_line,w.distance as walking_distance,apoc.agg.minItems(st,st.departure_time).items as targets unwind targets  as target match (target)-[:LOCATED_AT]->(s2:Stop)-[w:WALK_TO]->(stops) return id(source) as source, id(target) as target, ':CHANGE' as type,duration.inSeconds(source.arrival_time,target.departure_time).seconds + duration({seconds:toInteger(w.distance/%d)}).seconds as waiting_time, duration({seconds:toInteger(w.distance/%d)}).seconds as walking_time UNION ALL match (d:Day {day:date('%s')})<-[:VALID_IN]-(s:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)-[p:PRECEDES]->(st2) return id(st) as source, id(st2) as target, type(p) as type, p.waiting_time as waiting_time, 0 as walking_time")
            """ % (date, date, speed, speed, speed, date)
        print(query)
        with self.driver.session() as session:
            res = session.run(query)
            print(res)

    def get_metrics(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_metrics)
            return result

    @staticmethod
    def _get_metrics(tx):
        query = """CALL gds.graph.list(
                      'graph_walk'
                    ) YIELD
                      nodeCount,
                      relationshipCount,
                      degreeDistribution,
                      density,
                      sizeInBytes return nodeCount,
                      relationshipCount,
                      degreeDistribution,
                      density,
                      sizeInBytes"""
        result = tx.run(query)
        return result.values()

    def betweennessCentrality(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._betweennessCentrality)
            return result

    @staticmethod
    def _betweennessCentrality(tx):
        query = """CALL gds.betweenness.stream('graph_walk')
                    YIELD nodeId, score
                    match (st:Stoptime)-[:LOCATED_AT]->(s:Stop)
                    where id(st) = id(gds.util.asNode(nodeId))
                    return s.name,gds.util.asNode(nodeId).departure_time AS time,s.lat as lat,s.lon AS lon, score
                    ORDER BY score DESC"""
        result = tx.run(query)
        return result.values()

    def find_near_stops(self, date, start_lat, start_lon, radius):
        query = '''match (d:Day{day: date('%s')})<-[:VALID_IN]-(:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)-[:LOCATED_AT]->(s:Stop)
                    WITH distinct point({latitude: s.lat, longitude: s.lon}) AS coord, s as s
                    with s, coord, point({latitude: %f, longitude: %f}) as startPoint
                    where point.distance(coord, startPoint) < %d
                    return distinct s.name''' % (date, start_lat, start_lon, radius)
        print(query)
        with self.driver.session() as session:
            result = session.run(query)
            return result.values()

    def routing(self, date, speed, time, source, target, max_duration=4):
        with self.driver.session() as session:
            result = session.write_transaction(self._routing, date, speed, time, source, target, max_duration)
            return result

    @staticmethod
    def _routing(tx, date, speed, time, source, target, max_duration=4):
        endtime = datetime.strptime(time, "%H:%M:%S") + timedelta(hours=max_duration)
        endtime = endtime.strftime("%H:%M:%S")
        query = """match (d:Day {day:date('%s')})<-[:VALID_IN]-(ser:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)-[r:LOCATED_AT]->(s:Stop {name: '%s'})
        where st.departure_time > time('%s')
        match (st)-[:PART_OF_TRIP]->(t)-[:USES]->(rou:Route) 
        with rou.id as source_line ,apoc.agg.minItems(st, st.departure_time).items as sources
        with collect(sources) as sources
        unwind sources as s with s[0] as the_source
        match (d:Day {day:date('%s')})<-[:VALID_IN]-(s:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)
        with collect(id(st)) as target_ids,the_source as s
        unwind target_ids as tg_id
        match (st2:Stoptime)-[r:LOCATED_AT]->(stop:Stop {name:'%s'})
        where id(st2) = tg_id and st2.departure_time < time('%s') 
        and st2.departure_time > s.departure_time
        with s as s,st2 as t order by s.departure_time, t.departure_time
        CALL gds.shortestPath.dijkstra.stream('graph_walk', {
                                                    sourceNode: s,
                                                    targetNode: t,
                                                    relationshipWeightProperty: 'waiting_time'
                                                    })
            YIELD index, sourceNode, targetNode, totalCost, path, nodeIds 
            with s as source_,t as target_t,gds.util.asNode(sourceNode) as source,gds.util.asNode(targetNode) as target,gds.util.asNode(sourceNode).departure_time +duration({seconds:totalCost}) as seconds,totalCost as cost,gds.util.asNode(targetNode).arrival_time as arrival_time, [nodeId IN nodeIds | gds.util.asNode(nodeId)] AS nodes_in_path,[r in relationships(path)|[startNode(r),endNode(r)]] as pairs                                 
        order by arrival_time,cost limit 1
        unwind pairs as p
        match (s1:Stoptime)
        where id(s1)=id(p[0])
        match (s2:Stoptime)
        where id(s2)=id(p[1])
        match (r:Route)<-[:USES]-(t:Trip)<-[:PART_OF_TRIP]-(s1)
        match (s1)-[:LOCATED_AT]->(s:Stop)
        match (next_r:Route)<-[:USES]-(next_t:Trip)<-[:PART_OF_TRIP]-(s2)
        match (s2)-[:LOCATED_AT]->(next_s:Stop)
        return t.id as trip,s1.departure_time as departure,r.id as line, s.name as starting_stop_name,s.id as starting_stop_id,[s.lat,s.lon] as starting_stop_coordinates,
        next_t.id as next_trip,next_s.name as next_stop,next_s.id as next_stop_id,[next_s.lat,next_s.lon] as next_stop_coordinates,next_r.id as next_line
                ,s2.arrival_time as arrival""" % (date, source, time, date, target, endtime)
        print(query)
        result = tx.run(query)
        return result.values()

    def routing_between_two_points_in_space(self, date, start_lat, end_lat, start_lon, end_lon, start_list, end_list,
                                            speed, time, max_duration=4):
        with self.driver.session() as session:
            result = session.write_transaction(self._routing_between_two_points_in_space, date, start_lat, end_lat,
                                               start_lon, end_lon, start_list, end_list, speed, time, max_duration)
            return result

    @staticmethod
    def _routing_between_two_points_in_space(tx, date, start_lat, end_lat, start_lon, end_lon, start_list, end_list,
                                             speed, time, max_duration=4):
        endtime = datetime.strptime(time, "%H:%M:%S") + timedelta(hours=max_duration)
        endtime = endtime.strftime("%H:%M:%S")
        query = """match (d:Day {day:date('%s')})<-[:VALID_IN]-(ser:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)-[r:LOCATED_AT]->(s:Stop)
        where st.departure_time - duration({seconds: point.distance(point({latitude: s.lat, longitude: s.lon}),point({latitude: %f, longitude: %f}))/ %d})> time('%s') and any(x in s.name where x in %s)
        match (st)-[:PART_OF_TRIP]->(t)-[:USES]->(rou:Route) with rou.id as soruce_line ,apoc.agg.minItems(st, st.departure_time).items as sources
        with collect(sources) as sources
        unwind sources as s with s[0] as the_source
        match (d:Day {day:date('%s')})<-[:VALID_IN]-(s:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)
        with collect(id(st)) as target_ids,the_source as s
        unwind target_ids as tg_id
        match (st2:Stoptime)-[r:LOCATED_AT]->(stop:Stop)
        where id(st2) = tg_id and st2.departure_time + duration({seconds: point.distance(point({latitude: stop.lat, longitude: stop.lon}),point({latitude: %f, longitude: %f}))/ %d})< time('%s') 
        and st2.departure_time > s.departure_time and any(x in stop.name where x in %s)
        with s as s,st2 as t order by s.departure_time, t.departure_time
        CALL gds.shortestPath.dijkstra.stream('graph_walk', {
                                                    sourceNode: s,
                                                    targetNode: t,
                                                    relationshipWeightProperty: 'waiting_time'
                                                    })
        YIELD index, sourceNode, targetNode, totalCost, path, nodeIds 
        with s as source_,t as target_t,gds.util.asNode(sourceNode) as source,
        gds.util.asNode(targetNode) as target,
        gds.util.asNode(sourceNode).departure_time +duration({seconds:totalCost}) as seconds,
        totalCost as cost,
        gds.util.asNode(targetNode).arrival_time as arrival_time, 
        [nodeId IN nodeIds | gds.util.asNode(nodeId)] AS nodes_in_path,[r in relationships(path)|[startNode(r),endNode(r)]] as pairs
        match (target)-[:LOCATED_AT]->(endStop:Stop)
        match (source)-[:LOCATED_AT]->(startStop:Stop)
        with endStop as endStop, startStop as startStop, source_ as source_ , target_t as target_t, cost + point.distance(point({latitude: startStop.lat, longitude: startStop.lon}),point({latitude: %f, longitude: %f}))/ %d + point.distance(point({latitude: endStop.lat, longitude: endStop.lon}),point({latitude:  %f, longitude: %f }))/ %d  as cost, seconds as seconds, arrival_time as arrival_time, arrival_time + duration({seconds: point.distance(point({latitude: endStop.lat, longitude: endStop.lon}),point({latitude: %f, longitude: %f }))/ %d}) as final_time, pairs as pairs 
        order by final_time, cost
        limit 1
        unwind pairs as p
        match (s1:Stoptime)
        where id(s1)=id(p[0])
        match (s2:Stoptime)
        where id(s2)=id(p[1])
        match (r:Route)<-[:USES]-(t:Trip)<-[:PART_OF_TRIP]-(s1)
        match (s1)-[:LOCATED_AT]->(s:Stop)
        match (next_r:Route)<-[:USES]-(next_t:Trip)<-[:PART_OF_TRIP]-(s2)
        match (s2)-[:LOCATED_AT]->(next_s:Stop)
        return t.id as trip,s1.departure_time as departure,r.id as line, s.name as starting_stop_name,s.id as starting_stop_id,[s.lat,s.lon] as starting_stop_coordinates,
        next_t.id as next_trip,next_s.name as next_stop,next_s.id as next_stop_id,[next_s.lat,next_s.lon] as next_stop_coordinates,next_r.id as next_line
                ,s2.arrival_time as arrival""" % (
        date, start_lat, start_lon, speed, time, start_list, date, end_lat, end_lon, speed, endtime, end_list,
        start_lat, start_lon, speed, end_lat, end_lon, speed, end_lat, end_lon, speed)
        print(query)
        result = tx.run(query)
        return result.values()

    def get_nearest_footnode_with_distance(self, lat: float, lon: float) -> tuple:
        """
        Trova il FootNode più vicino alle coordinate specificate e restituisce la distanza euclidea.

        :param lat: Latitudine del punto di interesse.
        :param lon: Longitudine del punto di interesse.
        :return: Una tupla con l'ID o nome del FootNode più vicino e la distanza euclidea.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (footnode:FootNode)
                WITH footnode, point({latitude: $lat, longitude: $lon}) AS location
                RETURN footnode.id AS footnode_id, 
                       point.distance(point({latitude: footnode.longitude, longitude: footnode.latitude}), location) AS distance
                ORDER BY distance
                LIMIT 1
            """, lat=lat, lon=lon)

            record = result.single()

            if record is None:
                return None, float('inf')

            return record['footnode_id'], record['distance']

    def get_walking_distance(self, footnode_start: str, footnode_end: str) -> float:
        """
        Calcola la distanza a piedi tra due FootNode nel grafo utilizzando i percorsi pedonali nel database Neo4j.

        :param footnode_start: Il nome o ID del nodo di partenza (FootNode).
        :param footnode_end: Il nome o ID del nodo di arrivo (FootNode).
        :return: La distanza a piedi in metri o un valore molto grande se non esiste un percorso.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (start:FootNode {name: $footnode_start})
                OPTIONAL MATCH (start)-[:SHORTEST_ROUTE_TO|FOOT_ROUTE|CONTAINS|CONTINUE_ON_FOOTWAY|CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD*]->(end:FootNode {name: $footnode_end})
                WHERE end IS NOT NULL
                CALL apoc.algo.dijkstra(
                    start, end, 
                    'SHORTEST_ROUTE_TO|FOOT_ROUTE|CONTAINS>|<CONTAINS|CONTINUE_ON_FOOTWAY|CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD', 
                    'length'
                ) 
                YIELD weight
                RETURN weight AS distance
            """, footnode_start=footnode_start, footnode_end=footnode_end)

            record = result.single()
            if record is None:
                return float('inf')
            else:
                return record['distance']

    def calculate_distance(self, row):
        start_coords = (row.iloc[0], row.iloc[1])
        end_coords = (row.iloc[2], row.iloc[3])

        # Trova il FootNode più vicino a ciascuna coppia di coordinate
        footnode_start, _ = self.get_nearest_footnode_with_distance(start_coords[0], start_coords[1])
        footnode_end, _ = self.get_nearest_footnode_with_distance(end_coords[0], end_coords[1])

        if footnode_start and footnode_end:
            distance_meters = self.get_walking_distance(footnode_start, footnode_end)
            distance_kilometers = distance_meters / 1000.0
            return distance_kilometers
        else:
            # Se non si trova un percorso, ritorna un valore molto grande
            return float('inf')

    def distance_from_a_stop(self, node_id, lat, lon):
        # Trova il FootNode più vicino alle coordinate specificate
        footnode_id, footnode_distance = self.get_nearest_footnode_with_distance(lat, lon)

        if footnode_id is None:
            print(f"Nessun FootNode trovato vicino alle coordinate specificate.")
            return float('inf')

        print(f"FootNode trovato: {footnode_id} a distanza {footnode_distance} metri")

        # Calcola la distanza a piedi dal FootNode trovato allo Stop specificato
        walking_distance = self.get_walking_distance(footnode_id, node_id)

        if walking_distance == float('inf'):
            print("Non è stato possibile trovare un percorso pedonale tra il FootNode e lo Stop specificato.")
            return walking_distance

        # Somma la distanza euclidea iniziale (fino al FootNode più vicino) con la distanza pedonale calcolata
        total_distance = footnode_distance + walking_distance
        return total_distance / 1000.0  # Ritorna la distanza in chilometri

    def hours_of_service(self, date):
        query = """match (d:Day {day:date('%s')})<-[:VALID_IN]-(s:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)-[:LOCATED_AT]->(stops:Stop) 
                    match (r:Route)<-[:USES]-(t) 
                    with r.id as line,apoc.agg.minItems(st,st.departure_time).items as min_time,
                        apoc.agg.maxItems(st,st.arrival_time).items as max_time 
                    unwind min_time as starting 
                    unwind max_time as ending 
                    with line as line,starting.departure_time as departs,ending.arrival_time as arrive,
                        duration.inSeconds(starting.departure_time,ending.arrival_time).hours as duration 
                    return avg(duration)""" % (date)
        with self.driver.session() as session:
            res = session.run(query)
            print(res)

        from neo4j import GraphDatabase

