import pandas as pd
from neo4j import GraphDatabase
from datetime import datetime,timedelta, date
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

    def distance_from_a_stop(self, node_id, lat, lon):
        query = """match (s:Stop {id: '%s'})
                    with point({latitude: s.lat, longitude: s.lon}) as p1, point({latitude: %f, longitude: %f}) as p2
                    return point.distance(p1, p2)""" % (node_id, lat, lon)
        with self.driver.session() as session:
            res = session.run(query)
            return res.value()

    def number_of_stops(self, date):
        query = """match (d:Day {day:date('%s')})<-[:VALID_IN]-(ser:Service)<-[:SERVICE_TYPE]-(t:Trip)<-[:PART_OF_TRIP]-(st:Stoptime)-[:LOCATED_AT]->(s:Stop) return count(distinct s)""" % (
            date)
        with self.driver.session() as session:
            res = session.run(query)
            print(res)

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

def time_difference_seconds(time1, time2):
    delta = timedelta(hours=time1.hour, minutes=time1.minute, seconds=time1.second) - \
            timedelta(hours=time2.hour, minutes=time2.minute, seconds=time2.second)
    return delta.total_seconds()

def convert_to_datetime(value):
    return value.to_native()


def show_more_details(df_path):
    source = df_path['starting_stop_name'].head(1).iloc[0]
    print('start trip at ' + str((df_path[df_path['starting_stop_name'] == source].departure).loc[0].strftime(
        '%H:%M:%S')) + ' at station ' + source + ' coordinate: ' + str(
        df_path[df_path['starting_stop_name'] == source].starting_stop_coordinates.loc[0]) + ' line: ' + str(
        df_path[df_path['starting_stop_name'] == source].line.loc[0]))

    for i, row in df_path.iterrows():
        if (row.starting_stop_id == row.next_stop_id):
            print('drop at ' + str(row['departure'].strftime('%H:%M:%S')) + ' at station ' + str(
                row['starting_stop_name']) + ' change to line: ' + str(row.next_line))
        elif (row.trip != row.next_trip):
            print('drop at ' + str(row['departure'].strftime('%H:%M:%S')) + ' at station ' + str(
                row['starting_stop_name']) + ' walk_to_station ' + str(row.next_stop) + '\n from coordinates: ' + str(
                row.starting_stop_coordinates) + '\n to coordinates:' + str(
                row.next_stop_coordinates) + ' change to line: ' + str(row.next_line))

    target = df_path['next_stop'].tail(1).iloc[0]
    print('end trip at ' +
          str((df_path.tail(1).arrival).iloc[0].strftime('%H:%M:%S'))
          + ' at station ' + target + ' coordinate: ' + str(df_path.tail(1).next_stop_coordinates.iloc[0])
          + ' with line: ' + str(df_path[df_path['next_stop'] == target].next_line.iloc[0]))


greeter = App('neo4j://localhost:7687', 'neo4j', '12345678')


#Speed is in m/s
speed = 1
# date = '2023-11-01'
date = '2024-01-18'
time = '14:00:00'
radius = 200

# Start point 1
start_latitude = 44.649988
start_longitude = 10.917893

# End point 1: Tonini
end_latitude = 44.631281
end_longitude = 10.873258


#Find stops near the start point
result = greeter.find_near_stops(date, start_latitude, start_longitude, radius)
start_near_stops = pd.DataFrame(result, columns=['stop_name'])
start_near_stops



#Find stops near the end point
result = greeter.find_near_stops(date, end_latitude, end_longitude, radius)
end_near_stops = pd.DataFrame(result, columns=['stop_name'])
end_near_stops



result = greeter.routing_between_two_points_in_space(date, start_latitude, end_latitude, start_longitude, end_longitude, str(list(start_near_stops['stop_name'])), str(list(end_near_stops['stop_name'])), 1, time)
df_path = pd.DataFrame(result,columns=['trip','departure','line','starting_stop_name','starting_stop_id','starting_stop_coordinates','next_trip','next_stop','next_stop_id','next_stop_coordinates','next_line','arrival'])
df_path



df_path['departure'] = df_path['departure'].apply(lambda x: convert_to_datetime(x))
df_path['arrival'] = df_path['arrival'].apply(lambda x: convert_to_datetime(x))



count_changes = lambda x: 0 if x <= 1 else x - 1
changes = count_changes(len(df_path['line'].drop_duplicates()))


#Calculate the travel time from the start point to the first bus stop
start_distance = greeter.distance_from_a_stop(df_path['starting_stop_id'].iloc[0], start_latitude, start_longitude)[0]
#Calculate the travel time from the end point to the last bus stop
end_distance = greeter.distance_from_a_stop(df_path['next_stop_id'].tail(1).iloc[0], end_latitude, end_longitude)[0]

total_time = start_distance/speed + end_distance/speed + time_difference_seconds(df_path['arrival'].tail(1).iloc[0],df_path['departure'].head(1).iloc[0])


#Final results
print(f'The trip date is {date} and the time inserted is {time}')
print(f'Your trip starts at ({start_latitude},{start_longitude})')
print(f'You walk for {round(start_distance/speed/60, 1)} minutes to arrive at the bus stop')
show_more_details(df_path)
print(f'The number of changes to do are {changes}')
print(f'You arrive at the final point ({end_latitude},{end_longitude}) after {round(end_distance/speed/60, 1)} minutes')
print(f'The total time employed is {round(total_time/60, 0)} minutes')



import timeit, numpy as np
from geopy.distance import geodesic
import matplotlib.pyplot as plt

greeter = App('http://localhost:7687', 'neo4j', '12345678')

radius = 300



performance = pd.DataFrame(columns=['start_lat', 'start_lon', 'end_lat', 'end_lon','find_start_stops', 'find_end_stops', 'routing_time'])


def calculate_distance(row):
    start_coords = (row.iloc[0], row.iloc[1])
    end_coords = (row.iloc[2], row.iloc[3])
    return geodesic(start_coords, end_coords).kilometers


coords = [
    [(44.649988, 10.917893), (44.6274857758479, 10.947848294531108)],
    # modena autostazione - vignolese (vicino al dief)
    [(44.6274857758479, 10.947848294531108), (44.631281, 10.873258)],  # vignolese - Tonini
    [(44.649988, 10.917893), (44.631281, 10.873258)],  # via Emilia Ovest - Tonini
    [(44.651470232478886, 10.808315946994957), (44.63762254014361, 10.946646579247362)],  # Marzaglia - policlinico
    [(44.65311506793417, 10.93005907242455), (44.643695551154735, 10.930429915919644)],  # stazione - storchi
    [(44.63976076124965, 10.941661861492578), (44.63403438527048, 10.955699843803663)],
    # tratta su via emilia est molto breve
    [(44.65311506793417, 10.93005907242455), (44.725732352761995, 11.04233547009754)],  # stazione - nonantola
    [(44.60528176108951, 10.867088533100985), (44.64792367823092, 10.923039913894241)],  # baggiovara - centro modena
    [(44.52641453612295, 10.86607842381116), (44.60528176108951, 10.867088533100985)]  # Maranello - baggiovara
]

for coord in coords:
    # Find near start stops
    start_time = timeit.default_timer()
    result = greeter.find_near_stops(date, coord[0][0], coord[0][1], radius)
    end_time = timeit.default_timer()

    time_find_start_near_stops = end_time - end_time

    start_near_stops = pd.DataFrame(result, columns=['stop_name'])

    # Find near end stops
    start_time = timeit.default_timer()
    result = greeter.find_near_stops(date, coord[1][0], coord[1][1], radius)
    end_time = timeit.default_timer()

    time_find_end_near_stops = end_time - start_time

    end_near_stops = pd.DataFrame(result, columns=['stop_name'])

    # Routing
    start_time = timeit.default_timer()
    result = greeter.routing_between_two_points_in_space(date, coord[0][0], coord[1][0], coord[0][1], coord[1][1],
                                                         str(list(start_near_stops['stop_name'])),
                                                         str(list(end_near_stops['stop_name'])), 1, time)
    end_time = timeit.default_timer()

    routing_time = end_time - start_time

    performance.loc[len(performance.index)] = [coord[0][0], coord[0][1], coord[1][0], coord[1][1],
                                               time_find_start_near_stops, time_find_end_near_stops, routing_time]

plt.plot(performance['distance'], performance['routing_time'], marker='o', linestyle='-')
plt.xlabel('Euclidean Distance')
plt.ylabel('Routing Time (seconds)')
plt.title('Relationship between Distance and Routing Time')

plt.xticks([i for i in range(int(min(performance['distance'])), int(max(performance['distance'])) + 1, 1)])
plt.yticks(range(0, int(max(performance['routing_time'])) + 1, 2))

plt.grid(True, color='lightgray', alpha=0.5)
plt.show()



plt.plot(performance['distance'], performance['find_start_stops'],  marker='o', linestyle='-')
plt.plot(performance['distance'], performance['find_end_stops'],  marker='o', linestyle='-')
plt.xlabel('Euclidean Distance')
plt.ylabel('Query Execution Time (seconds)')
plt.title('Relationship between Distance and time to find the stops near the considered point')

plt.xticks([i for i in range(int(min(performance['distance'])), int(max(performance['distance']))+1, 1)])
plt.yticks(np.arange(0, 0.7, 0.1))

plt.grid(True, color='lightgray', alpha=0.5)
plt.show()


greeter.close()








