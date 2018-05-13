from shapely.geometry import Point

from .config import VEHICLE_CAPACITY, T_STEP
from .travel import parse_travel_path

def init_vehicle(x, y, rg_node):
    BASE_VEHICLE = {"capacity": VEHICLE_CAPACITY,
                    "passengers": [],
                    "cur_xy": Point(x, y),
                    "cur_node": rg_node,
                    "next_node": None,
                    "cur_route": [],
                    "ridership_history": []} # tuples of the form (start_t, end_t, n)
    
    return BASE_VEHICLE.copy()

def move_vehicles(assignment, vehicles, g, vmr, travel, t, joined_stops, lion_node):
    vehicles_by_id = dict(vehicles)
    passengers_to_remove = []
    for trips, vid in assignment:
        v = vehicles_by_id[vid]
        trips = list(set(trips) - set(v["passengers"]))
        cost, path = travel(t, v, trips)
        nodes, edges = parse_travel_path(path, v, joined_stops, g, vmr)

        total_time = 0.
        # this should be the stopping index (So 1 greater than last edge)
        max_i = 0
        events_ix = 0
        next_event = (path[0][0].road_o if path[0][1] == 'p' else path[0][0].road_d, path[0][1])
        for i, e in enumerate(edges):
            weight = g.edge_properties["weight"][e]
            next_node = g.vertex_properties['_graphml_vertex_id'][e.target()]
            if i > 0 and weight + total_time > T_STEP:
                max_i = i + 1
                next_node = g.vertex_properties['_graphml_vertex_id'][e.target()]
                events_ix, tr = handle_passengers(path, next_node, events_ix, v)
                passengers_to_remove += tr

                break
            elif i == 0 and weight + total_time > T_STEP:
                max_i = 1
                total_time = weight
                events_ix, tr = handle_passengers(path, next_node, events_ix, v)
                passengers_to_remove += tr
                break
            else:
                total_time = total_time + weight
                events_ix, tr = handle_passengers(path, next_node, events_ix, v)
                passengers_to_remove += tr

        v['cur_node'] = next_node
        v['cur_xy'] = lion_node.loc[v['cur_node']].geometry
        v['cur_route'] = nodes[max_i:]
    return set(passengers_to_remove)

def handle_passengers(path, next_node, events_ix, vehicle):
    passengers_to_remove = []
    again = True
    while again:
        again = False
        if events_ix == len(path):
            break
        next_event = (path[events_ix][0].road_o\
                      if path[events_ix][1] == 'p'\
                      else path[events_ix][0].road_d,
                      path[events_ix][1])
        if next_event[0] == next_node:
            again = True
            if next_event[1] == 'p':
                vehicle["passengers"].append(path[events_ix][0])
                passengers_to_remove.append(path[events_ix][0])
            else:
                vehicle["passengers"].remove(path[events_ix][0])
            events_ix = events_ix + 1

    return events_ix, passengers_to_remove
