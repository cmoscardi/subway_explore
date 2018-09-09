from collections import defaultdict, namedtuple
from datetime import timedelta
import logging
from itertools import permutations, chain
from joblib import Parallel, delayed

import geopandas as gpd
from graph_tool import topology
import matplotlib
import matplotlib.pyplot as plt
# big API changes make this a nightmare to run in nx 1.x
import networkx as nx
assert nx.__version__ == '2.1'
import numpy as np
import pandas as pd
from shapely.geometry import Point

from .config import VEHICLE_CAPACITY, N_VEHICLES, OMEGA, DELTA
from .loader import load_road_graph
from .helpers import memoize

rgs = None
joined_stops = None
def init_travel(joined_stops_init, rgs_init):
    global rgs
    global joined_stops
    joined_stops = joined_stops_init
    rgs = rgs_init

def travel(t, vehicle,
           requests, must_travel=False):
    """
    As per alonso-mora paper

    requests: (o, d, t) tuples
    """
    dropoffs = ((p, 'd') for p in vehicle['passengers'] + requests)
    logging.debug("dropoffs is %s", dropoffs)
    pickups = ((p, 'p') for p in requests)

    # TODO: parallelize nicely?
    g = permutations(chain(pickups, dropoffs))
    result = (proc_cost(pd_order, vehicle, joined_stops, t, must_travel) for pd_order in g)
    best_order, min_cost = min(result, key=lambda x: x[1]\
                                       if x[1] is not None\
                                       else float('inf'))

    return min_cost, best_order


def proc_cost(pd_order, vehicle, joined_stops, t, must_travel=False):
    # logging.debug("trying permutation %s... ", pd_order)
    if not legal(pd_order, 
                 len(vehicle["passengers"]),
                 vehicle["capacity"]):
        # logging.debug("... it was illegal.")
        return None, None
    first_stop = pd_order[0][0][0] if pd_order[0][1] == 'p' else pd_order[0][0][1]
    first_rg_node = joined_stops.loc[first_stop]["index_right"]
    time_to_first = rgs[2][rgs[1][vehicle["cur_node"]]][rgs[1][first_rg_node]]
    cost = compute_cost(t, time_to_first, joined_stops, rgs,
                        pd_order, must_travel)
    if cost == -1:
        return None, None

    return pd_order, cost
        
def legal(pd_order, n_passengers, capacity):
    indices_by_passenger = defaultdict(dict)
    cur_count = n_passengers
    for i, (passenger, p_or_d) in enumerate(pd_order):
        indices_by_passenger[passenger][p_or_d] = i
        cur_count += (1 if p_or_d == 'p' else -1)
        if cur_count > capacity:
            return False
    for indices in indices_by_passenger.values():
        if 'p' in indices and indices['p'] > indices['d']:
            return False
    return True

def compute_cost(t, time_to_first, joined_stops, rgs, pd_order, must_travel=False):
    costs_by_passenger = defaultdict(dict)
    cur_time = t
    cur_stop = None
    passenger, p_or_d = pd_order[0]
    #logging.debug("time is {}".format(t))
    ttf = timedelta(seconds=time_to_first)
    #logging.debug("ttf is {}".format(ttf))
    #import pdb; pdb.set_trace()
    if p_or_d == 'p':            
        costs_by_passenger[passenger]['pickup_time'] = t + ttf
        cur_stop = passenger[0]
    elif p_or_d == 'd':
        costs_by_passenger[passenger]['dropoff_time'] = t + ttf
        cur_stop = passenger[1]
        
    cur_time = t + ttf
    for (passenger, p_or_d) in pd_order[1:]:
        if p_or_d == "p":
            key = "pickup_time"
            od = 0 
        else:
            key = "dropoff_time"
            od = 1
        next_stop = passenger[od]
        cur_node = joined_stops.loc[cur_stop]['index_right']
        next_node = joined_stops.loc[next_stop]['index_right']
        ttn = rgs[2][rgs[1][cur_node]][rgs[1][next_node]]
        ttn = timedelta(seconds=ttn)
        next_time = cur_time + ttn
        costs_by_passenger[passenger][key] = next_time
        cur_stop = next_stop
        cur_time = next_time
    total_cost = 0
    for passenger, cost in costs_by_passenger.items():
        thing = (passenger.t_star + timedelta(seconds=DELTA + 5))
        if "pickup_time" in cost and cost["pickup_time"] > passenger.tpl:
            return -1
        elif cost["dropoff_time"] > thing:
            if must_travel:
                total_cost += 1000000
                continue
            else:
                return -1
        total_cost += (cost["dropoff_time"] - passenger.t_star).total_seconds()
    #return costs_by_passenger
    return total_cost

def parse_travel_path(path, v, joined_stops, g, vmr):
    path_nodes = []
    path_edges = []
    cur_node = v["cur_node"]
    for r, p_or_d in path:
        if p_or_d == 'p':
            o_or_d = r.road_o
        else:
            o_or_d = r.road_d
        nodes, edges = topology.shortest_path(g, vmr[cur_node], vmr[o_or_d])

        path_nodes += [g.vertex_properties['_graphml_vertex_id'][n] for n in nodes]
        path_edges += edges
        cur_node = o_or_d
    return path_nodes, path_edges
