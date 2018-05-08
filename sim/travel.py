from collections import defaultdict, namedtuple
from datetime import timedelta
from itertools import permutations, chain

import geopandas as gpd
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

def init_travel(joined_stops, skim_graph, road_skim_lookup):
    def travel(t, vehicle,
               requests):
        """
        As per alonso-mora paper
        
        requests: (o, d, t) tuples
        """
        dropoffs = ((p, 'd') for p in vehicle['passengers'] + requests)
        pickups = ((p, 'p') for p in requests)
        
        best_order = None
        min_cost = None
        # TODO: parallelize
        for pd_order in permutations(chain(pickups, dropoffs)):
            if not legal(pd_order, 
                         len(vehicle["passengers"]),
                         vehicle["capacity"]):
                continue
            first_stop = pd_order[0][0][0] if pd_order[0][1] == 'p' else pd_order[0][0][1]
            first_rg_node = joined_stops.loc[first_stop]["index_right"]
            time_to_first = road_skim_lookup(vehicle["cur_node"], first_rg_node)
            cost = compute_cost(t, time_to_first, joined_stops, road_skim_lookup,
                                pd_order)
            if cost == -1:
                continue
                
            if not min_cost or cost < min_cost:
                order = [p.o if pord == 'p' else p.d for p, pord in pd_order]
                min_cost = cost
                best_order = pd_order
        return min_cost, best_order
    return travel

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

def compute_cost(t, time_to_first, joined_stops, road_skim_lookup, pd_order):
    costs_by_passenger = defaultdict(dict)
    cur_time = t
    cur_stop = None
    passenger, p_or_d = pd_order[0]
    ttf = timedelta(seconds=time_to_first)
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
        next_node = joined_stops.loc[cur_stop]['index_right']
        ttn = timedelta(seconds=road_skim_lookup(cur_node, next_node))
        next_time = cur_time + ttn
        costs_by_passenger[passenger][key] = next_time
        cur_stop = next_stop
        cur_time = next_time
    total_cost = 0
    for passenger, cost in costs_by_passenger.items():
        if "pickup_time" in cost and cost["pickup_time"] > passenger.tpl:
            return -1
        elif cost["dropoff_time"] > (passenger.t_star + timedelta(seconds=DELTA)):
            return -1
        total_cost += (cost["dropoff_time"] - passenger.t_star).total_seconds()
    #return costs_by_passenger
    return total_cost
