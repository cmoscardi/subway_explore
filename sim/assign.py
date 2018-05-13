from collections import defaultdict

from graph_tool import topology
import geopandas as gpd
import numpy as np
import networkx as nx

from .config import T_STEP
from .travel import parse_travel_path

def greedy_assign(rtvg, T, vehicles):
    R_ok = set()
    V_ok = set()
    assignment = defaultdict(tuple)
    for k, Tk in zip(range(len(T), 0, -1), T[::-1]):
        if not Tk:
            continue
        Sk = sorted((e for e in rtvg.edges(Tk) if isinstance(e[1], int) or isinstance(e[0], int)),
                    key=lambda e: rtvg.edges[e]["weight"])
        for trip, v in Sk:
            if np.any([t in R_ok for t in trip]):
                continue
            if v in V_ok:
                continue
            [R_ok.add(t) for t in trip]
            assignment[v] = trip
            V_ok.add(v)
    for i, v in vehicles:
        for p in v["passengers"]:
            assignment[i] = assignment[i] + (p,)

    return set((v, k) for k, v in assignment.items())


assign = greedy_assign

def draw_assign(assignment, lion_nodes, lion_rg, joined_stops, road_graph, vehicles, travel, t, g, vmr):
    ax = lion_rg.plot(figsize=(16, 40))
    #joined_stops.loc[[a.o for b in assignment for a in b[0]]].plot(ax=ax, color='red', zorder=5)
    #joined_stops.loc[[a.d for b in assignment for a in b[0]]].plot(ax=ax, color='green', zorder=5)
    lion_nodes.loc[[a.road_o for b in assignment for a in b[0]]].plot(ax=ax, color='red', zorder=5)
    lion_nodes.loc[[a.road_d for b in assignment for a in b[0]]].plot(ax=ax, color='green', zorder=5)
    vehicles_by_index = dict(vehicles)
    for trips, v in assignment:
        print(v)
        print(trips)
        vehicle = vehicles_by_index[v]
        lion_nodes.loc[[vehicle["cur_node"]]].plot(ax=ax, color='orange', zorder=5)
        trips = list(set(trips) - set(vehicle["passengers"]))
        cost, path = travel(t, vehicle, trips)
        cur_node = vehicle["cur_node"]
        path_nodes, path_edges = parse_travel_path(path, vehicle, joined_stops, g, vmr)
        edges = list(zip(path_nodes, path_nodes[1:]))
        indices = [road_graph.edges[e]['ix'] for e in edges if e[0] != e[1]]
        lion_rg.loc[indices].plot(color='yellow', ax=ax, zorder=5)
    return ax
        

def draw_assign_graph(assignment, lion_nodes, lion_rg, joined_stops, road_graph, vehicles, travel, t):
     ax = lion_rg.plot(figsize=(16, 40))
     joined_stops["geometry"] = gpd.GeoSeries(joined_stops["geometry_old"])
     joined_stops.loc[[a.o for b in assignment for a in b[0]]].plot(ax=ax, color='red', zorder=5)
     vehicles_by_index = dict(vehicles)
     gpd.GeoSeries([v[1]["cur_xy"] for v in vehicles]).plot(ax=ax, color='orange', zorder=5)
     for trips, v in assignment:
         for tr in trips:
             ax.plot([vehicles_by_index[v]["cur_xy"].x, joined_stops.loc[tr.o].geometry.x],
                     [vehicles_by_index[v]["cur_xy"].y, joined_stops.loc[tr.o].geometry.y], 
                     color='yellow')

