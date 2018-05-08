from graph_tool import topology
import geopandas as gpd
import numpy as np
import networkx as nx

from .config import T_STEP

def greedy_assign(rtvg, T):
    R_ok = set()
    V_ok = set()
    assignment = set()
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
            assignment.add((trip, v))
            V_ok.add(v)
    return assignment


def move_vehicles(assignment, vehicles, g, vmr):
    vehicles_by_id = dict(vehicles)
    for trip, vid in assignment:
        v = vehicles_by_id[vid]
        nodes, edges = topology.shortest_path(g, vmr[v['cur_node']])
        total_time = 0.
        # this should be the stopping index (So 1 greater than last edge)
        max_i = 0
        for i, e in enumerate(edges):
            weight = g.edge_properties["weight"][e]
            if i > 0 and weight + total_time > T_STEP:
                max_i = i
                break
            elif i == 0 and weight + total_time > T_STEP:
                max_i = 1
                total_time = weight
                break
            else:
                total_time = total_time + weight

        v['cur_node'] = g.vertex_properties['_graphml_vertex_id'][e.source()]
        v['cur_xy'] = lion_nodes.loc[v['cur_node']]
        v['cur_route'] = [g.vertex_properties['_graphml_vertex_id'][n] for n in nodes]
        
        
        weights = [g.edge_properties["weight"][e] for e in edges]
        

assign = greedy_assign

def draw_assign(assignment, lion_nodes, lion_rg, joined_stops, road_graph, vehicles, travel, t):
    ax = lion_rg.plot(figsize=(16, 40))
    joined_stops["geometry"] = gpd.GeoSeries(joined_stops["geometry_old"])
    joined_stops.loc[[a.o for b in assignment for a in b[0]]].plot(ax=ax, color='red', zorder=5)
    joined_stops.loc[[a.d for b in assignment for a in b[0]]].plot(ax=ax, color='green', zorder=5)
    vehicles_by_index = dict(vehicles)
    for trips, v in assignment:
        vehicle = vehicles_by_index[v]
        ax.scatter([vehicle["cur_xy"].x], [vehicle["cur_xy"].y], zorder=5, color='orange')
        cost, path = travel(t, vehicle, list(trips))
        cur_node = vehicle["cur_node"]
        path_nodes = []
        for r, p_or_d in path:
            if p_or_d == 'p':
                o_or_d = joined_stops.loc[r.o]['index_right']
            else:
                o_or_d = joined_stops.loc[r.d]['index_right']
            path_nodes += nx.algorithms.dijkstra_path(road_graph, cur_node, o_or_d)
            cur_node = o_or_d
        edges = list(zip(path_nodes, path_nodes[1:]))
        indices = [road_graph.edges[e]['ix'] for e in edges if e[0] != e[1]]
        lion_rg.loc[indices].plot(color='yellow', ax=ax, zorder=5)
        return bad_boys
        

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

