import geopandas as gpd
import numpy as np
import networkx as nx

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
        cur_node = lion_nodes.geometry.distance(vehicle["cur_xy"]).argmin()
        path_nodes = []
        for r, p_or_d in path:
            if p_or_d == 'p':
                o_or_d = joined_stops.loc[r.o]['index_right']
            else:
                o_or_d = joined_stops.loc[r.d]['index_right']
            path_nodes += nx.algorithms.dijkstra_path(road_graph, cur_node, o_or_d)
            cur_node = o_or_d
        edges = list(zip(path_nodes, path_nodes[1:]))
        for e1, e2 in edges:
            lion_rg[(lion_rg.NodeIDFrom == e1) & (lion_rg.NodeIDTo == e2)].plot(ax=ax, color='yellow', zorder=5)
        
