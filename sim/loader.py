import geopandas as gpd
from graph_tool import load_graph
from graph_tool.topology import shortest_distance

import networkx as nx
assert nx.__version__ == "2.1"
import numpy as np
import pandas as pd
from shapely.geometry import Point

from .helpers import uniform_str

def load_road_graph(path="data/taxi_graphs/final_graph_hour_1.pkl",
                    fix=True):
    """
    fix: whether we need to update weights
    """
    road_graph = nx.read_gpickle(path)
    for k, v in road_graph.edges.items():
        v['weight'] = v['dist'] / v['speed']
    return road_graph


def merge_lion_road(lion, lion_nodes, road_graph):
    rg_indices = [(e["ix"], e["speed"])\
                  for _, e in road_graph.edges.items()]
    lion_only_rg = lion.loc[[r[0] for r in rg_indices]].copy()
    lion_only_rg["speed"] = [r[1] for r in rg_indices]
    concat = np.concatenate
    unique_nodes = set(concat([lion_only_rg["NodeIDFrom"].values,
                               lion_only_rg["NodeIDTo"].values]))
    only_rg_nodes = lion_nodes.loc[unique_nodes]
    return lion_only_rg, only_rg_nodes


def load_skim_graph(path="data/taxi_graphs/1_am_station_skim_20180506.pkl"):
    """
    See gen_skim.py for generating
    """
    return nx.read_gpickle(path)

def load_road_skim_graph(path="data/taxi_graphs/final_graph_hour_0.graphml"):
    g = load_graph(path)
    vm = g.vertex_properties["_graphml_vertex_id"]
    vmr = {vm[v]: v for v in g.vertices()}
    skim_table = shortest_distance(g, weights=g.edge_properties["weight"])
    return (g, vmr, skim_table)


def load_lion():
    lion = gpd.read_file("data/mn_lines.shp").to_crs(epsg=4326)
    lion_nodes = gpd.read_file("data/lion/lion.shp/node.shp").to_crs(epsg=4326)


    lion_nodes["IDSTR"] = lion_nodes["NODEID"].apply(uniform_str)
    lion_nodes.set_index("IDSTR", inplace=True, drop=True) 
    return lion, lion_nodes


def load_demands():
    demands = pd.read_pickle("data/nycmtc_final_od.pkl")

    demands["mn_O"] = demands["manhattan_path"].apply(lambda x: x[0])
    demands["mn_D"] = demands["manhattan_path"].apply(lambda x: x[-1])
    demands["mn_O_station"] = demands["mn_O"].apply(lambda x: x.split("-")[0])
    demands["mn_D_station"] = demands["mn_D"].apply(lambda x: x.split("-")[0])
    demands["demand_time_canonical"] = (demands["TRP_DEP_HR"].apply(lambda x: x - 24 if x > 23 else x)\
                                        + demands["TRP_DEP_MIN"].apply(lambda x: float(x) / 60.))

    stops = pd.read_csv("data/mta_gtfs/stops.txt")
    stops["geometry"] = stops[["stop_lon", "stop_lat"]].apply(Point, axis=1)
    stops["geometry_old"] = stops["geometry"]

    stops = gpd.GeoDataFrame(stops).set_index("stop_id")

    return demands, stops

def merge_stops_demands(stops, rg_nodes, demands):
    stops["geometry"] = gpd.GeoSeries(stops["geometry_old"]).buffer(.002)
    joined_stops = gpd.sjoin(stops, rg_nodes)
    joined_stops = joined_stops[~joined_stops.index.duplicated(keep='first')]
    demands_with_stops = demands.merge(joined_stops,
                                       right_index=True,
                                       left_on="mn_O_station")\
                            .merge(joined_stops, right_index=True, left_on="mn_D_station", suffixes=("_o", "_d"))
    demands_with_stops = gpd.GeoDataFrame(demands_with_stops)
    demands_with_stops["geometry"] = demands_with_stops["geometry_o"]
    joined_stops["geometry"] = gpd.GeoSeries(joined_stops["geometry_old"])
    return joined_stops, demands_with_stops

def load_turnstile_counts(joined_stops, sim_time, t_step):
    turnstile_o = gpd.read_file("data/turnstile/crappy_clean_12am_5am_ENTRIES.shp")
    turnstile_d = gpd.read_file("data/turnstile/crappy_clean_12am_5am_EXITS.shp")
    js = joined_stops
    parent_stations = js[js["parent_station"].isna()]
    o_merged = parent_stations.merge(turnstile_o[["stop_id", "0"]], left_index=True, right_on="stop_id", how='left')
    #NOTE: we fillna with the 25th percentile
    o_merged["OCOUNT"] = o_merged["0"]
    o_merged["OCOUNT"] = o_merged["OCOUNT"].fillna(o_merged["OCOUNT"].quantile(.25))
    del o_merged["0"]
    d_merged = o_merged.merge(turnstile_d[["stop_id", "0"]], left_on="stop_id", right_on="stop_id", how='left')
    d_merged["DCOUNT"] = d_merged["0"]
    d_merged["DCOUNT"] = d_merged["DCOUNT"].fillna(d_merged["DCOUNT"].quantile(.25))
    del d_merged["0"]

    # set poisson rate / transition prob
    d_merged["lambda"] = (d_merged["OCOUNT"] / sim_time) * t_step
    d_merged["dest_prob"] = (d_merged["DCOUNT"] / d_merged["DCOUNT"].sum())
   
    return d_merged

