
# coding: utf-8

# In[ ]:


import pandas as pd
import networkx as nx
import geopandas as gpd
import shapely.geometry

import matplotlib

import glob


# In[ ]:


nx.__version__


# In[ ]:


import sys
HOUR = int(sys.argv[1])
df = pd.read_pickle("data/taxi_clean/FINAL_HR_{}.pkl".format(HOUR))
print("Hour {} has {} trips".format(HOUR, len(df)))


# In[ ]:


def uniform_str(x):
    strd = str(x)
    while len(strd) < 7:
        strd = '0' + strd
    return strd
df["NODEID_O"] = df["NODEID_O"].apply(uniform_str)
df["NODEID_D"] = df["NODEID_D"].apply(uniform_str)
df["pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
df["dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
del df['tpep_dropoff_datetime']
del df['tpep_pickup_datetime']

mn_nodes = gpd.read_file("data/mn_nodes.shp")
init_graph = nx.read_gpickle("data/final_graph_1st_pass_nx_2.1.pkl")
for a, b in init_graph.edges.items():
    b['speed'] = 25.
    b['weight'] = (b['dist'] / b['speed']) * 3600


# In[ ]:


import numpy as np
from itertools import chain
from collections import OrderedDict
                    
def first_average(g):
    travel_time = ((g["dropoff_datetime"] - g["pickup_datetime"]).mean()).total_seconds()
    res = g.iloc[0]
    res["tt_avg"] = travel_time
    res["n_trips"] = len(g)
    return res


# In[ ]:


import dask.dataframe as dd
from dask.multiprocessing import get
from dask.diagnostics import ProgressBar
ddata = dd.from_pandas(df, npartitions=30)
averaged = ddata.groupby(["NODEID_O", "NODEID_D"])           .apply(first_average)           .reset_index(drop=True)
#averaged = averaged.reset_index()

# first trip filtering - > 2 mins, < 1 hour
first_filtered = averaged[(averaged["NODEID_O"] != averaged["NODEID_D"]) &
        (averaged["tt_avg"] > 120) &
        (averaged["tt_avg"] < 3600)].reset_index(drop=True)

with ProgressBar():
    fa = first_filtered.compute(get=get)

fa["NODEID_O"] = fa["NODEID_O"].apply(uniform_str)
fa["NODEID_D"] = fa["NODEID_D"].apply(uniform_str)


# In[ ]:


import numpy as np
import toolz


S_trip = None
T_s = None
O_s = None

def trip_dist(trip):
    try:
        p = nx.algorithms.shortest_path(init_graph,
                                    trip["NODEID_O"], 
                                    trip["NODEID_D"])
    except Exception as e:
        return np.nan
    edges = ((p[i], p[i+1]) for i in range(len(p) - 1))
    street_dist = sum(init_graph.get_edge_data(*e)['dist'] for e in edges)
    return street_dist

@toolz.curry
def trip_path(trip, graph=None):

    try:
        p = nx.algorithms.shortest_path(graph,
                                    trip["NODEID_O"], 
                                    trip["NODEID_D"])
        edges = ((p[i], p[i+1]) for i in range(len(p) - 1))
    except Exception as e:
        return np.nan
    return ",".join(p)


@toolz.curry
def trip_time(p,graph=None):
    p = p.split(",")
    edges = ((p[i], p[i+1]) for i in range(len(p) - 1))
    t = 0.
    for e in edges:
        try:
            w = graph.get_edge_data(*e)['weight']
        except Exception as q:
            bad = p
            raise q
        t += w
    return t


# In[ ]:


import matplotlib.pyplot as plt
dfa = dd.from_pandas(fa, npartitions=30)
# second trip filtering
dfa["dist"] = dfa.apply(trip_dist, axis=1, 
                        meta=("dist", "f8"))
dfa = dfa.dropna(subset=["dist"])
dfa["speed"] = dfa["dist"] / (dfa["tt_avg"])
dfa = dfa[((dfa["speed"] * 3600.) > 1)                    & (dfa["speed"] < (65 / (3600.)))]            .reset_index(drop=True)

with ProgressBar():
    sa = dfa.compute(get=get)


# In[ ]:


from collections import defaultdict
from datetime import datetime

# iterative steps
again = True
done = False
base_graph = init_graph.copy()
dp = sa

while again:
    tt = trip_time(graph=base_graph)
    path = trip_path(graph=base_graph)
    again = False
    S_trip = set() # all touched streets
    T_s = defaultdict(set) # basically trips_by_street
    O_s = defaultdict(np.float64) # offset_by_street
    dsa = dd.from_pandas(dp, npartitions=30)
    dsa["path"] = dsa.apply(path, axis=1, meta=("path", "O"))
    dsa["et"] = dsa["path"].apply(tt, meta=("tt", "f8"))
    dsa["rel_err"] = (dsa["et"] - dsa["tt_avg"]) / dsa["tt_avg"]
    dsa["offset"] = ((dsa["et"] - dsa["tt_avg"]) * dsa["n_trips"])
    with ProgressBar():
        dp = dsa.compute(get=get)
        
    print("time is {}".format(datetime.now()))
    for name, p in zip(dp.index, dp["path"].str.split(",")):
        edges = zip(p, p[1:])
        for e in edges:
            T_s[e].add(name)
            S_trip.add(e)
        
    print("time is {}".format(datetime.now()))
    print("Sets computed")
    for street, trips in T_s.items():
        trips_df = dp.loc[trips]
        O_s[street] = (trips_df["offset"]).sum()
    print("time is {}".format(datetime.now()))
    k = 1.2
    print("rel_err sum is {}".format(np.abs(dp["rel_err"]).sum()))
    while True:
        g_c = base_graph.copy()
        tt2 = trip_time(graph=g_c)
        for street in S_trip:
            a, b = street # street connects nodes a and b
            e = base_graph.edges[street]
            if O_s[street] < 0:
                g_c[a][b]['weight'] = e["weight"] * k
            else:
                g_c[a][b]['weight'] = e["weight"] / k
        dp["et_new"] = dp["path"].apply(tt2)
        dp["new_rel_err"] = (dp["et_new"] - dp["tt_avg"]) / dp["tt_avg"]
        print("new_rel_err sum is {}".format(np.abs(dp["new_rel_err"]).sum()))
        if np.abs(dp["new_rel_err"]).sum() < np.abs(dp["rel_err"]).sum():
            dp["et"] = dp["et_new"]
            dp["rel_err"] = dp["new_rel_err"]
            again = True
            base_graph = g_c
            break
        else:
            print("k updated to {}".format(k))
            k = 1 + (k - 1) * .75
            if k < 1.0001:
                break   


# In[ ]:


nx.write_gpickle(base_graph,"data/taxi_graphs/base_graph_hour_{}.pkl".format(HOUR))


# In[ ]:


for e, info in base_graph.edges.items():
    print(info)
    print("speed is {}".format(info['dist'] / (info['weight'] / 3600.)))
    break


# In[ ]:


#speeds = pd.Series([info["dist"] / (info['weight'] / 3600.) for info in final_graph.edges.values()])


# In[ ]:


final_graph = base_graph.copy()
for e, attrs in final_graph.edges.items():
    attrs["speed"] = attrs["dist"] / attrs["weight"]


# In[ ]:


S = set(final_graph.edges.keys())
ES = S_trip
NS = S - S_trip
N_S = nodes_by_street = {s: set(final_graph.edges(s)) for s in S}
n_s_i = n_by_street = sorted({s: len(N_S[s].intersection(S_trip)) for s in NS}.items(), 
                             key=lambda x: x[1], reverse=True)


# In[ ]:


for s, n in n_s_i:
    if n == 0:
        continue
    intersecting_speeds = [final_graph.edges[(e1,e2)]['speed'] for e1, e2 in N_S[s].intersection(S_trip)]
    final_e = final_graph.edges[s]
    v_s_i = sum(intersecting_speeds) / len(intersecting_speeds)
    t_s_i = final_e["dist"] / v_s_i
    final_e["speed"] = v_s_i
    final_e["weight"] = t_s_i
    ES.add(s)
    NS = NS - set([s])


# In[ ]:


nx.write_gpickle(final_graph, "data/taxi_graphs/final_graph_hour_{}.pkl".format(HOUR))


# In[ ]:


#speeds = [(thing['ix'], thing['speed'] * 3600) for _, thing in final_graph.edges.items()]
#speeds = pd.Series((s[1] for s in speeds), index=(s[0] for s in speeds))
#speeds.name='speed'
#
#
## In[ ]:
#
#
#import geopandas as gpd
#mn_lines = gpd.read_file("data/mn_lines.shp")
#
#
## In[ ]:
#
#
#mn_speed_cols = mn_lines.loc[speeds.index]
#mn_speed_cols["speed"] = speeds
#
#
## In[ ]:
#
#
#mn_speed_cols.plot("speed", figsize=(16, 40))
#
#
## In[ ]:
#
#
#mn_lines._geometry_array
#
