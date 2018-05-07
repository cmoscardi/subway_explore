import joblib

import networkx as nx
assert nx.__version__ == "2.1"


road_graph = None
def init_skim(rg):
    global road_graph
    road_graph = rg
    

def process_g(ix1, ix2, s1, s2):
    l = nx.algorithms.shortest_path_length(road_graph, 
                                           ix1,
                                           ix2)
    return (ix1, ix2, l)

def gen_skim_graph(rg, write_path=None, n_jobs=112):
    print("==={} rows to process===").format(len(rg) * len(rg))
    g = (joblib.delayed(process_g)(ix1, ix2, s1, s2)\
         for ix1, s1 in rg.iterrows()\
         for ix2, s2 in rg.iterrows())

    p = joblib.Parallel(n_jobs=n_jobs, verbose=1)(g)

    skim_graph = nx.DiGraph()
    for ix1, ix2, t in p:
        skim_graph.add_edge(ix1, ix2, weight=t)

    if write_path:
        nx.write_gpickle(skim_graph, write_path)

    return skim_graph
