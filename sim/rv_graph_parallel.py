import dill

import joblib as jl

#from .config import N_JOBS
from .inits import init_vehicle
from datetime import timedelta
N_JOBS =4 


import networkx as nx
assert nx.__version__ == "2.1"
def init_rv_graph(joined_stops, travel):

    def check_rr_edge(t, r1, r2, v):
        a, b = travel(t, v, [r1, r2])
        if not a:
            return False
        return a

    def check_rv_edge(t, v, r):
        a, b = travel(t, v, [r])
        if not a:
            return False
        return a

    def gen_rv_graph(t, requests, vehicles, debug=False):
        rv_g = nx.Graph()
        rr_g = nx.Graph()
        if debug:
            print("{} requests, {} vehicles"\
                  .format(len(requests), len(vehicles)))
        for n, r1 in enumerate(requests):
            if debug and not (n % 10):
                print("{} of {} requests checked".format(n, len(requests)))

            p = jl.Parallel(n_jobs=N_JOBS)
            def process_v(veh):
                return check_rv_edge(t, veh, r1)
            gen = ((i, jl.delayed(process_v)(v)) for i, v in vehicles)
            results = p(gen)
            [rv_g.add_edge(r1, i, weight=res) for i, res in results if res]
            
            stop = joined_stops.loc[r1.o]
            x, y = stop["geometry_old"].x, stop["geometry_old"].y
            fake_vehicle = init_vehicle(x, y)
            gen2 = ((r2, jl.delayed(check_rr_edge)(t, r1, r2, fake_vehicle)) for r2 in requests)
            results = p(gen2)
            [rr_g.add_edge(r1, r2, weight=res) for r2, res in results if res]
        return rr_g, rv_g
    return gen_rv_graph

def plot_rv_graph(rr_g, rv_g, vehicles, stops, lion_rg):
    ax = lion_rg.plot(alpha=.3, figsize=(16, 40))
    stops.loc[[n.o for n in rr_g.nodes]].plot(color='red', ax=ax)
    vehicles_by_index = dict(vehicles)
    for ix, v in vehicles:
        plt.scatter([v["cur_xy"].x], [v["cur_xy"].y], color='orange', s=36)
    for p1, p2 in rr_g.edges.iterkeys():
        xs = gpd.GeoSeries(stops.loc[[p1.o, p2.o]].geometry_old).x
        ys = gpd.GeoSeries(stops.loc[[p1.o, p2.o]].geometry_old).y
        plt.plot(xs, ys, color='green')
    for v, p in rv_g.edges.iterkeys():
        vehicle = vehicles_by_index[v]
        plt.plot([vehicle["cur_xy"].x, stops.loc[p.o].geometry_old.x],
                 [vehicle["cur_xy"].y, stops.loc[p.o].geometry_old.y],
                 color='yellow')
