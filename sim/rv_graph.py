from .inits import init_vehicle
from datetime import timedelta

import geopandas as gpd
import matplotlib.pyplot as plt
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

            for i, v in vehicles:
                a = check_rv_edge(t, v, r1)
                if a:
                    rv_g.add_edge(r1, i, weight=a)
            
            stop = joined_stops.loc[r1.o]
            x, y = stop["geometry_old"].x, stop["geometry_old"].y
            fake_vehicle = init_vehicle(x, y)
            for r2 in requests:
                a = check_rr_edge(t, r1, r2, fake_vehicle)
                if a:
                    rr_g.add_edge(r1, r2, weight=a)
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
        if isinstance(p, int):
            v, p = p, v
        vehicle = vehicles_by_index[v]
        plt.plot([vehicle["cur_xy"].x, stops.loc[p.o].geometry_old.x],
                 [vehicle["cur_xy"].y, stops.loc[p.o].geometry_old.y],
                 color='yellow')
