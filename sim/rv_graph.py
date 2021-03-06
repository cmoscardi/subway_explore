from datetime import timedelta

import geopandas as gpd
import matplotlib.pyplot as plt
import networkx as nx
assert nx.__version__ == "2.1"

from .vehicle import init_vehicle
from .travel import travel

def init_rv_graph(joined_stops):

    def check_rr_edge(t, r1, r2, v):
        a, b = travel(t, v, [r1, r2])
        if a is None:
            return False
        return a

    def check_rv_edge(t, v, r):
        a, b = travel(t, v, [r])
        if a is None:
            return False
        return a

    def gen_rv_graph(t, requests, vehicles, debug=False):
        rv_g = nx.Graph()
        rr_g = nx.Graph()
        if debug:
            print("{} requests, {} vehicles"\
                  .format(len(requests), len(vehicles)))
        lr = list(requests)
        for n, r1 in enumerate(lr):
            if debug and not (n % 10):
                print("{} of {} requests checked".format(n, len(requests)))

            for i, v in vehicles:
                a = check_rv_edge(t, v, r1)
                if a:
                    rv_g.add_edge(r1, i, weight=a)

            stop = joined_stops.loc[r1.o]
            x, y, node = stop["geometry_old"].x, stop["geometry_old"].y, stop["index_right"]
            fake_vehicle = init_vehicle(x, y, node)
            for r2 in lr[n:]:
                a = check_rr_edge(t, r1, r2, fake_vehicle)
                if a:
                    rr_g.add_edge(r1, r2, weight=a)
        return rr_g, rv_g
    return gen_rv_graph

def plot_rv_graph(rr_g, rv_g, vehicles, stops, lion_rg, requests):
    ax = lion_rg.plot(alpha=.3, figsize=(16, 40))
    stops.loc[[n.o for n in rr_g.nodes]].plot(color='red', ax=ax)
    vehicles_by_index = dict(vehicles)
    for ix, v in vehicles:
        plt.scatter([v["cur_xy"].x], [v["cur_xy"].y], color='orange', s=36)

    xs = gpd.GeoSeries(stops.loc[[r.o for r in requests]].geometry_old).x
    ys = gpd.GeoSeries(stops.loc[[r.o for r in requests]].geometry_old).y
    ax.scatter(xs, ys, color='red', zorder=999)

    for p1, p2 in rr_g.edges.keys():
        xs = gpd.GeoSeries(stops.loc[[p1.o, p2.o]].geometry_old).x
        ys = gpd.GeoSeries(stops.loc[[p1.o, p2.o]].geometry_old).y
        ax.plot(xs, ys, color='green')
    for v, p in rv_g.edges.keys():
        if isinstance(p, int):
            v, p = p, v
        vehicle = vehicles_by_index[v]
        ax.plot([vehicle["cur_xy"].x, stops.loc[p.o].geometry_old.x],
                 [vehicle["cur_xy"].y, stops.loc[p.o].geometry_old.y],
                 color='yellow')
