from collections import namedtuple
from datetime import datetime, timedelta

import geopandas as gpd
import numpy as np
from shapely.geometry import Point

from .config import OMEGA, DELTA, VEHICLE_CAPACITY, T_STEP


Passenger = namedtuple("Passenger", ["o", "d", "t", "tpl", "t_star", "road_o", "road_d"])
Passenger.__str__ = lambda p: p.o + "-" + p.d
Passenger.__repr__ = lambda p: p.o + "-" + p.d


def init_passenger(o, d, t, joined_stops, road_skim_lookup):
    try:
        road_o, road_d = joined_stops.loc[[o, d]]["index_right"]
        shortest = road_skim_lookup(road_o, road_d)
        rnd = np.random.uniform(0, T_STEP)
        rt = t - timedelta(seconds=rnd)
    except:
        raise
    return Passenger(o, d, rt, rt + timedelta(seconds=OMEGA),
                     rt + timedelta(seconds=(shortest)),
                     road_o, road_d)


def draw_requests(requests, lion_rg, joined_stops, with_d=True):
    ax = lion_rg.plot(figsize=(16, 40))
    joined_stops.geometry = gpd.GeoSeries(joined_stops.geometry_old)
    joined_stops.loc[[r.o for r in requests]].plot(ax=ax, c='red', zorder=3)
    if with_d:
        joined_stops.loc[[r.d for r in requests]].plot(ax=ax, c='green', zorder=3)
    return ax

def draw_vehicles(vehicles, lion_rg=None, ax=None):
    if ax is None:
        ax = lion_rg.plot(figsize=(16, 40))

    gpd.GeoSeries(v["cur_xy"] for i, v in vehicles).plot(ax=ax, color='orange', zorder=3)
