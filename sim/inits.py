from collections import namedtuple
from datetime import datetime, timedelta

from shapely.geometry import Point

from .config import OMEGA, DELTA, VEHICLE_CAPACITY


Passenger = namedtuple("Passenger", ["o", "d", "t", "tpl", "t_star"])
Passenger.__str__ = lambda p: p.o + "-" + p.d
Passenger.__repr__ = lambda p: p.o + "-" + p.d


def init_passenger(o, d, t, skim_graph):
    try:
        shortest = skim_graph[o][d]['weight']
    except:
        raise
    return Passenger(o, d, t, t + timedelta(minutes=OMEGA), 
                     t + timedelta(minutes=(shortest + DELTA)))

def init_vehicle(x, y):
    BASE_VEHICLE = {"capacity": VEHICLE_CAPACITY,
                    "passengers": [],
                    "cur_xy": Point(x, y),
                    "dest_node": 0,
                    "latest_node": "0",
                    "next_node": "-1",
                    "cur_route": [],
                    "ridership_history": []} # tuples of the form (start_t, end_t, n)
    
    return BASE_VEHICLE.copy()
