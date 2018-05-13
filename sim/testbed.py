
from sim import main
from .vehicle import init_vehicle
from .inits import init_passenger

import logging
l = logging.getLogger()
l.setLevel(logging.DEBUG)

STOP_IDS = "108", "109", "110", "111"

def dummy(*args, **kwargs):
    pass



def new_passenger(s, i):
    return init_passenger(STOP_IDS[i], STOP_IDS[i + 1], s.t, s.joined_stops, s.road_skim_lookup)

def new_vehicle(s):
    V_LOC = "107"
    stop_place = s.joined_stops.loc[V_LOC]
    return init_vehicle(stop_place.geometry.x, stop_place.geometry.y,
                 stop_place["index_right"])




def test():
    s = main.Sim()
    s.set_passengers = dummy

    s.init()
    s.passengers = set([new_passenger(s, i) for i in range(3)])
    s.vehicles = [(0, new_vehicle(s))]
    s.step()
    return s

def test_step(s, ax):
    s.step()
    print("====passengers======")
    print(s.vehicles[0][1]["passengers"])
    print("==========")
    for i, v in s.vehicles:
        s.lion_nodes.loc[[v["cur_node"]]].plot(ax=ax, color='orange', zorder=5)
