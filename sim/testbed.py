
from sim import main
from .vehicle import init_vehicle
from .inits import init_passenger

STOP_IDS = "107", "108", "109"

def dummy(*args, **kwargs):
    pass



def new_passenger(s):
    O = "107"
    D = "109"
    return init_passenger(O, D, s.t, s.joined_stops, s.road_skim_lookup)

def new_vehicle(s):
    V_LOC = "108"
    stop_place = s.joined_stops.loc[V_LOC]
    return init_vehicle(stop_place.geometry.x, stop_place.geometry.y,
                 stop_place["index_right"])




def test():
    s = main.Sim()
    s.set_passengers = dummy

    s.init()
    s.passengers = set([new_passenger(s)])
    s.vehicles = [(0, new_vehicle(s))]
    s.step()
    return s
