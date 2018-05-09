from datetime import datetime, timedelta
import logging

import numpy as np
import pandas as pd

from .assign import assign
from .config import N_VEHICLES, T_STEP, SIM_TIME
from .inits import init_passenger
from .loader import load_road_graph, load_lion, merge_lion_road, load_skim_graph, load_demands, merge_stops_demands, load_turnstile_counts, load_road_skim_graph
from .rv_graph import init_rv_graph
from .rtv_graph import init_rtv
from .travel import init_travel
from .vehicle import init_vehicle, move_vehicles

class Sim(object):
    def load(self):
        print("====Loading Data=====")
        self.road_graph = load_road_graph()
        self.lion, self.lion_nodes = load_lion()
        self.lion_rg, self.rg_nodes = merge_lion_road(self.lion, self.lion_nodes, self.road_graph)

        self.skim_graph = load_skim_graph()
        self.g, self.vmr, self.skim = self.rgs = load_road_skim_graph()
        self.road_skim_lookup = lambda x, y: self.rgs[2][self.rgs[1][x]][self.rgs[1][y]]
        self.demands, self.stops = load_demands()
        self.joined_stops, self.demands_with_stops = merge_stops_demands(self.stops, self.rg_nodes, self.demands)
        self.turnstile_counts = load_turnstile_counts(self.joined_stops, SIM_TIME, T_STEP)
        print("====Done=====")


    def set_passengers(self, origins_at_t, dests, seconds):
        self.passengers = []
        counter = 0
        for o, n in zip(origins_at_t.index, origins_at_t):
            for i in range(n):
                self.passengers.append(init_passenger(o, dests[counter],
						      self.t,
                                                      self.joined_stops,
                                                      self.road_skim_lookup))
                counter = counter + 1

        assert len(self.passengers) == origins_at_t.sum()

        canonical_requests = self.demands_with_stops[(self.demands_with_stops["sim_time"] <= seconds) & (self.demands_with_stops["sim_time"] > (seconds - T_STEP))]

        
        if len(canonical_requests) > 0:
            print("REQUEST!")

        canonical_passengers = [init_passenger(r["mn_O_station"], 
                                               r["mn_D_station"],
                                               self.t,
                                               self.joined_stops,
                                               self.road_skim_lookup)\
                                for ix, r in canonical_requests.iterrows()]
        self.passengers += canonical_passengers
        self.passengers = set(self.passengers)

    def step(self, debug=False):
        seconds = (self.t - self.start).total_seconds()
        origins_at_t = self.origins.loc[seconds]
        dests = np.random.choice(self.destination_probs.index,
                                 size=origins_at_t.sum(),
                                 p=self.destination_probs)

        self.set_passengers(origins_at_t, dests, seconds)

        logging.debug("RV graph generating....")
        self.rr_g, self.rv_g = self.gen_rv_graph(self.t, self.passengers, self.vehicles, debug=debug)
        logging.debug("RV graph generating....done.")

        logging.debug("RTV graph generating....")
        self.Tk, self.rtv_g = self.gen_rtv_graph(self.t, self.vehicles, self.rv_g, self.rr_g)
        logging.debug("RTV graph generating....done.")
        
        self.assignment = assign(self.rtv_g, self.Tk)
        logging.debug("Assignment is {}".format(self.assignment)

        self.update_vehicles_passengers_t()

    def update_vehicles_passengers_t(self):
        g, vmr, skim = self.rgs
        to_remove = move_vehicles(self.assignment,
                      self.vehicles,
                      self.g,
                      self.vmr,
                      self.travel,
                      self.t,
                      self.joined_stops,
                      self.lion_nodes)
        self.passengers = self.passengers - to_remove
        self.t = self.t + timedelta(seconds=T_STEP)

    def init_demands(self):
        """
        """
        self.origins = np.random.poisson(self.turnstile_counts["lambda"], size=(int(SIM_TIME / T_STEP) , len(self.turnstile_counts)))
        self.origins = pd.DataFrame(self.origins, index=np.arange(0, SIM_TIME, T_STEP))
        self.origins.columns = self.turnstile_counts["stop_id"]

        self.destination_probs = self.turnstile_counts.set_index("stop_id")["dest_prob"]

        self.demands_with_stops["sim_time"] = self.demands_with_stops["TRP_DEP_HR"].apply(lambda x: x if x < 5 else 0) * 3600. + self.demands_with_stops["TRP_DEP_MIN"] * 60.

    def get_x_ys(self,  n_vehicles):
        return ((s.geometry.x, s.geometry.y, ix) for ix, s in self.rg_nodes.sample(n_vehicles).iterrows())

    def init(self):
        self.load()
        self.travel = init_travel(self.joined_stops, self.skim_graph, self.road_skim_lookup)
        self.gen_rv_graph = init_rv_graph(self.joined_stops, self.travel)
        self.gen_rtv_graph = init_rtv(self.travel)
  


        x_ys = self.get_x_ys(N_VEHICLES)
        self.vehicles = [(i, init_vehicle(x, y, node)) for i, (x, y, node) in enumerate(x_ys)]


        self.start = self.t = datetime(2018, 1, 1)
        self.init_demands()
