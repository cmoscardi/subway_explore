from datetime import datetime
from itertools import groupby
import logging
import math
import os
import multiprocessing
import psutil

import joblib
import networkx as nx
assert nx.__version__ == "2.1"
import numpy as np

from .config import N_JOBS
from .inits import Passenger
from .travel import travel

# cutoff, in seconds
CUTOFF = 10

p = None
def init_rtv():
    global p
    p = joblib.Parallel(n_jobs=-2, backend='multiprocessing', max_nbytes=None, verbose=1, batch_size=1)
    p = p.__enter__()

def gen_rtv(t, vehicles, rv_g, rr_g):
    print("Start at {}".format(datetime.now()))
    rtv_g = nx.Graph()
    Tks_for_vehicle = p([joblib.delayed(handle_vehicle)(t, i, v, rv_g, rr_g) for i, v in vehicles])
    print("Tks done at {}".format(datetime.now()))
    logging.debug("Tks_for_vehicle is %s", Tks_for_vehicle)
    for (trips, weights), (i, v) in zip(Tks_for_vehicle, vehicles):
        for T in trips:
            rtv_g.add_edge(T, i, weight=weights[T])
            for r in T:
                rtv_g.add_edge(r, T)
    return [t[0] for t in Tks_for_vehicle], rtv_g

import time
def handle_vehicle(t, i, v, rv_g, rr_g):
    start_t = time.time()
    try:
        rv_edges = rv_g.edges(i)
    except nx.NetworkXError as e:
        logging.debug("vehicle {} failed".format(i))
        return (set(), {})
    Tk =[]
    # trips of size 1
    Tk.append([])
    weights_by_trip = {}
    for (k, r) in rv_edges:
        if isinstance(r, int):
            r, k = k, r
        T = tuple([r])
        Tk[0].append(T)
        weights_by_trip[T] = rv_g[k][r]['weight']
    
    logging.debug("Size 1 done")
    # trips of size 2
    Tk.append([])
    for j, (r1, ) in enumerate(Tk[0]):
        for (r2, ) in Tk[0][j+1:]:
            if time.time() - start_t > CUTOFF:
                ret = set.union(*[set(t) for t in Tk])
                return ret, weights_by_trip
            #return r1, r2
            T = tuple(sorted((r1, r2)))
            if T not in rr_g.edges:
                continue
            a, b = travel(t, v, list(T))
            if a:
                Tk[1].append(T)
                weights_by_trip[T] = a
                
    logging.debug("Size 2 done")
    if time.time() - start_t > CUTOFF:
        ret = set.union(*[set(t) for t in Tk])
        return ret, weights_by_trip

    # trips of size N
    for k in range(3, v["capacity"] + 1):
        Tk.append([])
        for j, t1 in enumerate(Tk[k - 2]):

            if time.time() - start_t > CUTOFF:
                ret = set.union(*[set(t) for t in Tk])
                return ret, weights_by_trip
            for t2 in Tk[k-2][j + 1:]:
                logging.debug("comparing {}, {}".format(t1, t2))
                U = set(t1).union(set(t2))
                logging.debug("U is {}".format(U))
                if len(U) != k:
                    logging.debug("YIKES!")
                    continue
                if not check_subtrips(U, Tk[k-2]):
                    logging.debug("DOUBLE YIKES!")
                    continue
                canonical = tuple(sorted(U))
                if canonical in Tk[k-1]:
                    logging.debug("Already found")
                    continue
                a, b = travel(t, v, list(canonical))
                if not a:
                    continue
                Tk[-1].append(canonical)
                weights_by_trip[canonical] = a
        logging.debug("Size %s done", k)
    ret = set.union(*[set(t) for t in Tk])
    return ret, weights_by_trip

def check_subtrips(U, tk):
    tk = set(tk)
    logging.debug("Tk is %s", tk)
    #print tk
    for trip in U:
        left_out = tuple(sorted((U - set([trip]))))
        logging.debug("Left out is %s", left_out)
        if left_out not in tk:
            return False
    return True


def draw_rtvg(rtv_g):

    pos = nx.spring_layout(rtv_g)
    def pos_sort(key):
        if isinstance(key, int):
            return -1
        if isinstance(key, tuple) and not isinstance(key, Passenger):
            return len(key)
        return 999999999

    for i, (typ, g) in enumerate(groupby(sorted(pos.keys(), key=pos_sort, reverse=True), key=type)):
        g = list(g)
        for j, key in enumerate(g):
            x = .3333 + (i / 3.)
            y = ((j + 1.) / float(len(g)))
            pos[key] = np.array([x, y])
        
    nx.draw(rtv_g, pos=pos, with_labels=True)
