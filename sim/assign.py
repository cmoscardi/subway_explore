import numpy as np

def greedy_assign(rtvg, T):
    R_ok = set()
    V_ok = set()
    assignment = set()
    for k, Tk in zip(range(len(T), 0, -1), T[::-1]):
        if not Tk:
            continue
        Sk = sorted((e for e in rtvg.edges(Tk) if isinstance(e[1], int) or isinstance(e[0], int)),
                    key=lambda e: rtvg.edges[e]["weight"])
        for trip, v in Sk:
            if np.any([t in R_ok for t in trip]):
                continue
            if v in V_ok:
                continue
            [R_ok.add(t) for t in trip]
            assignment.add((trip, v))
            V_ok.add(v)
    return assignment


assign = greedy_assign
