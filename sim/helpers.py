
class memodict(dict):
    def __init__(self, f):
        self.f = f
    def __call__(self, *args):
        return self[args]
    def __missing__(self, key):
        ret = self[key] = self.f(*key)
        return ret
def memoize(f):
    """ Memoization decorator for functions taking one or more arguments. """
    return memodict(f)

def uniform_str(x):
    strd = str(x)
    while len(strd) < 7:
        strd = '0' + strd
    return strd
