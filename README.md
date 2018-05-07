# Notebook order

## Data Prep
1. `routes.ipynb` -- prepares GTFS subway graph, gets O/D pairs for manhattan.
2. `lion.ipynb` -- prepares NYC road graph.
3. `taxi_clean.ipynb` -- prepares taxi trips.
4. `taxi_algorithm.ipynb` -- computes speeds on road network based on taxi data.


## graphtool
https://medium.com/@ronie/installing-graph-tool-for-python-3-on-anaconda-3f76d9004979


Run these manually (don't ask, I don't know)
```
conda create --name graphtool python=3.6.2=0
conda install gtk3 pygobject matplotlib graph-tool
conda install ipython
```
