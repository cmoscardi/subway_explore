# Notebook order

## Data Prep
1. `routes.ipynb` -- prepares GTFS subway graph, gets O/D pairs for manhattan.
2. `lion.ipynb` -- prepares NYC road graph.
3. `taxi_clean.ipynb` -- prepares taxi trips.
4. `taxi_algorithm.ipynb` -- computes speeds on road network based on taxi data.


## graphtool
https://medium.com/@ronie/installing-graph-tool-for-python-3-on-anaconda-3f76d9004979


Run *each line*  manually. The dependency solver breaks if you don't do exactly this, line-by-line. I kid you not.

```
conda config --add channels conda-forge
conda config --add channels ostrokach-forge
conda config --add channels pkgw-forge
```

```
conda create --name graphtool python=3.6
conda install graph-tool=2.26
conda install ipython
conda install gdal # MAKE SURE THIS COMES FROM CONDA-FORGE
conda install geopandas # the dev version works and is better: conda install --channel conda-forge/label/dev geopandas
conda install gtk3 pygobject
conda install networkx
```

You can turn the `conda install`s into a script by adding a `-y` flag, but you do this at your own risk.
