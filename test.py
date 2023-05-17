import warnings

## suppress warning 
warnings.filterwarnings("ignore", category=FutureWarning)
# Data cleaning
import operator

# The standard library to handle geospatial data. 
import geopandas as gpd
import libpysal
import mapclassify
import matplotlib.pyplot as plt
# THE library for morphometric analysis.
import momepy as mm
# 
import numpy as np
# Handling of data, w/o the spatial component.
import pandas as pd
import scipy as sp
# Mapping
import seaborn as sns
# Parallel
from dask.distributed import Client
from scipy.cluster import hierarchy
from sklearn import preprocessing
from sklearn.mixture import GaussianMixture
import owslib.wfs


path_to_data = '/project/stursdat/Data/ScalableGIS/Part1/'

ams = gpd.read_file(path_to_data + 'AMS_Buurten_lnglat.json').to_crs(28992)
# ams = ams.loc[(ams['Stadsdeelcode']  == 'A')] # Amsterdam center
ams = ams.loc[(ams['Buurtcode']  == 'AJ03')]  # you can also try other codes eg AJ01

print(len(ams))

print("Data loaded successfully!")