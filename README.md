# 3dgeo_scalable_gis


# Local installation

Prerequisites: Python 3.8  or higher

If you want to try running the code on your own machine, you need to first create a virtual environment and install the requirements. 

First make sure you've cloned the repository to your local machine.

```bash
git clone https://github.com/lukasbeuster/3dgeo_scalable_gis.git
```


Change directories to where you stored the repository (if not already there). Then make the virtual environment & install dependencies. 

You can change the name of the repository by replacing 'taxonomy' with your own name.

```bash
python3 -m venv .taxonomy
source .taxonomy/bin/activate
pip install -r requirements.txt
```
Then you can fire up jupyterlab and the notebooks by running:

```bash
export SCALABLE_GIS_DATA_PATH='../data/raw_data/'
jupyter lab
```

# Working on the server. 

First you need to connect to eh Spider server, based on the instructions that you can find [here]().

Then you need to clone this repo in the server by :

```bash
git clone https://github.com/lukasbeuster/3dgeo_scalable_gis.git
```

then activate the environment:

```
/project/stursdat/Software/mambaforge/bin/conda init
mamba init
source ~/.bashrc
conda activate jupyter_dask
```

Then run the test file:

```bash
cd 3dgeo_scalable_gis
export SCALABLE_GIS_DATA_PATH='/project/stursdat/Data/ScalableGIS/Part1/'
python3 test.py
```
 
