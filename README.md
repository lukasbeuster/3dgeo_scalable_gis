# Workshop on Scalable GIS - 3D Geoinformation Use Case
Based on the insightful paper and code by Fleischmann et al. -> https://github.com/martinfleis/numerical-taxonomy-paper

# Local installation

Prerequisites: Python 3.8  or higher

If you want to try running the code on your own machine, you need to first create a virtual environment and install the requirements. 

First make sure you've cloned the repository to your local machine.

```bash
git clone https://github.com/lukasbeuster/3dgeo_scalable_gis.git
```


Change directories to where you stored the repository (if not already there). Then make the virtual environment & install dependencies. 

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

From you home directory, run the following:
```
git clone http://github.com/RS-DAT/JupyterDaskOnSLURM.git
cd JupyterDaskOnSLURM
python3 -m venv .venv
source .venv/bin/activate
pip install fabric decorator
```


Then either run :

```
python runJupyterDaskOnSLURM.py --add_platform
```
or just substitute the  info on  config/platforms/platforms.ini
with this (make sure that the keypath is actually the path to you ssh key):

[spider]
platform = spider
host = spider.surf.nl
user = stursdat-30
keypath = /Users/YOURUSER/.ssh/rsa_stursdat_30


Then:

```
python runJupyterDaskOnSLURM.py --uid spider --mode run
```

And for the password: scalablegis
 
