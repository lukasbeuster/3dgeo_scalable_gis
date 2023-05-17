# 3dgeo_scalable_gis


# Local installation

Prerequisites: Python 3.8  or higher

If you want to try running the code on your own machine, you need to first create a virtual environment and install the requirements. 

First install MPICH, on a Mac:

```
brew install mpich 
```
or on Linux:
```
sudo apt install python3-mpi4py
```


Then make the virtual environment & install dependencies

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
Then you can fire up the notebooks by running:

```
jupyter notebook
```

# Working on the server. 

First you need to connect to eh Spider server, based on the instructions that you can find [here]().

The you need to clone this repo in the server by :

```bash
git clone https://github.com/lukasbeuster/3dgeo_scalable_gis.git
```

then:


```
cd 3dgeo_scalable_gis
python3 test.py
```
 
