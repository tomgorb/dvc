import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

import yaml
import json
import pandas as pd

from sklearn.linear_model import LinearRegression

def train(**params):
    
    data = pd.read_csv('data/intermediate/ts_pp.csv', sep='|', header=0)
    X = data['X'].values.reshape(-1, 1)
    y = data['y'].values.reshape(-1, 1)
    reg = LinearRegression().fit(X, y)
    score = reg.score(X,y)
    with open('artifacts/scores.json', 'w') as outfile:
        json.dump({"score": score}, outfile)
    data = [{"X": float(i), "y": float(j)} for i,j in zip(X,y)]
    with open('artifacts/data.json', 'w') as outfile:
        json.dump({"data": data}, outfile)

if __name__ == "__main__":

    with open("params.yaml", 'r') as fd:
        params = yaml.safe_load(fd)

    train(**params)
