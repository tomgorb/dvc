import os
import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

import yaml
import dvc.api
import pandas as pd

def preprocess(rev, remote):

    with dvc.api.open(
        "data/raw/ts.csv",
        repo=os.path.abspath("."),
        remote=remote,
        rev=rev,
        mode='rb'
        ) as fd:

        df = pd.read_csv(fd, sep='|', header=0)

        df_pp = df[(df.identifier==43)][['product_pages','amount']].dropna()
        df_pp.rename(columns={"product_pages": "X", "amount": "y"}, inplace=True)
        output = 'data/intermediate/ts_pp.csv'
        df_pp.to_csv(output, header=True, index=False, sep='|')


if __name__ == '__main__':


    with open("params.yaml", 'r') as fd:
        params = yaml.safe_load(fd)

    preprocess(params['rev'], params['remote'])