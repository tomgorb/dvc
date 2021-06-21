import os
import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

import yaml
import dvc.api
import pandas as pd
from io import StringIO

def read_data(rev, remote, sa):

    # USING DVC API OPEN
    with dvc.api.open(
        "data/raw/ts.csv",
        repo=os.path.abspath("."),
        remote=remote,
        rev=rev,
        mode='rb'
        ) as fd:

        df0 = pd.read_csv(fd, sep='|', header=0)
    logging.info('USING DVC API OPEN: %s', df0.shape)

    # USING DVC API READ
    data = dvc.api.read(
        "data/raw/ts.csv",
        repo=os.path.abspath("."),
        remote=remote,
        rev=rev,
        mode='rb'
        )
    df1 = pd.read_csv(StringIO(str(data,'utf-8')), sep='|', header=0)
    logging.info('USING DVC API READ: %s', df1.shape)

    # USING DVC API GET URL
    data_url = dvc.api.get_url(path="data/raw/ts.csv", repo=os.path.abspath("."), remote=remote, rev=rev) 
    df2 = pd.read_csv(data_url, sep='|', header=0, storage_options={"token": sa})
    logging.info('USING DVC API GET URL: %s', df2.shape)


if __name__ == '__main__':

    with open("params.yaml", 'r') as fd:
        params = yaml.safe_load(fd)

    read_data(params['rev'], params['remote'], '/tmp/sa.json')
