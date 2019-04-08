import os

import dask.dataframe as dd
import pandas as pd

path_data = 'data'

lexicon_filename = os.path.join(
    path_data, 'Lexique_open-DAMIR.xls'
)

sample_filename = os.path.join(
    path_data, 'A201501.csv'
)


df_month = dd.read_csv(sample_filename, sep=';')

