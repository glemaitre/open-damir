#%%
import os

import dask.dataframe as dd
import pandas as pd

#%%
path_data = 'data'
lexicon_filename = os.path.join(
    path_data, 'Lexique_open-DAMIR.xls'
)
sample_filename = os.path.join(
    path_data, '*.csv'
)

#%%
# Read a sample file to get the column
df_sample = dd.read_csv(sample_filename, sep=';', usecols=range(55))
df_sample.head()

#%%
df_lexicon = pd.read_excel(
    lexicon_filename,
)
