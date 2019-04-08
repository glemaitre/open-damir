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
    path_data, 'A2015*.csv'
)

#%%
# Read a sample file to get the column
df_sample = dd.read_csv(sample_filename, sep=';', usecols=range(55))
df_sample.head()

#%%
COL_TO_IGNORE = (
    'FLX_ANN_MOI',
    'PRS_ACT_COG', 'PRS_ACT_NBR', 'PRS_ACT_QTE',
    'PRS_DEP_MNT', 'PRS_PAI_MNT',
    'PRS_REM_BSE', 'PRS_REM_MNT', 'PRS_REM_TAU',
    'FLT_ACT_COG', 'FLT_ACT_NBR', 'FLT_ACT_QTE',
    'FLT_PAI_MNT',
    'FLT_DEP_MNT',
    'FLT_REM_MNT',
    'SOI_ANN', 'SOI_MOI'
)

lexicon = {}
for col_name in df_sample.columns:
    if col_name in COL_TO_IGNORE:
        continue
    lexicon[col_name] = pd.read_excel(
        lexicon_filename, sheet_name=col_name, index_col=0
    )
#%%
