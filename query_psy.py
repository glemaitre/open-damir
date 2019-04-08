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
    path_data, 'A201501.csv'
)

#%%
# Read a sample file to get the column
df_sample = dd.read_csv(sample_filename, sep=';', usecols=range(55))
df_sample.head()

#%%
df_act_cat = pd.read_excel(
    lexicon_filename, sheet_name='PRS_NAT', index_col=0
)

#%%
KEYWORDS = ('PSYCH', 'NEURO')

code = []
for key in KEYWORDS:
    code.append(
        df_act_cat['Libellé Nature de Prestation'][
            df_act_cat['Libellé Nature de Prestation'].str.contains('PSYCH')
        ]
    )
code = pd.concat(code, axis=0)

#%%
