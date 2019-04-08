#%%
import os

import dask.dataframe as dd
import pandas as pd

#%%
path_data = 'data'
lexicon_filename = os.path.join(
    path_data, 'Lexique_open-DAMIR.xls'
)

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
sample_filename = os.path.join(
    path_data, '2015*.csv'
)
df_sample = dd.read_csv(sample_filename, sep=';', usecols=range(55))
df_sample = df_sample[df_sample['PRS_NAT'].isin(code.index)]

#%%
