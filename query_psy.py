#%%
import os
import shutil

import dask.dataframe as dd
import pandas as pd
import numpy as np

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
            df_act_cat['Libellé Nature de Prestation'].str.contains(key)
        ]
    )
code = pd.concat(code, axis=0)

#%%
# Early years
sample_filename = os.path.join(
    path_data, 'P200911.csv'
    #path_data, 'P*.csv'
)
df_sample = dd.read_csv(sample_filename, sep=';', usecols=range(55),
                # We need to coerce dtypes, because sometimes there are
                # missing values
                dtype={'PSP_ACT_SNDS': 'float64'})
df_sample = df_sample[df_sample['PRS_NAT'].isin(code.index)]


#%%
zeat_to_gps = pd.read_csv('zeat_to_gps.csv')


def df_zeat_to_gps(df, key='ORG_CLE_REG'):
    df = df.merge(zeat_to_gps, left_on=key, right_on='zeat_code', how='left')
    df = df.drop([key, 'zeat_libelle', 'zeat_code'], axis=1)
    df.centers_x = df.centers_x.fillna(-100)
    df.centers_y = df.centers_y.fillna(-100)
    key = key[:-4]
    df = df.rename(columns=dict(centers_x='%s_x' % key, centers_y='%s_y' % key))
    return df


for col in df_sample.columns:
    if col.endswith('_ZEAT'):
        df_sample = df_zeat_to_gps(df_sample, key=col)


#%%
out_filename = os.path.join(path_data, 'clean_P.parquet')
shutil.rmtree(out_filename)
df_sample.to_parquet(out_filename)

print('Done with the P files')

##%%
## Later years
#sample_filename = os.path.join(
#    path_data, 'A201709.csv'
#    path_data, 'A*.csv'
#)
#df_sample = dd.read_csv(sample_filename, sep=';', usecols=range(55),
#                # We need to coerce dtypes, because sometimes there are
#                # missing values
#                dtype={'PSP_ACT_SNDS': 'float64'}
#            )
#df_sample = df_sample[df_sample['PRS_NAT'].isin(code.index)]
#
#reg_to_gps = pd.read_csv('regions_to_gps.csv')
#reg_to_gps = reg_to_gps[reg_to_gps.region_code != 'COM']
#reg_to_gps.region_code = reg_to_gps.region_code.astype(np.int64)
#
#
#def df_reg_to_gps(df, key='ORG_CLE_REG'):
#    df = df.merge(reg_to_gps, left_on=key, right_on='region_code', how='left')
#    df = df.drop([key, 'region_code'], axis=1)
#    df.centers_x = df.centers_x.fillna(-100)
#    df.centers_y = df.centers_y.fillna(-100)
#    key = key[:-4]
#    df = df.rename(columns=dict(centers_x='%s_x' % key, centers_y='%s_y' % key))
#    return df
#
#
#for col in df_sample.columns:
#    if col.endswith('_REG'):
#        df_sample = df_reg_to_gps(df_sample, key=col)
#
#
##%%
#df_sample.to_parquet(os.path.join(path_data, 'clean_A.parquet'))

