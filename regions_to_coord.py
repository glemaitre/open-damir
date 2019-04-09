"""
Convert regions and ZEAT to GPS coords
"""
import numpy as np
import pandas as pd

import geopandas as gpd


#####################################################################
# Convert regions to GPS coords
dept = pd.read_csv('data/departments.csv')

gdf_department = gpd.read_file('data/departements.geojson')
df = gdf_department.merge(dept, how='right', left_on='code',
                          right_on='code')


def extract_coords(geometry):
    if hasattr(geometry, 'centroid'):
        point = geometry.centroid
        return point.x, point.y
    else:
        # DOM/TOM are not present in the correspondance
        return -100, -100


coords = np.array(list(
            df.geometry.apply(extract_coords))).T

df['centers_x'] = coords[0]
df['centers_y'] = coords[1]
regions_center = df.groupby('region_code')[('centers_x', 'centers_y')].mean()
regions_center.to_csv('regions_to_gps.csv')

#####################################################################
# Convert ZEAT code to GPS coords
zeat = pd.read_csv('data/ZEAT_regions_departements.csv',
                   names=['zeat_code', 'zeat_name', 'zeat_libelle',
                          'region', 'dept'])

df = gdf_department.merge(zeat, how='right', left_on='nom',
                          right_on='dept')
coords = np.array(list(
            df.geometry.apply(extract_coords))).T

df['centers_x'] = coords[0]
df['centers_y'] = coords[1]
zeat_center = df.groupby('zeat_libelle')[('zeat_code', 'centers_x', 'centers_y')].mean()
zeat_center = zeat_center.reset_index()
zeat_center['zeat_libelle'] = zeat_center['zeat_libelle'].astype(np.int64)
zeat_center['zeat_code'] = zeat_center['zeat_code'].astype(np.int64)
zeat_center.to_csv('zeat_to_gps.csv', index=False)

