import os
import pandas as pd
import numpy as np

wd = '/disk/bulkw/karger/census_bulk/citylonglat/'
os.chdir(wd)

gmaps_df = pd.read_csv('data/geo_data/gmaps_offline.csv')
gmaps_df = gmaps_df.rename(columns={'lat': 'lat_gmaps', 'long': 'long_gmaps'})
gmaps_df['match_gmaps'] = 'gmaps'
for decade in np.arange(1790, 1950, 10):
    if decade == 1890:
        continue

    geo_df = pd.read_csv('intermediate/census_gnis_coords_' + str(decade) + '.csv')

    geo_df = geo_df.merge(gmaps_df[gmaps_df['decade'] == decade], how='left', on='indexcollapse')
    geo_df['lat'] = geo_df['lat'].fillna(geo_df['lat_gmaps'])
    geo_df['long'] = geo_df['long'].fillna(geo_df['long_gmaps'])
    geo_df['match_type'] = geo_df['match_type'].fillna(geo_df['match_gmaps'])

    geo_df = geo_df.drop(columns=['lat_gmaps', 'long_gmaps', 'match_gmaps'])

    geo_df = geo_df.to_csv('intermediate/census_gnis_gmaps_coords_' + str(decade) + '.csv', index=False)
