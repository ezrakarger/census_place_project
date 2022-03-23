import os
import pandas as pd
import geopandas as gpd
import numpy as np
import sys

wd = '/disk/bulkw/karger/census_bulk/citylonglat/'
os.chdir(wd)
sys.path.append(wd + 'programs/02_geocode')
import matching_functions as mf

# define dictionary with townvariables by decade here:
townvars_dict = {1790: ['township'],
                 1800: ['township'],
                 1810: ['township'],
                 1820: ['township'],
                 1830: ['general_township_orig'],
                 1840: ['locality'],
                 1850: ['stdcity', 'us1850c_0043', 'us1850c_0053',
                        'us1850c_0054', 'us1850c_0042'],
                 1860: ['us1860c_0040', 'us1860c_0042', 'us1860c_0036'],
                 1870: ['us1870c_0040', 'us1870c_0042', 'us1870c_0043',
                        'us1870c_0044', 'us1870c_0035', 'us1870c_0036'],
                 1880: ['mcdstr', 'us1880e_0071', 'us1880e_0069',
                        'us1880e_0072', 'us1880e_0070'],
                 1900: ['stdcity', 'us1900m_0045', 'us1900m_0052'],
                 1910: ['stdcity', 'us1910m_0052', 'us1910m_0053',
                        'us1910m_0063'],
                 1920: ['stdmcd', 'stdcity', 'us1920c_0057',
                        'us1920c_0058', 'us1920c_0068', 'us1920c_0069'],
                 1930: ['stdmcd', 'stdcity'],
                 1940: ['stdcity', 'us1940b_0073', 'us1940b_0074']}

# define dictionary with county and state types here:
county_state_types_dict = {1790: ('name', 'name'),
                           1800: ('name', 'name'),
                           1810: ('name', 'name'),
                           1820: ('name', 'name'),
                           1830: ('name', 'name'),
                           1840: ('name', 'name'),
                           1850: ('fips', 'icp'),
                           1860: ('fips', 'fips'),
                           1870: ('fips', 'fips'),
                           1880: ('fips', 'icp'),
                           1900: ('fips', 'fips'),
                           1910: ('fips', 'fips'),
                           1920: ('fips', 'fips'),
                           1930: ('fips', 'fips'),
                           1940: ('fips', 'fips')}

already_done = {}
first_round = True

while True:
    # enumeration district step
    for decade in np.arange(1850, 1950, 10):
        if decade == 1890:
            continue

        if decade == 1850 or decade >= 1880:
            if first_round:
                census_cities = pd.read_csv(
                    'intermediate/census_gnis_coords_gmaps_pre1910_' + str(decade) + '.csv')
                if 'index' in census_cities.columns:
                    census_cities = census_cities.drop(columns='index')
            else:
                census_cities = pd.read_csv('intermediate/census_gnis_coords_' + str(decade) + '_temp.csv')
                if 'index' in census_cities.columns:
                    census_cities = census_cities.drop(columns='index')
            census_cities.columns = [column.lower() for column in census_cities.columns]

            town_variables = townvars_dict[decade]

            census_cities['enumdist'] = census_cities['enumdist'].astype(int)
            census_cities = mf.assign_cities_by_enumdist(census_cities, town_variables)

            census_cities.to_csv('intermediate/census_gnis_coords_' + str(decade) + '_temp.csv', index=False)

    first_round = False

    # fill in the gaps
    tagged_cities_dict = {}
    # step 1: build the dictionary
    for decade in np.arange(1790, 1950, 10):
        if decade == 1890:
            continue

        if os.path.exists('intermediate/census_gnis_coords_' + str(decade) + '_temp.csv'):
            census_cities = pd.read_csv('intermediate/census_gnis_coords_' + str(decade) + '_temp.csv')
            if 'index' in census_cities.columns:
                census_cities = census_cities.drop(columns='index')
        else:
            census_cities = pd.read_csv('intermediate/census_gnis_coords_gmaps_pre1910_' + str(decade) + '.csv')
            if 'index' in census_cities.columns:
                census_cities = census_cities.drop(columns='index')
        town_variables = townvars_dict[decade]

        for townvar in town_variables:
            temp_df = census_cities[(census_cities[townvar].notnull()) &
                                    (census_cities['state_abb'].notnull()) &
                                    (census_cities['lat'].notnull())]
            tagged_cities_dict.update(dict(zip(list(temp_df[townvar] + ',' + temp_df['state_abb']),
                                               list(temp_df['lat'].astype(str) + ',' + temp_df['long'].astype(str)))))

    # step 2: assign lat, longs across decades
    nchanges = 0
    for decade in np.arange(1790, 1950, 10):
        if decade == 1890:
            continue

        if os.path.exists('intermediate/census_gnis_coords_' + str(decade) + '_temp.csv'):
            census_cities = pd.read_csv('intermediate/census_gnis_coords_' + str(decade) + '_temp.csv')
            if 'index' in census_cities.columns:
                census_cities = census_cities.drop(columns='index')
        else:
            census_cities = pd.read_csv('intermediate/census_gnis_coords_gmaps_pre1910_' + str(decade) + '.csv')
            if 'index' in census_cities.columns:
                census_cities = census_cities.drop(columns='index')
        town_variables = townvars_dict[decade]

        # determine city-variable order
        share_unique = []
        for townvar in town_variables:
            share_unique.append(
                len(census_cities.loc[(census_cities[townvar].notnull()) & (census_cities[townvar] != '')]
                    .drop_duplicates(['state', 'county', townvar], keep='first')) /
                len(census_cities.loc[(census_cities[townvar].notnull()) & (census_cities[townvar] != ''), townvar]))

        county_type, state_type = county_state_types_dict[decade]
        county_gdf = mf.load_county_shape('shape_files/US_county_' + str(decade) + '_conflated.shp', county_type)

        if county_type == 'fips':
            county_gdf['NHGISST'] = county_gdf['NHGISST'].astype(int) / 10
            county_gdf['ICPSRFIP'] = county_gdf['NHGISST'].astype(int).astype(str) + \
                                     county_gdf['ICPSRCTYI'].astype(int).astype(str).str.zfill(4)
            county_gdf['ICPSRFIP'] = county_gdf['ICPSRFIP'].astype(int)

        for townvar in list(np.array(town_variables)[np.argsort(share_unique)[::-1]]):
            for i in range(len(census_cities)):
                row = census_cities.iloc[i]
                if pd.notnull(row['lat']) or pd.isnull(row[townvar]) or \
                        pd.isnull(row['state_abb']):
                    continue
                elif row[townvar] + ',' + row['state_abb'] in tagged_cities_dict:
                    lat, long = tagged_cities_dict[row[townvar] + ',' + row['state_abb']].split(',')
                    lat = float(lat)
                    long = float(long)

                    # check that the city we got from the dictionary falls within the county
                    if county_type == 'name':
                        county = county_gdf[(county_gdf['STATENAM'] == row['state']) &
                                            (county_gdf['ICPSRNAM'] == row['county'])]
                    else:
                        county = county_gdf[county_gdf['ICPSRFIP'] == row['county']]

                    if len(county) == 0:
                        continue

                    city_point = pd.DataFrame([[lat, long]], columns=['lat', 'long'])
                    city_point['city_geometry'] = gpd.points_from_xy(city_point['long'], city_point['lat'],
                                                                     crs='epsg:4326')
                    if city_point.loc[0, 'city_geometry'].within(county.reset_index().loc[0, 'geometry']):
                        nchanges += 1
                        census_cities.iloc[i, census_cities.columns.get_loc('lat')] = lat
                        census_cities.iloc[i, census_cities.columns.get_loc('long')] = long
                        census_cities.iloc[i, census_cities.columns.get_loc('match_type')] = 'across_decades'

        census_cities.to_csv('intermediate/census_gnis_coords_' + str(decade) + '_temp.csv', index=False)

    print(nchanges)
    if nchanges == 0:
        break

for decade in np.arange(1790, 1950, 10):
    if decade == 1890:
        continue

    if os.path.exists('intermediate/census_gnis_coords_' + str(decade) + '_temp.csv'):
        census_cities = pd.read_csv('intermediate/census_gnis_coords_' + str(decade) + '_temp.csv')
    else:
        census_cities = pd.read_csv('intermediate/census_gnis_coords_gmaps_pre1910_' + str(decade) + '.csv')
    census_cities.to_csv('final/census_gnis_coords_' + str(decade) + '_final.csv', index=False)
