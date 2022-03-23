import re
import math
from sklearn.neighbors import DistanceMetric
from fuzzywuzzy import process
import pandas as pd
import numpy as np
import Levenshtein as lv
import geopandas as gpd
import urllib.request
import time


def clean_city_name(city):
    if pd.isnull(city):
        return ''

    city = city.lower()
    if 'not stated' == city or ' county' in city:
        return ''

    city = city.replace('mt.', 'mount').replace('mt ', 'mount ').replace('st.', 'saint')\
               .replace('indian reservation', 'reservation').strip()
    city = re.sub('^st ', 'saint ', city)
    
    # remove stuff in parentheses
    city = re.sub('\(.*?\)', '', city).strip()
    # substitute all the numbers, commas, ?, periods, (), / with an empty string
    city = re.sub('[0-9,\?\.\(\)/]', '', city).strip()

    # range or ranges with a space before and after (or space before and end of the string)
    city = re.sub('\srange(s)?(\s|$)', '', city)

    words_to_delete = ['police jury', 'justice ward', 'court house', 'militia district',
                       'civil district', 'justice precinct', 'election district',
                       'undetermined', 'not stated', ' village',
                       'tract', ' ward', 'assembly district', 'district', 'no\.', 'precinct', 'subdivision',
                       'beat', 'plantation', 'census designated place', 'post office',
                       'township of ', 'town of ', 'borough of ', 'city of ']
    for word in words_to_delete:
        city = re.sub(word, '', city)
        
    # discard these words also if they are at the very beginning of the string
    # followed by a space
    city = re.sub('^ward\s', '', city)
    city = re.sub('^township\s', '', city)

    # This is after the cycle above otherwise it also picks up cities with "subdivision" in them
    if 'division' in city:
        # I do this because divisions are too large geographical units
        return ''

    cardinal_points = ['east', 'west', 'south', 'north']
    for point in cardinal_points:
        # e.g., or east side, or eastern side followed by a space or at the end of the string
        city = re.sub(point + '(\sside|ern\sside)(\s|$)', '', city)

    # substitute multiple spaces with just one, also get rid of spaces at the beginning and end of the string
    city = re.sub('\s+', ' ', city).strip()

    if len(city) > 1 and city[0] == '-':
        city = city[1:]

    if len(city) <= 2 or city == 'township' or city == 'ward' or \
            city == 'townships' or city == 'wards':
        city = ''

    return city


def mydist(coords_1, coords_2):
    R = 6373.0    
    lat1 = math.radians(coords_1[0])    
    lon1 = math.radians(coords_1[1])
    
    lat2 = math.radians(coords_2[0])
    lon2 = math.radians(coords_2[1])
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    
    return distance

def drop_far_duplicates(df, dup_columns, coords_columns, dist_threshold):
    dist = DistanceMetric.get_metric('haversine')
    
    # this function is a little bit tricky to read, but it basically calculates the max distance
    # between all the duplicate entries in terms of name/state and if they are closer
    # than dist_threshold km, then it keeps the first one, otherwise it drops all of them
    df['duplicates'] = df.duplicated(subset=dup_columns, keep=False)
    duplicates_df = df[df['duplicates']].copy()

    max_dup_dist = duplicates_df.groupby(dup_columns)[coords_columns].apply(lambda x: np.max(dist.pairwise(x.to_numpy())*6373))
    max_dup_dist = max_dup_dist.reset_index().rename(columns={0: 'max_dist'})
    max_dup_dist = max_dup_dist[max_dup_dist['max_dist'] > dist_threshold]

    df = df.merge(max_dup_dist, how='left', on=dup_columns)
    df = df[df['max_dist'].isnull()].drop(columns=['duplicates', 'max_dist'])
    df = df.drop_duplicates(subset=dup_columns, keep='first')

    return df


def lev_dist1(candidate, mylist):
    possible_matches = []
    for element in mylist:
        if lv.distance(candidate, element) == 1:
            possible_matches.append(element)
            
    if len(possible_matches) == 1:
        return possible_matches[0]
    else:
        return np.nan
    

def load_gnis(state_type):
    gnis_cities = pd.read_csv('data/geo_data/NationalFile_20200901.txt', delimiter='|')
    gnis_cities = gnis_cities[(gnis_cities['PRIM_LAT_DEC'] != 0) & (gnis_cities['PRIM_LONG_DEC'] != 0)]
    gnis_cities['PRIM_LAT_DEC_RAD'] = gnis_cities['PRIM_LAT_DEC'].apply(lambda x: np.radians(x))
    gnis_cities['PRIM_LONG_DEC_RAD'] = gnis_cities['PRIM_LONG_DEC'].apply(lambda x: np.radians(x))
    gnis_cities['FEATURE_CLASS'] = gnis_cities['FEATURE_CLASS'].str.lower()
    gnis_cities['FEATURE_NAME_original'] = gnis_cities['FEATURE_NAME'].copy()
    gnis_cities['FEATURE_NAME'] = gnis_cities['FEATURE_NAME'].apply(lambda x: clean_city_name(x))
    if state_type == 'name':
        gnis_cities['STATE_ALPHA'] = gnis_cities['STATE_ALPHA'].str.lower()
        
    return gnis_cities
        
        
def load_county_shape(county_file_path, county_type):
    county_gdf = gpd.read_file(county_file_path)
    county_gdf = county_gdf.to_crs(epsg=4326)
    
    if county_type == 'name':
        county_gdf = county_gdf[county_gdf['ICPSRCTY'].notnull()]
        county_gdf['STATENAM'] = county_gdf['STATENAM'].str.lower()
        county_gdf['ICPSRNAM'] = county_gdf['ICPSRNAM'].str.lower()
        county_gdf.loc[county_gdf['STATENAM'] == 'district of columbia', 'ICPSRNAM'] = 'washington'
        county_gdf = county_gdf.dissolve(by=['STATENAM', 'ICPSRNAM']).reset_index().copy()
    elif county_type == 'fips':
        county_gdf['ICPSRFIP'] = county_gdf['ICPSRFIP'].astype(int)
        county_gdf = county_gdf[county_gdf['ICPSRFIP'] != 0]
    else:
        print('county_type needs to be either "name" or "fips"')
        stop

    county_gdf['centroid_lat'] = county_gdf.centroid.x
    county_gdf['centroid_long'] = county_gdf.centroid.y

    # fix FIPS in MD
    if county_type == 'fips':
        fips_fix_df = pd.read_csv('data/geo_data/maryland_fips_fix.csv')
        fips_fix_df['old_cty_fips'] = fips_fix_df['old_cty_fips']
        fips_fix_df['old_fips'] = fips_fix_df['STATEFIPS'].astype(int).astype(str) + fips_fix_df['old_cty_fips'].astype(int).astype(str).str.zfill(4)
        fips_fix_df['old_fips'] = fips_fix_df['old_fips'].astype(int)
        fips_fix_df['ICPSRFIP'] = fips_fix_df['STATEFIPS'].astype(int).astype(str) + fips_fix_df['new_cty_fips'].astype(int).astype(str).str.zfill(4)
        fips_fix_df['ICPSRFIP'] = fips_fix_df['ICPSRFIP'].astype(int)
        county_gdf = county_gdf.merge(fips_fix_df[['old_fips', 'ICPSRFIP']], how='left', on='ICPSRFIP')
        county_gdf['ICPSRFIP'] = county_gdf.apply(lambda x: x['ICPSRFIP'] if pd.isnull(x['old_fips']) else x['old_fips'], axis=1)
        county_gdf = county_gdf.drop(columns='old_fips')

    # create buffer around counties in case the city falls for example in water or the coordinates are slightly outside the county boundaries
    county_gdf['geometry'] = county_gdf.to_crs(epsg=3310)['geometry'].buffer(1000)
    county_gdf = county_gdf.to_crs(epsg=4326)
        
    return county_gdf


def load_census(census_file_path, town_variables, state_variable,
                county_variable, county_type, state_type, decade):

    census_cities = pd.read_stata(census_file_path)
    census_cities.columns = [column.lower() for column in census_cities.columns]

    if 'county' in census_cities.columns and county_variable != 'county':
        census_cities = census_cities.drop(columns='county')
    census_cities = census_cities.rename(columns={state_variable: 'state',
                                                  county_variable: 'county'})
    for townvar in town_variables:
        census_cities[townvar + '_original'] = census_cities[townvar].copy()
        census_cities[townvar] = census_cities[townvar].apply(lambda x: clean_city_name(x) if not isinstance(x, float) else x)
    
    if state_type == 'name':
        census_cities['state'] = census_cities['state'].str.lower()
        census_cities['state'] = census_cities['state'].apply(lambda x: x.replace(' territory', '') if pd.notnull(x) else x)
    
    if county_type == 'name':
        census_cities['county'] = census_cities['county'].str.lower()
    else:
        if decade == 1860 or decade == 1870 or decade == 1940:
            census_cities['county'] = census_cities.apply(lambda x: str(x['state']) + str(x['county']).zfill(4), axis=1)
        census_cities['county'] = census_cities['county'].astype(int)
        census_cities['county'] = census_cities['county']
    
    census_cities[['potential_match', 'score', 'match_type', 'lat', 'long']] = (np.nan, np.nan, np.nan, np.nan, np.nan)
    
    if state_type == 'name':
        states_df = pd.read_csv('data/geo_data/state_fips.csv', encoding='latin1')
        states_df = states_df[states_df['state_abb'].notnull()]
        states_df['state_name'] = states_df['state_name'].str.lower()
        states_df['state_abb'] = states_df['state_abb'].str.lower()
    
        census_cities = census_cities.merge(states_df, how='left', left_on='state', right_on='state_name', validate='m:1')
    elif state_type == 'icp':
        icp_to_fips_df = pd.read_csv('data/geo_data/stateicp_to_statefips.csv', usecols=['stateicp', 'statefips'])
        icp_to_fips_df = icp_to_fips_df.rename(columns={'stateicp': 'state'})
        icp_to_fips_df = icp_to_fips_df.drop_duplicates(['state', 'statefips'])
        
        census_cities = census_cities.merge(icp_to_fips_df, how='left', on='state', validate='m:1')
        census_cities = census_cities.rename(columns={'state': 'stateicp'})
        census_cities = census_cities.rename(columns={'statefips': 'state'})
        
        fips_to_abb = pd.read_csv('data/geo_data/state_name_fips_abbr.csv', usecols=['state_fips', 'state_abbr'])
        fips_to_abb['state_abbr'] = fips_to_abb['state_abbr'].str.lower()
        fips_to_abb = fips_to_abb.rename(columns={'state_abbr': 'state_abb'})
        
        census_cities = census_cities.merge(fips_to_abb, how='left', left_on='state', right_on='state_fips', validate='m:1')
    else:
        fips_to_abb = pd.read_csv('data/geo_data/state_name_fips_abbr.csv', usecols=['state_fips', 'state_abbr'])
        fips_to_abb['state_abbr'] = fips_to_abb['state_abbr'].str.lower()
        fips_to_abb = fips_to_abb.rename(columns={'state_abbr': 'state_abb'})
        
        census_cities = census_cities.merge(fips_to_abb, how='left', left_on='state', right_on='state_fips', validate='m:1')
        
    return census_cities


def load_place_points(place_file_path):
    place_gdf = gpd.read_file(place_file_path)
    place_gdf = place_gdf.to_crs('epsg:4326')
    place_gdf['PLACE_original'] = place_gdf['PLACE'].copy()
    place_gdf['PLACE'] = place_gdf['PLACE'].apply(lambda x: ' '.join(x.split(' ')[:-1]) if x.split(' ')[-1][0].islower() or x.split(' ')[-1] == 'CDP' else x)
    place_gdf['PLACE'] = place_gdf['PLACE'].apply(lambda x: clean_city_name(x))
    place_gdf['NHGISST'] = place_gdf['NHGISST'].astype('int')
    place_gdf['lat_place'] = place_gdf['geometry'].y
    place_gdf['long_place'] = place_gdf['geometry'].x
    # drop duplicates and think how to put them back in (only 72 observations)
    place_gdf = place_gdf.drop_duplicates(['PLACE', 'NHGISST'], keep=False)
    
    return place_gdf


def falls_within_county(census_cities_loop, county_gdf, county_type, place_match):
    if county_type == 'name':
        if county_gdf['DECADE'].iloc[0] == '1790' or county_gdf['DECADE'].iloc[0] == '1800' or county_gdf['DECADE'].iloc[0] == '1810':
            census_cities_loop = census_cities_loop.copy()
            census_cities_loop.loc[census_cities_loop['state'] == 'maine', 'state'] = 'massachusetts'

        census_cities_loop = census_cities_loop.reset_index() \
                                               .merge(county_gdf, how='left', left_on=['state', 'county'],
                                                      right_on=['STATENAM', 'ICPSRNAM'], validate='m:1') \
                                               .set_index('index')
    else:
        census_cities_loop = census_cities_loop.reset_index() \
                                                       .merge(county_gdf, how='left', left_on='county',
                                                              right_on='ICPSRFIP', validate='m:1') \
                                                       .set_index('index')
    if place_match:                                               
        census_cities_loop['city_geometry'] = gpd.points_from_xy(census_cities_loop['long_place'], census_cities_loop['lat_place'], crs='epsg:4326')
    else:
        census_cities_loop['city_geometry'] = gpd.points_from_xy(census_cities_loop['PRIM_LONG_DEC'], census_cities_loop['PRIM_LAT_DEC'], crs='epsg:4326')
        
        
    census_cities_loop['within_county'] = \
        census_cities_loop.apply(lambda x: (pd.notnull(x['city_geometry'])
                                            and pd.notnull(x['geometry'])
                                            and x['city_geometry'].within(x['geometry'])), axis=1)
    
    return census_cities_loop
    

def assign_places_to_census(census_cities, place_gdf, county_gdf, townvar, county_type, nround):
    census_cities_loop = census_cities[(census_cities['potential_match'].isnull()) & 
                                       (census_cities[townvar] != '') &
                                       (census_cities[townvar].notnull())].copy()
    
    # if empty
    if len(census_cities_loop) == 0:
        return census_cities
    
    if nround == 1:
        census_cities_loop['potential_match'] = census_cities_loop.apply(lambda x: lev_dist1(x[townvar],
                                                                                             list(place_gdf.loc[place_gdf['NHGISST'] == x['state'], 'PLACE'])),
                                                                         axis=1)
        census_cities_loop = census_cities_loop[census_cities_loop['potential_match'].notnull()]
        
        # exact match
        census_cities_loop['score'] = 100
    else:
        census_cities_loop['potential_match_score'] = \
            census_cities_loop.apply(lambda x:
                                     process.extractOne(x[townvar],
                                                        list(place_gdf.loc[place_gdf['NHGISST'] == x['state'], 'PLACE']))
                                     if len(list(place_gdf.loc[place_gdf['NHGISST'] == x['state'], 'PLACE'])) > 0 else ('', 0),
                                     axis=1)
        census_cities_loop[['potential_match', 'score']] = pd.DataFrame(census_cities_loop['potential_match_score'].tolist(), index=census_cities_loop.index)
    
    census_cities_loop = census_cities_loop.reset_index() \
                                           .merge(place_gdf[['PLACE', 'NHGISST', 'lat_place', 'long_place']],
                                                  how='left', left_on=['potential_match', 'state'],
                                                  right_on=['PLACE', 'NHGISST'], validate='m:1') \
                                           .set_index('index')
                                           
    # if empty
    if len(census_cities_loop) == 0:
        return census_cities

    # control that the city falls within the right county
    census_cities_loop = falls_within_county(census_cities_loop, county_gdf, county_type, place_match=True)

    census_cities_loop.loc[(~census_cities_loop['within_county']) | (census_cities_loop['score'] < 95),
                           ['potential_match', 'score', 'lat_place', 'long_place']] = np.nan
    
    # assign to the original data set
    census_cities.loc[(census_cities['potential_match'].isnull()) & 
                      (census_cities[townvar] != '') &
                      (census_cities[townvar].notnull()),
                      ['potential_match', 'score', 'lat', 'long']] = \
        census_cities_loop[['potential_match', 'score', 'lat_place', 'long_place']].rename(columns={'lat_place': 'lat', 'long_place': 'long'}).copy()

    census_cities.loc[(census_cities['potential_match'].notnull()) & (census_cities['match_type'].isnull()), 'match_type'] = 'place_round' + str(nround)

    return census_cities


def assign_gnis_to_census(census_cities, gnis_cities_class, county_gdf, class_considered,
                          townvar, county_type, state_type, nround):
    
    if state_type == 'name':
        state_var = 'state_abb'
        state_var_gnis = 'STATE_ALPHA'
    else:
        state_var = 'state'
        state_var_gnis = 'STATE_NUMERIC'
    
    census_cities_loop = census_cities[(census_cities['potential_match'].isnull()) & 
                                       (census_cities[townvar] != '') &
                                       (census_cities[townvar].notnull())].copy()
    
    # if empty
    if len(census_cities_loop) == 0:
        return census_cities
    
    if nround == 1:
        census_cities_loop['potential_match'] = census_cities_loop.apply(
            lambda x: lev_dist1(x[townvar],
                                list(gnis_cities_class.loc[gnis_cities_class[state_var_gnis] == x[state_var], 'FEATURE_NAME'])), axis=1
            )
        census_cities_loop = census_cities_loop[census_cities_loop['potential_match'].notnull()]
        
        # exact match
        census_cities_loop['score'] = 100
    else:
        census_cities_loop['potential_match_score'] = \
            census_cities_loop.apply(
                lambda x: process.extractOne(x[townvar], list(gnis_cities_class.loc[gnis_cities_class[state_var_gnis] == x[state_var], 'FEATURE_NAME']))
                if len(list(gnis_cities_class.loc[gnis_cities_class[state_var_gnis] == x[state_var], 'FEATURE_NAME'])) > 0 else ('', 0),
                axis=1
            )
        census_cities_loop[['potential_match', 'score']] = pd.DataFrame(census_cities_loop['potential_match_score'].tolist(), index=census_cities_loop.index)
    
    census_cities_loop = census_cities_loop.reset_index() \
                                           .merge(gnis_cities_class[['FEATURE_NAME', state_var_gnis, 'PRIM_LAT_DEC', 'PRIM_LONG_DEC']],
                                                  how='left', left_on=['potential_match', state_var],
                                                  right_on=['FEATURE_NAME', state_var_gnis], validate='m:1') \
                                           .set_index('index')
                                           
    # if empty
    if len(census_cities_loop) == 0:
        return census_cities

    # control that the city falls within the right county
    census_cities_loop = falls_within_county(census_cities_loop, county_gdf, county_type, place_match=False)

    census_cities_loop.loc[(~census_cities_loop['within_county']) | (census_cities_loop['score'] < 95),
                           ['potential_match', 'score', 'PRIM_LAT_DEC', 'PRIM_LONG_DEC']] = np.nan
        
    # assign to the original data set
    census_cities.loc[(census_cities['potential_match'].isnull()) & 
                      (census_cities[townvar] != '') &
                      (census_cities[townvar].notnull()),
                      ['potential_match', 'score', 'lat', 'long']] = \
        census_cities_loop[['potential_match', 'score', 'PRIM_LAT_DEC', 'PRIM_LONG_DEC']].rename(columns={'PRIM_LAT_DEC':'lat', 'PRIM_LONG_DEC':'long'}).copy()    

    census_cities.loc[(census_cities['potential_match'].notnull()) & (census_cities['match_type'].isnull()), 'match_type'] = class_considered + ' round' + str(nround)
    
    return census_cities


def assign_gnis_to_census_w_duplicates(census_cities, gnis_cities_class, county_gdf, class_considered,
                                       townvar, county_type, state_type, nround):
    
    if state_type == 'name':
        state_var = 'state_abb'
        state_var_gnis = 'STATE_ALPHA'
    else:
        state_var = 'state'
        state_var_gnis = 'STATE_NUMERIC'
                
    # only perfect matches are considered here
    census_cities_loop = census_cities[(census_cities['potential_match'].isnull()) & (census_cities[townvar] != '')].copy()
    census_cities_loop = census_cities_loop.reset_index() \
                                           .merge(gnis_cities_class, how='left',
                                                  left_on=[state_var, townvar],
                                                  right_on=[state_var_gnis, 'FEATURE_NAME']) \
                                           .set_index('index')
    
    # keep the duplicate observations with valid latitude and longitude
    census_cities_loop = census_cities_loop[(census_cities_loop.duplicated(subset=[state_var, townvar], keep=False)) & (census_cities_loop['PRIM_LONG_DEC'].notnull())]
    census_cities_loop['potential_match'] = census_cities_loop[townvar]
    
    # if empty
    if len(census_cities_loop) == 0:
        return census_cities

    # control that the city falls within the right county
    census_cities_loop = falls_within_county(census_cities_loop, county_gdf, county_type, place_match=False)
        
    census_cities_loop = census_cities_loop[census_cities_loop['within_county']].copy()
        
    # In case there are multiple matches take the one with the lowest latitude... it should never happen, but...
    census_cities_loop = census_cities_loop.reset_index() \
                                           .sort_values(by='PRIM_LAT_DEC') \
                                           .drop_duplicates(subset='index', keep='first') \
                                           .set_index('index')
                                           
    # if empty
    if len(census_cities_loop) == 0:
        return census_cities
    
    # assign to the original data set
    census_cities.loc[(census_cities['potential_match'].isnull()) & 
                      (census_cities[townvar] != '') &
                      (census_cities[townvar].notnull()),
                      ['potential_match', 'score', 'lat', 'long']] = \
        census_cities_loop[['potential_match', 'score', 'PRIM_LAT_DEC', 'PRIM_LONG_DEC']].rename(columns={'PRIM_LAT_DEC':'lat', 'PRIM_LONG_DEC':'long'}).copy()  

    census_cities.loc[(census_cities['potential_match'].notnull()) & (census_cities['match_type'].isnull()), 'match_type'] = \
        class_considered + ' duplicated_round' + str(nround)
        
    return census_cities


def assign_cities_by_enumdist(census_cities, town_variables):
    census_cities_loop = census_cities[census_cities['potential_match'].isnull()].copy()
    if len(census_cities_loop) == 0:
        return census_cities
    # if other cities in the same enumeration district
    census_cities_enumdist = census_cities.groupby(['state', 'county', 'enumdist']).agg({'lat': 'mean', 'long': 'mean'}).reset_index()
    # assign the average lat/long to places in the same state/county/enumdist as places
    # that were geotagged
    temp_enumdist = census_cities.loc[census_cities['lat'].isnull()] \
                                 .reset_index() \
                                 .merge(census_cities_enumdist, how='left',
                                        on=['state', 'county', 'enumdist']) \
                                 .set_index('index')[['lat_y', 'long_y']]
    census_cities = census_cities.merge(temp_enumdist, how='left', left_index = True, right_index=True).reset_index()
    
    census_cities.loc[census_cities['lat'].isnull(), 'lat'] = census_cities.loc[census_cities['lat'].isnull(), 'lat_y']
    census_cities.loc[census_cities['long'].isnull(), 'long'] = census_cities.loc[census_cities['long'].isnull(), 'long_y']
    census_cities.loc[census_cities['lat_y'].notnull(), 'match_type'] = 'enum_district'
    census_cities = census_cities.drop(columns=['lat_y', 'long_y'])
    
    # assign lat long to places without it based on their surrounding enumeration districts
    temp_enumdist_null = census_cities.loc[census_cities['lat'].isnull()].copy()
    temp_enumdist_notnull = census_cities.loc[census_cities['lat'].notnull()].copy()
    
    temp_enumdist_notnull = temp_enumdist_notnull[['state', 'county', 'enumdist', 'lat', 'long']]
    temp_enumdist_notnull = temp_enumdist_notnull.rename(columns={'lat': 'lat_enum', 'long': 'long_enum', 'enumdist': 'enumdist_for_imput'})
    
    temp_enumdist_null = temp_enumdist_null.reset_index() \
                                           .merge(temp_enumdist_notnull, how='left',
                                                  on=['state', 'county']) \
                                           .set_index('index')
                                           
    temp_enumdist_null['enumdist_distance'] = temp_enumdist_null.apply(lambda x: x['enumdist']-x['enumdist_for_imput'], axis=1)
    # the ones = 0 are already taken care outside this loop
    temp_enumdist_null['enumdist_distance_above'] = temp_enumdist_null['enumdist_distance'].apply(lambda x: x if x > 0 else 99999)
    temp_enumdist_null['enumdist_distance_below'] = temp_enumdist_null['enumdist_distance'].apply(lambda x: np.abs(x) if x < 0 else 99999)
    
    temp_enumdist_null['min_enum_above'] = temp_enumdist_null.groupby(['state', 'county', 'enumdist'])['enumdist_distance_above'].transform('min')
    temp_enumdist_null['min_enum_below'] = temp_enumdist_null.groupby(['state', 'county', 'enumdist'])['enumdist_distance_below'].transform('min')

    temp_enumdist_null = temp_enumdist_null.loc[(temp_enumdist_null['min_enum_above'] < 99999) & (temp_enumdist_null['min_enum_below'] < 99999)]
    temp_enumdist_null = temp_enumdist_null.drop_duplicates(subset=['state', 'county', 'enumdist'] + town_variables)
    
    temp_enumdist_null = temp_enumdist_null[['state', 'county', 'enumdist'] + town_variables + 
                                            [townvar + '_original' for townvar in town_variables] +
                                            ['numobs', 'potential_match', 'score', 'match_type', 'lat',
                                             'long', 'min_enum_above', 'min_enum_below']]
    
    # if enumeration district on the left and right
    temp_enumdist_null['enumdist_left'] = temp_enumdist_null['enumdist'] - temp_enumdist_null['min_enum_above']
    temp_enumdist_null['enumdist_right'] = temp_enumdist_null['enumdist'] + temp_enumdist_null['min_enum_below']
    temp_enumdist_null = temp_enumdist_null.reset_index() \
        .merge(census_cities_enumdist.rename(columns={'lat': 'lat_left', 'long': 'long_left'}),
               how='left', left_on=['state', 'county', 'enumdist_left'],
               right_on=['state', 'county', 'enumdist']) \
        .set_index('index')
    temp_enumdist_null = temp_enumdist_null.reset_index() \
        .merge(census_cities_enumdist.rename(columns={'lat': 'lat_right', 'long': 'long_right'}),
               how='left', left_on=['state', 'county', 'enumdist_right'],
               right_on=['state', 'county', 'enumdist']) \
        .set_index('index')
        
    if len(temp_enumdist_null) > 0:
        temp_enumdist_null['left_right_distance'] = temp_enumdist_null.apply(lambda x: mydist((x['lat_left'], x['long_left']),
                                                                                              (x['lat_right'], x['long_right'])), axis=1)
        
        temp_enumdist_null = temp_enumdist_null[temp_enumdist_null['left_right_distance'] < 50]
        temp_enumdist_null['lat'] = temp_enumdist_null.apply(lambda x: (x['lat_left']+x['lat_right'])/2 if
                                                   (pd.notnull(x['lat_left']) and pd.notnull(x['lat_right'])) else np.nan,
                                                   axis=1)
        temp_enumdist_null['long'] = temp_enumdist_null.apply(lambda x: (x['long_left']+x['long_right'])/2 if
                                                   (pd.notnull(x['long_left']) and pd.notnull(x['long_right'])) else np.nan,
                                                   axis=1)
        
        
        census_cities.loc[census_cities['lat'].isnull(), 'lat'] = temp_enumdist_null['lat'].copy()
        census_cities.loc[census_cities['long'].isnull(), 'long'] = temp_enumdist_null['long'].copy()
        census_cities.loc[(census_cities['lat'].notnull()) & (census_cities['match_type'].isnull()), 'match_type'] = 'enum_district_middle_point'
    
    return census_cities


def gmaps_geotagging(census_cities, county_gdf, county_type, townvar):
    # This function was used to send queries to Google Maps. We are currently using the resulting offline dataset.
    census_cities_loop = census_cities[(census_cities['potential_match'].isnull()) & (census_cities[townvar] != '') &
                                       (census_cities[townvar].notnull()) & (census_cities['state_fips'].notnull())].copy()

    already_done = {}
    for i in range(len(census_cities_loop)):
        town = census_cities_loop[townvar].iloc[i].replace('\'', '')
        town = '"' + town + '"'
        state = census_cities_loop['state_abb'].iloc[i]
        
        if town + ',' + state in already_done:
            lat, long = already_done[town + ',' + state]
            census_cities_loop['lat'].iloc[i] = lat
            census_cities_loop['long'].iloc[i] = long
            continue
        
        columbus_lat = 40.022016
        columbus_long = -83.0201856
        
        addressURL = 'https://www.google.com/maps/place/' + town.replace(' ', '+') + ',+' + state.replace(' ', '+')
        
        count = 0
        while count < 10:
            try:
                request = urllib.request.Request(addressURL)
                response = urllib.request.urlopen(request)
                geoTagPage = response.read().decode('utf-8')
            except:
                count += 1
                continue
            break
    
        if count == 10:
            lat = '-999'
            long = '-999'
        else:
            print(town, state)
            print(addressURL)
            if re.search('google.com.*?@([0-9]{2,3}\.[0-9]*?),(-?[0-9]{2,3}\.[0-9]*?),',  geoTagPage):
                lat = re.search('google.com.*?@([0-9]{2,3}\.[0-9]*?),(-?[0-9]{2,3}\.[0-9]*?),',  geoTagPage).group(1)
                long = re.search('google.com.*?@([0-9]{2,3}\.[0-9]*?),(-?[0-9]{2,3}\.[0-9]*?),',  geoTagPage).group(2)
            else:
                lat = '-999'
                long = '-999'
    
            if lat == None or (mydist((columbus_lat, columbus_long), (float(lat), float(long))) < 10 and state != 'oh'):
                lat = '-999'
                long = '-999'
                
        

        
        if lat != '-999':
            census_cities_loop['lat'].iloc[i] = lat
            census_cities_loop['long'].iloc[i] = long
            already_done.setdefault(town + ',' + state, (lat, long))
        else:
            already_done.setdefault(town + ',' + state, (np.nan, np.nan))

        sleep_time = np.random.uniform(np.random.uniform(35, 42), np.random.uniform(53, 70))
        print(sleep_time)
        time.sleep(sleep_time)
        
    # control that the city falls within the right county
    census_cities_loop = census_cities_loop.rename(columns={'long': 'PRIM_LONG_DEC', 'lat': 'PRIM_LAT_DEC'})
    census_cities_loop = falls_within_county(census_cities_loop, county_gdf, county_type, place_match=False)
        
    census_cities_loop = census_cities_loop[census_cities_loop['within_county']].copy()
        
    # In case there are multiple matches take the one with the lowest latitude... it should never happen, but...
    census_cities_loop = census_cities_loop.reset_index() \
                                           .sort_values(by='PRIM_LAT_DEC') \
                                           .drop_duplicates(subset='index', keep='first') \
                                           .set_index('index')
                                           
    # if empty
    if len(census_cities_loop) == 0:
        return census_cities
    
    # assign to the original data set
    census_cities.loc[(census_cities['potential_match'].isnull()) & 
                      (census_cities[townvar] != '') &
                      (census_cities[townvar].notnull()),
                      ['potential_match', 'score', 'lat', 'long']] = \
        census_cities_loop[['potential_match', 'score', 'PRIM_LAT_DEC', 'PRIM_LONG_DEC']].rename(columns={'PRIM_LAT_DEC':'lat', 'PRIM_LONG_DEC':'long'}).copy() 

    census_cities.loc[(census_cities['lat'].notnull()) & (census_cities['match_type'].isnull()), 'match_type'] = 'gmaps'
    
    return census_cities


def load_census_w_pre1910(census_file_path, town_variables, state_variable,
                          county_variable, county_type, state_type, decade):
    census_cities = pd.read_csv(census_file_path)
    census_cities.columns = [column.lower() for column in census_cities.columns]

    if 'index' in census_cities.columns:
        census_cities = census_cities.drop(columns='index')

    for townvar in town_variables:
        census_cities = census_cities.drop(columns=townvar)
        census_cities = census_cities.rename(columns={townvar + '_original': townvar})

    for townvar in town_variables:
        census_cities[townvar + '_original'] = census_cities[townvar].copy()
        census_cities[townvar] = census_cities[townvar].apply(
            lambda x: clean_city_name(x) if not isinstance(x, float) else x)

    return census_cities
