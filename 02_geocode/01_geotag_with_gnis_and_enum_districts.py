import os
import pandas as pd
import numpy as np
import sys

wd = '/disk/bulkw/karger/census_bulk/citylonglat/'
os.chdir(wd)
sys.path.append(wd + 'programs/02_geocode')

import matching_functions as mf

def geotag_procedure(decade, census_file_path, county_file_path,
                     place_file_path, town_variables, state_variable,
                     state_type, county_variable, county_type):
    if state_type not in ['name', 'icp', 'fips'] or county_type not in ['name', 'fips']:
        print('state or county types not valid')
        stop
    
    # load census data and prepare them for the geotaginng procedure
    census_cities = mf.load_census(census_file_path, town_variables,
                                   state_variable, county_variable,
                                   county_type, state_type, decade)

    # load and cleanup the GNIS file
    gnis_cities = mf.load_gnis(state_type)
    
    # load and cleanup historical county shape file
    county_gdf = mf.load_county_shape(county_file_path, county_type)

    # clean up FIPS
    if county_type == 'fips':
        county_gdf['NHGISST'] = county_gdf['NHGISST'].astype(int)/10
        county_gdf['ICPSRFIP'] = county_gdf['NHGISST'].astype(int).astype(str) + \
            county_gdf['ICPSRCTYI'].astype(int).astype(str).str.zfill(4)
        county_gdf['ICPSRFIP'] = county_gdf['ICPSRFIP'].astype(int)
    
    if decade >= 1900:
        # load place points
        place_gdf = mf.load_place_points(place_file_path)
    
    # determine city-variable order
    share_unique = []
    for townvar in town_variables:
        share_unique.append(len(census_cities.loc[(census_cities[townvar].notnull()) & (census_cities[townvar] != '')]
                                .drop_duplicates(['state', 'county', townvar], keep='first')) /
                            len(census_cities.loc[(census_cities[townvar].notnull()) & (census_cities[townvar] != ''), townvar]))

    for townvar in list(np.array(town_variables)[np.argsort(share_unique)[::-1]]):
        print(townvar)
        for nround in range(3):
            # in the last loop drop hyphens, spaces and cardinal points in all the dataframes
            if nround == 2:
                # less efficient than using .str.replace, but easier to modify since defined only once
                clean_second_round = lambda name: name.replace('-', '') \
                                                      .replace('south ', '').replace('north ', '') \
                                                      .replace('east ', '').replace('west ', '') \
                                                      .replace('southern ', '').replace('northern ', '') \
                                                      .replace('eastern ', '').replace('western ', '') \
                                                      .replace(' township', '').replace(' point', '') \
                                                      .replace(' ', '').strip()

                census_cities[townvar] = census_cities[townvar].apply(lambda x: clean_second_round(x) if not isinstance(x, float) else x)
                if decade >= 1900:
                    place_gdf['PLACE'] = place_gdf['PLACE'].apply(lambda x: clean_second_round(x))
                    place_gdf = place_gdf.drop_duplicates(['PLACE', 'NHGISST'], keep=False)
                gnis_cities['FEATURE_NAME'] = gnis_cities['FEATURE_NAME'].apply(lambda x: clean_second_round(x))

            if decade >= 1900:
                # start by assigning nhgis places
                census_cities = mf.assign_places_to_census(census_cities, place_gdf, county_gdf, townvar, county_type, nround)

            # use gnis
            classes_to_consider = ['populated place', 'locale', 'civil', 'census', 'area',
                                    'beach', 'harbor', 'island', 'military', 'mine', 'park',
                                    'post office', 'unknown', 'basin', 'bay', 'falls',
                                    'rapids', 'reserve', 'reservoir', 'ridge', 'spring',
                                    'stream', 'valley']

            for i, class_considered in enumerate(classes_to_consider):
                gnis_cities_class = gnis_cities[gnis_cities['FEATURE_CLASS'] == class_considered].copy()
                gnis_cities_class = mf.drop_far_duplicates(
                    gnis_cities_class, ['FEATURE_NAME', 'STATE_NUMERIC'],
                    ['PRIM_LAT_DEC_RAD', 'PRIM_LONG_DEC_RAD'], 5).copy()

                census_cities = mf.assign_gnis_to_census(
                    census_cities, gnis_cities_class, county_gdf,
                    class_considered, townvar,
                    county_type, state_type,
                    nround)

                # here take the cities associated to multiple cities in the GNIS database and try to distinguish between them
                # based on county names (only for rounds > 1)
                if nround == 1:
                    continue

                gnis_cities_class = gnis_cities[gnis_cities['FEATURE_CLASS'] == class_considered].copy()
                census_cities = mf.assign_gnis_to_census_w_duplicates(
                    census_cities, gnis_cities_class, county_gdf,
                    class_considered, townvar, county_type, state_type,
                    nround)

            if nround == 2:
                gnis_cities['FEATURE_NAME'] = gnis_cities['FEATURE_NAME_original'].apply(lambda x: mf.clean_city_name(x))
                census_cities[townvar] = census_cities[townvar + '_original'].apply(
                    lambda x: mf.clean_city_name(x) if not isinstance(x, float) else x)

                if decade >= 1900:
                    place_gdf['PLACE'] = place_gdf['PLACE_original'].apply(lambda x: mf.clean_city_name(x))
                    place_gdf = place_gdf.drop_duplicates(['PLACE', 'NHGISST'], keep=False)
                    
    if decade == 1850 or decade >= 1880:
        census_cities['enumdist'] = census_cities['enumdist'].astype(int)
        census_cities = mf.assign_cities_by_enumdist(census_cities, town_variables)
      
    census_cities.to_csv('intermediate/census_gnis_coords_' + str(decade) + '.csv', index=False)


# call the main function for the different decades
#geotag_procedure(1790,
#                 'intermediate/placecounts_1790.dta',
#                 'shape_files/US_county_1790_conflated.shp',
#                 '',
#                 ['township'],
#                 'fullstate',
#                 'name',
#                 'county',
#                 'name')
#
#geotag_procedure(1800,
#                 'intermediate/placecounts_1800.dta',
#                 'shape_files/US_county_1800_conflated.shp',
#                 '',
#                 ['township'],
#                 'fullstate',
#                 'name',
#                 'county',
#                 'name')
#
#geotag_procedure(1810,
#                 'intermediate/placecounts_1810.dta',
#                 'shape_files/US_county_1810_conflated.shp',
#                 '',
#                 ['township'],
#                 'fullstate',
#                 'name',
#                 'county',
#                 'name')
#
#geotag_procedure(1820,
#                 'intermediate/placecounts_1820.dta',
#                 'shape_files/US_county_1820_conflated.shp',
#                 '',
#                 ['township'],
#                 'fullstate',
#                 'name',
#                 'county',
#                 'name')
#
#geotag_procedure(1830,
#                 'intermediate/placecounts_1830.dta',
#                 'shape_files/US_county_1830_conflated.shp',
#                 '',
#                 ['general_township_orig'],
#                 'self_residence_place_state',
#                 'name',
#                 'self_residence_place_county',
#                 'name')
#
#geotag_procedure(1840,
#                 'intermediate/placecounts_1840.dta',
#                 'shape_files/US_county_1840_conflated.shp',
#                 '',
#                 ['locality'],
#                 'fullstate',
#                 'name',
#                 'county',
#                 'name')
#
#geotag_procedure(1850,
#                 'intermediate/placecounts_1850.dta',
#                 'shape_files/US_county_1850_conflated.shp',
#                 '',
#                 ['stdcity', 'us1850c_0043', 'us1850c_0053', 'us1850c_0054', 'us1850c_0042'],
#                 'stateicp',
#                 'icp',
#                 'stcounty',
#                 'fips')
#
#geotag_procedure(1860,
#                 'intermediate/placecounts_1860.dta',
#                 'shape_files/US_county_1860_conflated.shp',
#                 '',
#                 ['us1860c_0040', 'us1860c_0042', 'us1860c_0036'],
#                 'statefip',
#                 'fips',
#                 'countyicp',
#                 'fips')
#
#geotag_procedure(1870,
#                 'intermediate/placecounts_1870.dta',
#                 'shape_files/US_county_1870_conflated.shp',
#                 '',
#                 ['us1870c_0040', 'us1870c_0042', 'us1870c_0043',
#                 'us1870c_0044', 'us1870c_0035', 'us1870c_0036'],
#                 'statefip',
#                 'fips',
#                 'countyicp',
#                 'fips')
#
#geotag_procedure(1880,
#                 'intermediate/placecounts_1880.dta',
#                 'shape_files/US_county_1880_conflated.shp',
#                 '',
#                 ['mcdstr', 'us1880e_0071', 'us1880e_0069',
#                 'us1880e_0072', 'us1880e_0070'],
#                 'stateicp',
#                 'icp',
#                 'stcounty',
#                 'fips')
#
#geotag_procedure(1900,
#                 'intermediate/placecounts_1900.dta',
#                 'shape_files/US_county_1900_conflated.shp',
#                 'shape_files/US_place_point_1900.shp',
#                 ['stdcity', 'us1900m_0045', 'us1900m_0052'],
#                 'statefip',
#                 'fips',
#                 'stcounty',
#                 'fips')
#
geotag_procedure(1910,
                 'intermediate/placecounts_1910.dta',
                 'shape_files/US_county_1910_conflated.shp',
                 'shape_files/US_place_point_1910.shp',
                 ['stdcity', 'us1910m_0052', 'us1910m_0053',
                 'us1910m_0063'],
                 'statefip',
                 'fips',
                 'stcounty',
                 'fips')

geotag_procedure(1920,
                  'intermediate/placecounts_1920.dta',
                  'shape_files/US_county_1920_conflated.shp',
                  'shape_files/US_place_point_1920.shp',
                  ['stdmcd', 'stdcity', 'us1920c_0057',
                  'us1920c_0058', 'us1920c_0068', 'us1920c_0069'],
                  'statefip',
                  'fips',
                  'stcounty',
                  'fips')

geotag_procedure(1930,
                  'intermediate/placecounts_1930.dta',
                  'shape_files/US_county_1930_conflated.shp',
                  'shape_files/US_place_point_1930.shp',
                  ['stdmcd', 'stdcity'],
                  'statefip',
                  'fips',
                  'stcounty',
                  'fips')

geotag_procedure(1940,
                  'intermediate/placecounts_1940.dta',
                  'shape_files/US_county_1940_conflated.shp',
                  'shape_files/US_place_point_1940.shp',
                  ['stdcity', 'us1940b_0073', 'us1940b_0074'],
                  'statefip',
                  'fips',
                  'countyicp',
                  'fips')
