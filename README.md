# Census Place Project - README.md

## Overview

This is a README for "The Census Place Project: A Method for Geolocating Unstructured Place Names" by Enrico Berkes, Ezra Karger, and Peter Nencka. Please contact nenckap@miamioh.edu if you have any questions about the content of this file.

We first describe the datasets that we use in our paper. Then we describe our code, script by script.


## Data


### IPUMS Historical Census Data
Ruggles, Steven, Sarah Flood, Sophia Foster, Ronald Goeken, Jose Pacas, Megan Schouweiler, and Matthew Sobek. IPUMS USA: Version 11.0 [dataset]. Minneapolis, MN: IPUMS, 2021. https://doi.org/10.18128/D010.V11.0

Accessed at IPUMS https://www.ipums.org/


### IPUMS Restricted-Access Census Data
Ruggles, Steven, Sarah Flood, Sophia Foster, Ronald Goeken, Jose Pacas, Megan Schouweiler, and Matthew Sobek. IPUMS USA: Version 11.0 [dataset]. Minneapolis, MN: IPUMS, 2021. https://doi.org/10.18128/D010.V11.0

Accessed on the NBER server

### Google Maps

Accessed sending queries to website in Python. Results superior to the ones obtained by using the API.

### IPUMS Historical Place Files

Steven Manson, Jonathan Schroeder, David Van Riper, Tracy Kugler, and Steven Ruggles. IPUMS National Historical Geographic Information System: Version 16.0 [dataset]. Minneapolis, MN: IPUMS. 2021. http://doi.org/10.18128/D050.V16.0.

Accessed at IPUMS https://www.ipums.org/

### Geographic Names Information System

United States Geological Survey. Geographic Names Information System''. Updated August 27, 2021. 

Accessed at https://www.usgs.gov/core-science-systems/ngp/board-on-geographic-names/download-gnis-data. 

### 2016 United States County and State Shapefiles

Cartographic Boundary Files - Shapefile

Accessed at https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html

### IPUMS Historical County Shape Files

Steven Manson, Jonathan Schroeder, David Van Riper, Tracy Kugler, and Steven Ruggles. IPUMS National Historical Geographic Information System: Version 16.0 [dataset]. Minneapolis, MN: IPUMS. 2021. http://doi.org/10.18128/D050.V16.0.

Accessed at IPUMS https://data2.nhgis.org/main


## Replication

To fully replicate our results, it is necessary to get access to the restricted historical censuses. The process for gaining access to these data via NBER is detailed here:
https://www.nber.org/research/data/ancestrycom-and-ipums-complete-count-restricted-file


We provide a copy of our code and the resulting intermediate output at:
/disk/bulkw/karger/census_bulk/citylonglat/

The crosswalks are publicly available at
http://www.fsb.miamioh.edu/nenckap/

Below, we provide details of our code. Our code takes four broad steps. First, we extract places from the historical censuses. Second, we geocode these places. Third, we create crosswalks by merging these geocoded places back to the census. Forth, we compare our data to public IPUMS data and generate maps. To replicate our results, the files below should be run in order.

## Place extraction codes

### clean_1790to1840_data.do and clean_1850-1940_data.do

These files load the indicated census year raw data from the NBER server and create a count of observations by each unique combination of string location names.

## Geocoding codes

### matching_functions.py

This file contains several functions that are used to load datasets, match cities, and other tasks. 

```python
clean_city_name(city)
```

This function pre-processes the city names to standardize some elements (e.g., *mt.* and *mt* are both substituted with *mount*) and remove words that are not directly related to the name of the city(e.g., *township of*).

```python
mydist(coords_1, coords_2)
```

This function calculates the distance (in km) between two sets of coordinates. Code source: https://gist.github.com/rochacbruno/2883505

```python
drop_far_duplicates(df, dup_columns, coords_columns, dist_threshold)
```

This function calculates the maximum distance between all the duplicate entries (in terms of name/state). If they are closer than dist_threshold km, then it keeps the first one, otherwise it drops all of them.

```python
lev_dist1(candidate, mylist)
```

This function finds all the elements in mylist that have a Levenshtein distance of 1 from candidate. If there is only one such element it returns that element, otherwise NaN.

```python
load_gnis(state_type)
```

This function loads the GNIS file and prepares it for matching (e.g., calls ```clean_city_name(city)``` to clean up city names).

```python
load_county_shape(county_file_path, county_type)
```

This function loads the county shape file, standardizes the variable names (which sometimes are different across decades), calculates the county centroids, fixes the FIPS codes in Maryland as described in the paper, and creates a buffer around the various polygons.

```python
load_census(
    census_file_path,
    town_variables,
    state_variable,
    county_variable,
    county_type,
    state_type,
    decade
)
```

This function loads data from the decennial historical Censuses and prepares them for matching (e.g., calls `clean_city_name(city)` to clean up city names)

```python
load_place_points(place_file_path)
```

This function loads data from the Places file and prepares them for matching (e.g., calls `clean_city_name(city)` to clean up city names)

```python
falls_within_county(census_cities_loop, county_gdf, county_type, place_match)
```

This function checks whether the matched city falls within the county reported in the Census.

```python
assign_places_to_census(census_cities, place_gdf, county_gdf, townvar, county_type, nround)
```

This function matches cities in the Census to cities in the Places file. When a match is found, it assigns the corresponding coordinates to the cities in the Census.

```python
assign_gnis_to_census(
    census_cities,
    gnis_cities_class,
    county_gdf,
    class_considered,
    townvar,
    county_type,
    state_type,
    nround
)
```

This function matches cities in the Census to cities in the GNIS file. When a match is found, it assigns the corresponding coordinates to the cities in the Census.

```python
assign_gnis_to_census_w_duplicates(
    census_cities,
    gnis_cities_class,
    county_gdf,
    class_considered,
    townvar,
    county_type,
    state_type,
    nround
)
```

This function matches cities in the Census to cities in the GNIS file. When a match is found, it assigns the corresponding coordinates to the cities in the Census.

```python
assign_cities_by_enumdist(census_cities, town_variables)
```

When possible, this function assigns coordinates to cities in the census based on their enumeration district.

```python
gmaps_geotagging(census_cities, county_gdf, county_type, townvar)
```

This function was used to send queries to Google Maps. After retrieving the coordinates from Google Maps it checks whether they fall within the correct county. Note that the function is included here for reference, since in our code we are currently using the resulting dataset offline.

```python
load_census_w_pre1910(
    census_file_path,
    town_variables,
    state_variable,
    county_variable,
    county_type,
    state_type,
    decade
)
```

Loads census data for the step that uses 1910 counties to check that the town falls within the correct county boundaries. The last matching round of Step 1 changes the town name (e.g., by dropping cardinal references). Here, after loading the data the function retrieves the original town names.

### 01_geotag_with_gnis_and_enum_districts.py

This script implements Step 1 of our matching procedure. The main function in the file,

```python
geotag_procedure(
    decade,
    census_file_path,
    county_file_path,
    place_file_path,
    town_variables,
    state_variable,
    state_type,
    county_variable,
    county_type
)
```

takes as arguments the census decade (*decade*), the location of the census file (*census_file_path*), the location of the county shape file (*county_file_path*), the location of the NHGIS Place file (*place_file_path*) [1] [^1 Note that this file is not available for all the decades. The function acceppts an empty string in this case.], the name of the town variables in the census data that we consider (*town_variables*), the name of the state variable in the census data in that decade (*state_variable*), the type of the state variable (values can be: 'name', 'icp', 'fips') (*state_type*), the name of the county variable in the census data in that decade (*county_variable*), the type of the county variable (values can be: 'name', 'fips') (*county_type*).

The file calls this function for all the decades and saves the resulting datasets in intermediate/census_gnis_coords_*decade*.csv

### 02_gmaps_step_for_server.py

This code takes the output from the previous code and merges it with a table of city coordinates that we previously obtained sending queries Google Maps. This table was built using the function described above in **matching_functions.py** 

The output is saved in intermediate/census_gnis_gmaps_coords_*decade*.csv

### 03_pre_1910_with_1910_counties_step.py

Same thing as Step 1, but using 1910 counties to check whether the coordinates fall within the right county boundaries. The Census data seem to use some county or state names before they were made official (e.g., West Virginia in 1860). For this reason, when comparing the coordinates we obtain through our matching procedure and the location on a contemporaneous map, we sometimes get false negatives (i.e., towns that are outside a county although they should fall within). This step is meant to fix this problem by using a map with state and county boundaries that should be somewhat stable.

The output is saved in intermediate/census_gnis_coords_gmaps_pre1910_*decade*.csv

### 04_across_decades_step.py

This code implements the fourth and last step of our procedure. This step assigns geo coordinates to cities exploiting information across decades. For example, it could be the case that because of its position in a certain enumeration district, we are able to geocode town X in county Y in 1910 but not in 1900. This step would use the coordinates from 1910 and assign them to town X in county Y in 1900.

The output is saved in final/census_gnis_coords_*decade*_final.csv





## Crosswalk construction code

### construct_raw_place_data.do

Constructs a raw dataset of all places, consistent lon/lat and place names, and consistent county/state identifiers for the unique places we geocoded in each year.

Adds consistent state and county-IDs to each place based on modern US shapefiles

### cluster_places.do

Creates clusters of nearby places

Process is as follows:
Step 1: for every place, find all other places within 20 miles
Step 2: label a pair of places within 20 miles as 'close neighbors' if they are within 3 miles OR
*               the distance between the two places < X * 100 * (% of population in the sample)
*       For example, Chicago has 2.5% of the people in the sample from 1790-1940,
*           New York has 2%, Philadelphia has 1.8%, Boston has 0.7%, and Cambridge has 0.1%
*       So Chicago will be close neighbors with any place within 7.5 miles (for x=3), and that number will be 6 miles for
*           New York as well, 5.4 miles for Philadelphia, 2.1 miles for Boston, and 0.3 miles for Cambridge
Step 3: Identify all connected components of pairs of places. These connected
*           components are our consistent places across time
Step 4: output a long/lat -> connected component crosswalk for use across all years


### construct_histid_longlat_crosswalk.do

This file makes histid->place crosswalks with these variables: histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch

The output of this file is the crosswalks that we publicly release.

### construct_histid_longlat_crosswalk.do

This file makes histid->place crosswalks with these variables: histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch

The output of this file is the crosswalks that we publicly release.


### construct_histid_longlat_crosswalk.do

This file makes histid->place crosswalks with these variables: histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch

The output of this file is the crosswalks that we publicly release.

### collapse_ipums_microdata.do

Constructs summary statistics from the census data about each long/lat in each year. We use the output of this file in the analysis scripts below.

## Analysis code

### 01_match_rate_comparison.do

Compares the share of observations that we geocode in each year to the public IPUMS data

### 02_wisconsin.do

Used to compare match rates in Wisconsin, an illustrative example

### 03_city_dist.do

Calculates Zipf law relationships using both CPP and IPUMS data in 1940

### 04_city_dist1870.do

Calculates Zipf law relationships using both CPP and IPUMS data in 1870

### 05_gibrat.do

Calculates Gibrat's law relationships using both CPP and IPUMS data

### cluster_map.py
 
Generate maps that show clustering nationwide and by state (on a state and state divided into counties map). It also produces state maps adding the name of the 4 most populous cities in that state.


