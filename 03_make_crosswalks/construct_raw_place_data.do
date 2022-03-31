*construct_raw_place_data.do
*Written by Ezra Karger
*Written on: February 24, 2021
*Last modified on: August X, 2021

global ipumscollapsed "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/03_make_crosswalks"
global crosswalks "/disk/bulkw/karger/census_bulk/citylonglat/final"
global shapefiles "/disk/bulkw/karger/census_bulk/citylonglat/intermediate/modern_county_shapefiles"

global plusdir "/disk/homedirs/nber/karger/programs/plus"
sysdir set PLUS $plusdir

capture log close
clear
log using $logs/construct_raw_place_data.log, text replace

set more off
set linesize 255
set rmsg on






*Now construct a place crosswalk for 1790

import delimited using "$crosswalks/census_gnis_coords_1790_final.csv", clear
desc, fullnames

rename v10 lon 
rename state_fips statefip

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1790

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1790, ///
	keepusing(county township fullstate)
tab _merge, m
drop _merge

tempfile cross1790
save "`cross1790'"




*Now construct a place crosswalk for 1800

import delimited using "$crosswalks/census_gnis_coords_1800_final.csv", clear
desc, fullnames

rename v10 lon 
rename state_fips statefip

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1800

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1800, ///
	keepusing(county township township_original fullstate)
tab _merge, m
drop _merge

tempfile cross1800
save "`cross1800'"




*Now construct a place crosswalk for 1810

import delimited using "$crosswalks/census_gnis_coords_1810_final.csv", clear
desc, fullnames

rename v10 lon 
rename state_fips statefip

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1810

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1810, ///
	keepusing(county township fullstate)
tab _merge, m
drop _merge

tempfile cross1810
save "`cross1810'"



*Now construct a place crosswalk for 1820

import delimited using "$crosswalks/census_gnis_coords_1820_final.csv", clear
desc, fullnames
	
rename v10 lon 
rename state_fips statefip

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1820

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1820, ///
	keepusing(county township fullstate)
tab _merge, m
drop _merge

tempfile cross1820
save "`cross1820'"




*Now construct a place crosswalk for 1830

import delimited using "$crosswalks/census_gnis_coords_1830_final.csv", clear
desc, fullnames
	
rename v10 lon 
rename state_fips statefip

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1830

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1830, ///
	keepusing(self_residence_place_county general_township_orig self_residence_place_state)
tab _merge, m
drop _merge

tempfile cross1830
save "`cross1830'"




*Now construct a place crosswalk for 1840

import delimited using "$crosswalks/census_gnis_coords_1840_final.csv", clear
desc, fullnames
	
rename v10 lon 
rename state_fips statefip

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1840

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1840, ///
	keepusing(fullstate county locality)
tab _merge, m
drop _merge

tempfile cross1840
save "`cross1840'"




*Now construct a place crosswalk for 1850

import delimited using "$crosswalks/census_gnis_coords_1850_final.csv", clear
desc, fullnames

rename v16 lon 
rename state_fips statefip

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1850

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1850, ///
	keepusing(stateicp stcounty enumdist stdcity city us1850c_0042 us1850c_0043 us1850c_0053 us1850c_0054)
tab _merge, m
drop _merge

tempfile cross1850
save "`cross1850'"




*Now construct a place crosswalk for 1860

import delimited using "$crosswalks/census_gnis_coords_1860_final.csv", clear
desc, fullnames

rename v13 lon 
rename state_fips statefip

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1860	

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1860, ///
	keepusing(stateicp statefip countyicp US1860C_0036 US1860C_0040 US1860C_0042)
tab _merge, m
drop _merge

tempfile cross1860
save "`cross1860'"



*Now construct a place crosswalk for 1870

import delimited using $crosswalks/census_gnis_coords_1870_final.csv, clear
desc, fullnames

rename v16 lon

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1870

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1870, ///
	keepusing(stateicp statefip countyicp US1870C_0035 US1870C_0036 US1870C_0040 US1870C_0042 US1870C_0043 US1870C_0044)
tab _merge, m
drop _merge

tempfile cross1870
save "`cross1870'"



*Now construct a place crosswalk for 1880

import delimited using $crosswalks/census_gnis_coords_1880_final.csv, clear
desc, fullnames

rename v18 lon

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1880

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1880, ///
	keepusing(stateicp stcounty enumdist supdist city mcd mcdstr US1880E_0069 US1880E_0070 US1880E_0071 US1880E_0072)
tab _merge, m
drop _merge

destring enumdist, replace

tempfile cross1880
save "`cross1880'"




*Now construct a place crosswalk for 1900

import delimited using $crosswalks/census_gnis_coords_1900_final.csv, clear
desc, fullnames

rename v14 lon

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1900

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1900, ///
	keepusing(stateicp statefip county stcounty stdcity US1900M_0045 US1900M_0052 enumdist)
tab _merge, m
drop _merge
tostring county, replace

tempfile cross1900
save "`cross1900'"




*Now construct a place crosswalk for 1910

import delimited using $crosswalks/census_gnis_coords_1910_final.csv, clear
desc, fullnames

rename v19 lon

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1910

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1910, ///
	keepusing(stateicp statefip stcounty stdcity US1910M_0052 US1910M_0053 US1910M_0063 enumdist)
tab _merge, m
drop _merge

tempfile cross1910
save "`cross1910'"




*Now construct a place crosswalk for 1920

import delimited using $crosswalks/census_gnis_coords_1920_final.csv, clear
desc, fullnames

rename v23 lon

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1920

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1920, ///
	keepusing(stateicp statefip stcounty stdmcd stdcity US1920C_0057 US1920C_0058 US1920C_0068 US1920C_0069 enumdist)
tab _merge, m
drop _merge

tempfile cross1920
save "`cross1920'"



*Now construct a place crosswalk for 1930

import delimited using $crosswalks/census_gnis_coords_1930_final.csv, clear
desc, fullnames

rename v15 lon

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1930

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1930, ///
	keepusing(stateicp statefip stcounty stdmcd stdcity enumdist)
tab _merge, m
drop _merge

tempfile cross1930
save "`cross1930'"




*Now construct a place crosswalk for 1940

import delimited using $crosswalks/census_gnis_coords_1940_final.csv, clear
desc, fullnames

rename v17 lon

keep match_type lat lon potential_match indexcollapse numobs state_abb
gen year=1940

merge 1:1 indexcollapse using $ipumscollapsed/placecounts_1940, ///
	keepusing(stateicp statefip countyicp stdcity enumdist US1940B_0073 US1940B_0074)
tab _merge, m
drop _merge
destring countyicp, replace

tempfile cross1940
save "`cross1940'"


*Append together all crosswalks for merge onto microdata

clear 
*Loop together to create single dataset

gen temp = ""
forval yearval = 1790(10)1940 { 
	di "`yearval'"
	capture list in 1/5

	if (`yearval'!=1890) {
		append using `cross`yearval''
	}
	di _N
}
desc, fullnames
di _N
tab year, m


*Now, first make sure that any exact longitude and latitude only has ONE place name (potential_match)

duplicates report lat lon
sort lat lon potential_match

bys lat lon: egen _potential_match=mode(potential_match), maxmode
count if potential_match != _potential_match & !missing(lat) & !missing(lon)
list lat lon potential_match _potential_match if potential_match != _potential_match

replace potential_match = _potential_match if !missing(_potential_match) & !missing(lat) & !missing(lon)
drop _potential_match



*Now, for any two places within 3 miles of each other that share the same name,
*	standardize the coordinates to be the mean of the available coordinates

preserve

collapse (sum) numobs (first) potential_match, by(lat lon)
list if missing(lat) & missing(lon)

gen id=_n
rename * *_neighbor

tempfile neighbors
save "`neighbors'"

rename *_neighbor *_base
tempfile basedata
save "`basedata'"



*Use geonear function to construct (long) pairs of all places
*	that are within 3 miles of each other

geonear id_base lat_base lon_base using "`neighbors'", neighbors(id_neighbor lat_neighbor lon_neighbor) long within(3) miles

merge m:1 id_base using "`basedata'"
drop _merge
merge m:1 id_neighbor using "`neighbors'"
drop _merge

compress
desc, fullnames
list in 1/100


*Only standardize place names and locations if the nearby places have
*	quite similar names (within one edit length)
*NOTE: KEEP MATCHES OF PLACE X TO X SO THAT IT WILL BE CAPTURED BY THE
*	CONNECTED COMPONENT CONSTRUCTION BELOW

levenshtein potential_match_base potential_match_neighbor, gen(strdist)
desc, fullnames
tab strdist, m

keep if !missing(potential_match_base) & !missing(potential_match_neighbor)
keep if strdist<=1

group_twoway id_base id_neighbor, gen(consistent_place)
duplicates report consistent_place

bys consistent_place: egen newplacename = mode(potential_match_base), maxmode
bys consistent_place: egen newlat = mode(lat_base), maxmode
bys consistent_place: egen newlon = mode(lon_base), maxmode

save $ipumscollapsed/fixes_to_consistent_places.dta, replace

keep id_base newplacename newlat newlon
duplicates drop
list in 1/100

tempfile consistentplaces
save "`consistentplaces'"

restore


*Merge back the new places and long/lats 

rename lon lon_base
rename lat lat_base
merge m:1 lon_base lat_base using "`basedata'", keepusing(lon lat id_base)
drop _merge
rename lon_base lon
rename lat_base lat


merge m:1 id_base using "`consistentplaces'"
drop _merge
list potential_match lon lat newplacename newlat newlon if lon!=newlon & lat!=newlat & !missing(lat) & !missing(lon) & !missing(newlat) & !missing(newlon)

replace potential_match = newplacename if !missing(newplacename) & !missing(lat) & !missing(lon) & !missing(newlat) & !missing(newlon)
replace lon = newlon if !missing(newplacename) & !missing(lat) & !missing(lon) & !missing(newlat) & !missing(newlon)
replace lat = newlat if !missing(newplacename) & !missing(lat) & !missing(lon) & !missing(newlat) & !missing(newlon)

drop newplacename newlon newlat


*Now, add consistent county and state identifiers to this data

shp2dta using "$shapefiles/cb_2016_us_county_500k.shp", ///
       data("$shapefiles/cb_data.dta") coor("$shapefiles/cb_coor.dta") genid(cid) gencentroids(cent) replace
 
gen _Y=lat
gen _X=lon

geoinpoly _Y _X using "$shapefiles/cb_coor.dta"

rename _ID cid
merge m:1 cid using "$shapefiles/cb_data.dta"
drop if _m == 2

destring STATEFP COUNTYFP, replace

rename STATEFP state_fips_geomatch
rename COUNTYFP county_fips_geomatch

gen _tempstate = !missing(state_fips_geomatch)
gen _tempcounty = !missing(county_fips_geomatch)
tab year _tempstate, m
tab year _tempcounty, m
drop _*

*Save this year-place dataset with all variables for matching on microdata
desc, fullnames
di _N
tab year, m
tab year state_fips_geomatch, m

rename state_abb orig_state_abb

save $ipumscollapsed/all_raw_place_data.dta, replace


log close

