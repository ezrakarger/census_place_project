*construct_histid_longlat_crosswalk.do
*Written by Ezra Karger
*Written on: February 24, 2021
*Last modified on: March 15, 2022

global ipumscollapsed "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/03_make_crosswalks"
global crosswalks "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global shapefiles "/disk/bulkw/karger/census_bulk/citylonglat/intermediate/modern_county_shapefiles"

global oldcensus "/disk/data3/cens1930/"
global cens1840 "/disk/data3/cens1930/Ancestry"

global plusdir "/disk/homedirs/nber/karger/programs/plus"
sysdir set PLUS $plusdir

capture log close
clear
log using $logs/construct_histid_longlat_crosswalk.log, text replace

set more off
set linesize 255
set rmsg on




use $ipumscollapsed/all_raw_place_data.dta, clear
compress

desc, fullnames
tab match_type, m
tab match_type, m sort

replace match_type = subinstr(match_type,"round2","round3",.)
replace match_type = subinstr(match_type,"round1","round2",.)
replace match_type = subinstr(match_type,"round0","round1",.)

tab match_type, m
tab match_type, m sort


merge m:1 lon lat using "$crosswalks/place_component_crosswalk", keepusing(lat lon consistent_place_5)
drop _merge

rename consistent_place_5 clusterid_k5

*Construct consistent place ID

preserve
keep lat lon
duplicates drop

sort lat lon

gen cpp_placeid = _n

desc, fullnames
di _N
list in 1/5

tempfile latlon
save "`latlon'"
restore

merge m:1 lat lon using "`latlon'"
drop _merge


*******

preserve
keep if year==1790
missings dropvars, force
merge 1:m county township fullstate using $oldcensus/usfedcen1790
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

gen histid=string(_n)+"0.1"
keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1790.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1790.csv, replace
restore



preserve
keep if year==1800
missings dropvars, force
merge 1:m county township township_original fullstate using $oldcensus/usfedcen1800
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

gen histid=string(_n)+"0.1"
keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1800.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1800.csv, replace
restore



preserve
keep if year==1810
missings dropvars, force
merge 1:m county township fullstate using $oldcensus/usfedcen1810
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

gen histid=string(_n)+"0.1"
keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1810.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1810.csv, replace
restore



preserve
keep if year==1820
missings dropvars, force
merge 1:m county township fullstate using $oldcensus/usfedcen1820
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

gen histid=string(_n)+"0.1"
keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1820.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1820.csv, replace
restore



preserve
keep if year==1830
missings dropvars, force
merge 1:m self_residence_place_county general_township_orig self_residence_place_state using $oldcensus/usfedcen1830
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge


gen histid=string(_n)+"0.1"
keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1830.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1830.csv, replace
restore




*NOTE: 1840, WE NEED TO RECAST THE LOCALITY VARIABLE SO THE MERGE WORKS,
*	OTHERWISE WE GET THIS ERROR:
*		"key variable locality is strL in using data.
*		The key variables -- the variables on which observations are matched --
*		can be str#, but they cannot be strLs."

preserve
use $cens1840/usfedcen_1840, clear

recast str200 locality
compress

tempfile cens1840
save "`cens1840'"
restore


preserve
keep if year==1840
missings dropvars, force

recast str200 locality
compress
merge 1:m fullstate county locality using "`cens1840'"
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge


gen histid=string(_n)+"0.1"
keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1840.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1840.csv, replace
restore



preserve
keep if year==1850
missings dropvars, force
merge 1:m stateicp stcounty enumdist stdcity city us1850c_0042 us1850c_0043 us1850c_0053 us1850c_0054 using $ipumscollapsed/raw_1850_data
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1850.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1850.csv, replace
restore



preserve
keep if year==1860
missings dropvars, force
merge 1:m stateicp statefip countyicp US1860C_0036 US1860C_0040 US1860C_0042 using $ipumscollapsed/raw_1860_data
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1860.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1860.csv, replace
restore



preserve
keep if year==1870
missings dropvars, force
merge 1:m stateicp statefip countyicp US1870C_0035 US1870C_0036 US1870C_0040 US1870C_0042 US1870C_0043 US1870C_0044 using $ipumscollapsed/raw_1870_data
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1870.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1870.csv, replace
restore



preserve
keep if year==1880
missings dropvars, force
tostring enumdist, replace
gen length_enum = length(enumdist)
replace enumdist = "000"+enumdist if length_enum == 1
replace enumdist = "00"+enumdist if length_enum == 2
replace enumdist = "0"+enumdist if length_enum == 3
drop length_enum
merge 1:m stateicp stcounty enumdist supdist city mcd mcdstr US1880E_0069 US1880E_0070 US1880E_0071 US1880E_0072 using $ipumscollapsed/raw_1880_data
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1880.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1880.csv, replace
restore



preserve
keep if year==1900
missings dropvars, force
destring county, replace
merge 1:m stateicp statefip county stcounty stdcity US1900M_0045 US1900M_0052 enumdist using $ipumscollapsed/raw_1900_data
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1900.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1900.csv, replace
restore



preserve
keep if year==1910
missings dropvars, force
merge 1:m stateicp statefip stcounty stdcity US1910M_0052 US1910M_0053 US1910M_0063 enumdist using $ipumscollapsed/raw_1910_data
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1910.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1910.csv, replace
restore




preserve
keep if year==1920
missings dropvars, force
merge 1:m stateicp statefip stcounty stdmcd stdcity US1920C_0057 US1920C_0058 US1920C_0068 US1920C_0069 enumdist using $ipumscollapsed/raw_1920_data
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1920.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1920.csv, replace
restore




preserve
keep if year==1930
missings dropvars, force
merge 1:m stateicp statefip stcounty stdmcd stdcity enumdist using $ipumscollapsed/raw_1930_data
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1930.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1930.csv, replace
restore



preserve
keep if year==1940
missings dropvars, force
tostring countyicp, replace
gen length_countyicp = length(countyicp)
replace countyicp = "000"+countyicp if length_countyicp == 1
replace countyicp = "00"+countyicp if length_countyicp == 2
replace countyicp = "0"+countyicp if length_countyicp == 3
drop length_countyicp


merge 1:m stateicp statefip countyicp stdcity enumdist US1940B_0073 US1940B_0074 using $ipumscollapsed/raw_1940_data
tab _merge, m
keep if inlist(_merge,2,3)
drop _merge

keep histid match_type lat lon potential_match state_fips_geomatch county_fips_geomatch clusterid_k5 cpp_placeid
desc, fullnames
list in 1/10

compress
save $ipumscollapsed/histid_place_crosswalk_1940.dta, replace
export delimited using $ipumscollapsed/histid_place_crosswalk_1940.csv, replace
restore


log close

