*cluster_places.do
*Written by: Peter Nencka and Ezra Karger
*First version: January 17, 2021
*Latest version: November 21, 2021
*Last modified by: Ezra Karger

**Script to cluster 1790-1940 places into consistent places over time and space
**Process is as follows:
**	Step 1: for every place, find all other places within 20 miles
**	Step 2: label a pair of places within 20 miles as 'close neighbors' if they are within 3 miles OR
*				the distance between the two places < X * 100 * (% of population in the sample)
*		For example, Chicago has 2.5% of the people in the sample from 1790-1940,
*			New York has 2%, Philadelphia has 1.8%, Boston has 0.7%, and Cambridge has 0.1%
*		So Chicago will be close neighbors with any place within 7.5 miles (for x=3), and that number will be 6 miles for
*			New York as well, 5.4 miles for Philadelphia, 2.1 miles for Boston, and 0.3 miles for Cambridge
*	Step 3: Identify all connected components of pairs of places. These connected
*			components are our consistent places across time
*	Step 4: output a long/lat -> connected component crosswalk for use across all years

clear all

global root     "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global data     "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global clustout "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"

global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/03_make_crosswalks"

global plusdir "/disk/homedirs/nber/karger/programs/plus"
sysdir set PLUS $plusdir

capture log close
clear
log using $logs/cluster_places.log, text replace




use $root/all_raw_place_data.dta, clear
tab year, m


keep year numobs potential_match lat lon
order year numobs potential_match lat lon



*Now collapse data down to year-by-lat/lon to combine (for example) different
*	enumeration districts that were all matched to the same place.
*Also, drop places with missing lat/lon values


drop if missing(lat) | missing(lon)

foreach var of varlist potential_match {
	bys year lat lon: egen tempmode=mode(`var')
	replace `var' = tempmode
	drop tempmode
}

collapse (sum) numobs (first) potential_match, by(year lat lon)


*Now, generate population count for each lat/lon point as a fraction of total population
*	in a given year. Pre-1850, numobs is #households, 1850-1940, numobs is #people.
*This standardizes those values across years.

bys year: egen temp=total(numobs)
gen fracpop = numobs/temp
drop numobs temp

sum fracpop, d
gsort -fracpop
list in 1/10



*Now, collapse data to place-level (ignoring year), so that the resulting dataset
*	reflects all of the unique places in the census, across all years. And fracpop
*	is the average fraction of the population in that place, across years.
*Include zeroes for years where a given long/lat tuple has no people, so that fracpop
*	sums to 1 in the collapsed dataset.

egen latlon = concat(lat lon), punct(";")

fillin latlon year 

foreach var of varlist lat lon potential_match {
	bys latlon: egen temp = mode(`var'), minmode
	replace `var'=temp if missing(`var')
	drop temp
}
replace fracpop=0 if missing(fracpop)

collapse (mean) fracpop (first) potential_match, by(lat lon)
sum fracpop, d


*Now, save collapsed places in new dataset

gen id=_n
rename * *_neighbor
save "$clustout/collapsed_places_neighbors", replace

rename *_neighbor *_base
save "$clustout/collapsed_places_base", replace


*Now use geonear function to construct (long) pairs of all places that are within
*	20 miles of each other

geonear id_base lat_base lon_base using "$clustout/collapsed_places_neighbors", neighbors(id_neighbor lat_neighbor lon_neighbor) long within(20) miles

merge m:1 id_base using "$clustout/collapsed_places_base"
drop _merge
merge m:1 id_neighbor using "$clustout/collapsed_places_neighbors"
drop _merge
compress

save "$clustout/connected_components", replace


*Now, apply clustering rule for a variety of thresholding kernel parameters

foreach multiple in 3 5 10 50 100 200 300 500 {

use "$clustout/connected_components", clear

gen maxfracpop = max(fracpop_base, fracpop_neighbor)

gen thresholdtokeep = `multiple'*100*maxfracpop
sum mi_to_id_neighbor thresholdtokeep, d
count if mi_to_id_neighbor < thresholdtokeep
count if mi_to_id_neighbor < 2

keep if mi_to_id_neighbor < thresholdtokeep | mi_to_id_neighbor <= 3


group_twoway id_base id_neighbor, gen(consistent_place)
duplicates report id_base id_neighbor
duplicates report consistent_place

bys consistent_place: egen consistent_place_name = mode(potential_match_base)

rename *_base *
keep lat lon consistent_place consistent_place_name
duplicates drop

rename consistent_place consistent_place_`multiple'
rename consistent_place_name consistent_place_name_`multiple'

*Save place-level crosswalk

tempfile clust`multiple'
save `clust`multiple''

}

use `clust3'

foreach multiple in 5 10 50 100 200 300 500 {
	merge 1:1 lat lon using `clust`multiple''
	drop _merge
}

sort consistent_place_500 consistent_place_300 consistent_place_200 consistent_place_100 consistent_place_50 consistent_place_10 consistent_place_5 consistent_place_3

rename lat lat_base
rename lon lon_base

merge 1:1 lat_base lon_base using "$clustout/collapsed_places_base"
drop _merge
rename *_base *

desc, fullnames

save "$clustout/place_component_crosswalk", replace

