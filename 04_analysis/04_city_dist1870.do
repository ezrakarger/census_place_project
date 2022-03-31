*Calculate Zipf law relationships for 1870
*Written by Peter Nencka
*Written on: March 17, 2022

global ipumsnew       "/disk/data3/census-ipums/v2020/dta/"
global ipums          "/disk/data3/census-ipums/v2019/dta/"

global ipumscollapsed "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global longlats       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global charsdir       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums_public   "/disk/bulkw/karger/census_bulk/citylonglat/ipums"

global figures "/disk/bulkw/karger/census_bulk/citylonglat/figures/zipf_1870"

capture log close
clear

set more off
set linesize 255
set rmsg on

set scheme plotplain, perm


*load 1870 city distribution from lon-lats

use "$ipumscollapsed/placechars_1870.dta"

drop if mi(lat) | mi(lon)

gsort -population

gen count = _n
gen ln_rank = ln(count)
gen ln_pop  = ln(population)

aaplot ln_rank ln_pop

graph export "$figures/geocode1870_base.png", replace

aaplot ln_rank ln_pop if population > 500
graph export "$figures/geocode1870_base_500.png", replace

aaplot ln_rank ln_pop if population > 1000
graph export "$figures/geocode1870_base_1000.png", replace

aaplot ln_rank ln_pop if population > 5000
graph export "$figures/geocode1870_base_5000.png", replace

aaplot ln_rank ln_pop if population > 10000
graph export "$figures/geocode1870_base_10000.png", replace

aaplot ln_rank ln_pop if population > 20000
graph export "$figures/geocode1870_base_20000.png", replace



*load 1870 city distribution from lon-lats + clustering

use "$ipumscollapsed/placechars_1870.dta", clear

drop if mi(lat) | mi(lon)

merge 1:1 lat lon using "$ipumscollapsed/place_component_crosswalk.dta"

assert _m != 1
drop if _m == 2
drop _m 

preserve 
 foreach i in 3 5 10 50 100 200 500 {

 	collapse (sum) population, by(consistent_place_`i')

 	gsort -population
	gen count = _n
	gen ln_rank = ln(count)
	gen ln_pop  = ln(population)

	aaplot ln_rank ln_pop
	graph export "$figures/geocode1870_cluster`i'.png", replace

	aaplot ln_rank ln_pop if population > 500
	graph export "$figures/geocode1870_cluster`i'_500.png", replace

	aaplot ln_rank ln_pop if population > 1000
	graph export "$figures/geocode1870_cluster`i'_1000.png", replace

	aaplot ln_rank ln_pop if population > 5000
	graph export "$figures/geocode1870_cluster`i'_5000.png", replace

	aaplot ln_rank ln_pop if population > 10000
	graph export "$figures/geocode1870_cluster`i'_10000.png", replace

	aaplot ln_rank ln_pop if population > 20000
	graph export "$figures/geocode1870_cluster`i'_20000.png", replace

restore, preserve

 }




*load 1870 city distribution from ipums
use "$ipums/1870.dta", clear 
desc, fullnames
list in 1/100
replace city=. if city==0

gen population = 1 
gcollapse (sum) population, by(statefip city)

gsort -population

gen count = _n
gen ln_rank = ln(count)
gen ln_pop  = ln(population)


aaplot ln_rank ln_pop
graph export "$figures/ipums1870_base.png", replace

aaplot ln_rank ln_pop if population > 500
graph export "$figures/ipums1870_base_500.png", replace

aaplot ln_rank ln_pop if population > 1000
graph export "$figures/ipums1870_base_1000.png", replace

aaplot ln_rank ln_pop if population > 5000
graph export "$figures/ipums1870_base_5000.png", replace

aaplot ln_rank ln_pop if population > 10000
graph export "$figures/ipums1870_base_10000.png", replace

aaplot ln_rank ln_pop if population > 20000
graph export "$figures/ipums1870_base_20000.png", replace

