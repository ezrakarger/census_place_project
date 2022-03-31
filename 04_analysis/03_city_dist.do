*Calculate Zipf law relationships
*Written by Peter Nencka
*Written on: March 17, 2022

global ipumsnew       "/disk/data3/census-ipums/v2020/dta/"
global ipums          "/disk/data3/census-ipums/v2019/dta/"

global ipumscollapsed "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global longlats       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global charsdir       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums_public   "/disk/bulkw/karger/census_bulk/citylonglat/ipums"

global figures "/disk/bulkw/karger/census_bulk/citylonglat/figures/zipf"

capture log close
clear

set more off
set linesize 255
set rmsg on

set scheme plotplain, perm


*load 1940 city distribution from lon-lats

use "$ipumscollapsed/placechars_1940.dta"

drop if mi(lat) | mi(lon)

gsort -population

gen count = _n
gen ln_rank = ln(count)
gen ln_pop  = ln(population)

aaplot ln_rank ln_pop

graph export "$figures/geocode1940_base.eps", replace

aaplot ln_rank ln_pop if population > 500
graph export "$figures/geocode1940_base_500.eps", replace

aaplot ln_rank ln_pop if population > 1000
graph export "$figures/geocode1940_base_1000.eps", replace

aaplot ln_rank ln_pop if population > 5000
graph export "$figures/geocode1940_base_5000.eps", replace

aaplot ln_rank ln_pop if population > 10000
graph export "$figures/geocode1940_base_10000.eps", replace

aaplot ln_rank ln_pop if population > 20000
graph export "$figures/geocode1940_base_20000.eps", replace



*load 1940 city distribution from lon-lats + clustering

use "$ipumscollapsed/placechars_1940.dta", clear

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
	graph export "$figures/geocode1940_cluster`i'.eps", replace

	aaplot ln_rank ln_pop if population > 500
	graph export "$figures/geocode1940_cluster`i'_500.eps", replace

	aaplot ln_rank ln_pop if population > 1000
	graph export "$figures/geocode1940_cluster`i'_1000.eps", replace

	aaplot ln_rank ln_pop if population > 5000
	graph export "$figures/geocode1940_cluster`i'_5000.eps", replace

	aaplot ln_rank ln_pop if population > 10000
	graph export "$figures/geocode1940_cluster`i'_10000.eps", replace

	aaplot ln_rank ln_pop if population > 20000
	graph export "$figures/geocode1940_cluster`i'_20000.eps", replace

restore, preserve

 }



clear
quietly infix               ///
	int     year       1-4    ///
	byte    statefip   5-6    ///
	int     countyicp  7-10   ///
	str     placenhg   11-20  ///
	int     city       21-24  ///
	using `"$ipums_public/usa_00025.dat"'
keep if year==1940


replace placenhg = "" if placenhg == "9999999999"

replace city=. if city==0
tostring city, gen(scity)
replace scity="" if scity=="."

replace placenhg = scity if missing(placenhg)		

desc, fullnames

gen population = 1 
gcollapse (sum) population, by(statefip placenhg)
rename placenhg city

gsort -population

gen count = _n
gen ln_rank = ln(count)
gen ln_pop  = ln(population)


aaplot ln_rank ln_pop
graph export "$figures/ipums1940_base.eps", replace

aaplot ln_rank ln_pop if population > 500
graph export "$figures/ipums1940_base_500.eps", replace

aaplot ln_rank ln_pop if population > 1000
graph export "$figures/ipums1940_base_1000.eps", replace

aaplot ln_rank ln_pop if population > 5000
graph export "$figures/ipums1940_base_5000.eps", replace

aaplot ln_rank ln_pop if population > 10000
graph export "$figures/ipums1940_base_10000.eps", replace

aaplot ln_rank ln_pop if population > 20000
graph export "$figures/ipums1940_base_20000.eps", replace


	cd $figures
	!mogrify -density 2000 -format png -- *.eps
