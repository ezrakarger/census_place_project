*Calculate Gibrat's law relationships
*Written by Peter Nencka
*Written on: March 17, 2022

global ipumsnew       "/disk/data3/census-ipums/v2020/dta/"
global ipums          "/disk/data3/census-ipums/v2019/dta/"

global ipumscollapsed "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global longlats       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global charsdir       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"

global figures "/disk/bulkw/karger/census_bulk/citylonglat/figures/gibrat"

capture log close
clear

set more off
set linesize 255
set rmsg on

set scheme plotplain, perm


*Focus on 1870 and 1940 data for growth rates from our clusters and IPUMS
*Binscatter using: net install binsreg, from(https://raw.githubusercontent.com/nppackages/binsreg/master/stata) replace

*load 1870 and 1940 city distribution from lon-lats

use "$ipumscollapsed/placechars_1870.dta"
duplicates report lat lon

append using "$ipumscollapsed/placechars_1940.dta"
duplicates report lat lon

desc, fullnames
keep lat lon population year

drop if mi(lat) | mi(lon)

reshape wide population, i(lat lon) j(year)
desc, fullnames
list in 1/10

keep if population1870>=100 & !missing(population1870)
replace population1940=0 if missing(population1940)
sum population1870, d

gen growth = population1940/population1870-1
binsreg growth population1870, binspos(250 500 1000 2000 3000 4000 5000 10000 20000 30000 40000 50000) ///
	ci(0,0) plotxrange(0 50000) ytitle("Population Growth 1870-1940") xtitle("1870 Population")
graph export "$figures/gibrat_nocluster.png", replace



*Now use clustered places to show changes in the plot


use "$ipumscollapsed/placechars_1870.dta", clear
duplicates report lat lon

append using "$ipumscollapsed/placechars_1940.dta"
duplicates report lat lon

drop if mi(lat) | mi(lon)
keep lat lon population year

merge m:1 lat lon using "$ipumscollapsed/place_component_crosswalk.dta"
keep if _merge==3
tab year, m

collapse (sum) population, by(consistent_place_5 year)

reshape wide population, i(consistent_place_5) j(year)
desc, fullnames
list in 1/10

keep if population1870>=100 & !missing(population1870)
sum population1870, d

gen growth = population1940/population1870-1
binsreg growth population1870,  binspos(250 500 1000 2000 3000 4000 5000 10000 20000 30000 40000 50000) ///
	ci(0,0) plotxrange(0 50000) ytitle("Population Growth 1870-1940") xtitle("1870 Population")
graph export "$figures/gibrat_cluster.png", replace




*load 1870 and 1940 city distribution from ipums

use statefip city using "$ipums/1870.dta", clear
gen year=1870
gen population=1
gcollapse (sum) population, by(statefip city year)

tempfile t1870
save "`t1870'"

use statefip city using "$ipums/1940.dta", clear
gen year=1940
gen population=1
gcollapse (sum) population, by(statefip city year)

append using "`t1870'"



reshape wide population, i(statefip city) j(year)

desc, fullnames
list in 1/10
tab city, m
tab city, m nolabel

drop if city==0


keep if population1870>=100 & !missing(population1870)
sum population1870, d

gen growth = population1940/population1870-1
binsreg growth population1870,  binspos(20000 30000 40000 50000) ///
	ci(0,0) plotxrange(0 50000) ytitle("Population Growth 1870-1940") xtitle("1870 Population")
graph export "$figures/ipums_gibrat_cluster.png", replace


