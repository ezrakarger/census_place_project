*wiconsin ipums cities
*Written by Peter Nencka
*Written on: March 17, 2022

global ipumsnew       "/disk/data3/census-ipums/v2020/dta/"
global ipums          "/disk/data3/census-ipums/v2019/dta/"

global ipumscollapsed "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global longlats       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global charsdir       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums_public       "/disk/bulkw/karger/census_bulk/citylonglat/ipums"

global figures "/disk/bulkw/karger/census_bulk/citylonglat/figures"

capture log close
clear

set more off
set linesize 255
set rmsg on


	*Load late ipums data (persons)
	clear
	quietly infix               ///
	  int     year       1-4    ///
	  double  serial     5-12   ///
	  byte    statefip   13-14  ///
	  int     countyicp  15-18  ///
	  int     city       19-22  ///
	  int     pernum     23-26  ///
	  using `"$ipums_public/usa_00024.dat"'

	gen count = 1

	keep if statefip == 55

	gcollapse (sum) count, by(year city countyicp)
	
	save "$ipumscollapsed/wisconsin_ipums_cities.dta", replace
