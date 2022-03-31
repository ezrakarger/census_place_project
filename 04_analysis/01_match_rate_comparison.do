*match_rates
*Written by Peter Nencka
*Written on: November 15, 2021

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

set scheme plotplainblind

*Overall match rate comparison
	*Load early ipums data (households)

		use "$ipums_public/H_1790_1840.dta"

		gen count = 1
		gen count_in_ipums_city = 1 if city != 0
		replace count_in_ipums_city = 0 if city == 0

		gcollapse (sum) count_in_ipums_city count, by(year)

		gen match_rate_ipums = count_in_ipums_city/count

		save "$ipumscollapsed/ipums_early_match_rates.dta", replace


	*Load late ipums data (persons)
	clear
	quietly infix               ///
	  int     year       1-4    ///
	  byte    statefip   5-6    ///
	  int     countyicp  7-10   ///
	  str     placenhg   11-20  ///
	  int     city       21-24  ///
	  using `"$ipums_public/usa_00025.dat"'


		gen count = 1
		replace placenhg = "" if placenhg == "9999999999"
		assert mi(placenhg) if year != 1940
		
		gen count_in_ipums_city = 1 if city != 0 | ~mi(placenhg)
		replace count_in_ipums_city = 0 if city == 0 & mi(placenhg)

		gcollapse (sum) count_in_ipums_city count, by(year)

		gen match_rate_ipums = count_in_ipums_city/count

		save "$ipumscollapsed/ipums_late_match_rates.dta", replace

		append using "$ipumscollapsed/ipums_early_match_rates.dta"
		sort year

		save "$ipumscollapsed/ipums_match_rates.dta", replace

	*Load our data
		use "$ipumscollapsed/all_raw_place_data.dta"

		gen count = numobs 
		gen count_matched = count if ~mi(lat)
		replace count_matched = 0 if mi(lat)
		collapse (sum) count count_matched, by(year)
		
		gen match_rate_clp = count_matched/count
		

		sort  year
		merge 1:1 year using "$ipumscollapsed/ipums_match_rates.dta"

		label variable match_rate_ipums   "Public match rate" 
		label variable match_rate_clp "CPP match rate "

		keep year match_rate_ipums match_rate_clp

		save "$ipumscollapsed/match_place_comparison.dta", replace 
		
		graph bar match_rate_ipums match_rate_clp, over(year) legend(label(1 "Public data") label(2 "CPP data") position(6) rows(1)) ytitle("Share of observations with valid sub-county location")
				graph export "$figures/match_rates.eps", replace 

	cd $figures
	!mogrify -density 2000 -format png -- *.eps

