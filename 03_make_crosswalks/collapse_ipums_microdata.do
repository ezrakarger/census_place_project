*collapse_ipums_microdata.do
*Written by Ezra Karger
*Written on: June X, 2021
*Last modified on: November 21, 2021

global ipumsnew       "/disk/data3/census-ipums/v2020/dta/"
global ipums          "/disk/data3/census-ipums/v2019/dta/"

global ipumscollapsed "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global longlats       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global charsdir       "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"

global logs "/disk/homedirs/nber/karger/programs/citylonglat"

capture log close
clear
log using $logs/collapse_ipums_microdata.log, text replace

set more off
set linesize 255
set rmsg on


*Read in IPUMS data, and by long-lat, calculate rates of:
/*
Population
% of prime-age men in each occupational sector (9 big census occupations)
% of prime-age men in each industrial sector (?10ish big census industries)
Occscore of prime-age men
%native born
%born in same state
literacy (save for later--not available in all years)
*/


forval yearval = 1870(10)1940 { 
	di `yearval'

	if ("`yearval'" != "1890") {
		preserve

		use $longlats/histid_place_crosswalk_`yearval'.dta, clear
		list in 1/10

		bys histid: gen numobs=_N
		tab numobs, m
		drop if numobs>1
		drop numobs

		tempfile crosswalk
		save "`crosswalk'"		
		restore


		capture confirm file $ipumsnew/`yearval'
		if _rc != 0 {
			use histid age sex occ1950 ind1950 occscore presgl bpl statefip using $ipumsnew/`yearval', clear
		}
		else {
			use histid age sex occ1950 ind1950 occscore presgl bpl statefip using $ipums/`yearval', clear
		}

		list in 1/20
		compress

		bys histid: gen numobs=_N
		tab numobs, m
		drop if numobs>1
		drop numobs

		*Merge on longitude-latitude for each year
		merge 1:1 histid using "`crosswalk'"

		gen population=1
		gen men_primeage = (sex==1) & inrange(age,25,54)

		destring bpl statefip, replace
		tab1 bpl statefip, m
		replace bpl=trunc(bpl/100)

		gen same_state = (bpl==statefip) & inrange(statefip,1,56)
		tab bpl same_state, m
		tab statefip same_state, m

		gen born_in_us=inrange(bpl,1,99)


		gen occ_proftech=inrange(occ1950,0,99) & (men_primeage==1)
		gen occ_farmers=occ1950==100 & (men_primeage==1)
		gen occ_managers=inrange(occ1950,123,290) & (men_primeage==1)
		gen occ_clerical=inrange(occ1950,300,390) & (men_primeage==1)
		gen occ_sales=inrange(occ1950,400,490) & (men_primeage==1)
		gen occ_craftsmen=inrange(occ1950,500,595) & (men_primeage==1)
		gen occ_operatives=inrange(occ1950,600,690) & (men_primeage==1)
		gen occ_service=inrange(occ1950,700,790) & (men_primeage==1)
		gen occ_laborers=inrange(occ1950,810,970) & (men_primeage==1)
		gen occ_none=(inrange(occ1950,979,999) | missing(occ1950)) & (men_primeage==1)

		gen ind_agriculture=inrange(ind1950,105,126) & (men_primeage==1)
		gen ind_mining=inrange(ind1950,206,239) & (men_primeage==1)
		gen ind_construction=inrange(ind1950,246,246) & (men_primeage==1)
		gen ind_manufacturing=inrange(ind1950,306,499) & (men_primeage==1)
		gen ind_transportation=inrange(ind1950,506,568) & (men_primeage==1)
		gen ind_utilities=inrange(ind1950,578,598) & (men_primeage==1)
		gen ind_trade=inrange(ind1950,606,699) & (men_primeage==1)
		gen ind_services=inrange(ind1950,716,946) & (men_primeage==1)
		gen ind_other=(inrange(ind1950,976,999) | missing(ind1950) | ind1950==0) & (men_primeage==1)

		gen occscore_men = occscore if men_primeage==1
		gen presgl_men = presgl if men_primeage==1

		collapse (sum) population men_primeage same_state occ_* ind_* born_in_us (mean) occscore_men presgl_men, by(lon lat)
	
		gen year = `yearval'

		save $charsdir/placechars_`yearval'.dta, replace

	}

}

log close

