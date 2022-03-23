*clean_1790to1840_data.do
*Written by Ezra Karger
*Written on: December 14, 2020
*Last modified: October 11, 2021

clear all

global bulk_path "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums "/disk/data3/census-ipums/v2019/dat/"
global oldcensus "/disk/data3/cens1930/"
global cens1840 "/disk/data3/cens1930/Ancestry"
global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/01_extract_places"

capture log close
log using $logs/clean_1790to1840_data.log, text replace

set more off
set rmsg on
set linesize 255


****************ANALYZE 1790****************

use $oldcensus/usfedcen1790, clear
desc, fullnames
list in 1/10

gen numobs=1
collapse (sum) numobs, by(county township fullstate)

gen long indexcollapse=_n
save $bulk_path/placecounts_1790.dta, replace



****************ANALYZE 1800****************

use $oldcensus/usfedcen1800, clear
desc, fullnames
list in 1/10

gen numobs=1
collapse (sum) numobs, by(county township township_original fullstate)

gen long indexcollapse=_n
save $bulk_path/placecounts_1800.dta, replace



****************ANALYZE 1810****************

use $oldcensus/usfedcen1810, clear
desc, fullnames
list in 1/10

gen numobs=1
collapse (sum) numobs, by(county township fullstate)

gen long indexcollapse=_n
save $bulk_path/placecounts_1810.dta, replace



****************ANALYZE 1820****************

use $oldcensus/usfedcen1820, clear
desc, fullnames
list in 1/10

gen numobs=1
collapse (sum) numobs, by(county township fullstate)

gen long indexcollapse=_n
save $bulk_path/placecounts_1820.dta, replace



****************ANALYZE 1830****************

use $oldcensus/usfedcen1830, clear
desc, fullnames
list in 1/10

gen numobs=1
collapse (sum) numobs, by(self_residence_place_county general_township_orig self_residence_place_state)

gen long indexcollapse=_n
save $bulk_path/placecounts_1830.dta, replace



****************ANALYZE 1840****************

use $cens1840/usfedcen_1840, clear
desc, fullnames
list in 1/10

gen numobs=1
collapse (sum) numobs, by(fullstate county locality)

gen long indexcollapse=_n
save $bulk_path/placecounts_1840.dta, replace

log close

