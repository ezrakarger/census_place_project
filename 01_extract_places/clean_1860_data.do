*clean_1860_data.do
*Written by Ezra Karger
*Written on: September 12, 2020
*Last modified: October 11, 2021

global bulk_path "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums "/disk/data3/census-ipums/v2019/dat/"
global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/01_extract_places"

capture log close
log using $logs/clean_1860_data.log, text replace

set more off
set rmsg on


quietly infix                          ///
  str     rectypep              1-1        ///
  str     samplep               6-11       ///
  str     serialp               12-19       ///
  str     pernum                20-23       ///
  str     momloc                32-33       ///
  str     poploc                36-37       ///
  str     relate                54-57       ///
  str     age                   58-60       ///
  str     sex                   61-61       ///
  str     race                  62-64       ///
  str     bpl                   73-77       ///
  str     school               110-110    ///
  str     lit                  114-114    ///
  str     occ1950              119-121    ///
  str     ind1950              126-128    ///
  str     namelast             1697-1712    ///
  str     namefrst             1713-1728    ///
  str     histid               2349-2384    ///
  using $ipums/us1860c_usa.dat if rectypep=="P" 

rename serialp serial
rename samplep sample

tab sample, m

compress
tempfile personlev
save "`personlev'"

clear

quietly infix                          ///
  str     rectype              1-1        ///
  str     sample               6-11        ///
  str     serial               12-19       ///
  byte    stateicp             39-40      ///
  byte    statefip             41-42      ///
  int     sea                  48-50      ///
  int     city                 60-63      ///
  byte    gq                   76-76      ///
  int     gqtype               77-79      ///
  int     cntry                1055-1057  ///
  str     countynhg            1112-1118  ///
  str     yrstcounty           1119-1128  ///
  long    stcounty             1129-1134  ///
  int     countyicp            1139-1142  ///
  str     reel                 1818-1821  ///
  str     line                 1834-1836  ///
  str 	  US1860C_0036		   2261-2330  ///
  str 	  US1860C_0040		   2383-2457  ///
  str 	  US1860C_0042		   2533-2582  ///
  using $ipums/us1860c_usa.dat if rectype=="H" 

merge 1:m sample serial using "`personlev'"
tab _merge, m
drop _merge

compress
desc, fullnames
list in 1/50

save $bulk_path/raw_1860_data.dta, replace

preserve

gen temp = mod(_n,round(_N/100000))
keep if temp==1
di _N
save $bulk_path/raw_1860_data_100krows.dta, replace

restore

gen numobs=1

collapse (sum) numobs, by(stateicp statefip countyicp US1860C_0036 US1860C_0040 US1860C_0042 )

gen long indexcollapse=_n
save $bulk_path/placecounts_1860.dta, replace

log close

