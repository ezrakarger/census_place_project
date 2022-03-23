*clean_1880_data.do
*Written by Ezra Karger
*Written on: September 12, 2020
*Last modified: October 11, 2021

clear all

global bulk_path "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums "/disk/data3/census-ipums/v2020/dat"
global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/01_extract_places"


capture log close
log using $logs/clean_1880_data.log, text replace

set more off
set rmsg on


quietly infix                               ///
  str     rectypep              1-1         ///
  str     serialp               12-19       ///
  str     pernum                20-23       ///
  str     relate                54-57       ///
  str     age                   58-60       ///
  str     sex                   61-61       ///
  str     race                  62-64       ///
  str     bpl                   73-77       ///
  str     school               110-110      ///
  str     lit                  114-114      ///
  str     occ1950              119-121      ///
  str     ind1950              126-128      ///
  str     namelast             1697-1712    ///
  str     namefrst             1713-1728    ///
  str     histid               2349-2384    ///
  using $ipums/us1880e_usa.dat if rectypep=="P" 

rename serialp serial

compress
tempfile personlev
save "`personlev'"

clear

quietly infix                             ///
  str     rectype              1-1        ///
  int     year                 2-5        ///
  str     serial               12-19      ///
  str     subsamp              22-23      ///
  byte    stateicp             39-40      ///
  byte    statefip             41-42      ///
  int     sea                  48-50      ///
  int     city                 60-63      ///
  long    citypop              64-68      ///
  int     urbarea              72-75      ///
  byte    gq                   76-76      ///
  int     gqtype               77-79      ///
  byte    gqfunds              80-81      ///
  byte    farm                 82-82      ///
  str     mcd                  462-466    ///
  byte    hhtype               513-513    ///
  int     cntry                1055-1057  ///
  str     nhgisjoin            1112-1118  ///
  str     yrstcounty           1119-1128  ///
  long    stcounty             1129-1134  ///
  str     county               1139-1142  ///
  str     mcdstr               1478-1526  ///
  str     incstr               1568-1597  ///
  str     reel                 1818-1821  ///
  str     line                 1834-1836  ///
  str     enumdist             1858-1861  ///
  str     supdist              1867-1869  ///
  str 	  US1880E_0069         2601-2630  ///
  str 	  US1880E_0070         2631-2670  ///
  str 	  US1880E_0071         2671-2719  ///
  str 	  US1880E_0072         2720-2789  ///
  using $ipums/us1880e_usa.dat if rectype=="H" 


duplicates report serial

merge 1:m serial using "`personlev'"
tab _merge, m
drop _merge

compress
desc, fullnames
list in 1/50

save $bulk_path/raw_1880_data.dta, replace

preserve

gen temp = mod(_n,round(_N/100000))
keep if temp==1
di _N
save $bulk_path/raw_1880_data_100krows.dta, replace

restore

gen numobs=1

collapse (sum) numobs, by(stateicp stcounty enumdist supdist city mcd mcdstr US1880E_0069 US1880E_0070 US1880E_0071 US1880E_0072 )

gen long indexcollapse=_n
save $bulk_path/placecounts_1880.dta, replace

log close

