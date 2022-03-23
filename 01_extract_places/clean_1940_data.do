*clean_1940_data.do
*Written by Peter Nencka and Ezra Karger
*Written on: September 2, 2020
*Last modified on: October 11, 2021

clear all

global bulk_path "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums "/disk/data3/census-ipums/v2019/dat/"
global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/01_extract_places"


capture log close
clear
log using $logs/clean_1940_data.log, text replace

set more off
set rmsg on

quietly infix                               ///
  str     rectypep              1-1         ///
  str     yearp                  2-5        ///
  str     serialp               8-15        ///
  str     pernum                16-19       ///
  str     momloc                28-29       ///
  str     poploc                32-33       ///
  str     relate                50-53       ///
  str     age                   54-56       ///
  str     sex                   57-57       ///
  str     race                  58-60       ///
  str     bpl                   69-73       ///
  str     hispan                84-86       ///
  str     school                106-106     ///
  str     lit                   110-111     ///
  str     labforce              114-114     ///
  str     occ1950               115-117     ///
  str     occscore              118-119     ///
  str     ind1950               122-124     ///
  str     racesing              497-498     ///
  str     presgl                616-618     ///
  str     namelast              1693-1708   ///
  str     namefrst              1709-1724   ///
  str     mbpl                  2082-2086   ///
  str     fbpl                  2087-2091   ///
  str     histid                2345-2380   ///
  using "/disk/data3/census-ipums/v2019/dat/us1940b_usa.dat" if rectypep=="P" 

rename serialp serial
destring serial, replace

compress
tempfile personlev
save "`personlev'"

clear

quietly infix                          ///
  str     rectype              1-1        ///
  int     year                 2-5        ///
  byte    datanum              6-7        ///
  double  serial               8-15       ///
  byte    numprec              16-17      ///
  byte    subsamp              18-19      ///
  byte    region               33-34      ///
  byte    stateicp             35-36      ///
  byte    statefip             37-38      ///
  int     sea                  44-46      ///
  byte    metro                47-47      ///
  int     metarea              48-51      ///
  int     metdist              52-55      ///
  int     city                 56-59      ///
  long    citypop              60-64      ///
  byte    sizepl               65-66      ///
  byte    urban                67-67      ///
  int     urbarea              68-71      ///
  byte    gq                   72-72      ///
  byte    farm                 78-78      ///
  byte    ownershp             80-81      ///
  int     pageno               103-106    ///
  byte    nfams                182-183    ///
  byte    ncouples             184-184    ///
  byte    nmothers             185-185    ///
  byte    nfathers             186-186    ///
  byte    qmortgag             200-200    ///
  byte    qfarm                206-206    ///
  byte    qownersh             215-215    ///
  long    urbpop               480-484    ///
  byte    hhtype               509-509    ///
  int     cntry                1051-1053  ///
  byte    nsubfam              1059-1059  ///
  byte    headloc              1068-1069  ///
  byte    multgen              1083-1084  ///
  str     countyicp            1135-1138  ///
  double  hhwt                 1151-1160  ///
  str     stdcity              1388-1417  ///
  double  dwelling             1709-1716  ///
  byte    mdstatus             1785-1785  ///
  int     reel                 1814-1817  ///
  int     line                 1830-1832  ///
  int     enumdist             1854-1857  ///
  str     street               1877-1908  ///
  byte    split                2003-2003  ///
  double  splithid             2004-2011  ///
  int     splitnum             2012-2015  ///
  str     US1940B_0073         3088-3192  ///
  str     US1940B_0074         3193-3291  ///
  using "/disk/data3/census-ipums/v2019/dat/us1940b_usa.dat" if rectype=="H" 

destring serial, replace

merge 1:m serial using "`personlev'"
tab _merge, m
drop _merge

compress

desc, fullnames
list in 1/50


save $bulk_path/raw_1940_data.dta, replace

preserve

gen temp = mod(_n,round(_N/100000))
keep if temp==1
di _N
save $bulk_path/raw_1940_data_100krows.dta, replace

restore

gen numobs=1

collapse (sum) numobs, by(stateicp statefip countyicp stdcity enumdist US1940B_0073 US1940B_0074)

gen long indexcollapse=_n
save $bulk_path/placecounts_1940.dta, replace

log close

