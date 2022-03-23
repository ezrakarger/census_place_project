*clean_1870_data.do
*Written by Ezra Karger
*Written on: September 12, 2020
*Last modified: October 11, 2021

clear all

global bulk_path "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums "/disk/data3/census-ipums/v2019/dat/"
global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/01_extract_places"

capture log close
log using $logs/clean_1870_data.log, text replace

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
  str     marst                 65-65       ///
  str     bpl                   73-77       ///
  str     nativity             86-86      ///
  str     citizen              87-87      ///
  str     hispan               88-90      ///
  str     school               110-110    ///
  str     lit                  114-114    ///
  str     labforce             118-118    ///
  str     occ1950              119-121    ///
  str     ind1950              126-128    ///
  str     realprop             188-193    ///
  str     racesing             501-502    ///
  str     namelast             1697-1712    ///
  str     namefrst             1713-1728    ///
  str     vote                 2085-2085    ///
  str     agemonth             2174-2175    ///
  str     histid               2349-2384    ///
  using $ipums/us1870c_usa.dat if rectypep=="P" 

rename serialp serial
rename samplep sample

tab sample, m

compress
tempfile personlev
save "`personlev'"

clear

quietly infix                          ///
  str     rectype              1-1        ///
  int     year                 2-5        ///
  str     sample               6-11        ///
  str     serial               12-19       ///
  str     numprec              20-21      ///
  str     subsamp              22-23      ///
  byte    region               37-38      ///
  byte    stateicp             39-40      ///
  byte    statefip             41-42      ///
  int     sea                  48-50      ///
  byte    metro                51-51      ///
  int     metarea              52-55      ///
  int     metdist              56-59      ///
  int     city                 60-63      ///
  long    citypop              64-68      ///
  byte    sizepl               69-70      ///
  byte    urban                71-71      ///
  int     urbarea              72-75      ///
  byte    gq                   76-76      ///
  int     gqtype               77-79      ///
  byte    gqfunds              80-81      ///
  byte    farm                 82-82      ///
  byte    nfams                186-187    ///
  byte    ncouples             188-188    ///
  byte    nmothers             189-189    ///
  byte    nfathers             190-190    ///
  byte    qmortgag             200-200    ///
  byte    qgqtype              237-237    ///
  long    nengpop              473-477    ///
  long    urbpop               484-488    ///
  byte    hhtype               513-513    ///
  int     cntry                1055-1057  ///
  byte    headloc              1072-1073  ///
  str     countynhg            1112-1118  ///
  str     yrstcounty           1119-1128  ///
  long    stcounty             1129-1134  ///
  int     countyicp            1139-1142  ///
  double  hhwt                 1155-1164  ///
  str     reel                 1818-1821  ///
  str     line                 1834-1836  ///
  byte    qgqfunds             1950-1950  ///
  byte    split                2007-2007  ///
  double  splithid             2008-2015  ///
  int     splitnum             2016-2019  ///
  str 	  US1870C_0010		   2114-2114  ///
  str 	  US1870C_0011		   2115-2122  ///
  str 	  US1870C_0012		   2123-2125  ///
  str 	  US1870C_0013		   2126-2129  ///
  str 	  US1870C_0014		   2130-2133  ///
  str 	  US1870C_0015		   2134-2141 ///
  str 	  US1870C_0016		   2142-2144 ///
  str 	  US1870C_0017		   2145-2151 ///
  str 	  US1870C_0018		   2152-2156 ///
  str 	  US1870C_0019		   2157-2158 ///
  str 	  US1870C_0020		   2159-2160 ///
  str 	  US1870C_0021		   2161-2164 ///
  str 	  US1870C_0022		   2165-2166 ///
  str 	  US1870C_0023		   2167-2170 ///
  str 	  US1870C_0024		   2171-2177 ///
  str 	  US1870C_0025		   2183-2189 ///
  str 	  US1870C_0026		   2195-2198 ///
  str 	  US1870C_0027		   2199-2199 ///
  str 	  US1870C_0028		   2200-2203 ///
  str 	  US1870C_0029		   2204-2204 ///
  str 	  US1870C_0030		   2205-2208 ///
  str 	  US1870C_0031		   2209-2210 ///
  str 	  US1870C_0032		   2211-2211 ///
  str 	  US1870C_0033		   2212-2212 ///
  str 	  US1870C_0034		   2213-2219 ///
  str 	  US1870C_0035		   2225-2264 ///
  str 	  US1870C_0036		   2265-2334 ///
  str 	  US1870C_0037		   2335-2337 ///
  str 	  US1870C_0038		   2338-2397 ///
  str 	  US1870C_0039		   2398-2416 ///
  str 	  US1870C_0040		   2417-2457 ///
  str 	  US1870C_0041		   2458-2477 ///
  str 	  US1870C_0042		   2478-2508 ///
  str 	  US1870C_0043		   2509-2583 ///
  str 	  US1870C_0044		   2584-2658 ///
  str 	  US1870C_0045		   2659-2664 ///
  str 	  US1870C_0046		   2665-2665 ///
  str 	  US1870C_0047		   2666-2765 ///
  str 	  US1870C_0049		   2766-2768 ///
  str 	  US1870C_0050		   2769-2770 ///
  str 	  US1870C_0051		   2803-2810 ///
  str 	  US1870C_0052		   2811-2814 ///
  str 	  US1870C_0053		   2815-2815 ///
  str 	  US1870C_0054		   2178-2182 ///
  str 	  US1870C_0055		   2190-2194 ///
  str 	  US1870C_0056		   2220-2224 ///
  str 	  US1870C_0057		   2771-2779 ///
  str 	  US1870C_0058		   2780-2780 ///
  str 	  US1870C_0059		   2781-2781 ///   
  str 	  US1870C_0060		   2782-2782 ///
  str 	  US1870C_0061		   2783-2801 ///
  str 	  US1870C_0062		   2802-2802 ///
  using $ipums/us1870c_usa.dat if rectype=="H" 

merge 1:m sample serial using "`personlev'"
tab _merge, m
drop _merge

compress
desc, fullnames
list in 1/50

save $bulk_path/raw_1870_data.dta, replace

preserve

gen temp = mod(_n,round(_N/100000))
keep if temp==1
di _N
save $bulk_path/raw_1870_data_100krows.dta, replace

restore

gen numobs=1

collapse (sum) numobs, by(stateicp statefip countyicp US1870C_0035 US1870C_0036 US1870C_0040 US1870C_0042 US1870C_0043 US1870C_0044 )

gen long indexcollapse=_n
save $bulk_path/placecounts_1870.dta, replace

log close

