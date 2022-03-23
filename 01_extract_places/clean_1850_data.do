*clean_1850_data.do
*written by Peter Nencka and Ezra Karger
*written on: September 23, 2020
*last modified: October 11, 2021

clear all

global bulk_path "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums "/disk/data3/census-ipums/v2019/dat/"
global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/01_extract_places"


capture log close
log using $logs/clean_1850_data.log, text replace

set more off
set rmsg on


infix                          ///
  str     rectypep              1-1        ///
  str     serialp               8-15       ///
  str     pernum                16-19       ///
  str     age                   54-56       ///
  str     sex                   57-57       ///
  str     race                  58-60       ///
  str     histid                3109-3144   ///
  using "$ipums/us1850c_usa.dat" if rectypep=="P" 

rename serialp serial
destring serial, replace 
sort serial
compress
tempfile personlev
save "`personlev'"

clear


infix                             ///
  str     rectype              1-1        ///
  int     year                 2-5        ///
  str     datanum              6-7        ///
  str     serial               8-15       ///
  str     numprec              16-17      ///
  str     subsamp              18-19      ///
  int     dwsize               26-29      ///
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
  int     gqtype               73-75      ///
  byte    gqfunds              76-77      ///
  byte    farm                 78-78      ///
  byte    pageno               103-106    ///
  byte    nfams                182-183    ///
  byte    ncouples             184-184    ///
  byte    nmothers             185-185    ///
  byte    nfathers             186-186    ///
  long    nengpop              469-473    ///
  long    urbpop               484-488    ///
  byte    hhtype               509-509    ///
  int     cntry                1051-1053  ///
  byte    headloc              1068-1069  ///
  int     nhgisjoin            1108-1114  ///
  int     yrstcounty           1115-1124  ///
  long    stcounty             1125-1130  ///
  byte    appal                1131-1132  ///
  byte    county               1135-1138  ///
  double  hhwt                 1151-1160  ///
  str     stdcity              1388-1417  ///
  str     gqstr                1594-1693  ///
  str     dwelling             1709-1716  ///
  str     mdstatus             1785-1785  ///
  str     ward                 1798-1800  ///
  str  samprule             1808-1808  ///
  str     reel                 1814-1817  ///
  str     line                 1830-1832  ///
  int     enumdist             1854-1857  ///
  int     split                2003-2003  ///
  double  splithid             2004-2011  ///
  int     splitnum             2012-2015  ///
  str     us1850c_0010                2515-2515  ///
  str     us1850c_0011                2516-2523  ///
  str     us1850c_0012                2524-2526  ///
  str     us1850c_0013                2527-2530  ///
  str     us1850c_0014                2531-2534  ///
  str     us1850c_0015                2535-2538  ///
  str     us1850c_0016                2539-2541  ///
  str     us1850c_0017                2542-2543  ///
  str     us1850c_0018                2544-2545  ///
  str     us1850c_0019                2546-2549  ///
  str     us1850c_0020                2550-2551  ///
  str     us1850c_0021                2552-2555  ///
  str     us1850c_0022                2556-2562  ///
  str     us1850c_0023                2563-2567  ///
  str     us1850c_0024                2568-2571  ///
  str     us1850c_0025                2572-2574  ///
  str     us1850c_0026                2575-2578  ///
  str     us1850c_0027                2579-2581  ///
  str     us1850c_0028                2582-2583  ///
  str     us1850c_0029                2584-2590  ///
  str     us1850c_0030                2591-2595  ///
  str     us1850c_0031                2596-2599  ///
  str     us1850c_0032                2600-2600  ///
  str     us1850c_0033                2601-2604  ///
  str     us1850c_0034                2605-2605  ///
  str     us1850c_0035                2606-2609  ///
  str     us1850c_0036                2610-2611  ///
  str     us1850c_0037                2612-2612  ///
  str     us1850c_0038                2613-2613  ///
  str     us1850c_0039                2614-2620  ///
  str     us1850c_0040                2621-2625  ///
  str     us1850c_0041                2626-2725  ///
  str     us1850c_0042                2726-2765  ///
  str     us1850c_0043                2766-2835  ///
  str     us1850c_0044                2836-2839  ///
  str     us1850c_0045                2840-2844  ///
  str     us1850c_0046                2845-2869  ///
  str     us1850c_0047                2870-2886  ///
  str     us1850c_0048                2890-2894  ///
  str     us1850c_0049                2895-2899  ///
  str     us1850c_0050                2900-2900  ///
  str     us1850c_0051                2901-2950  ///
  str     us1850c_0052                2951-3000  ///
  str     us1850c_0053                3001-3100  ///
  str     us1850c_0054                3101-3150  ///
  str     us1850c_0055                3151-3153  ///
  str     us1850c_0056                3154-3161  ///
  str     us1850c_0057                3162-3165  ///
  str     us1850c_0058                3166-3166  ///
  using "$ipums/us1850c_usa.dat" if rectype=="H"


destring serial, replace 
sort serial
merge 1:m serial using "`personlev'"
tab _merge, m
drop _merge

compress
desc, fullnames
list in 1/50

save "$bulk_path/raw_1850_data.dta", replace

preserve

gen temp = mod(_n,round(_n/100000))
keep if temp==1
di _n
save "$bulk_path/raw_1850_data_100krows.dta", replace

restore


gen numobs=1

collapse (sum) numobs, by(stateicp stcounty enumdist stdcity city us1850c_0042 us1850c_0043 us1850c_0053 us1850c_0054 )

gen long indexcollapse=_n
save $bulk_path/placecounts_1850.dta, replace


cap log close

