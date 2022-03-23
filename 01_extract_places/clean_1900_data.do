*clean_1900_data.do
*Written by Peter Nencka and Ezra Karger
*Written on: September 2, 2020
*Last modified on: October 11, 2021

clear all

global bulk_path "/disk/bulkw/karger/census_bulk/citylonglat/intermediate"
global ipums "/disk/data3/census-ipums/v2019/dat/"
global logs "/disk/bulkw/karger/census_bulk/citylonglat/programs/01_extract_places"



capture log close
clear
log using $logs/clean_1900_data.log, text replace

set more off
set rmsg on

quietly infix                          ///
  str     rectypep              1-1        ///
  str     serialp               8-15       ///
  str     pernum                16-19       ///
  str     age                   54-56       ///
  str     sex                   57-57       ///
  str     race                  58-60       ///
  str     histid                2345-2380   ///
  using "/disk/data3/census-ipums/v2019/dat/us1900m_usa.dat" if rectypep=="P" 

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
  int     hhwtreg              20-23      ///
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
  byte    ownershp             80-81      ///
  byte    mortgage             82-82      ///
  int     pageno               103-106    ///
  byte    nfams                182-183    ///
  byte    ncouples             184-184    ///
  byte    nmothers             185-185    ///
  byte    nfathers             186-186    ///
  byte    qmortgag             200-200    ///
  byte    qfarm                206-206    ///
  byte    qownersh             215-215    ///
  byte    qgqtype              233-233    ///
  long    nengpop              469-473    ///
  long    urbpop               480-484    ///
  byte    hhtype               509-509    ///
  int     cntry                1051-1053  ///
  byte    nsubfam              1059-1059  ///
  byte    headloc              1068-1069  ///
  byte    multgen              1083-1084  ///
  long    stcounty             1125-1130  ///
  byte    appal                1131-1132  ///
  int     county               1135-1138  ///
  double  hhwt                 1151-1160  ///
  str     stdcity              1388-1417  ///
  str     gqstr                1594-1693  ///
  double  dwelling             1709-1716  ///
  byte    mdstatus             1785-1785  ///
  int     reel                 1814-1817  ///
  int     numperhh             1824-1827  ///
  int     line                 1830-1832  ///
  int     enumdist             1854-1857  ///
  str     street               1877-1908  ///
  byte    qgqfunds             1946-1946  ///
  byte    split                2003-2003  ///
  double  splithid             2004-2011  ///
  int     splitnum             2012-2015  ///
  str 	  US1900M_0010		   2515-2515  ///
  str 	  US1900M_0011		   2516-2523  ///
  str 	  US1900M_0012		   2524-2526  ///
  str 	  US1900M_0013		   2527-2530  ///
  str 	  US1900M_0014		   2531-2534  ///
  str 	  US1900M_0015		   2535-2538 ///
  str 	  US1900M_0016		   2539-2541 ///
  str 	  US1900M_0017		   2542-2543 ///
  str 	  US1900M_0018		   2544-2545 ///
  str 	  US1900M_0019		   2546-2549 ///
  str 	  US1900M_0020		   2550-2551 ///
  str 	  US1900M_0021		   2552-2555 ///
  str 	  US1900M_0022		   2556-2562 ///
  str 	  US1900M_0023		   2568-2571 ///
  str 	  US1900M_0024		   2572-2574 ///
  str 	  US1900M_0025		   2575-2576 ///
  str 	  US1900M_0026		   2577-2583 ///
  str 	  US1900M_0027		   2589-2592 ///
  str 	  US1900M_0028		   2593-2593 ///
  str 	  US1900M_0029		   2594-2597 ///
  str 	  US1900M_0030		   2598-2598 ///
  str 	  US1900M_0031		   2599-2602 ///
  str 	  US1900M_0032		   2603-2606 ///
  str 	  US1900M_0033		   2607-2746 ///
  str 	  US1900M_0034		   2747-2778 ///
  str 	  US1900M_0035		   2779-2779 ///
  str 	  US1900M_0036		   2780-2780 ///
  str 	  US1900M_0037		   2781-2781 ///
  str 	  US1900M_0038		   2782-2783 ///
  str 	  US1900M_0039		   2784-2784 ///
  str 	  US1900M_0040		   2785-2785 ///
  str 	  US1900M_0041		   2786-2792 ///
  str 	  US1900M_0042		   2798-2964 ///
  str 	  US1900M_0043		   2965-3064 ///
  str 	  US1900M_0044		   3065-3104 ///
  str 	  US1900M_0045		   3105-3174 ///
  str 	  US1900M_0046		   3175-3224 ///
  str 	  US1900M_0047		   3225-3231 ///
  str 	  US1900M_0048		   3232-3234 ///
  str 	  US1900M_0049		   3235-3259 ///
  str 	  US1900M_0050		   3260-3306 ///
  str 	  US1900M_0051		   3307-3336 ///
  str 	  US1900M_0052		   3337-3411 ///
  str 	  US1900M_0053		   3412-3486 ///
  str 	  US1900M_0054		   3487-3515 ///
  str 	  US1900M_0055		   3516-3523 ///
  str 	  US1900M_0056		   3524-3538 ///
  str 	  US1900M_0057		   3539-3553 ///
  str 	  US1900M_0058		   3554-3561 ///
  str 	  US1900M_0059		   3562-3565  ///   
  str 	  US1900M_0060		   3566-3566 ///
  str 	  US1900M_0061		   2563-2567 ///
  str 	  US1900M_0062		   2584-2588 ///
  str 	  US1900M_0063		   2793-2797 ///
  using "/disk/data3/census-ipums/v2019/dat/us1900m_usa.dat" if rectype=="H" 

destring serial, replace

merge 1:m serial using "`personlev'"
tab _merge, m
drop _merge

compress

desc, fullnames
list in 1/50


save $bulk_path/raw_1900_data.dta, replace

preserve

gen temp = mod(_n,round(_N/100000))
keep if temp==1
di _N
save $bulk_path/raw_1900_data_100krows.dta, replace

restore

gen numobs=1

collapse (sum) numobs, by(stateicp statefip county stcounty stdcity US1900M_0045 US1900M_0052 enumdist)

gen long indexcollapse=_n
save $bulk_path/placecounts_1900.dta, replace

log close

