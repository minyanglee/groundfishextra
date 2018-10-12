/*This file is designed to a DAS available variable
The trade restrictions are 
	1.10  length
	1.20  HP


A. for each year, pull out the permitted vessels and their lengths, and hp.
B. Pull out initial allocations, joined to lengths and hp.
	For each vessel in A, determine the initial number of DAS that they could buy from
	
	SUM(Initial_DAS) where own_ves_len/1.1<=other_ves_len & own_ves_hp/1.2<=other_ves_hp

For each vessel in A, determine the initial number of DAS they are competing with in the selling market 
	
	SUM(Initial_DAS) where 1.1*own_ves_len<=other_ves_len & 1.2*own_ves_hp<=other_ves_hp


B. For the DAS usage data, join the used DAS to 

A few ways to do this
1. Form the list of permits that are feasible counterparties for each entity
	Subset the DAS_allocation, DAS_used data to retain just people you can sell to.
	Sum up DAS_USED for each day
	Construct DAS_remaining on each day
	Do it again to get the people you can buy from
	
	Loop over each fishing vessel (1400)
	
2. Round everything to the nearest foot.
	Construct a daily sum by size class
		DAS_USED_10 would be DAS_USED by all vessels under 10 foot
		DAS_USED_12 would be DAS_USED by all vessels under 12 foot (>=DAS_USED_10)
		START_DAS_10 would be initial allocation of DAS for all vessels under 10foot

	I'd have to do this by hp as well

	Leases and PT are a bit of a pain for both of these.
	Sub-leasing is not allowed, so once something is leased, it can be considered used.

*/








global my_codedir "/home/mlee/Documents/projects/Birkenbach/MAM_code_folder/MAM"
global my_workdir "/home/mlee/Documents/projects/Birkenbach/data_folder"

pause on


global version_string 2017_11_02


clear

#delimit;
quietly do "/home/mlee/Documents/Workspace/technical folder/do file scraps/odbc_connection_macros.do";
global oracle_cxn "conn("$mysole_conn") lower";
local date: display %td_CCYY_NN_DD date(c(current_date), "DMY");
global today_date_string = subinstr(trim("`date'"), " " , "_", .);



/* construct permit-mri linkages 


*/

clear;

odbc load,  exec("SELECT app_num,
		PER_NUM AS PERMIT,
		right_id,
		AUTH_ID,
		DATE_ELIGIBLE,
		DATE_CANCELLED,
		LEN as LENMQ,
		HP as HPMQ,
		AUTH_TYPE
	  FROM MQRS.MORT_ELIG_CRITERIA 
	WHERE AUTH_ID in (1179,1183,1187,1196,1219,1255,1261,1293,1296,1362,1374,2423,1174, 1184, 1176, 1209,1298,1358,1372,2423, 1192, 2101, 1205, 1181, 1259, 1315,1204, 1297, 1308 )
	  AND FISHERY = 'MULTISPECIES'
	  AND not ((TRUNC(DATE_ELIGIBLE) =  TRUNC(NVL(DATE_CANCELLED,SYSDATE+20000))) AND (CANCEL_REASON_CODE = 7 AND AUTH_TYPE = 'BASELINE'))
	  AND DATE_ELIGIBLE IS NOT NULL
	  AND (TRUNC(DATE_CANCELLED) >= '01-MAY-03' or DATE_CANCELLED IS NULL);") $oracle_cxn;  
tempfile new_old_mri;
keep right_id auth_id date_e date_c lenmq hpmq;
save `new_old_mri', replace;
clear;


tempfile p1 permit_mri;
odbc load,  exec("SELECT app_num,
		PER_NUM AS PERMIT,
		AUTH_ID,
		RIGHT_ID AS MRI,
		DATE_ELIGIBLE,
		DATE_CANCELLED,
		LEN as LENMQ,
		HP as HPMQ,
		AUTH_TYPE
	  FROM MQRS.MORT_ELIG_CRITERIA 
	  WHERE FISHERY = 'MULTISPECIES'
		AND not ((TRUNC(DATE_ELIGIBLE) =  TRUNC(NVL(DATE_CANCELLED,SYSDATE+20000))) AND (CANCEL_REASON_CODE = 7 AND AUTH_TYPE = 'BASELINE'))
		AND DATE_ELIGIBLE IS NOT NULL
		AND (TRUNC(DATE_CANCELLED) >= '01-MAY-03' or DATE_CANCELLED IS NULL);") $oracle_cxn;  
save `p1', replace;
clear;
odbc load,  exec("SELECT app_num,
		PER_NUM AS PERMIT,
		AUTH_ID AS MRI,
		DATE_ELIGIBLE,
		DATE_CANCELLED,
		LEN as LENMQ,
		HP as HPMQ,
		AUTH_TYPE
	  FROM MQRS.MORT_ELIG_CRITERIA 
	WHERE AUTH_ID in (1179,1183,1187,1196,1219,1255,1261,1293,1296,1362,1374,2423,1174, 1184, 1176, 1209,1298,1358,1372,2423)
	  AND FISHERY = 'MULTISPECIES'
	  AND not ((TRUNC(DATE_ELIGIBLE) =  TRUNC(NVL(DATE_CANCELLED,SYSDATE+20000))) AND (CANCEL_REASON_CODE = 7 AND AUTH_TYPE = 'BASELINE'))
	  AND DATE_ELIGIBLE IS NOT NULL
	  AND (TRUNC(DATE_CANCELLED) >= '01-MAY-03' or DATE_CANCELLED IS NULL);") $oracle_cxn;  
append using `p1';

#delimit cr

replace date_eligible=dofc(date_eligible)
replace date_cancelled=dofc(date_cancelled)

format date_e date_c %td

/* one duplicate entry in mqrs */
drop if app_num==1027271
drop app_num
replace auth_type="CPH" if auth_type=="HISTORY RETENTION"
dups , drop terse

save permit_mri, replace
save `permit_mri', replace


/* Fix the baselines */
 use "$my_workdir/right_id_baselines_2018_10_09.dta", clear

 merge m:1 right_id using `new_old_mri', keep(3)
rename right right_old
rename auth right_id
keep right_old right_id

merge 1:m right_id using "$my_workdir/right_id_baselines_2018_10_09.dta", keep(3)

keep right_old right_id hp len start end

rename right_id auth_id
rename right_old right_id 
tempfile nob
save `nob', replace

 use "$my_workdir/right_id_baselines_2018_10_09.dta", clear
 append using `nob'
 gen hasa=0
 sort right_id
bysort right_id: egen tt=total(auth_id)
drop if auth_id==. & tt~=0
drop hasa tt
notes: right_id_baselines updated ONLY fills in the hp and len for the 'broken' right_ids with the correct values
drop auth_id 




save "$my_workdir/right_id_baselines_updated_2018_10_09.dta", replace























/****************************************************/
/****************************************************/
/* beginning  of leasing segment */
/* the leases are constructed on Right ids*/
/****************************************************/
/****************************************************/
tempfile leaseout leaseall

/* read in das-leasing dataset */
/* construct the lease-out subset*/
use "$my_workdir/leases_$version_string.dta", clear
keep permit_seller right_id_seller quantity date_of_trade fishing_year
rename date date
rename permit permit
rename right_id right_id
/* collapse to the right_id-date level, retain fishing year for convenience */
collapse (sum) quantity, by(date right_id fishing_year)
/* lease-outs are negative */
replace quantity=-1*quantity
/* usage marked as a negative */
assert quantity<=0

gen type="lease out"

save `leaseout', replace
/* read in das-leasing dataset */
/* construct the lease-in subset*/

use "$my_workdir/leases_$version_string.dta", clear
keep permit_buyer right_id_buyer quantity date_of_trade fishing_year
rename permit permit
rename date date
rename right_id right_id
/* collapse to the right_id-date level, retain fishing year for convenience */

collapse (sum) quantity, by(date right_id fishing_year)
gen type="lease in"


/* lease-ins are negative */

append using `leaseout'

/* after appending the leaseout data, collapse again to take care of permits that lease in and lease out on the same day */
collapse (sum) quantity, by(date right_id fishing_year)
gen type= "net lease"
rename right_id mri

/* use the permit-mri table to pull in length and hp from MQRS as the 2nd alternative to BASELINE HP and LEN*/
count
local start=r(N)
gen id=_n
tempfile tester
save `tester'
joinby mri using `permit_mri', unmatched(master)
format date %td

assert _merge==3
drop _merge
count if date>=date_eligible & (date<=date_cancelled | date_cancelled==.)

gen mark=0
replace mark=1 if date>=date_eligible & (date<=date_cancelled | date_cancelled==.)

 /*1179,1183,1187,1196,1219,1255,1261,1293,1296,1362,1374,2423,1174, 1184, 1176, 1209,1219,1298,1358,1372,2423*/

bysort id: egen matched=total(mark)
order matched, after(mark)
count if match==0
/* need to make sure that there is exactly one id that doesn't match */ 
qui tab id if match==0
assert r(N)==r(r)
/* manually fix these */ 
replace mark=1 if inlist(mri,1179,1183,1187,1196,1219,1255,1261,1293,1296,1362,1374,2423,1174, 1184, 1176, 1209,1219,1298,1358,1372,2423) & match==0
keep if mark==1

 
/* there's still a couple double-matched. This happens if I have the bad luck for a trade to occur on the same day an MRI was switched
I will keep the record with the larger lenmq
*/
bysort id (lenmq): keep if _n==_N

 
count
local end=r(N)
assert `start'==`end'
drop id _expand mark matched
save `leaseall'

/* end of leasing segment */




/****************************************************/
/****************************************************/
/* beginning  of das usage segment */
/* these are built on permit numbers*/
/****************************************************/
/****************************************************/





/* read in das-usage dataset */
use "$my_workdir/das_usage_$version_string.dta",clear
keep permit date_sail date_land fishing_year charge schema
/* I'm going to use date_sail as date_used, unless date_sail is in april of the previous  FY . Then I will use dateland */

replace date_sail=dofc(date_sail)
replace date_land=dofc(date_land)
format date_sail date_land %td

rename date_sail date_used

replace date_used=mdy(9,25,2008) if date_used==mdy(9,25,3008)

/* cast to td and fix a data error */

gen fyhand=year(date_used)
replace fyhand=fyhand-1 if month(date_used)<=4


replace date_used=date_land if month(date_used)==4 & fishing_year>fyhand
drop fyhand 

gen fyhand=year(date_used)
replace fyhand=fyhand-1 if month(date_used)<=4
drop if fyhand~=fishing

gen type="used"
rename charge quantity

/* collapse to the permit-date level, retain fishing year and schema for convenience and error checking */
collapse (sum) quantity, by(permit date_used fishing_year schema)
/* usage marked as a negative */
replace quantity=-1*quantity
assert quantity<=0

rename date_used date
gen type= "trips"
gen id=_n
count
tempfile  all
save `all'

joinby permit using `permit_mri', unmatched(master)
rename _merge _jbm
count
/*  
1. Trip entries must match to 'valid' links using the date fields
2. If a trip matches to a CPH record, this is invalid
3. If there is a DAS used, but no MRI, this is invalid and an artifact of the MONK counting of A-Days

  */
/* deal with #1 */
keep if date>=date_eligible & (date<=date_cancelled | date_cancelled==.)
/* deal with #2 */
drop if auth_type=="CPH"
format date %td
bysort id: gen count=_N

/* fix the stupid auth_id/right_id bullshit */
gen tag=0
replace tag=1 if inlist(mri,1179,1183,1187,1196,1219,1255,1261,1293,1296,1362,1374,2423,1174, 1184, 1176, 1209,1219,1298,1358,1372,2423)
bysort id: egen tt=total(tag)
drop if count>=2 & tt==1 & tag==0
drop count
bysort id: gen count=_N
drop tag tt count _expand
/* if the trip matched to two right_ids, keep the one with the largest date_cancelled or where date_cancelled is null */

bysort id (date_cancelled): keep if _n==_N
count
desc

drop _jbm id
count

append using `leaseall'
notes: A little wrong because leased days cannot be subleased.






save "$my_workdir/DAS_counterparties_$today_date_string.dta", replace




/* work on initial allocations */

clear
use "$my_workdir/mqrs_annual_2018_10_05.dta", clear

/*zero out the permit numbers for the CPH vessels */
bysort permit fishing_year: gen tt=_N
replace permit=. if tt>=2 & type_auth=="CPH"
drop tt

/*recheck */
bysort permit fishing_year: gen tt=_N
/* the only entries with tt>1 should be for permit==.*/
drop tt
gen date=mdy(5,1,fishing_year)
gen tag_start=-1
rename mri right_id
gen schema="mqannual"
format date %td





append using "$my_workdir/DAS_counterparties_$today_date_string.dta"
/* there may be duplicates here, if there were leases that were processed or trips that started on May 1  */


/*permit_mri allows me to associate permits with mris and mqrs lengths.  


there will be some entries in the DAS_Usage_lease that do not have MRIS. this is because of the MNK silliness.
So my joinby leaves out all mismatches*/

replace right_id=mri if right_id==.

drop mri date_e date_c type_auth auth_id

replace quantity=categoryA if quant==. & schema=="mqannual"
/*******************************************
ONE MORE THING TO DO.  THERE are "dupes" if there are 2 things happening on the same day.  

I think  You should

collapse (sum) quantity, by(right_id permit fishing_year date lenmq hpmq)

**************************************/





/*fillin missing hp and len from mq data */
gen n=date*-1


bysort right_id (date): replace hp=hp[_n-1] if hp==.
bysort right_id (n): replace hp=hp[_n-1] if hp==.


bysort right_id (date): replace len=len[_n-1] if len==.
bysort right_id (n): replace len=len[_n-1] if len==.

/* aggregate to the permit-right_id date level */
collapse (sum) quantity (first) lenmq hpmq, by(right_id permit fishing_year date)

keep fishing_year right_id permit  date len hp quantity

count
gen id=_n





/* I need to retain any das usages that do not match to a baseline. Not sure why something wouldn't have a baseline, but whatver*/
joinby right_id using "$my_workdir/right_id_baselines_updated_2018_10_09.dta" , unmatched(master)

count
keep if (date>=start_date & date<=end_date) | _merge==1
tsset id
local m=r(gaps)
assert `m'==0

/* this verifies  there's nothing missing*/
replace len=lenmq if len==.,
replace hp=hpmq if hp==.


drop lenmq hpmq


replace hp=450 if hp==. & right_id==3
replace hp=540 if hp==. & right_id==1880
replace hp=308 if hp==. & right_id==1917
replace hp=300 if hp==. & right_id==4295
replace len=35 if len==. & right_id==4295

replace hp=250 if permit==132200 & hp==.
replace len=23 if permit==132200 & len==.

replace hp=200 if permit==147512 & hp==.
replace len=25 if permit==147512 & len==.

replace hp=250 if permit==148652 & hp==.
replace len=24 if permit==148652 & len==.




replace hp=240 if permit==149065 & hp==.
replace len=25 if permit==149065 & len==.

replace hp=260  if permit==149603 & hp==.
replace len=24 if permit==149603 & len==.




replace hp=90 if permit==150558 & hp==.
replace len=21 if permit==150558 & len==.

replace hp=375 if permit==213369 & hp==.
replace len=35 if permit==213369 & len==.


replace hp=135 if permit==214072 & hp==.
replace len=31 if permit==214072 & len==.

replace hp=600 if permit==214351 & hp==.
replace len=31 if permit==214351 & len==.

replace hp=200  if permit==220227 & hp==.
replace len=34 if permit==220227 & len==.

replace hp=450 if permit==233204 & hp==.
replace len=36 if permit==233204 & len==.

replace hp=176 if permit==240278 & hp==.
replace len=45 if permit==240278 & len==.

replace hp=437 if permit==241948 & hp==.
replace len=39 if permit==241948 & len==.

replace hp=220 if permit==146603 & hp==.
replace len=30 if permit==146603 & len==.

replace hp=460 if permit==242611 & hp==.
replace len=42 if permit==242611 & len==.

replace hp=25 if permit==149793 & hp==.
replace len=17 if permit==242611 & len==.


replace hp=450 if right_id==2223& hp==.









drop _merge
drop start_date end_date
drop id


collapse (sum) quantity, by(right_id fishing_year date hp len)
drop if date<=mdy(4,30,2004)
tsset right_id date

tsfill, full
gen neg=date*-1

/* I want to fill in hp and len. */

bysort right_id (date): replace hp=hp[_n-1] if hp==.
bysort right_id (date): replace len=len[_n-1] if len==.


bysort right_id (neg): replace hp=hp[_n-1] if hp==.
bysort right_id (neg): replace len=len[_n-1] if len==.
replace fishing_year=year(date)
replace fishing_year=fishing_year-1 if month(date)<=4
replace quantity=0 if quantity==.

drop neg

bysort right fishing (date): gen daysleft=sum(quantity)
/* this is a little broken because there are some right id's that didn't get an allocation on may 1 that eventually leased or transferred in days and fished. 
So there's negative daysleft. this isn't abig deal -it should
be offset by right_ids that did get an allocation */

*bysort right_id (date): replace daysleft=daysleft[_n-1] if daysleft==.

keep if fishing_year<=2016
save "$my_workdir/DAS_counterparties_$today_date_string.dta", replace





/* if this code runs, you can just paste it to the bottom of DAS_counterparties 
 The rangejoins take a long time to run (12 hours or so total)
 */


global working_date_string "2018_10_11"
use "$my_workdir/DAS_counterparties_$working_date_string.dta", replace
timer clear
tempfile joiner sellers buyers

drop if daysleft==0
gen float df=float(round(daysleft,.01))

gen float len2=float(round(len,.1))
drop daysleft len

rename df daysleft
rename len2 len
compress
save `joiner', replace

/* build a dataset for the sellers...how many DAS are owned by entities that this seller can sell to 

A chunk of 50 right_ids takes about 240seconds to run. So about 90 minutes to run the counterparties. 23 loops.

*/




rename right_id right_id_seller
rename hp hp_seller
rename len len_seller
keep right_id_seller hp_seller len date
gen len_high=len_seller*1.1
egen int myg=group(right_id)

/* how many groups to loop over */
bysort myg: gen byte nvals=_n==1
count if nvals
local distinct=r(N)
drop nvals


save `sellers'

local loopnum 1

local chunk 19

local first=1
local last=`first'+`chunk'

while `last'<=`distinct'+`chunk'{
timer on 11
/*loop admin stuff */
	tempfile new
	local files `"`files'"`new'" "'  
	/*end loop admin stuff */

	use `sellers', clear
	keep if myg>=`first' & myg<=`last'
	noisily di "Now Joining  observations `first' to `last'"
	rangejoin len 0 len_high using `joiner', by(date)
	noisily di "Finshed"

	drop if right_id_seller==right_id
	gen hp_high=hp_seller*1.2
	keep if hp<=hp_high
	collapse (sum) daysleft, by(right_id_seller date)
	replace daysleft=round(daysleft,.01)
	quietly save `new'
	clear
/* more loop admin stuff */
	local first=`last'+1
	local last=`first'+`chunk'
	local ++loopnum
/* end more loop admin stuff */
timer off 11
}
di "last is `last'"
di "loopnum is `loopnum'"
clear
dsconcat `files'
timer list
save "$my_workdir/seller_days_left_$working_date_string.dta", replace

/*

You could probably run  this in parallel by splitting into 2 instances. But that's a pain.
*/


/* build a dataset for the buyers...how many DAS are owned by entities that this buyer could buy from

A chunk of 50 right_ids takes about 240seconds to run. So about 90 minutes to run the counterparties. 23 loops.

*/


use `joiner', replace



rename right_id right_id_buyer
rename hp hp_buyer
rename len len_buyer
keep right_id_buyer hp_buyer len_buyer date
gen len_low=len_buyer/1.1
egen int myg=group(right_id)

timer on 7


bysort myg: gen byte nvals=_n==1
count if nvals
local distinct=r(N)
drop nvals


save `buyers'



local loopnum 1

local chunk 5
local distinct 9

local first=1
local last=`first'+`chunk'

while `last'<=`distinct'+`chunk'{
timer on 22
/*loop admin stuff */
	tempfile new2
	local files2 `"`files2'"`new2'" "'  
	/*end loop admin stuff */

	use `buyers', clear
	keep if myg>=`first' & myg<=`last'
	noisily di "Now Joining  observations `first' to `last'"
rangejoin len len_low . using  `joiner', by(date)
	noisily di "Finshed"
	drop if right_id_buyer==right_id
	gen hp_low=hp_buyer/1.2

	drop if hp<hp_low
	collapse (sum) daysleft, by(right_id_buyer date)
	replace daysleft=round(daysleft,.01)
	quietly save `new2'
	clear
/* more loop admin stuff */
	local first=`last'+1
	local last=`first'+`chunk'
	local ++loopnum
/* end more loop admin stuff */
timer off 22
}
di "last is `last'"
di "loopnum is `loopnum'"
clear
dsconcat `files2'
timer list
save "$my_workdir/buyers_days_left_$working_date_string.dta", replace











