global my_codedir "/home/mlee/Documents/projects/Birkenbach/MAM_code_folder/MAM"
global my_workdir "/home/mlee/Documents/projects/Birkenbach/data_folder"
pause on
global version_string 2017_11_02

#delimit cr


cd $my_workdir


/*Running sums of usage */
tempfile running_days
use $my_workdir/das_usage_$version_string.dta, clear




gen date=dofc(date_land)
sort fishing_year date
drop if date<mdy(5,1,2004)
/*dates in the wrong fy 
I did date based on land date. I'll drop out those where the land date is in the wrong fishing year.
*/
gen fy_hand=year(date)
gen month=month(date)
replace fy_hand=fy_hand-1 if month<=4
drop if fy_hand>fishing
drop if fy_hand<fishing



collapse (sum) charge, by(fishing_year date)
bysort fishing_year (date): gen running_DAS_used=sum(charge)
keep fishing_year date running_DAS
sort fishing_year date
save `running_days'

use $my_workdir/leases_$version_string.dta, clear

/* three small data corrections on date */
replace date_of_trade=mdy(11,19,2004) if transfer_id==815
replace date_of_trade=mdy(3,4,2005) if transfer_id==1902
replace date_of_trade=mdy(2,28,2005) if transfer_id==1859


gen price=dollar/quantity


/* NOTES
1.  There's a pretty sharp divide in May 2010 when catch share starts. The value of DAS is now for use in the common pool and in monkfish.  
	Sector vessels that aren't fishing for monk, don't need to buy DAS, but still have an allocation.
2.
*/



/* Add in length and hp from MQRS
These are not baselines
 */

preserve 
tempfile seller buyer




use mqrs_old_2018_09_26.dta, clear
replace date_cancelled=dofc(date_cancelled)
replace date_eligible=dofc(date_eligible)
format date_cancelled date_eligible %td

keep right_id len hp date_e date_c
rename right_id right_id_seller 
rename len len_seller
rename hp hp_seller
rename date_cancelled date_cancelled_seller
rename date_eligible date_eligible_seller

sort right_id
save `seller'

rename right_id right_id_buyer
rename len len_buyer
rename hp hp_buyer
sort right_id
rename date_cancelled date_cancelled_buyer
rename date_eligible date_eligible_buyer

save `buyer'

restore
gen markorig=1

joinby right_id_seller using `seller', unmatched(master)
assert _merge==3

rename _merge m1
gen marksell=0
replace marksell=1 if date_of_trade>=date_eligible_seller & (date_of_trade<=date_cancelled_seller | date_cancelled_seller==.)

keep if marksell==1 

joinby right_id_buyer using `buyer', unmatched(master)
assert _merge==3
rename _merge m2
gen markbuy=0
replace markbuy=1 if date_of_trade>=date_eligible_buyer & (date_of_trade<=date_cancelled_buyer | date_cancelled_buyer==.)
keep if markbuy==1 



drop  date_cancelled_*  date_eligible_*
drop markbuy marksell m2 m1 


foreach var of varlist len_seller hp_seller len_buyer hp_buyer{
rename `var'  mqrs_`var'
}


preserve
/* Add in length and hp from Permit
These are not baselines
 */
tempfile perm_s perm_b
use permit_portfolio_2017_01_18.dta, clear
keep permit len vhp fishing_year
rename permit permit_seller
rename len len_seller
rename vhp hp_seller
save `perm_s'
rename permit permit_buyer
rename len len_buyer
rename hp hp_buyer
save `perm_b'

restore

merge m:1 permit_seller fishing_year using `perm_s', keep(1 3)
rename _merge  mps

merge m:1 permit_buyer fishing_year using `perm_b', keep(1 3)
rename _merge  mpb

foreach var of varlist len_seller hp_seller len_buyer hp_buyer{
rename `var'  perm_`var'
}
order mps mpb, last

gen len_s=mqrs_len_seller
replace len_s=perm_len_seller if len_s==.
gen len_b=mqrs_len_b

replace len_b=perm_len_b if len_b==.
gen lens=len_s+len_b
gen lend=len_s-len_b


gen hp_s=mqrs_hp_seller
replace hp_s=perm_hp_seller if hp_s==.

gen hp_b=mqrs_hp_b
replace hp_b=perm_hp_b if hp_b==.
gen  hps=hp_s+hp_b
gen hpd=hp_s-hp_b

gen fystart=mdy(5,1,fishing_year)
gen elapsed=date_of_trade-fys
save $my_workdir/DAS_prices.dta, replace



/* try a few regressions */
/* linear */
regress price elapsed  ibn.fishing_year lens lend hps hpd if price>=50 & price<=2000   & fishing_year<=2009, robust
regress price elapsed  ibn.fishing_year lens lend hps hpd if price>=5 & price<=2000   & fishing_year>2009, robust



/* log-linear */



