global my_codedir "/home/mlee/Documents/projects/Birkenbach/MAM_code_folder/MAM"
global my_workdir "/home/mlee/Documents/projects/Birkenbach/data_folder"
pause on
global version_string 2017_11_02
est drop _all
#delimit cr


/**************data processing ****************/



/*Running sums of usage */
tempfile running_days
use "$my_workdir/das_usage_$version_string.dta", clear




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
rename date date_of_trade
tsset date_of_trade
tsfill, full
gen fy_hand=year(date)
gen month=month(date)
replace fy_hand=fy_hand-1 if month<=4
replace fishing_year=fy_hand if fishing_year==.
drop month fy
bysort fishing_year (date_of_trade): replace running=running[_n-1] if running==.
save `running_days'



use "$my_workdir/leases_$version_string.dta", clear

/* three small data corrections on date */
replace date_of_trade=mdy(11,19,2004) if transfer_id==815
replace date_of_trade=mdy(3,4,2005) if transfer_id==1902
replace date_of_trade=mdy(2,28,2005) if transfer_id==1859


gen price=dollar/quantity


/* Add in length and hp from MQRS
These are not baselines
 */

preserve 
tempfile seller buyer




 use "$my_workdir/mqrs_old_2018_09_26.dta", clear
replace date_cancelled=dofc(date_cancelled)
replace date_eligible=dofc(date_eligible)
format date_cancelled date_eligible %td

gen cph_seller=(strmatch(auth_type,"CPH") | strmatch(auth_type,"*HISTORY*"))

keep right_id len hp date_e date_c cph
rename right_id right_id_seller 
rename len len_seller
rename hp hp_seller
rename date_cancelled date_cancelled_seller
rename date_eligible date_eligible_seller

sort right_id
save `seller'


use "$my_workdir/mqrs_old_2018_09_26.dta", clear
replace date_cancelled=dofc(date_cancelled)
replace date_eligible=dofc(date_eligible)
format date_cancelled date_eligible %td

gen cph_buyer=(strmatch(auth_type,"CPH") | strmatch(auth_type,"*HISTORY*"))

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
use "$my_workdir/permit_portfolio_2017_01_18.dta", clear
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


/* Add in length and hp from DAS.baselines
These are  baselines
 */
 preserve
global date_string "2018_10_03"

use "$my_workdir/right_id_baselines_$date_string.dta", replace
tempfile base_s base_b
rename right_id right_id_seller
rename hp hpb_s
rename len lenb_s
save `base_s'

rename right_id right_id_buyer
rename hp hpb_b
rename len lenb_b

save `base_b'
restore

joinby right_id_seller using `base_s', unmatched(master)
assert _merge==3

rename _merge m3
gen marksellbase=0
replace marksellbase=1 if date_of_trade>=start_date & (date_of_trade<=end_date | end_date==.)
keep if marksellbase==1 
drop start_date end_date




joinby right_id_buyer using `base_b', unmatched(master)
assert _merge==3
rename _merge m4
gen markbuybase=0
replace markbuybase=1 if date_of_trade>=start_date & (date_of_trade<=end_date | end_date==.)
keep if markbuybase==1 






/* construct a length variable for buyers based on baseline. If that match fails, construct it from MQRS. If that fails, construct it from the permit data 
repeat for sellers. repeat for horsepower */

order mps mpb , last
gen len_s=hpb_s

replace len_s=mqrs_len_seller if len_s==.
replace len_s=perm_len_seller if len_s==.


gen len_b=hpb_b
replace len_b=mqrs_len_b if len_b==.
replace len_b=perm_len_b if len_b==.



gen lensum=len_s+len_b
gen lendiff=len_s-len_b



gen hp_s=hpb_s
replace hp_s=mqrs_hp_seller if hp_s==.
replace hp_s=perm_hp_seller if hp_s==.

gen hp_b=hpb_b
replace hp_b=mqrs_hp_b if hp_b==.
replace hp_b=perm_hp_b if hp_b==.



gen  hpsum=hp_s+hp_b
gen hpdiff=hp_s-hp_b

gen fystart=mdy(5,1,fishing_year)
gen elapsed=date_of_trade-fys



drop mqrs_len_seller mqrs_hp_seller mqrs_len_buyer mqrs_hp_buyer remark1 remark2 perm_len_seller perm_hp_seller perm_len_buyer perm_hp_buyer m3 hpb_s lenb_s marksellbase m4 hpb_b lenb_b fystart end_date start_date markbuybase mps mpb

save $my_workdir/DAS_prices.dta, replace















/* code the emergency action and differential DAS based on trade date */
gen emergency= (date_of_trade>=mdy(5,1,2006) & date_of_trade<=mdy(11,21,2006))
gen differential= (date_of_trade>=mdy(11,22,2006))

/* merge in running DAS used */
merge m:1 date_of_trade using `running_days', keep(1 3)
assert _merge==3
drop _merge


/**************try a few regressions ****************/

/*  */
/* linear */
local pre_conditional price>=5 & price<=2000   & fishing_year<=2009
local post_conditional  price>=5 & price<=2000   & fishing_year>2009
local rhs_vars elapsed ib(freq).fishing_year lens lend hps hpd i.emergency i.differential i.cph_buyer i.cph_seller

foreach var of varlist price elapsed len_s len_b hp_s hp_b{
	gen ln`var'=ln(`var')
}


regress price `rhs_vars'  if `pre_conditional', robust
est store pre_linear_full
estat ic
test (2005.fishing_year) (2006.fishing_year)  (2007.fishing_year)   
test (2005.fishing_year) (2006.fishing_year)  (2007.fishing_year)  (hpd)
/*These tests support a short model*/

/* estimate a short linear model */
regress price elapsed i(2004).fishing_year lens lend hps i.emergency i.cph_buyer i.cph_seller if `pre_conditional', robust
est store pre_linear_parsim

/* post linear model */
regress price `rhs_vars' if `post_conditional', robust
est store post_linear
estat ic
test (2011.fishing_year) (2012.fishing_year)  (2013.fishing_year)  (2014.fishing_year)  (2015.fishing_year)  (2016.fishing_year) (2017.fishing_year)
test (lens) (hpd) (hps) 
test (lens) (hpd) 
test (2011.fishing_year) (2012.fishing_year)  (2013.fishing_year)  (2014.fishing_year)  (2015.fishing_year)  (2016.fishing_year)  (2017.fishing_year) (lens) (hpd) 
regress price elapsed i(2010).fishing_year lend hps i.cph_seller if `post_conditional', robust
est store post_linear_parsim


/* log-linear using a glm instead of log transforming lhs*/
glm price `rhs_vars' if `pre_conditional', link(log) family(poisson) robust
est store pre_semilog
test (2005.fishing_year)  (2007.fishing_year)   (2008.fishing_year)  
test (2005.fishing_year)  (2007.fishing_year)   (2008.fishing_year)  (hpd)

glm price elapsed i(2004 2006 2009).fishing_year lens lend hps i.emergency i.cph_buyer i.cph_seller if `pre_conditional', link(log) family(poisson) robust
glm price elapsed i(2004 2006).fishing_year lens lend hps i.emergency i.cph_buyer i.cph_seller if `pre_conditional', link(log) family(poisson) robust
est store pre_semilog_parsim


glm price `rhs_vars' if `post_conditional', link(log) family(poisson) robust
est store post_semilog
test (2011.fishing_year) (2012.fishing_year)  (2013.fishing_year)  (2014.fishing_year)  (2015.fishing_year)  (2016.fishing_year) (2017.fishing_year)
test (lens) (hpd) (elapsed)
test (2011.fishing_year) (2012.fishing_year)  (2013.fishing_year)  (2014.fishing_year)  (2015.fishing_year)  (2016.fishing_year) (2017.fishing_year)  (lens) (hpd) (elapsed)

glm price i(2010).fishing_year lend hps i.cph_buyer i.cph_seller if `post_conditional', link(log) family(poisson) robust
est store post_semilog_parsim



/* log-log */
local lnrhs lnelapsed ib(freq).fishing_year lnlen_s lnlen_b lnhp_s lnhp_b i.emergency i.differential i.cph_buyer i.cph_seller


regress lnprice `lnrhs' if `pre_conditional', robust
test (2004.fishing_year)  (2005.fishing_year)   (2006.fishing_year)  (lnhp_s) (lnhp_b) (1.emergency)
est store pre_loglog

regress lnprice lnelapsed i(2007 2009).fishing_year lnlen_s lnlen_b  i.cph_buyer i.cph_seller if `pre_conditional', robust
est store pre_loglog_parsim


regress lnprice `lnrhs' if `post_conditional', robust
est store post_loglog

test (2011.fishing_year)  (2012.fishing_year)   (2013.fishing_year) (2014.fishing_year)  (2016.fishing_year) (2017.fishing_year) (lnhp_s) (lnhp_b)

regress lnprice lnelapsed i( 2015 ).fishing_year lnlen_s lnlen_b i.cph_seller if `post_conditional', robust
est store post_loglog_parsim







/*
local rhs_vars elapsed ib(freq).fishing_year len_s* len_b* i.emergency i.differential i.cph_buyer i.cph_seller c.len_s#c.elapsed c.len_b#c.elapsed i.fishing_year#c.elapsed
where the len*'s include up to third order polynomials.
*/
local rhs_vars elapsed ib(freq).fishing_year c.len_s##(c.len_s#c.len_s)  c.len_b##(c.len_b#c.len_b)  (c.len_s c.len_b)#c.elapsed  i.emergency i.differential i.cph_buyer i.cph_seller i.fishing_year#c.elapsed

regress price `rhs_vars' if `pre_conditional', robust
est store linear_ab_pre


local rhs_vars elapsed ib(freq).fishing_year c.len_s##(c.len_s#c.len_s)  c.len_b##(c.len_b#c.len_b)  (c.len_s c.len_b)#c.elapsed  i.emergency i.differential i.cph_buyer i.cph_seller i.fishing_year#c.elapsed

regress price `rhs_vars' if `post_conditional', robust
est store linear_ab_post


























/* use the results of pre_linear to predict the smallest buy price and the largest sell price for each vessel on each day.*/





/* step 1 - build a panel of permit's and fishing_years 
from 2014 to 2018. There may be duplicate permit numbers due to the way CPH is done.
*/
use "$my_workdir/mqrs_old_2018_09_26.dta", clear
drop remark*
gen cph=(strmatch(auth_type,"CPH") | strmatch(auth_type,"*HISTORY*"))


/* cast the date_cancelled to td and replace null cancelled with the end of FY2018 */
replace date_cancelled=dofc(date_cancelled)
format date_c %td
replace date_cancelled=mdy(4,30,2019) if date_cancelled==.
drop if date_cancelled<mdy(5,1,2004)
/* cast the eligible to td. Replace anything before with beginning of FY 2004 */
replace date_eligible=dofc(date_eligible)
format date_e %td
replace date_eligible=mdy(5,1,2004) if date_eligible<mdy(5,1,2004)

rename date_eligible date1
rename date_c date2
gen exp=date2-date1+1

keep per_num right_id hull_id date1 date2 cph len hp exp

gen id=_n
expand exp

bysort id: gen mydate=date1+_n-1 
format mydate %td


foreach var of varlist len hp{
rename `var'  mqrs_`var'
}
rename per_num permit
drop date1 date2 exp
gen fishing_year=year(mydate)
replace fishing_year=fishing_year-1 if month(mydate)<=4



preserve
/* Add in length and hp from Permit
These are not baselines
 */
tempfile permits
use "$my_workdir/permit_portfolio_2017_01_18.dta", clear
keep permit len vhp fishing_year
save `permits'
restore

merge m:1 permit fishing_year using `permits', keep(1 3)
rename vhp hp
foreach var of varlist len hp{
rename `var'  perm_`var'
}

gen length=mqrs_len
replace length=perm_len if length==.

gen hp=mqrs_hp
replace hp=perm_hp if hp==.

bysort id: replace hp = hp[_n-1] if hp >= . 
bysort id: replace len = len[_n-1] if len >= . 

rename mydate date_of_trade
gen emergency= (date_of_trade>=mdy(5,1,2006) & date_of_trade<=mdy(11,21,2006))
gen differential= (date_of_trade>=mdy(11,22,2006))
gen fystart=mdy(5,1,fishing_year)
gen elapsed=date_of_trade-fys

/* now, I have permit number, hull id, cph, length, hp, emergency, differential, and elapsed) */
/* Construct RHS variables to compute sellers price 

sellers get the highest prices when selling to a large, powerful vessel. 

1.10  length
1.20  HP
 
gen lens=len_s+len_b
gen lend=len_s-len_b
*/
preserve
gen len_b=length*1.10
gen len_s=length
gen hp_b=hp*1.20
gen hp_s=hp

gen lens=len_s+len_b
gen lend=len_s-len_b

gen hps=len_s+len_b
gen hpd=len_s-len_b

gen cph_buyer=1
gen cph_seller=cph
foreach var of varlist elapsed len_s len_b hp_s hp_b{
	gen ln`var'=ln(`var')
}

/* actually do the predictions */
est restore pre_linear_parsim
predict linear_pre_price_sell, xb

replace linear_pre_price_sell=. if fishing_year>=2010

est restore pre_semilog_parsim
predict semilog_pre_price_sell, mu
replace semilog_pre_price_sell=. if fishing_year>=2010

est restore pre_loglog_parsim
predict loglog_pre_price_sell, xb
replace loglog_pre_price_sell = exp(loglog_pre_price_sell)*exp(e(rmse)^2/2) 
replace loglog_pre_price_sell=. if fishing_year>=2010



est restore linear_ab_pre
predict ab_pre_price_sell, xb
replace ab_pre_price_sell=. if fishing_year>=2010




est restore post_linear_parsim
predict linear_post_price_sell, xb
replace linear_post_price_sell=. if fishing_year<2010

est restore post_semilog_parsim
predict semilog_post_price_sell, mu
replace semilog_post_price_sell=. if fishing_year<2010


est restore post_loglog_parsim
predict loglog_post_price_sell, xb
replace loglog_post_price_sell = exp(loglog_post_price_sell)*exp(e(rmse)^2/2) 
replace loglog_post_price_sell=. if fishing_year<2010






est restore linear_ab_post
predict ab_post_price_sell, xb
replace ab_post_price_sell=. if fishing_year<2010











summ linear_post_price_sell semilog_post_price_sell linear_pre_price_sell semilog_pre_price_sell ab_*

save $my_workdir/predicted_sell_prices.dta, replace


restore
/* Construct RHS variables to compute a buyers price 

buyers get the lowest prices when buying from small vessels. 

1.10  length
1.20  HP
 
gen lens=len_s+len_b
gen lend=len_s-len_b


*/
gen len_s=length/1.10
gen len_b=length
gen hp_s=hp/1.20
gen hp_b=hp



gen lens=len_s+len_b
gen lend=len_s-len_b

gen hps=len_s+len_b
gen hpd=len_s-len_b

gen cph_buyer=1
gen cph_seller=cph



foreach var of varlist elapsed len_s len_b hp_s hp_b{
	gen ln`var'=ln(`var')
}

est restore pre_linear_parsim
predict linear_pre_price_buy, xb

replace linear_pre_price_buy=. if fishing_year>=2010


est restore linear_ab_pre
predict ab_pre_price_buy, xb
replace ab_pre_price_buy=. if fishing_year>=2010



est restore pre_semilog_parsim
predict semilog_pre_price_buy, mu
replace semilog_pre_price_buy=. if fishing_year>=2010


est restore pre_loglog_parsim
predict loglog_pre_price_buy, xb
replace loglog_pre_price_buy = exp(loglog_pre_price_buy)*exp(e(rmse)^2/2) 
replace loglog_pre_price_buy=. if fishing_year>=2010



est restore post_linear_parsim
predict linear_post_price_buy, xb
replace linear_post_price_buy=. if fishing_year<2010

est restore post_semilog_parsim
predict semilog_post_price_buy, mu
replace semilog_post_price_buy=. if fishing_year<2010



est restore post_loglog_parsim
predict loglog_post_price_buy, xb
replace loglog_post_price_buy = exp(loglog_post_price_buy)*exp(e(rmse)^2/2) 
replace loglog_post_price_buy=. if fishing_year<2010



est restore linear_ab_post
predict ab_post_price_buy, xb
replace ab_post_price_buy=. if fishing_year<2010







summ *price*
save "$my_workdir/predicted_buy_prices.dta", replace

/* take a look at the sell prices compared to the buy prices */
use "$my_workdir/predicted_sell_prices.dta", replace
summ *price*


/* NOTES
1.  There's a pretty sharp divide in May 2010 when catch share starts. The value of DAS is now for use in the common pool and in monkfish.  
	Sector vessels that aren't fishing for monk, don't need to buy DAS, but still have an allocation.



  
*/


