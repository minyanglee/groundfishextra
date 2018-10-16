



/* This set of code takes the results of DAS_extra_regressions.do and does some post-estimation simulations on it. */




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
