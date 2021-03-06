/****************************************************/
/****************************************************/
/*
In the pre-period, there were length and horsepower trading restrictions.  
	In the model of prices, length and hp of each party could measure bargaining power. This is derived from a 'walk-away or threat-point' reservation utility. This the utility when a trade is not done -- it comes from having someone else to trade with
	  
	
	Alternatively, the number of DAS remaining (to buy or sell) will affect the price. So these can go in directly. I'd imagine that (a) the effect isn't linear and (b) if this is properly in the model, then 
		Length and HP shouldn't explain anything anymore
		
	There's a real-option value. elapsed should have a negative value 
	
	aggregate_days_left is a reasonable alternative to put in.


In the post period, vessels in a sector could only trade with vessels in a sector. Vessels in the CP could only trade with vessels in the CP.  It looks like the length and HP restrictions were not enforced.
	So, for the post period, the buyer_cp_days_left and seller_cp_days_left were formed with the rule above. 
	
	Len and HP shouldn't matter.  
	
	There's probably not enough variation to put in different values for buyer and seller counterparties. Probably want to just use aggregate_days_left

*/
/****************************************************/
/****************************************************/

local date: display %td_CCYY_NN_DD date(c(current_date), "DMY")
global today_date_string = subinstr(trim("`date'"), " " , "_", .)

use $my_workdir/DAS_prices_$today_date_string.dta, replace

/*scale daysleft by 1000 and construct a sum and difference variable*/
replace buyer_cp_days_left=buyer_cp_days_left/1000
replace seller_cp_days_left=seller_cp_days_left/1000


gen cpsum=buyer_cp+seller_cp
gen cpdiff=buyer_cp-seller_cp

gen hpsum=hp_s+hp_b
gen hpdiff=hp_s-hp_b

gen lensum=len_s+len_b
gen lendiff=len_s-len_b

/* Couldn't sell out of CPH prior to at least 2010. So, cph_seller doesn't belong in the pre regressions. cph_buyer doesn't either, becaue that doesn't make much sense. */

/* some exploratory */
/* Do a simple CORR */
local pre_conditional price>=5 & price<=2000   & fishing_year<=2009
local post_conditional  price>=5 & price<=2000   & fishing_year>2009


corr fishing_year cpsum cpdiff lens lend hps hpd emergency differential elapsed aggregate_days_left if `pre_conditional'
corr fishing_year lens lend hps hpd emergency differential cph_seller elapsed if `post_conditional'

graph matrix cpsum cpdiff lens lend hps hpd elapsed if `pre_conditional', half

graph matrix lens lend hps hpd cph_seller elapsed aggregate_days_left if `post_conditional', half


/**************try a few regressions ****************/

/*  */
/* linear */
local rhs_vars elapsed ib(freq).fishing_year lens lend hps hpd i.emergency i.differential i.cph_buyer i.cph_seller

foreach var of varlist price elapsed len_s len_b hp_s hp_b buyer_cp seller_cp{
	gen ln`var'=ln(`var')
}

/* The length and horsepower sums are very highly correlated.  */

local rhs_vars elapsed ib(freq).fishing_year lens lend hps hpd i.emergency i.differential



regress price `rhs_vars'  if `pre_conditional', robust



est store pre_linear_full
estat ic
/*These tests support a short model
test (2005.fishing_year) (2006.fishing_year)  (2007.fishing_year)   
test (2005.fishing_year) (2006.fishing_year)  (2007.fishing_year)  (hpd)

*/

/* estimate a short linear model 
regress price elapsed i(2004).fishing_year lens lend hps i.emergency i.cph_buyer i.cph_seller if `pre_conditional', robust
est store pre_linear_parsim
*/
/* post linear model */
regress price `rhs_vars' if `post_conditional', robust
est store post_linear
estat ic
test (2011.fishing_year) (2012.fishing_year)  (2013.fishing_year)  (2014.fishing_year)  (2015.fishing_year)  (2016.fishing_year) 
test (lens) (hpd) (hps) 
test (lens) (hpd) 
test (2011.fishing_year) (2012.fishing_year)  (2013.fishing_year)  (2014.fishing_year)  (2015.fishing_year)  (2016.fishing_year)   (lens) (hpd) 
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
test (2011.fishing_year) (2012.fishing_year)  (2013.fishing_year)  (2014.fishing_year)  (2015.fishing_year)  (2016.fishing_year)
test (lens) (hpd) (elapsed)
test (2011.fishing_year) (2012.fishing_year)  (2013.fishing_year)  (2014.fishing_year)  (2015.fishing_year)  (2016.fishing_year)  (lens) (hpd) (elapsed)

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

test (2011.fishing_year)  (2012.fishing_year)   (2013.fishing_year) (2014.fishing_year)  (2016.fishing_year) (lnhp_s) (lnhp_b)

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





