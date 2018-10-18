cap program drop cleanout_right_ids
program cleanout_right_ids
    version 15.1
	syntax varlist

	foreach myv in `varlist' {
		replace `myv'=1174 if `myv'== 1880 
		replace `myv'=1176 if `myv'==	1881 
		replace `myv'=1179 if `myv'==	1883 
		replace `myv'=1183 if `myv'==	1887
		replace `myv'=1358 if `myv'==	2362
		replace `myv'=1184 if `myv'==	1888
		replace `myv'=1187 if `myv'==	3
		replace `myv'=1196 if `myv'==	119
		replace `myv'=1209 if `myv'==	804
		replace `myv'=1219 if `myv'==	1987
		replace `myv'=1255 if `myv'==	2216
		replace `myv'=1261 if `myv'==	1835
		replace `myv'=1293 if `myv'==	910
		replace `myv'=1296 if `myv'==	1734
		replace `myv'=1298 if `myv'==	1993
		replace `myv'=1358 if `myv'==	2362
		replace `myv'=1362 if `myv'==	1917
		replace `myv'=1372 if `myv'==	2804
		replace `myv'=1374 if `myv'==	465
		replace `myv'=2423 if `myv'==	59

	       
	}
end
cap program drop reverse_cleanout

program reverse_cleanout
    version 15.1
	syntax varlist

	foreach myv in `varlist' {
	replace `myv'=1880 if `myv'== 1174
replace `myv'=1881 if `myv'==1176
replace `myv'=1883 if `myv'==1179
replace `myv'=1887 if `myv'== 1183
replace `myv'=1888 if `myv'==1184
replace `myv'=3 if `myv'==1187
replace `myv'=119 if `myv'==1196
replace `myv'=804 if `myv'==1209
replace `myv'=1987 if `myv'==1219
replace `myv'=2216 if `myv'==1255
replace `myv'=1835 if `myv'==1261
replace `myv'=910 if `myv'==1293
replace `myv'=1734 if `myv'==1296
replace `myv'=1993 if `myv'==1298
replace `myv'=2362 if `myv'==1358
replace `myv'=1917 if `myv'==1362
replace `myv'=2804 if `myv'==1372
replace `myv'=465 if `myv'==1374
replace `myv'=59 if `myv'==2423
	       
	}
end














/*

*/


