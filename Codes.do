///Import and code exposure dates CPRD GOLD
{
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/AVF2_ReproductiveAgedWomenCohort20230612045053.csv", encoding(ISO-8859-9) clear
ds bd_* 
foreach var in `r(varlist)' {
local newname = substr("`var'", 8, .)
gen EDate_`newname' = date(`var', "YMD")
format EDate_`newname' %tdDD/NN/CCYY
}
//
rename *_birm* *
rename *_and* * 
rename *_mm* *
rename *_mump* *
rename *_bham* *

rename *_11_3_21* ** 
rename *systemic_lupus_erythemato *SLE
rename *_120421 * 
rename *2021 * 


gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
format patient_id %12.0g
keep patient_id EDate_*
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/Exposure_dates.csv", replace
}
//

//Ascertain exposure 
{
use "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnanciesAnalysis.dta" , clear 
merge m:1 patient_id using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/Exposure_dates.csv"
keep if _merge == 3 

ds EDate*
foreach var in `r(varlist)' {
local newname = substr("`var'", 7 , .)
gen E_`newname' = 0 
replace E_`newname' = 1 if `var' != . & `var' < PregStartDate
}


save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithExposuresAnalysis.dta" , replace
}

gen E_AI=0

replace E_AI=1 if ( E_addisons_disease ==1) | ( E_alopeciaareata ==1) | ( E_ankylosingspondylitis ==1) |( E_rheumatoidarthritis ==1) | ( E_coeliac_disease ==1) | ( E_ulcerative_colitis ==1) | ( E_crohns_disease ==1) | ( E_inflammatory_bowel ==1) | ( E_ms ==1) | ( E_myasthenia_gravis ==1) | ( E_psoriasis ==1) | ( E_psoriaticarthritis ==1) | ( E_sjogrenssyndrome ==1) | ( E_SLE ==1) | ( E_systemic_sclerosis ==1) | ( E_graves ==1) | ( E_hashimoto ==1) |  ( E_type1diabetes18 ==1) | ( E_vitiligo ==1)

gen EDate_AI= min(EDate_addisons_disease, EDate_alopeciaareata, EDate_ankylosingspondylitis, EDate_rheumatoidarthritis, EDate_coeliac_disease, EDate_crohns_disease, EDate_ms, EDate_myasthenia_gravis, EDate_psoriasis, EDate_psoriaticarthritis, EDate_sjogrenssyndrome, EDate_SLE, EDate_systemic_sclerosis, EDate_graves, EDate_hashimoto, EDate_type1diabetes18, EDate_vitiligo)

format EDate_AI %12.0g

save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithExposuresAnalysis.dta" , replace

//Importing the long type file for BMI in order to obtain the latest record of BMI prior to the pregnancy start date 
{
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/AVF3_ReproductiveAgedWomenCohort20230613120933_BMI_1.csv" , clear 
gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
format patient_id %12.0g


////Remove extra number in the pateint id
//rename patient_id patid
//gen double old_patid = patid
//drop patid
//tostring old_patid, replace
//gen patid = substr(old_patid,1,strlen(old_patid)-3) + "10" + substr(old_patid,-3,.)

//rename patid patient_id


gen BD_BMI = date(event_date, "YMD")
format BD_BMI %tdDD/NN/CCYY

gen BMI =real(bmivalue)
replace BMI = . if BMI <14 | BMI > 75
replace BD_BMI = . if BMI == . 

keep patient_id BD_BMI BMI 

append using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithExposuresAnalysis.dta" ,  force
gen Date1 = min(BD_BMI , PregStartDate)
format  Date1 %tdDD/NN/CCYY
sort patient_id Date1 

gen BDate_BMI = .
forval i = 1/100 {
replace BDate_BMI = BD_BMI[_n-`i'] ///
				if PregStartDate != . & ///
				BDate_BMI == . & ///
				patient_id == patient_id[_n-`i'] & ///
				BD_BMI[_n-`i'] < PregStartDate +(16*7) ///
			
				//Latest recorded BMI date before pregnancy start date 
}
format BDate_BMI %tdDD/NN/CCYY

gen B_BMI = .
forval i = 1/100 {
replace B_BMI = BMI[_n-`i'] ///
				if PregStartDate != . & ///
				BDate_BMI == BD_BMI[_n-`i'] & ///
				patient_id == patient_id[_n-`i'] & ///
				BD_BMI[_n-`i'] < PregStartDate +(16*7) ///
			
				//Latest recorded BMI before pregnancy start date 
}
order patient_id BD_BMI BMI PregStartDate PregEndDate BDate_BMI B_BMI 
br patient_id BD_BMI BMI PregStartDate PregEndDate BDate_BMI B_BMI 

drop if PregStartDate == . 
drop BD_BMI BMI Date1
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithExpBMIAnalysis.dta", replace 
}
//



//Importing the long type file for smoking status in order to obtain the latest record of smoking prior to the pregnancy start date 
**#
{
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/AVF9_ReproductiveAgedWomenCohort20230719115850_Smoking_1.csv" , clear 
gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
format patient_id %12.0g

gen BD_Smoking = date(event_date, "YMD")
format BD_Smoking %tdDD/NN/CCYY
keep patient_id BD_Smoking status 

append using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithExpBMIAnalysis.dta" 
gen Date1 = min(BD_Smoking , PregStartDate)
format  Date1 %tdDD/NN/CCYY
sort patient_id Date1 

gen BDate_Smoking = .
forval i = 1/100 {
replace BDate_Smoking = BD_Smoking[_n-`i'] ///
				if PregStartDate != . & ///
				BDate_Smoking == . & ///
				patient_id == patient_id[_n-`i'] & ///
				BD_Smoking[_n-`i'] < PregStartDate ///
			
				//Latest recorded BMI date before pregnancy start date 
}
format BDate_Smoking %tdDD/NN/CCYY

gen Smoking = .
forval i = 1/100 {
replace Smoking = status[_n-`i'] ///
				if PregStartDate != . & ///
				BDate_Smoking == BD_Smoking[_n-`i'] & ///
				patient_id == patient_id[_n-`i'] & ///
				BD_Smoking[_n-`i'] < PregStartDate ///
			
				//Latest recorded BMI before pregnancy start date 
}
recode Smoking (2=1 "Non-Smoker") (3=2 "Ex-Smoker") (1=3 "Current Smoker") (0/.=4 "Missing data"), gen(B_Smoking)

order patient_id BD_Smoking status PregStartDate PregEndDate BDate_Smoking Smoking B_Smoking
br patient_id BD_Smoking status PregStartDate PregEndDate BDate_Smoking Smoking B_Smoking

drop if PregStartDate == . 
drop BD_Smoking status Smoking Date1
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithExpBMIAndSmokingAnalysis.dta" , replace 
//count 1854496
}
//

//Importing ethnicity data from Snomed CT/Read codes to create ethnicity categorisation
{
//////import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/AVF10_ReproductiveAgedWomenCohort20230719024938.csv"  , clear 
//gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
//format patient_id %12.0g

//local newname = substr("`var'", 8, strpos("`var'", "_ethnicity_cprd")- 8)
/////di "`newname'"

///gen BD_`newname'_ethnicity = date(`var', "YMD")
////format BD_`newname'_ethnicity  %tdDD/NN/CCYY
}

//gen BD_Ethnicity = max(BD_white_ethnicity, BD_asian_ethnicity, BD_black_ethnicity, BD_mixed_ethnicity, BD_other_ethnicity, BD_missing_ethnicity)
////format BD_Ethnicity %tdDD/NN/CCYY

//gen Ethnicity = 1 if Ethnicity == WHITE
//replace Ethnicity = 2 if Ethnicity ==
//replace Ethnicity = 3 if BD_Ethnicity == BD_asian_ethnicity
//replace Ethnicity = 5 if BD_Ethnicity == BD_other_ethnicity
//replace Ethnicity = 4 if BD_Ethnicity == BD_mixed_ethnicity

//recode Ethnicity (1=1 "White") (2=2 "Black") (3=3 "Asian") (4=4 "Mixed") (5=5 "Other") (.=6 "Missing") , gen(B_Ethnicity)

//save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/Ethnicity.dta" , replace
//}

//Importing linked IMD data
{
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/patient_2019_imd_22_002283.txt", clear 
rename patid patient_id 
keep patient_id e2019_imd_10
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/IMD.dta" , replace
} 
//MERGE Eligible pregnancies with exposure dates with eligible pregnancy with BMI and smoking


{
////Merge with Ethinicity

//use "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithExpBMIAndSmoking.dta" , clear
//drop _merge 
//merge m:1 patient_id using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/Ethnicity.dta"
//keep if _merge == 3 
//drop _merge 
//save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/Temp1.dta" ,replace


// merge IMD data
use "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithExpBMIAndSmokingAnalysis.dta", clear
drop _merge
merge m:1 patient_id using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/IMD.dta"
drop if _merge == 2 
recode e2019_imd_10 (.= 21 "Missing"), gen(B_IMD)
recode e2019_imd_10 (.= 21 "Missing"), gen(IMD)


replace IMD =5 if e2019_imd_10 == 1 | e2019_imd_10 == 2
replace IMD =4 if e2019_imd_10 == 3 | e2019_imd_10 == 4
replace IMD =3 if e2019_imd_10 == 5 | e2019_imd_10 == 6
replace IMD =2 if e2019_imd_10 == 7 | e2019_imd_10 == 8
replace IMD =1 if e2019_imd_10 == 9 | e2019_imd_10 ==10

drop _merge e2019_imd_10 

******ethnicity 
encode ethnicity, gen(Ethnicity)
recode Ethnicity (6 = 1 "White" ) (2 = 2 "Missing" ) (3 = 3 "Mixed" )(5 = 4 "Asian" ) (4 = 5 "Other" )(1 = 6 "Black" ), gen(B_Ethnicity)
************


gen gravidity = totalpregs
replace gravidity = 5 if totalpregs>5
tab gravidity
recode gravidity (1 = 1 "1") (2 = 2 "2") (3 = 3 "3") (4 = 4 "4") (5 = 5 "5+"), gen(B_gravidity)



gen parity = pregnumber
replace parity = 5 if pregnumber>5
tab parity
recode parity (1 = 1 "1") (2 = 2 "2") (3 = 3 "3") (4 = 4 "4") (5 = 5 "5+"), gen(B_parity)


//rename Gravidity B_gravidity

//recode Gravidity (1 = 1 "1") (2 = 2 "2") (3 = 3 "3") (4 = 4 "4") (5 = 5 "5+"), gen (B_gravidity)

//replace B_gravidity = 1 if B_gravidity  == 1 
//replace B_gravidity = 2 if B_gravidity == 2
//replace B_gravidity = 3 if B_gravidity  == 3
//replace B_gravidity = 4 if B_gravidity  == 4
//replace B_gravidity = 5 if B_gravidity  == 5|B_gravidity  == 6| B_gravidity  == 7|B_gravidity  == 8|B_gravidity  == 9|B_gravidity  == 10| B_gravidity  == 11|B_gravidity  == 12|B_gravidity  == 13| B_gravidity  == 14|B_gravidity  == 15|

//replace B_gravidity = 0 if B_gravidity == 16|B_gravidity  == 17|B_gravidity  == 18|B_gravidity  == 19|B_gravidity  == 20| B_gravidity  == 21| B_gravidity  == 22|B_gravidity  == 23| B_gravidity  == 24| B_gravidity  == 25| B_gravidity  == 26|


tab B_gravidity



////Getting age category
rename AgeAtStartOfPregnancy age
gen ageround = round(age)
recode ageround (15/20=1 "15 - 20 years") (21/30=2 "21 - 30 years") (31/40=3 "31 - 40  years") (41/50=4 "41 - 50  years") (51/60=5 "51 - 60 years") , gen (agecat)
label var agecat "age categories (grouped)"
tab agecat


//////BMI age  categories

egen bmigp = cut(B_BMI), at(14, 18.5, 25, 30, 75)
recode bmigp (14=1 "14-18.5") (18.5=2 "18.5-25") (25=3 "25-30")(30=4 "30-75")(.=9 "missing"),gen(bmicat)
tab bmicat if bmicat != 9
tab bmicat



rename gestdays B_GestAge
rename EndDate PatientEndDate 
drop E_AI
drop EDate_AI

gen E_AI=0

replace E_AI=1 if ( E_addisons_disease ==1) | ( E_alopeciaareata ==1) | ( E_ankylosingspondylitis ==1) |( E_rheumatoidarthritis ==1) | ( E_coeliac_disease ==1) | ( E_ulcerative_colitis ==1) | ( E_crohns_disease ==1)  | ( E_ms ==1) | ( E_myasthenia_gravis ==1) | ( E_psoriasis ==1) | ( E_psoriaticarthritis ==1) | ( E_sjogrenssyndrome ==1) | ( E_SLE ==1) | ( E_systemic_sclerosis ==1) | ( E_graves ==1) | ( E_hashimoto ==1) | ( E_hyperthyroidism_v2 ==1) | ( E_type1diabetes18 ==1) | ( E_vitiligo ==1)



gen EDate_AI= min(EDate_alopeciaareata, EDate_addisons_disease, EDate_ankylosingspondylitis, EDate_rheumatoidarthritis, EDate_coeliac_disease, EDate_ulcerative_colitis, EDate_crohns_disease, EDate_ms, EDate_myasthenia_gravis, EDate_psoriasis, EDate_psoriaticarthritis, EDate_sjogrenssyndrome, EDate_SLE , EDate_systemic_sclerosis , EDate_graves, EDate_hashimoto,  EDate_hyperthyroidism_v2, EDate_type1diabetes18, EDate_vitiligo)

format EDate_AI %tdDD/NN/CCYY


save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithCovariatesAnalysis.dta" ,replace


//////CPRD Aurum


//Import and code exposure dates 
{
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/AVF1_ReproductiveAgedWomenCohort_AURUM20230612115756.csv", encoding(ISO-8859-9) clear
ds bd_* 
foreach var in `r(varlist)' {
local newname = substr("`var'", 8, .)
gen EDate_`newname' = date(`var', "YMD")
format EDate_`newname' %tdDD/NN/CCYY
}
//
rename *_birm* *
rename *_and* * 
rename *_mm* *
rename *_mump* *
rename *_bham* *

rename *_11_3_21* **
rename *systemic_lupus_erythemato *SLE
rename *_120421 * 
rename *2021 * 


gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
format patient_id %12.0g
keep patient_id EDate_*

save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/Exposure_dates.csv", replace
}
//

//Ascertain exposure 
{
use "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/EligiblePregnanciesAnalysis.dta" , clear 
drop _merge
merge m:1 patient_id using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/Exposure_dates.csv"
keep if _merge == 3 

ds EDate*
foreach var in `r(varlist)' {
local newname = substr("`var'", 7 , .)
gen E_`newname' = 0 
replace E_`newname' = 1 if `var' != . & `var' < PregStartDate
}
//
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/EligiblePregnancies_WithExposuresAnalysis.dta" , replace
}
///4,732,797
gen E_AI=0

replace E_AI=1 if ( E_addisons_disease ==1) | ( E_alopeciaareata ==1) | ( E_ankylosingspondylitis ==1) |( E_rheumatoidarthritis ==1) | ( E_coeliac_disease ==1) | ( E_ulcerative_colitis ==1) | ( E_crohns_disease ==1) | ( E_inflammatory_bowel ==1) | ( E_ms ==1) | ( E_myasthenia_gravis ==1) | ( E_psoriasis ==1) | ( E_psoriaticarthritis ==1) | ( E_sjogrenssyndrome ==1) | ( E_SLE ==1) | ( E_systemic_sclerosis ==1) | ( E_graves ==1) | ( E_hashimoto ==1) | ( E_hyperthyroidism_v2 ==1) | ( E_type1dm ==1) | ( E_vitiligo ==1)

//
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithExposuresAnalysis.dta" , replace

/Importing the long type file for BMI in order to obtain the latest record of BMI prior to the pregnancy start date 
{
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/AVF2_ReproductiveAgedWomenCohort_AURUM20230613120955_Body_mass_index_2.csv" , clear 
gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
format patient_id %12.0g

gen BD_BMI = date(event_date, "YMD")
format BD_BMI %tdDD/NN/CCYY

gen BMI = value
replace BMI = . if BMI <14 | BMI > 75
replace BD_BMI = . if BMI == . 

keep patient_id BD_BMI BMI 

append using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/EligiblePregnancies_WithExposuresAnalysis.dta" 
gen Date1 = min(BD_BMI , PregStartDate)
format  Date1 %tdDD/NN/CCYY
sort patient_id Date1 

gen BDate_BMI = .
forval i = 1/100 {
replace BDate_BMI = BD_BMI[_n-`i'] ///
				if PregStartDate != . & ///
				BDate_BMI == . & ///
				patient_id == patient_id[_n-`i'] & ///
				BD_BMI[_n-`i'] < PregStartDate +(16*7) ///
			
				//Latest recorded BMI date before pregnancy start date 
}
format BDate_BMI %tdDD/NN/CCYY

gen B_BMI = .
forval i = 1/100 {
replace B_BMI = BMI[_n-`i'] ///
				if PregStartDate != . & ///
				BDate_BMI == BD_BMI[_n-`i'] & ///
				patient_id == patient_id[_n-`i'] & ///
				BD_BMI[_n-`i'] < PregStartDate +(16*7) ///
			
				//Latest recorded BMI before pregnancy start date 
}
order patient_id BD_BMI BMI PregStartDate PregEndDate BDate_BMI B_BMI 
br patient_id BD_BMI BMI PregStartDate PregEndDate BDate_BMI B_BMI 

drop if PregStartDate == . 
drop BD_BMI BMI Date1
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/EligiblePregnancies_WithExpBMIAnalysis.dta", replace 
}
//
//Importing the long type file for smoking status in order to obtain the latest record of smoking prior to the pregnancy start date 
**#

///////Neversmoked
	
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/AVF11_ReproductiveAgedWomenCohort_AURUM20230925121035_SmokingStatus_NeverSmoked.csv", clear  

gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
format patient_id %12.0g

generate D_NeverSmoked = date(event_date, "YMD")
format D_NeverSmoked %tdDD/NN/CCYY

generate BD_Smoking = date(event_date, "YMD")
format BD_Smoking %tdDD/NN/CCYY


gen status = 2 //coding non-smoker as 2 similar to gold

keep patient_id BD_Smoking status

save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/SmokingStatus_NeverSmoked020124Analysis.dta", replace


////// ExSmoker
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/AVF11_ReproductiveAgedWomenCohort_AURUM20230925121035_Ex_Smoker.csv", clear  

gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
format patient_id %12.0g

generate D_ExSmoker = date(event_date, "YMD")
format D_ExSmoker %tdDD/NN/CCYY

generate BD_Smoking = date(event_date, "YMD")
format BD_Smoking %tdDD/NN/CCYY


gen status = 3 //coding ex-smoker as 3 similar to gold

keep patient_id BD_Smoking status

append using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/SmokingStatus_NeverSmoked020124Analysis.dta",


save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/SmokingStatus_NeverSmoked and ExSmoker020124Analysis.dta", replace 


///// Current smoker
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/AVF11_ReproductiveAgedWomenCohort_AURUM20230925121035_Current_smoker_PUSHAsthma2", clear  

gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
format patient_id %12.0g

generate D_Smoker = date(event_date, "YMD")
format D_Smoker %tdDD/NN/CCYY

generate BD_Smoking = date(event_date, "YMD")
format BD_Smoking %tdDD/NN/CCYY

gen status = 1 //coding smoker as 1 similar to gold

keep patient_id BD_Smoking status

append using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/SmokingStatus_NeverSmoked and ExSmoker020124Analysis.dta",

tab status

save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/SmokingStatusFinal020124Analysis.dta", replace

///////Append with main file with  eligible preg exp and BMI

use "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/SmokingStatusFinal020124Analysis.dta", clear


append using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/EligiblePregnancies_WithExpBMIAnalysis.dta" 
gen Date1 = min(BD_Smoking , PregStartDate)
format  Date1 %tdDD/NN/CCYY
sort patient_id Date1 

gen BDate_Smoking = .
forval i = 1/100 {
replace BDate_Smoking = BD_Smoking[_n-`i'] ///
				if PregStartDate != . & ///
				BDate_Smoking == . & ///
				patient_id == patient_id[_n-`i'] & ///
				BD_Smoking[_n-`i'] < PregStartDate ///
			
				//Latest recorded BMI date before pregnancy start date 
}
format BDate_Smoking %tdDD/NN/CCYY


gen Smoking1 = .
forval i = 1/100 {
replace Smoking1 =  status[_n-`i'] ///
				if PregStartDate != . & ///
				BDate_Smoking == BD_Smoking[_n-`i'] & ///
				patient_id == patient_id[_n-`i'] & ///
				BD_Smoking[_n-`i'] < PregStartDate ///
			
				//Latest recorded BMI before pregnancy start date 
}
recode Smoking1 (2=1 "Non-Smoker") (3=2 "Ex-Smoker") (1=3 "Current Smoker") (0/.=4 "Missing data"), gen(B_Smoking)

//order patient_id D_Smoking SmokingStatus PregStartDate PregEndDate BDate_Smoking Smoking1 B_Smoking

order patient_id BD_Smoking status PregStartDate PregEndDate BDate_Smoking Smoking B_Smoking
br patient_id BD_Smoking status PregStartDate PregEndDate BDate_Smoking Smoking B_Smoking

drop if PregStartDate == . 
drop BD_Smoking status Smoking Date1

save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/EligiblePregnancies_WithExpBMIAndSmokingAnalysis.dta" , replace 



//Importing linked IMD data
{
import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/patient_2019_imd_22_002283.txt", clear 
rename patid patient_id 
keep patient_id e2019_imd_10
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/IMD.dta" , replace
} 
//MERGE Eligible pregnancies with exposure dates with eligible pregnancy with BMI and smoking


// merge IMD data
use "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/EligiblePregnancies_WithExpBMIAndSmokingAnalysis.dta", clear
drop _merge
merge m:1 patient_id using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Raw Data/IMD.dta"
drop if _merge == 2 


recode e2019_imd_10 (.= 21 "Missing"), gen(IMD)


replace IMD =5 if e2019_imd_10 == 1 | e2019_imd_10 == 2
replace IMD =4 if e2019_imd_10 == 3 | e2019_imd_10 == 4
replace IMD =3 if e2019_imd_10 == 5 | e2019_imd_10 == 6
replace IMD =2 if e2019_imd_10 == 7 | e2019_imd_10 == 8
replace IMD =1 if e2019_imd_10 == 9 | e2019_imd_10 ==10


drop _merge e2019_imd_10 


******ethnicity 
encode ethnicity, gen(Ethnicity)
recode Ethnicity (6 = 1 "White" ) (4 = 2 "Mixed Race" ) ( 5 = 3 "Others")(2 = 4 "Black" ) (1 = 5 "South Asians" )(3 = 6 "Missing" ), gen(B_Ethnicity)
************


gen gravidity = pregnumber
replace gravidity = 5 if pregnumber>5
tab gravidity
recode gravidity (1 = 1 "1") (2 = 2 "2") (3 = 3 "3") (4 = 4 "4") (5 = 5 "5+"), gen(Gravidity)



rename Gravidity B_gravidity


////Getting age category
rename AgeAtStartOfPregnancy age
gen ageround = round(age)
recode ageround (15/20=1 "15 - 20 years") (21/30=2 "21 - 30 years") (31/40=3 "31 - 40  years") (41/50=4 "40 - 50  years") (51/60=5 "51 - 60 years") , gen (agecat)
label var agecat "age categories (grouped)"
tab agecat


//////BMI age  categories

egen bmigp = cut(B_BMI), at(14, 18.5, 25, 30, 75)
recode bmigp (14=1 "14-18.5") (18.5=2 "18.5-25") (25=3 "25-30")(30=4 "30-75")(.=9 "missing"),gen(bmicat)
tab bmicat if bmicat != 9
tab bmicat



////////Outcome miscarriage


//////Miscarriage
/Obtaining primary care based records of miscarriage. 
local OutcomeConditionName Miscarriage
local OutcomeWindowStartTime 0 //from the start date of pregnancy in weeks
local OutcomeWindowEndTime 30 //from the start date of pregnancy in weeks

local ListOfPrimaryCareCodeLists AVF2_ReproductiveAgedWomenCohort20230612045053_MiscarriageIncidence_mumpredict
local ListOfICD10codes O03 O03.0 O03.1 O03.2 O03.3 O03.4 O03.5 O03.6 O03.7 O03.8 O03.9 O02.0 O02.1

foreach var in `ListOfPrimaryCareCodeLists' {

import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/`var'.csv", clear 
gen double patient_id = real(substr(practice_patient_id, strpos(practice_patient_id, "_")+1 , .))
format patient_id %12.0g

gen OD_PC_`OutcomeConditionName' = date(event_date, "YMD")
format OD_PC_`OutcomeConditionName' %tdDD/NN/CCYY

keep patient_id OD_PC_`OutcomeConditionName'

append using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithCovariatesAnalysis.dta"  
gen Date1 = min(OD_PC_`OutcomeConditionName' , PregStartDate)
gen Date2 = min(OD_PC_`OutcomeConditionName' , PregEndDate)
format Date2 Date1 %tdDD/NN/CCYY
sort patient_id Date1 Date2

gen ODate_PC_`OutcomeConditionName' = .
forval i = 100(-1)1 {
replace ODate_PC_`OutcomeConditionName' = OD_PC_`OutcomeConditionName'[_n+`i'] ///
				if patient_id == patient_id[_n+`i'] & ///
				OD_PC_`OutcomeConditionName'[_n+`i'] >= PregStartDate + (`OutcomeWindowStartTime'*7) & ///
				OD_PC_`OutcomeConditionName'[_n+`i'] <= PregStartDate + (`OutcomeWindowEndTime'*7)
				//Only interested in outcome event recorded between upto 30 weeks from pregnancy start date 
}
format ODate_PC_`OutcomeConditionName' %tdDD/NN/CCYY

gen O_PC_`OutcomeConditionName' = 0 
replace O_PC_`OutcomeConditionName' = 1 if ODate_PC_`OutcomeConditionName' != . 

drop if PregStartDate == . 
drop OD_PC_`OutcomeConditionName' Date1 Date2 
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Miscarriage/OutcomeFromPrimaryCare.dta" , replace
}


import delimited "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/hes_diagnosis_epi_22_002283_DM.txt", encoding(ISO-8859-2) clear 
rename patid patient_id 
format patient_id %15.0g
gen Date = date(epistart, "DMY")
format Date %tdDD/NN/CCYY
keep patient_id Date icd
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/HES_episodes.dta" , replace

//Obtaining secondary care based records of miscarriage. 
{

use "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/HES_episodes.dta" , clear
//Find the relevant ICD-10 codes from Dexter code builder
gen `OutcomeConditionName'_ICD_flag = 0 
/*
replace `OutcomeConditionName'_ICD_flag = 1 if icd == "O03"
forval i = 0/9 {
replace `OutcomeConditionName'_ICD_flag = 1 if icd == "O03.`i'"
}
forval i = 0/1 {
replace `OutcomeConditionName'_ICD_flag = 1 if icd == "O02.`i'"
}
*/
foreach icd in `ListOfICD10codes' {
di as error "`icd'"
replace `OutcomeConditionName'_ICD_flag = 1 if icd == "`icd'"
}

keep if `OutcomeConditionName'_ICD_flag == 1 

append using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Raw Data/EligiblePregnancies_WithCovariatesAnalysis.dta"  
gen Date1 = min(Date , PregStartDate)
gen Date2 = min(Date , PregEndDate)
format Date2 Date1 %tdDD/NN/CCYY
sort patient_id Date1 Date2


gen ODate_HES_`OutcomeConditionName' = .
forval i = 100(-1)1 {
replace ODate_HES_`OutcomeConditionName' = Date[_n+`i'] ///
				if patient_id == patient_id[_n+`i'] & ///
				Date[_n+`i'] >= PregStartDate + (`OutcomeWindowStartTime'*7) & ///
				Date[_n+`i'] <= PregStartDate + (`OutcomeWindowEndTime'*7)
				//Only interested in outcome event recorded between upto 30 weeks from pregnancy start date 
}
format Date %tdDD/NN/CCYY

gen O_HES_`OutcomeConditionName' = 0 
replace O_HES_`OutcomeConditionName' = 1 if ODate_HES_`OutcomeConditionName' != . 

keep patient_id PregStartDate PregEndDate ODate_HES_`OutcomeConditionName' O_HES_`OutcomeConditionName'
duplicates drop patient_id, force
save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Miscarriage/OutcomeFromHES.dta" , replace
}

use "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Miscarriage/OutcomeFromPrimaryCare.dta", clear
duplicates drop patient_id PregStartDate, force 
merge 1:1 patient_id PregStartDate using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Miscarriage/OutcomeFromHES.dta"
drop if _merge == 2
drop _merge 
gen O_`OutcomeConditionName' = 0 
replace O_`OutcomeConditionName' = 1 if O_HES_`OutcomeConditionName' == 1 | O_PC_`OutcomeConditionName' == 1 | outcome == 4 //Only outcome = 4 is considered as miscarriage. 
//Please note that there are other non-specific outcomes referring to termination of pregnancy. 
//Outcome = 6 is Miscarriage/TOP
//Outcome = 10 is Unspecified loss

//Generating follow-up time window variable for the outcomes to be recorded
gen OutcomeWindow_StartDate = PregStartDate
gen OutcomeWindow_EndDate = min(PatientEndDate, PregStartDate + (30*7)) if O_`OutcomeConditionName' == 0 
replace OutcomeWindow_EndDate = min(ODate_HES_`OutcomeConditionName', ODate_PC_`OutcomeConditionName', PregEndDate) if O_`OutcomeConditionName' == 1 
gen OutcomeWindow = (OutcomeWindow_EndDate - OutcomeWindow_StartDate)


egen ID = group(patient_id PregStartDate)

 rename O_Miscarriage O_Outcome
 rename ODate_HES_Miscarriage ODate_HES
 rename ODate_PC_Miscarriage ODate_PC
 rename O_HES_Miscarriage O_HES
 rename O_PC_Miscarriage O_PC
 
 
 format ODate_HES %tdDD/NN/CCYY

save "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Miscarriage/EligiblePregnancies_CovariatesAndOutcomes.dta", replace





/////Analysis



////MERGE DATASETS
use "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort Gold/Miscarriage/EligiblePregnancies_CovariatesAndOutcomes.dta", clear


rename E_type1diabetes18 E_T1DM
rename EDate_type1diabetes18 EDate_T1DM
format EDate_T1DM %tdDD/NN/CC


merge 1:1 patient_id PregStartDate PregEndDate using "/rds/projects/s/subramaa-mum-predict/Cohort study Megha/Pregnancy Base Cohort/Miscarriage/EligiblePregnancies_CovariatesAndOutcomes.dta"
drop if _merge==3

ds E_*
local ExposureVarList `" `r(varlist)' "'


foreach exposure in `ExposureVarList' {
glm  O_Outcome `exposure' , family(poisson) link(log) robust eform
estout, cells(b ci) eform
matrix A = r(coefs)
matrix list A, format(%12.0e)
estout , cells(p)
matrix B = r(coefs)
matrix list B, format(%9.3f)
local pval = (B[1,1])
if `pval' >= 0.001 {
local txt1 = "p=" + string(`pval',"%9.3f") 
}
if `pval' < 0.001 {
local txt1 = "p<0.001" 
}
local HR = string(A[1,1],"%04.2fc")
local lowerCI = string(A[1,2],"%04.2fc")
local upperCI = string(A[1,3],"%04.2fc")


global Unadj_`exposure' = "`HR'" + " (" + "`lowerCI'" + "-" + "`upperCI'" + "); " + "`txt1'"
global UHR_`exposure' = "`HR'" 
global UPV_`exposure' = "`pval'" 
eststo clear

***************Exposed N(%) among cases***************
count if `exposure' == 1 & O_Outcome == 1
local Case_N = `r(N)'
count if O_Outcome == 1
local Case_Per = string(((`Case_N' / `r(N)') * 100), "%04.2fc")
global Case_N_`exposure' = "`Case_N'" + " (" + "`Case_Per'" + "%)"

***************Exposed N(%) among Conts***************
count if `exposure' == 1 & O_Outcome == 0
local Cont_N = `r(N)'
count if O_Outcome == 0
local Cont_Per = string(((`Cont_N' / `r(N)') * 100), "%04.2fc")
global Cont_N_`exposure' = "`Cont_N'" + " (" + "`Cont_Per'" + "%)"



//Adjusted mode
glm   O_Outcome `exposure' i.B_gravidity i.Ethnicity i.B_Smoking i.agecat i.bmicat i.IMD, family(poisson) link(log) robust eform
estout , cells(b ci) eform
matrix A = r(coefs)
matrix list A, format(%12.0e)
estout , cells(p)
matrix B = r(coefs)
matrix list B, format(%9.3f)
local pval = (B[1,1])
if `pval' >= 0.001 {
local txt1 = "p=" + string(`pval',"%9.3f") 
}
if `pval' < 0.001 {
local txt1 = "p<0.001" 
}
local HR = string(A[1,1],"%04.2fc")
local lowerCI = string(A[1,2],"%04.2fc")
local upperCI = string(A[1,3],"%04.2fc")
			
global Adj_`exposure' = "`HR'" + " (" + "`lowerCI'" + "-" + "`upperCI'" + "); " + "`txt1'"
global AHR_`exposure' = "`HR'" 
global APV_`exposure' = "`pval'" 
eststo clear
*/

}


ds E_*
local ExposureVarList `" `r(varlist)' "'
di "`ExposureVarList'"
local count : word count `ExposureVarList'
di `count'

clear 
set obs `count'
gen Exposure = ""
gen UnadjHR = ""
gen AdjHR = ""
gen OutcomeInExposed = ""
gen OutcomeInUnexposed = ""


local i = 1
foreach exposure in `ExposureVarList' {
replace Exposure = "`exposure'" if _n == `i'
replace UnadjHR = "${Unadj_`exposure'}" if _n == `i'
replace AdjHR = "${Adj_`exposure'}" if _n == `i'
replace OutcomeInExposed = "${Case_N_`exposure'}" if _n == `i'
replace OutcomeInUnexposed = "${Cont_N_`exposure'}" if _n == `i'
local i = `i' + 1
}














