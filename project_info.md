Requirements to participate as a CLIF site: 

Clean CLIF tables 

ADT 

CRRT therapy 

Hospitalization  

Labs 

Medications admin continuous 

Medications admin intermittent 

Microbiology culture 

Patient  

Respiratory support  

Vitals  

Need to have ED, hospital ward, ICU data irrespective of whether patient went to the ICU 

Number of hospitals per site: no restriction 

Academic vs community sites: no restriction  

 

Introduction 

 

Aim: to validate and compare the ASE with lactic acid, ASE without lactic acid, BSE  

 

Inclusion criteria  

Adult hospitalized patients aged 18 years or older 

2018-2024 

Admitted via the ED  

Admitted to academic or community hospital_type_category (removing LTACH)  

 

Exclusion criteria: none  

 

Censoring: 1st (index) ASE episode within hospitalization_id.  

 

Primary outcome: number of hospitalizations who meet presumed infection, ASE with lactate, ASE without lactate,  the total number of patients (prevalence)  

 

Secondary outcome  

- compare demographics between four groups: presumed infection, ASE with lactate, ASE without lactate

Age, sex, race, ethnicity, language 

CCI 

Highest SOFA score during hospitalization  

1st ICU admission during hospitalization id  

Any ICU (all types combined) 

Cardiac, neuro, surgical, medical  

Types of ICU using CLIF mcide 

Any of life support during hospitalization id 

CRRT 

IMV 

NIPPV 

HFNO 

Vasopressor infusion  

Criteria of organ failure from CDC documentation 

AKI 

Vasopressor 

Bilirubin 

Thrombocytopenia 

Lactic acid  

Microbiology  

Counts of non-contaminant positive blood culture buffy coat  

List the top 20 most common prevalent organism_category 

(we will then only present the top 5 and there should be enough overlap between sites using the top 20 that there is agreement in the top 5) 

Discharge category 

Length of stay, days 

Hospital  

ICU, if had ICU stay  

In hospital death  

- Repeat the above looking at community-onset vs hospital-onset for each of the4 criteria (presumed infection, ASE with lactate, ASE without lactate, and BSE)  

- time to first organ failure per CDC criteria (baseline time 0 is the time that the first vital signs (earliest from HR, RR, BP, SpO2) are recorded in the ED) 

- number of lactates collected before presumed infection, ASE with lactic, ASE without lactic and BSE criteria met  

 

Statistical Analysis  

Mean, SD, counts of patients who meet criteria, all counts (see attached example posted in teams) 

Weighted aggregation, each patient inside a healthcare system carries the same contribution to overall full cohort 

Then we can calculate pooled mean and pooled variance   

Compares rates of presumed infection, ASE lactic, ASE no lactic, BSE across  

unique hospital_ids 

academic vs community hospitals  

unique health systems  

Run a regression on the aggregated data using the rates and standard error 

 

Dependent variable = One of the sepsis criteria   

Independent (predictor) variables =  year + average age + average cci (comorbidities) + average highest SOFA (acuity) + hospital_number (continuous numbered across full dataset) + health_system + hospital_type  

 

Data output saved locally at each site  

Prevalence of all onset, community onset and hospital onset ASE lactic, all onset, community onset and hospital onset ASE no lactic, all onset, community onset and hospital onset BSE 

Stratified by hospital_id and type of hospitalfor health systems with more than one hospital  

Table 1s to allow for the 12 comparison of groups (all, community, hospital onset x4 for  presumed infection, ASE lactic, ASE no lactic, BSE) 

Aggregated counts by year, hospital for each criteria (all only, no neeed to split into community hospital onset for this) -> example file in teams  

output 1:
per site 1 parquet file
year 	health system 	hospital_id	hospital_type 	n_presumed_infection_community	n_presumed_infection_hospital	n_presumed_infection_all	n_ASE_nolactic_community 	n_ASE_nolactic_hospital 	n_ASE_nolactic_all	n_ASE_lactic_community 	n_ASE_lactic_hospital 	n_ASE_lactic_all	n_BSE_community 	n_BSE_hospital 	n_BSE_all	total_n_patients	female_allpatients_average	female_allpatients_sd	age_allpatients_average	age_allpatients_sd	CCI_average_allpatients	CCI_sd_allpatients	SOFA_average_allpatients	SOFA_sd_allpatients