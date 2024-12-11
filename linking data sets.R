## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: linking data sets.R

# Corresponding author: Freya Tracey (freya.tracey@health.org.uk)

# Descripton: this will bring all the national data sets together and then do 
# hierachy decision making for demographics.

# Inputs:
# All the outputs of the previous scripts

# Outputs:
# One row per person dataset with one column per variable (post hierarchy)

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

# Source relevant scripts ------------------------------------------------

# Set up ####

library (aws.s3)
library (tidyverse)
library (janitor)

project_bucket <- '' # assign project directory

# Import data sets

SUS <- s3read_using(read.csv, object = "person level data sets for linkage/SUS_combo_pre_linkage.csv", bucket = project_bucket, na.strings = c("", "NA")) %>% 
  select(-X)
# this is combo of APC, ECDS, OP and demo

ambulance <- s3read_using(read.csv, object = "person level data sets for linkage/ambulance_pre_linkage.csv", bucket = project_bucket, na.strings = c("", "NA")) %>% 
  select(-X)

ASC <- s3read_using(read.csv, object = "person level data sets for linkage/ASC_pre_linkage.csv", bucket = project_bucket, na.strings = c("", "NA")) %>% 
 select(-X)

PDS <- s3read_using(read.csv, object = "person level data sets for linkage/PDS_pre_linkage.csv", bucket = project_bucket, na.strings = c("", "NA")) %>% 
 select(-X)

CHR <- s3read_using(read.csv, object = "person level data sets for linkage/CHR_pre_linkage.csv", bucket = project_bucket, na.strings = c("", "NA")) %>% 
 select(-X)

care_home <- s3read_using(readRDS, object = "ch_dataset_linkage.rds", bucket = project_bucket) 

CSDS_demo <- s3read_using(read.csv, object = "person level data sets for linkage/csds_demo.csv", bucket = project_bucket, na.strings = c("", "NA")) %>% 
  select(-X)

CSDS_activity <- s3read_using(read.csv, object = "person level data sets for linkage/csds.csv", bucket = project_bucket, na.strings = c("", "NA")) %>% 
  select(-X)

CH_flags <- s3read_using(read.csv, object = "person level data sets for linkage/ch_flags.csv", bucket = project_bucket, na.strings = c("", "NA")) %>% 
  select(-X)

index_CH_dates <- s3read_using(readRDS, object = 'residents_all_linkage.rds', bucket = project_bucket)
allidsforlinkage <- index_CH_dates %>% 
  select(pseudonhsno)

linked_data_sets <- allidsforlinkage %>%
  left_join(SUS, by = "pseudonhsno") %>% 
  left_join(ambulance, by = c( "pseudonhsno" = "Der_Pseudo_NHS_Number"))%>%
  left_join(ASC, by = c( "pseudonhsno" = "Der_Pseudo_NHS_Number"))%>%
  left_join(PDS, by =  "pseudonhsno" ) %>%
  left_join(CSDS_demo, by = c( "pseudonhsno" = "Der_Pseudo_NHS_Number"))%>%
  left_join(CHR, by =  "pseudonhsno" ) %>% 
  left_join(care_home, by = "pseudonhsno") %>%
  left_join (CSDS_activity, by = c( "pseudonhsno" = "Der_Pseudo_NHS_Number")) %>%
  left_join (CH_flags, by = "pseudonhsno") %>%
#  select (-X.1, -X.x, -X.y, -X.y.y, -X.x.x.x, -X.x.x, -X.y.y.y)  #more to drop - dealing with this above as easier to check for them there
# %>%  #these got added at export to csv in some cases
  mutate (n_ambulance_call_outs_5m = coalesce (n_ambulance_call_outs_5m,0), # want it to be zero for overall activity rather than NA as we know they didnt have one
         n_ED_attendances = coalesce(n_ED_attendances,0),
         n_cs_appts = coalesce(n_cs_appts,0),
         n_ED_attendances_5m = coalesce (n_ED_attendances_5m, 0),
         # adm_avoid_12h = coalesce (adm_avoid_12h, 0), want them to be NA as cant be avoidable if dont have emergency admission
         adm_el_12h = coalesce (adm_el_12h, 0),
         adm_em_12h = coalesce (adm_em_12h, 0), 
         adm_bedsore_12h = coalesce (adm_bedsore_12h, 0),
         adm_lrti_12h = coalesce (adm_lrti_12h, 0),
         adm_urti_12h = coalesce (adm_urti_12h, 0),
         discharge_ch_12h = coalesce(discharge_ch_12h, 0),
         n_admissions_CH = coalesce (n_admissions_CH, 0),                        
         n_elective_admissions_CH = coalesce (n_elective_admissions_CH, 0),                
         n_emergency_admissions_CH   = coalesce (n_emergency_admissions_CH, 0),             
         n_avoidable_emergency_admissions_CH = coalesce (n_avoidable_emergency_admissions_CH, 0),    
         n_admissions_5m  = coalesce (n_admissions_5m, 0),                       
         n_elective_admissions_5m   = coalesce (n_elective_admissions_5m, 0),              
         n_emergency_admissions_5m   = coalesce (n_emergency_admissions_5m, 0),            
         n_avoidable_emergency_admissions_5m  = coalesce (n_avoidable_emergency_admissions_5m, 0),    
         n_admissions_wave    = coalesce (n_admissions_wave, 0),                   
         n_elective_admissions_wave     = coalesce (n_elective_admissions_wave, 0),         
         n_emergency_admissions_wave       = coalesce (n_emergency_admissions_wave, 0),       
         n_avoidable_emergency_admissions_wave= coalesce (n_avoidable_emergency_admissions_wave, 0),
         adm_avoid_12h = ifelse (adm_avoid_12h ==0, NA, adm_avoid_12h ), # want 0s to be NA 
         n_OP_appts = coalesce(n_OP_appts, 0),
         dementia_sus = if_else (f_dementia_h36 ==1 | c_dementia_h36 ==1,1,0)
  )


# adding in IMD - source from gov.uk 
IMD <- s3read_using(read.csv, object = "File_7_-_All_IoD2019_Scores__Ranks__Deciles_and_Population_Denominators_3.csv", bucket = project_bucket, na.strings = c("", "NA")) %>%
  select (LSOA.code..2011., Index.of.Multiple.Deprivation..IMD..Decile..where.1.is.most.deprived.10..of.LSOAs.) %>%
  rename (LSOA_IMD= LSOA.code..2011., 
          IMD_decile = Index.of.Multiple.Deprivation..IMD..Decile..where.1.is.most.deprived.10..of.LSOAs.)

linked_data_sets <- linked_data_sets %>% #adding in IMD for LSOA from PDS
  left_join(IMD, by = c("LSOA_PDS" = "LSOA_IMD")) %>%
  mutate (IMD_quintile = case_when(
    IMD_decile == "1" ~ "Most deprived fifth",
    IMD_decile == "2" ~ "Most deprived fifth",
    IMD_decile == "3" ~ "Second most deprived fifth",
    IMD_decile == "4" ~ "Second most deprived fifth",
    IMD_decile == "5" ~ "Middle fifth",
    IMD_decile == "6" ~ "Middle fifth",
    IMD_decile == "7" ~ "Second least deprived fifth",
    IMD_decile == "8" ~ "Second least deprived fifth",
    IMD_decile == "9" ~ "Least deprived fifth",
    IMD_decile == "10" ~ "Least deprived fifth",
    IMD_decile == NA ~ "Missing"
    
  ))

# start of hierarchy process 

### ethnicity - in CSDS, SUS, CH record ####
LDS_variables <- colnames(linked_data_sets) %>%
as.data.frame()

ethnicity <- linked_data_sets %>%
  select(pseudonhsno, ethnicity_sus, EthnicCategory, ethnicity_new) %>%
  mutate(ethnicity_sus = case_when(
    ethnicity_sus == 1 ~ "White",
    ethnicity_sus == 2 ~ "Mixed",
    ethnicity_sus == 3 ~ "Asian",
    ethnicity_sus == 4 ~ "Black or Black British",
    ethnicity_sus == 5 ~ "Other",
    ethnicity_sus == 0 ~ "Missing",
    ethnicity_sus == 9 ~ "Missing"),
    ethnicity_sus = coalesce(ethnicity_sus, "Missing")
  ) %>%
  mutate(ethnicity_csds = case_when(
    EthnicCategory %in% c("A", "B", "C") ~ "White",
    EthnicCategory %in% c("M", "P") ~ "Black or Black British",
    EthnicCategory == "S" ~ "Other",
    EthnicCategory == "NA_" ~ "Missing"),
    ethnicity_csds = coalesce(ethnicity_csds, "Missing")) %>%
  select (-EthnicCategory) %>%
  mutate (
    ethnicity_new = coalesce(ethnicity_new, "Missing")
  ) %>%
  as_tibble ()



sumamry_ethnicity <- ethnicity %>%
  tabyl (ethnicity_sus, ethnicity_csds) %>%
  adorn_totals (c("row", "col")) %>%
  as.data.frame()
# SUS is already an aggregate of data sources so will take this where available and compliment with CSDS where SUS is missing


ethnicity_result <- ethnicity %>%
  mutate(ethnicity_final = ifelse(
    ethnicity_new == "Missing" ,
    ethnicity_sus,
    ethnicity_new
  ))      %>%
  mutate(ethnicity_final = ifelse(
    ethnicity_final == "Missing" ,
    ethnicity_csds,
    ethnicity_final
  ))      %>%
  select (pseudonhsno, ethnicity_final)


### gender ####

gender <- linked_data_sets %>%
  select (pseudonhsno, gender_PDS, Gender, sex_sus, gender_new) %>% # now including from CH record too 
  rename (gender_csds = Gender) %>%
  mutate (
    gender_PDS = case_when(
      gender_PDS == 1 ~ "Male",
      gender_PDS == 2 ~ "Female"),
    gender_csds = case_when(
      gender_csds == 1 ~ "Male",
      gender_csds == 2 ~ "Female"),
    sex_sus = case_when(
      sex_sus == 1 ~ "Male",
      sex_sus == 2 ~ "Female"),
    gender_PDS = coalesce(gender_PDS, "Missing"),
    gender_csds = coalesce(gender_csds, "Missing"),
    sex_sus = coalesce(sex_sus, "Missing"),
    gender_new = coalesce(gender_new, "Missing")
  )

sumamry_gender_1 <- gender %>%
  tabyl (gender_PDS, gender_csds) %>%
  adorn_totals (c("row", "col")) %>%
  as.data.frame()
# no disagreement in gender here, just cases where available in one and not other 

sumamry_gender_2 <- gender %>%
  tabyl (gender_PDS, sex_sus) %>%
  adorn_totals (c("row", "col")) %>%
  as.data.frame()
# no disagreement in gender here, just cases where available in one and not other 


# gender missing in some PDS records and in some csds records
# taking SUS, then PDS, then csds

sex_result <- gender %>% # should be sex not gender, just changing here 
  mutate(
    sex_final = ifelse(
    gender_PDS == "Missing",
    sex_sus,
    gender_PDS)) %>%
  mutate(
    sex_final = ifelse (
      sex_final == "Missing",
      gender_csds,
      sex_final)) %>%
      mutate (
    sex_final = ifelse (
      sex_final == "Missing",
      gender_new,
      sex_final)
    ) %>%
  select (pseudonhsno, sex_final)
  

    
### DOB ####

dob <- linked_data_sets %>%
  select (pseudonhsno, dob_sus, DOB_yr_mth_PDS)

#sus dob is generated from modal month and year of birth, and takes first of month for all so need to do same to pds first 

dob <- dob %>%
  mutate (dob_pds = as.Date(sprintf("%06d01", DOB_yr_mth_PDS),format = "%Y%m%d"),
          dob_pds = format (dob_pds, "%d/%m/%Y"),
          dob_disagree = ifelse (!is.na(dob_sus) & dob_sus != dob_pds, "Different", "Same"))

dob_summary <- dob %>%
  summarize (
    n_dob_pds_missing  = sum (is.na (dob_pds)),
    n_dob_sus_missing = sum (is.na (dob_sus)),
    n_dob_disagre = sum (dob_disagree == "Different", na.rm = TRUE)
)
# no disagreements in dob

dob_result <- dob %>%
  mutate (dob_final = ifelse(
    is.na(dob_pds),
    dob_sus,
    dob_pds
  ))     %>%
  select (pseudonhsno, dob_final)


### date of death ####

dod <- linked_data_sets %>%
  select (pseudonhsno, deathdate_sus, date_of_death_PDS, YearMonthDeath_YYYYMM_CHR ) %>%
  mutate (dod_chr = as.Date(sprintf("%06d01", YearMonthDeath_YYYYMM_CHR),format = "%Y%m%d"),
          dod_chr = as.Date(dod_chr, format = "%d/%m/%Y"),
          dod_chr = if_else (dod_chr  >= "2023-11-01", NA, dod_chr), # dont want to include deaths which happened after end of study
          dod_pds = as.Date(date_of_death_PDS, format = "%Y-%m-%d"),
          dod_pds = if_else (dod_pds  >= "2023-11-01", NA, dod_pds), # dont want to include deaths which happened after end of study
          dod_sus = as.Date(deathdate_sus, format = "%d/%m/%Y"),
          dod_sus = if_else (dod_sus  >= "2023-11-01", NA, dod_sus), # dont want to include deaths which happened after end of study
          dod_disagree_1 = ifelse (abs(difftime (dod_sus, dod_chr, units = "days")) >30,
                                 "Disagree",
                                 "Agree"),
          dod_disagree_2 = ifelse (abs(difftime (dod_sus, dod_pds, units = "days")) >30,
                              "Disagree",
                              "Agree"),
          dod_disagree_3 = ifelse (abs(difftime (dod_chr, dod_pds, units = "days")) >30,
                                "Disagree",
                                "Agree")
       )
# no disagreements across any of these

dod_summary <- dod %>%
  summarize (
    n_dod_pds  = sum (!is.na (dod_pds)),
    n_dod_sus = sum (!is.na (dod_sus)),
    n_dod_chr = sum (!is.na (dod_chr))
  )
    
dod_result <- dod %>%
  mutate (dod_final = if_else (
    is.na(dod_pds),
    dod_sus,
    dod_pds
  ))  %>%
  mutate (dod_final = if_else(
    is.na(dod_final),
    dod_chr,
    dod_final
  )) %>%
  mutate (dod_final = if_else (dod_final >= "2023-11-01", NA, dod_final
                        )) %>% # just to double check none slipped through earlier 
  select (dod_final, pseudonhsno)
  
# dementia 

dementia_result <- linked_data_sets %>%
  select (pseudonhsno, dementia_sus, dementia) %>%
  mutate (
    dementia_num = if_else (is.na(dementia), NA_real_, if_else (dementia == "Yes",1,0))
  )%>%
  mutate (dementia_final = case_when(
    !is.na (dementia_sus) & dementia_sus ==1 ~1,
    !is.na (dementia_num) & dementia_num ==1 ~1,
    is.na (dementia_sus) & is.na (dementia_num) ~ NA_integer_,
            TRUE ~0 )) %>%

  select (pseudonhsno, dementia_final)



## now to add the final ones to the linked data set ####

MDS <- linked_data_sets %>%
  full_join(dod_result, by = "pseudonhsno") %>%
  full_join(dob_result, by = "pseudonhsno") %>%
  full_join(ethnicity_result, by = "pseudonhsno") %>%
  full_join(sex_result, by = "pseudonhsno") %>%
  full_join(dementia_result, by = "pseudonhsno") %>%
  select (
    - ethnicity_sus, 
    - EthnicCategory,
    - gender_PDS, 
    - Gender,
    - sex_sus,
    - dob_sus, 
    - DOB_yr_mth_PDS
  #  ,- deathdate_sus.y,
  #  - deathdate_sus.x,
  #  - date_of_death_PDS.y,
  #  - date_of_death_PDS.x,
  #  - YearMonthDeath_YYYYMM_CHR.x,
  #  - YearMonthDeath_YYYYMM_CHR.y
    
    )# these have all been used to generate the final versions so dont need to be in MDS 

MDS <- MDS %>% 
  mutate(
    CH_admission_flag = as.numeric(CH_admission_flag),
    wave = as.numeric(wave.y)
  ) %>%
  select (-wave.x, -wave.y)

MDS <- MDS %>%
  mutate (nur_res = case_when(
    Der_CQC_Service_Type_CHR == "Nursing" ~ "Nursing",
    Der_CQC_Service_Type_CHR == "Nursing and Residential" ~ "Nursing",
    Der_CQC_Service_Type_CHR == "Residential" ~ "Residential",
    Der_CQC_Service_Type_CHR == NA ~ "Missing")) %>%
  mutate (
    dob_available = ifelse(
      is.na(dob_final), "Missing","Available"),
    dod_available = ifelse(
      is.na(dod_final), "Missing","Available")
  )

for (col_name in colnames (MDS)) { # this turns all NAs in the n_ columns into 0s so the denominator is always the same for the frequency section of tables 
  if (startsWith (col_name, "n_")) {
    MDS [[col_name]] <- coalesce(MDS [[col_name]],0)
  }
}

s3write_using(MDS, FUN=write.csv, object = "Linked data sets/MDS_personlevel.csv", bucket = project_bucket)

## need to have a table which looks at all the variables that make up the derived variables
# need to use linked data sets rather than MDS


# ethnicity
View (ethnicity)

sumamry_ethnicity_sus <- ethnicity %>%
  tabyl (ethnicity_sus) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_sus = n,
          percent_sus =percent) %>%
  as.data.frame()

sumamry_ethnicity_csds <- ethnicity %>%
  tabyl (ethnicity_csds) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_csds = n,
          percent_csds =percent) %>%
  
  as.data.frame()

sumamry_ethnicity_CH <- ethnicity %>%
  tabyl (ethnicity_new) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_CH = n,
          percent_CH =percent) %>%
  as.data.frame()

summary_ethnicity_table <- c("White", "Black or Black British", "Asian or Asian British", "Mixed", "Other", "Missing") %>%
  as.data.frame() 

colnames(summary_ethnicity_table)[1] <- "Ethnicity"

summary_ethnicity_table <- summary_ethnicity_table %>%
  left_join(sumamry_ethnicity_sus, by = c("Ethnicity"= "ethnicity_sus")) %>%
  left_join(sumamry_ethnicity_csds, by =  c("Ethnicity"= "ethnicity_csds")) %>%
  left_join(sumamry_ethnicity_CH, by =  c("Ethnicity"= "ethnicity_new"))

summary_ethnicity_table_1 <- summary_ethnicity_table %>%
  select (-Ethnicity) %>%
  colSums (na.rm = TRUE) %>%
  as.data.frame() # confirm there are all people for each and percent =1 

# if not missing for both, what % match across the datasets, then the matching across all
not_missing_ethnicity_sus_csds <- ethnicity %>%
  filter (ethnicity_sus != "Missing", ethnicity_csds != "Missing") %>%
  mutate(ethnicity_disagree_sus_csds = ifelse  (ethnicity_sus != ethnicity_csds, "Different", "Same"))

not_missing_ethnicity_sus_csds_summary <- not_missing_ethnicity_sus_csds %>%
  tabyl (ethnicity_disagree_sus_csds) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_sus_csds = n,
          percent_sus_csds = percent )

not_missing_ethnicity_sus_CH <- ethnicity %>%
  filter (ethnicity_sus != "Missing", ethnicity_new != "Missing") %>%
  mutate(ethnicity_disagree_sus_CH = ifelse  (ethnicity_sus != ethnicity_new, "Different", "Same"))

not_missing_ethnicity_sus_CH_summary <- not_missing_ethnicity_sus_CH %>%
  tabyl (ethnicity_disagree_sus_CH) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_sus_CH = n,
          percent_sus_CH = percent )

not_missing_ethnicity_csds_CH <- ethnicity %>%
  filter (ethnicity_csds != "Missing", ethnicity_new != "Missing") %>%
  mutate(ethnicity_disagree_csds_CH = ifelse  (ethnicity_csds != ethnicity_new, "Different", "Same"))

not_missing_ethnicity_csds_CH_summary <- not_missing_ethnicity_csds_CH %>%
  tabyl (ethnicity_disagree_csds_CH) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_csds_CH = n,
          percent_csds_CH = percent )

not_missing_ethnicity_all <- ethnicity %>%
  filter (ethnicity_csds != "Missing", ethnicity_new != "Missing", ethnicity_sus != "Missing") %>%
  mutate(
    ethnicity_disagree_1 = ifelse  (ethnicity_csds != ethnicity_new, 1,0), #1 means different 
    ethnicity_disagree_2 = ifelse  (ethnicity_sus != ethnicity_new, 1,0),
    ethnicity_disagree_3 = ifelse  (ethnicity_csds != ethnicity_sus, 1,0)
    ) %>%
  mutate (
    ethnicity_all = case_when(
      ethnicity_disagree_1 == 0 & ethnicity_disagree_2 == 0 & ethnicity_disagree_3 ==0 ~ "Same"
    ),
    ethnicity_all  =  (coalesce(ethnicity_all, "Different"))
      
  )

not_missing_ethnicity_all_summary <- not_missing_ethnicity_all %>%
  tabyl (ethnicity_all) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_all = n,
          percent_all = percent)

not_missing_ethnicity_summary <- not_missing_ethnicity_all_summary %>%
  left_join(not_missing_ethnicity_csds_CH_summary, by = c("ethnicity_all" = "ethnicity_disagree_csds_CH")) %>%
  left_join(not_missing_ethnicity_sus_CH_summary, by = c("ethnicity_all" = "ethnicity_disagree_sus_CH")) %>%
  left_join(not_missing_ethnicity_sus_csds_summary, by = c("ethnicity_all" = "ethnicity_disagree_sus_csds")) 
  
# so the tables needed are not_missing_ethnicity_summary and summary_ethnicity_table 
s3write_using(not_missing_ethnicity_summary, FUN=write.csv, object = "Hierarchy table/not_missing_ethnicity_summary.csv", bucket = project_bucket)
s3write_using(summary_ethnicity_table, FUN=write.csv, object = "Hierarchy table/summary_ethnicity_table.csv", bucket = project_bucket)


#sex
summary_sex_sus <- gender %>%
  tabyl (sex_sus) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_sus = n,
          percent_sus =percent) %>%
  as.data.frame()

summary_sex_csds <- gender %>%
  tabyl (gender_csds) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_csds = n,
          percent_csds =percent) %>%
  as.data.frame()

summary_sex_CH <- gender %>%
  tabyl (gender_new) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_CH = n,
          percent_CH =percent) %>%
  as.data.frame()

summary_sex_PDS <- gender %>%
  tabyl (gender_PDS) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_PDS = n,
          percent_PDS =percent) %>%
  as.data.frame()


summary_sex_table <- summary_sex_PDS %>%
  left_join (summary_sex_CH, by = c("gender_PDS" = "gender_new")) %>%
  left_join (summary_sex_csds, by = c("gender_PDS" = "gender_csds")) %>%
  left_join (summary_sex_sus, by = c("gender_PDS" = "sex_sus")) %>%
  rename (sex = gender_PDS)

# if not missing for both, what % match across the datasets, then the matching across all
sex <-gender

not_missing_sex_sus_csds <- sex %>%
  filter (sex_sus != "Missing", gender_csds != "Missing") %>%
  mutate(sex_disagree_sus_csds = ifelse  (sex_sus != gender_csds, "Different", "Same"))

not_missing_sex_sus_csds_summary <- not_missing_sex_sus_csds %>%
  tabyl (sex_disagree_sus_csds) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_sus_csds = n,
          percent_sus_csds = percent )

not_missing_sex_sus_CH <- sex %>%
  filter (sex_sus != "Missing", gender_new != "Missing") %>%
  mutate(sex_disagree_sus_CH = ifelse  (sex_sus != gender_new, "Different", "Same"))

not_missing_sex_sus_CH_summary <- not_missing_sex_sus_CH %>%
  tabyl (sex_disagree_sus_CH) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_sus_CH = n,
          percent_sus_CH = percent )

not_missing_sex_csds_CH <- sex %>%
  filter (gender_csds != "Missing", gender_new != "Missing") %>%
  mutate(sex_disagree_csds_CH = ifelse  (gender_csds != gender_new, "Different", "Same"))

not_missing_sex_csds_CH_summary <- not_missing_sex_csds_CH %>%
  tabyl (sex_disagree_csds_CH) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_csds_CH = n,
          percent_csds_CH = percent )

not_missing_sex_csds_PDS <- sex %>%
  filter (gender_csds != "Missing", gender_PDS != "Missing") %>%
  mutate(sex_disagree_csds_PDS = ifelse  (gender_csds != gender_PDS, "Different", "Same"))

not_missing_sex_csds_PDS_summary <- not_missing_sex_csds_PDS %>%
  tabyl (sex_disagree_csds_PDS) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_csds_PDS = n,
          percent_csds_PDS = percent )

not_missing_sex_sus_PDS <- sex %>%
  filter (sex_sus != "Missing", gender_PDS != "Missing") %>%
  mutate(sex_disagree_sus_PDS = ifelse  (sex_sus != gender_PDS, "Different", "Same"))

not_missing_sex_sus_PDS_summary <- not_missing_sex_sus_PDS %>%
  tabyl (sex_disagree_sus_PDS) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_sus_PDS = n,
          percent_sus_PDS = percent )

not_missing_sex_CH_PDS <- sex %>%
  filter (gender_new != "Missing", gender_PDS != "Missing") %>%
  mutate(sex_disagree_CH_PDS = ifelse  (gender_new != gender_PDS, "Different", "Same"))

not_missing_sex_CH_PDS_summary <- not_missing_sex_CH_PDS %>%
  tabyl (sex_disagree_CH_PDS) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_CH_PDS = n,
          percent_CH_PDS = percent)


not_missing_sex_all <- sex %>%
  filter (gender_csds != "Missing", gender_new != "Missing", sex_sus != "Missing", gender_PDS != "Missing") %>%
  mutate(
    sex_disagree_1 = ifelse  (gender_PDS != gender_new, 1,0), #1 means different 
    sex_disagree_2 = ifelse  (gender_PDS != sex_sus, 1,0),
    sex_disagree_3 = ifelse  (gender_PDS != gender_csds, 1,0),
    sex_disagree_4 = ifelse  (gender_new != sex_sus, 1,0),
    sex_disagree_5 = ifelse  (gender_new != gender_csds, 1,0),
    sex_disagree_6 = ifelse  (gender_csds != sex_sus, 1,0)
  ) %>%
  mutate (
    sex_all = case_when(
      sex_disagree_1 == 0 & 
        sex_disagree_2 == 0 & 
        sex_disagree_3 ==0 &
        sex_disagree_4 ==0 &
        sex_disagree_5 ==0 &
        sex_disagree_6 ==0 
      ~ "Same"
    ),
    sex_all  =  (coalesce(sex_all, "Different"))
    
  )

not_missing_sex_all_summary <- not_missing_sex_all %>%
  tabyl (sex_all) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_all = n,
          percent_all = percent)

not_missing_sex_summary <- not_missing_sex_all_summary %>%
  left_join(not_missing_sex_csds_CH_summary, by = c("sex_all" = "sex_disagree_csds_CH")) %>%
  left_join(not_missing_sex_sus_CH_summary, by = c("sex_all" = "sex_disagree_sus_CH")) %>%
  left_join(not_missing_sex_sus_csds_summary, by = c("sex_all" = "sex_disagree_sus_csds")) 

# so the tables needed are not_missing_sex_summary and summary_sex_table 
s3write_using(not_missing_sex_summary, FUN=write.csv, object = "Hierarchy table/not_missing_sex_summary.csv", bucket = project_bucket)
s3write_using(summary_sex_table, FUN=write.csv, object = "Hierarchy table/summary_sex_table.csv", bucket = project_bucket)


#dob 

View(dob)

dob <- dob%>%
  mutate (
    sus_available = ifelse(
      is.na(dob_sus), "Missing","Available"),
    pds_available = ifelse(
  is.na(dob_pds), "Missing","Available"))

summary_dob_final <- dob_result %>%
  mutate (
    dob_available = ifelse(
      is.na(dob_final), "Missing","Available") )%>%
  tabyl(dob_available) %>%
  adorn_totals (c("row", "col"))%>%
  rename (n_final = n,
          percent_final =percent) %>%
  as.data.frame()
  
summary_dob_sus <- dob %>%
  tabyl (sus_available) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_sus = n,
          percent_sus =percent) %>%
  as.data.frame()

summary_dob_pds <- dob %>%
  tabyl (pds_available) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_pds = n,
          percent_pds =percent) %>%
  as.data.frame()

summary_dob_table <- summary_dob_pds %>%
  left_join(summary_dob_sus, by =c("pds_available" = "sus_available")) %>%
  left_join(summary_dob_final, by =c("pds_available" = "dob_available"))

not_missing_dob_sus_PDS <- dob %>%
  filter (sus_available != "Missing", pds_available != "Missing") %>%
  mutate(dob_disagree_sus_PDS = ifelse  (dob_sus != dob_pds, "Different", "Same"))

not_missing_dob_sus_PDS_summary <- not_missing_dob_sus_PDS %>%
  tabyl (dob_disagree_sus_PDS) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_SUS_PDS = n,
          percent_SUS_PDS = percent)

# need summary_dob_table and not_missing_dob_sus_PDS_summary
s3write_using(not_missing_dob_sus_PDS_summary, FUN=write.csv, object = "Hierarchy table/not_missing_dob_sus_PDS_summary.csv", bucket = project_bucket)
s3write_using(summary_dob_table, FUN=write.csv, object = "Hierarchy table/summary_dob_table.csv", bucket = project_bucket)


#dod 

View(dod)

dod <- dod%>%
  mutate (
    sus_available = ifelse(
      is.na(dod_sus), "Missing","Available"),
    pds_available = ifelse(
      is.na(dod_pds), "Missing","Available"),
    chr_available = ifelse(
      is.na(dod_chr), "Missing","Available"))

summary_dod_final <- dod_result %>%
  mutate (
    dod_available = ifelse(
      is.na(dod_final), "Missing","Available") )%>%
  tabyl(dod_available) %>%
  adorn_totals (c("row", "col"))%>%
  rename (n_final = n,
          percent_final =percent) %>%
  as.data.frame()



summary_dod_sus <- dod %>%
  tabyl (sus_available) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_sus = n,
          percent_sus =percent) %>%
  as.data.frame()

summary_dod_pds <- dod %>%
  tabyl (pds_available) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_pds = n,
          percent_pds =percent) %>%
  as.data.frame()

summary_dod_chr <- dod %>%
  tabyl (chr_available) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_chr = n,
          percent_chr =percent) %>%
  as.data.frame()

summary_dod_table <- summary_dod_chr %>%
  left_join (summary_dod_pds, by = c("chr_available" = "pds_available")) %>%
  left_join (summary_dod_sus, by = c("chr_available" = "sus_available")) %>%
  left_join (summary_dod_final, by = c("chr_available" = "dod_available")) 
  
  

not_missing_dod_sus_pds <- dod %>%
  filter (sus_available != "Missing", pds_available != "Missing") %>%
  mutate(dod_disagree_sus_pds = ifelse (abs(difftime (dod_sus, dod_pds, units = "days")) >30,
        "Different",
        "Same"))

# need to be within 30 days rather than excact 

not_missing_dod_sus_pds_summary <- not_missing_dod_sus_pds %>%
  tabyl (dod_disagree_sus_pds) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_sus_pds = n,
          percent_sus_pds = percent)

not_missing_dod_sus_chr <- dod %>%
  filter (sus_available != "Missing", chr_available != "Missing") %>%
  mutate(dod_disagree_sus_chr = ifelse (abs(difftime (dod_sus, dod_chr, units = "days")) >30,
                                        "Different",
                                        "Same"))

not_missing_dod_sus_chr_summary <- not_missing_dod_sus_chr %>%
  tabyl (dod_disagree_sus_chr) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_SUS_chr = n,
          percent_SUS_chr = percent)

not_missing_dod_pds_chr <- dod %>%
  filter (pds_available != "Missing", chr_available != "Missing") %>%
  mutate(dod_disagree_pds_chr =ifelse (abs(difftime (dod_pds, dod_chr, units = "days")) >30,
                                       "Different",
                                       "Same"))

not_missing_dod_pds_chr_summary <- not_missing_dod_pds_chr %>%
  tabyl (dod_disagree_pds_chr) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_pds_chr = n,
          percent_pds_chr = percent)


not_missing_dod_all <- dod %>%
  filter (sus_available != "Missing", pds_available != "Missing", chr_available != "Missing") %>%
  mutate(
    dod_disagree_1 =ifelse (abs(difftime (dod_sus, dod_pds, units = "days")) >30,
                              "Different",
                              "Same"),  
    dod_disagree_2 = ifelse (abs(difftime (dod_pds, dod_chr, units = "days")) >30,
                             "Different",
                             "Same"),
    dod_disagree_3 = ifelse (abs(difftime (dod_chr, dod_sus, units = "days")) >30,
                             "Different",
                             "Same")  
  ) %>%
  mutate (
    dod_all = case_when(
      dod_disagree_1 == "Same" & dod_disagree_2 == "Same" & dod_disagree_3 =="Same" ~ "Same"
    ),
    dod_all  =  (coalesce(dod_all, "Different"))
    
  )



not_missing_dod_all_summary <- not_missing_dod_all %>%
  tabyl (dod_all) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total) %>%
  rename (n_all = n,
          percent_all = percent)

not_missing_dod_summary <- not_missing_dod_all_summary %>%
  left_join(not_missing_dod_pds_chr_summary, by = c("dod_all" = "dod_disagree_pds_chr")) %>%
  left_join(not_missing_dod_sus_chr_summary, by = c("dod_all" = "dod_disagree_sus_chr")) %>%
  left_join(not_missing_dod_sus_pds_summary, by = c("dod_all" = "dod_disagree_sus_pds")) 

# tables needed - summary dod table and not missing dod summary 
s3write_using(not_missing_dod_summary, FUN=write.csv, object = "Hierarchy table/not_missing_dod_summary.csv", bucket = project_bucket)
s3write_using(summary_dod_table, FUN=write.csv, object = "Hierarchy table/summary_dod_table.csv", bucket = project_bucket)


# dementia

dementia_result <- linked_data_sets %>%
  select (pseudonhsno, dementia_sus, dementia) %>%
  mutate (
    dementia_num = if_else (is.na(dementia), NA_real_, if_else (dementia == "Yes",1,0))
  )%>%
  mutate (dementia_final = if_else (
    dementia_sus ==1 | dementia_num ==1,
    1,0  )) %>%
  select (pseudonhsno, dementia_final)



dementia <-MDS %>%
  select (pseudonhsno, dementia_sus, dementia, dementia_final) %>%
  mutate (
    dementia_num = if_else (is.na(dementia), NA_real_, if_else (dementia == "Yes",1,0))
  )


dementia_sus_table <- dementia %>%
  tabyl(dementia_sus) %>%
  adorn_totals (c("row", "col"))%>%
  rename (n_SUS = n,
          percent_SUS =percent) %>%
  as.data.frame()

dementia_ch_table <- dementia %>%
  tabyl(dementia) %>%
  adorn_totals (c("row", "col"))%>%
  rename (n_ch = n,
          percent_ch =percent) %>%
  as.data.frame()

not_missing_dementia_sus_ch <- dementia %>%
  filter (!is.na (dementia) & !is.na (dementia_sus)) %>% # need to do this sentence with is.na 
  mutate(dementia_disagree_sus_ch = ifelse(dementia_sus != dementia, "Different", "Same"))

not_missing_dementia_sus_ch_summary <- not_missing_dementia_sus_ch %>%
  tabyl (dementia_disagree_sus_ch) %>%
  adorn_totals (c("row", "col")) %>%
  select (-Total)  %>%
  rename (n_sus_ch = n,
          percent_sus_ch = percent)

# have added dementia as have in ambulance and ED crosstabs




# get a list of all the variables


MDS_variables <- colnames(MDS) %>%
as.data.frame()

s3write_using(MDS_variables, FUN=write.csv, object = "Linked data sets/MDS_variables.csv", bucket = project_bucket)


