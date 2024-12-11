## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: derived_variables_ASC.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description: Information about care home stay to be organised and prepped for
# linkage

# Inputs: Care home residency info 

# Outputs:
# One row per person dataset 

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

# Source relevant scripts -------------------------------------------------

# Set up 

library (aws.s3)
library (tidyverse)

project_bucket <- '' # assign project directory

# Import data set 

CHR <- s3read_using(readRDS, object = "care_home_residency.rds", bucket = project_bucket) %>% 
  select (-ch_admission)
test <- CHR %>%
  distinct (pseudonhsno) 


relevant_record <- CHR %>%
  mutate (index_wave1 = as.POSIXct(index_wave1)) %>% # need to have in same format as the der_start_date 
  group_by (pseudonhsno) %>%
  filter (Der_Start_Date<= index_wave1)  %>% # only look at records which are before the index date
  arrange(pseudonhsno, (index_wave1 - Der_Start_Date)) %>%
  fill (-Der_End_Date, .direction = "up") %>% # fills values above where there is NA, for all except end date 
  slice (1) %>% # take most recent record which will have info from prev records if most recent had NA 
  ungroup () %>%
  distinct () 

# change this to have the right variables and add correct suffix 
CHR_final <- relevant_record %>%
  select (pseudonhsno,
          Der_Start_Date,
          Der_End_Date,
          Der_CQC_Location,
          Der_CQC_Service_Type,
          CCG_Of_Residence, 
          YearMonthDeath_YYYYMM,
          CareHomeIndicator,
          DeathFlag,
          DeathInHospFlag) %>%
  rename (
    Der_Start_Date_CHR = Der_Start_Date,
    Der_End_Date_CHR = Der_End_Date,
    Der_CQC_Location_CHR = Der_CQC_Location ,
    Der_CQC_Service_Type_CHR = Der_CQC_Service_Type,
    CCG_Of_Residence_CHR = CCG_Of_Residence, 
    YearMonthDeath_YYYYMM_CHR  = YearMonthDeath_YYYYMM,
    CareHomeIndicator_CHR = CareHomeIndicator,
    DeathFlag_CHR = DeathFlag,
    DeathInHospFlag_CHR = DeathInHospFlag)

  
#save into folder for all pre linkage datasets 
s3write_using(CHR_final, FUN=write.csv, object = "person level data sets for linkage/CHR_pre_linkage.csv", bucket = project_bucket)





  