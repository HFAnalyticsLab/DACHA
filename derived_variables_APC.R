## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: derived_variables_APC.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:Clean and derive variables for MDS from APC data and save a
# version where we have one row per person 
#  

# Inputs:
# APC data (cleaned)

# Outputs:
# One row per person data set 

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

# Source relevant scripts -------------------------------------------------

# Set up 

library (aws.s3)
library (tidyverse)
library(janitor)

project_bucket <- '' # assign project directory

# Import data sets

source (filepaths.R) #script with all filepaths in

APC_clean <- s3read_using(read.csv, object = "APC.csv", bucket = project_bucket, na.strings = c("", "NA"))%>%
  mutate (activity_date = dmy(activity_date) ,
          cip_end = dmy(cip_end) ) %>%
  select(-index_date)

index_CH_dates <- s3read_using(readRDS, object = 'residents_all_linkage.rds', bucket = project_bucket)

#Begin deriving variables
APC_final <- APC_clean %>%
  left_join(., index_CH_dates, by='pseudonhsno') %>%
  mutate (
    CH_admission_flag = ch_admission >= index_wave1 - 365 & ch_admission <= index_wave1
  )

APC_person_level_deathhosp <- APC_final %>% #flag for if died in hospital between first index date and 31 Oct
  group_by (pseudonhsno) %>% 
  filter (activity_date >= index_wave1 & activity_date <= as.Date("2023-10-31")) %>%  #appts between index date and 31 Oct.
  summarise (
    death_hosp_postindex = max(death_hosp ==1)
  )

APC_person_level_WP5_wave <- APC_final %>% 
  group_by (pseudonhsno) %>%
  filter (ascotwave1 ==1 & ascotwave2 ==1) %>% # this means there is a true index date for both  
  filter (activity_date >= (index_wave1) & activity_date <= index_wave2) %>% #Only want to see appointments which are between wave 1 and wave 2 index dates 
  summarise (
    n_admissions_wave = n(), 
    n_elective_admissions_wave = sum (adm_el ==1)  ,
    n_emergency_admissions_wave  = sum (adm_em ==1),
    n_avoidable_emergency_admissions_wave = sum (adm_avoid ==1) , 
    n_admissions_bedsore_wave = sum (bedsore ==1) 
  )

APC_person_level_WP5_5m <- APC_final %>%
  group_by (pseudonhsno) %>%
  filter (activity_date >=  (as.Date("2023-06-01")) & activity_date <= as.Date("2023-10-31")) %>% 
  summarise (
    n_admissions_5m = n(), 
    n_elective_admissions_5m = sum (adm_el ==1)  ,
    n_emergency_admissions_5m  = sum (adm_em ==1),
    n_avoidable_emergency_admissions_5m = sum (adm_avoid ==1) , 
    n_admissions_bedsore_5m = sum (bedsore ==1) 
  )

APC_person_level <-  APC_person_level_deathhosp %>% 
  left_join(APC_person_level_WP5_5m, by = "pseudonhsno") %>%
  left_join(APC_person_level_WP5_wave, by = "pseudonhsno")



#save into folder for all pre linkage datasets
s3write_using(APC_person_level, 
              FUN=write.csv, 
              object = "person level data sets for linkage/APC_CH.csv",
              bucket = project_bucket)


