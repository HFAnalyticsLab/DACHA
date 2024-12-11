## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: derived_variables_CSDS.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:Clean and derive variables for MDS from CSDS data and save a
# version where we have one row per person 
#  

# Inputs:
# CSDS activity data 

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

CSDS_activity <- s3read_using(read.csv, object = "CSDS.csv", bucket = project_bucket, na.strings = c("", "NA")) %>%
  select (-X)

index_CH_dates <- s3read_using(readRDS, object = 'residents_all_linkage.rds', bucket = project_bucket)

CSDS_activity <- CSDS_activity %>%
  left_join(index_CH_dates, by = c("Der_Pseudo_NHS_Number" = "pseudonhsno"))


CSDS_activity <- CSDS_activity %>% 
  mutate (
    CH_admission_flag = ch_admission >= index_wave1 - 365 & ch_admission <= index_wave1, # create a flag to show whether the CH admission is between index date and a year before the index date
    missed_app = case_when(AttendanceStatus %in% c(7,2,3) ~ 1,
                           TRUE ~ 0), #7 - patient arrived late and wasnt seen, 2- appt cancelled by patient, 3- DNA
    f2f = case_when (
      Consultation_MediumUsed ==1 ~1, TRUE ~0),
    dn = case_when(
      Referral_TeamType ==12 ~1, TRUE ~0),
    SALT = case_when(
      Referral_TeamType ==33 ~1, TRUE ~0),
    podiatry = case_when(
      Referral_TeamType ==27 ~1, TRUE ~0),
    continence = case_when(
      Referral_TeamType ==07 ~1, TRUE ~0),
    rehab = case_when(
      Referral_TeamType ==29 ~1, TRUE ~0)
      )

# Person level data set

CSDS_person_level <- CSDS_activity %>%
  group_by (Der_Pseudo_NHS_Number) %>%
  filter (Contact_Date >= (index_wave1 - 365) & Contact_Date <= index_wave1) %>% # only want to see appts which were between the index date and a year before
  summarise (
    n_cs_appts= n(),
    n_cs_missed_appts = sum(missed_app),
    n_cs_f2f_appts = sum (f2f),
    n_cs_appts_dn = sum (dn),
    n_cs_appts_SALT = sum (SALT),
    n_cs_appts_podiatry = sum (podiatry),
    n_cs_appts_continence = sum (continence),
    n_cs_appts_rehab = sum (rehab),
    n_cs_services = length (unique (Referral_TeamType))
      )

CSDS_person_level_5m <- CSDS_activity %>%         
  group_by (Der_Pseudo_NHS_Number) %>%
  filter (Contact_Date >=  "2023-06-01" & Contact_Date <= "2023-10-31") %>% #Only want to see appointments which are between 1 June and 31 Oct index dates 
    summarise (
    n_cs_appts_5m= n(),
    n_cs_missed_appts_5m = sum(missed_app),
    n_cs_f2f_appts_5m = sum (f2f),
    n_cs_appts_dn_5m = sum (dn),
    n_cs_appts_SALT_5m = sum (SALT),
    n_cs_appts_podiatry_5m = sum (podiatry),
    n_cs_appts_continence_5m = sum (continence),
    n_cs_appts_rehab_5m = sum (rehab),
    n_cs_services_5m = length (unique (Referral_TeamType))
  )

CSDS_person_level_wave <- CSDS_activity %>%
  group_by (Der_Pseudo_NHS_Number) %>%
  filter (ascotwave1 ==1 & ascotwave2 ==1) %>% # this means there is a true index date for both  
  filter (Contact_Date >= (index_wave1) & Contact_Date <= index_wave2) %>% #Only want to see appointments which are between wave 1 and wave 2 index dates 
  summarise (
    n_cs_appts_wave= n(),
    n_cs_missed_appts_wave = sum(missed_app),
    n_cs_f2f_appts_wave = sum (f2f),
    n_cs_appts_dn_wave = sum (dn),
    n_cs_appts_SALT_wave = sum (SALT),
    n_cs_appts_podiatry_wave = sum (podiatry),
    n_cs_appts_continence_wave = sum (continence),
    n_cs_appts_rehab_wave = sum (rehab),
    n_cs_services_wave = length (unique (Referral_TeamType))
  )

csds_person_level_final <- CSDS_person_level %>%
  full_join(CSDS_person_level_5m, by = "Der_Pseudo_NHS_Number") %>%
  full_join(CSDS_person_level_wave, by = "Der_Pseudo_NHS_Number")


#save into folder for all pre linkage datasets 
s3write_using(csds_person_level_final, 
              FUN=write.csv, 
              object = "person level data sets for linkage/csds.csv", 
              bucket = project_bucket)
