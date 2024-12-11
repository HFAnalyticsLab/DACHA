## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: derived_variables_OP.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:Clean and derive variables for MDS from ouptatient (OP) data and save a
# version where we have one row per person 
#  

# Inputs:
# OP activity data (cleaned)

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

index_CH_dates <- s3read_using(readRDS, object = 'residents_all_linkage.rds', bucket = project_bucket)

outpatients_raw <- s3read_using(read.csv, object = "OP.csv", bucket = project_bucket, na.strings = c("", "NA")) 

# Attendance status is numeric
# 0 - Not applicable
# 2 - Appt cancelled by patient
# 3 - patient did not attend
# 4 - appt cancelled by healthcare provider
# 5 - attended
# 6 - arrived late but was seen
# 7 - arrived late and unable to be seen 

# doesnt tab correctly with Der_Attendance_Type (categorical variable - admin event, attend, cancel (hos), cancel (pat), DNA, unknown)
# For 0, all are either admin event or unknown - makes sense
# For 2, 30 are admin event but rest are cancelled patient - mostly makes sense
# For 3, all are DNA - makes sense
# For 4, 65 are admin event and rest are cancelled hosp - mostly makes sense
# For 5, all are attend - makes sense
# For 6, all are attend - makes sense
# For 7, all are DNA- makes sense
# So only problems seen where appt is cancelled - dont think we will look at these so is ok - only interested in attended and missed

# Person level data set

outpatients_person_level <- outpatients_final %>%
  mutate(missed_app = case_when(attstatus_op %in% c(3, 1, 7) ~ 1, 
                                TRUE ~ 0)) %>%
  group_by (pseudonhsno) %>%
  filter(cntnondupe_op==1) %>%   
  filter (appdate_op >= (index_wave1 - 365) & appdate_op <= index_wave1) %>% # only want to see appts which were between the index date and a year before
  summarise (
    n_OP_appts = n(), 
    n_missed_OP_appts = sum(missed_app),
    n_ngproc_op = sum(ngproc_op)
             )

# this is going forward from index date 1 whereas above is going backwards

OP_person_level_WP5_wave <- outpatients_final %>%
  mutate(missed_app = case_when(attstatus_op %in% c(3, 1, 7) ~ 1, 
                                TRUE ~ 0)) %>%
  group_by (pseudonhsno) %>%
  filter(cntnondupe_op==1) %>%  
  filter (ascotwave1 ==1 & ascotwave2 ==1) %>% # this means there is a true index date for both  
  filter (appdate_op >= (index_wave1) & appdate_op <= index_wave2) %>% #Only want to see appointments which are between wave 1 and wave 2 index dates 
  summarise (
    n_OP_appts_wave = n(), 
    n_missed_OP_appts_wave = sum(missed_app) 
  )

OP_person_level_WP5_5m <- outpatients_final %>% 
  mutate(missed_app = case_when(attstatus_op %in% c(3, 1, 7) ~ 1,  
                                TRUE ~ 0)) %>%
  group_by (pseudonhsno) %>%
  filter(cntnondupe_op==1) %>%   
  filter (appdate_op >=  as.Date("2023-06-01") & appdate_op <= as.Date("2023-10-31")) %>% #Only want to see appointments which are between 1 June and 31 Oct index dates 
  summarise (
    n_OP_appts_5m = n(), 
    n_missed_OP_appts_5m = sum(missed_app) 
  )

outpatients_person_level_final <- outpatients_person_level %>%
  full_join(OP_person_level_WP5_5m, by = "pseudonhsno") 

outpatients_person_level_final <- outpatients_person_level_final %>%
  full_join(OP_person_level_WP5_wave, by = "pseudonhsno") 


#save into folder for all pre linkage datasets  
s3write_using(outpatients_person_level_final, 
              FUN=write.csv, 
              object = "person level data sets for linkage/outpatients.csv", 
              bucket = project_bucket)


  
