## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: derived_variables_PDS.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:Clean and derive variables for MDS from PDS data and save a
# version where we have one row per person 
#  

# Inputs:
# PDS data 

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

PDS <- s3read_using(readRDS, 
                    object = 'PDS.rds', 
                    bucket = project_bucket)


# want to get the PDS record which covers the period that the index date falls into 

most_recent_PDS <- PDS_test %>%
  group_by(pseudonhsno) %>% 
  filter (Change_Time_Stamp <= index_date) %>%
  slice_max(order_by = Change_Time_Stamp) %>%
  ungroup() %>%
  select(-Change_Time_Stamp) %>% 
  rename (
    practice_code_PDS = Der_Practice_Code,
    CCG_residence_PDS = Der_CCGofResidence,
    gender_PDS = Gender,
    date_of_death_PDS  = DateOfDeath,
    LSOA_PDS  = Der_Postcode_LSOA_Code,
    DOB_yr_mth_PDS =Der_DOBYearMth
  )

s3write_using(most_recent_PDS, 
              FUN=write.csv, 
              object = "person level data sets for linkage/PDS_pre_linkage.csv", 
              bucket = project_bucket)

