## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: derived_variables_ASC.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:Clean and derive variables for MDS from ASC data and save a
# version where we have one row per person 
#  

# Inputs:
# ASC data 

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

ASC_raw <- s3read_using(read.csv, object = "ASC.csv", bucket = raw_data_bucket, na.strings = c("", "NA")) %>%
  mutate(
    Event_Start_Date = ymd (Event_Start_Date),
    Event_End_Date = ymd (Event_End_Date))

#start with removing total duplicates 
ASC <- distinct(ASC_raw) 

# Want to find the observation which covers the period of the index date

index_CH_dates <- s3read_using(readRDS, object = 'residents_all_linkage.rds', bucket = project_bucket)

ASC <- ASC %>%
  left_join(index_CH_dates, by= c("Der_Pseudo_NHS_Number" = "pseudonhsno"))

relevant_record <- ASC %>%
  group_by (Der_Pseudo_NHS_Number) %>%
  filter (Event_Start_Date<= index_wave1)  %>% # only look at records which are before the index date
  arrange(Der_Pseudo_NHS_Number, (index_wave1 - Event_Start_Date)) %>%
  slice (1) %>% # take highest placing one
  ungroup () %>%
  distinct () # some may only have records which come after the index date

ASC_final <- relevant_record %>%
  select (Der_Pseudo_NHS_Number,
          Accommodation_Status,
          Client_Funding_Status,
          Service_Component,
          Service_Type) %>%
  rename (
    Accommodation_Status_ASC = Accommodation_Status,
    Client_Funding_Status_ASC = Client_Funding_Status,
    Service_Component_ASC = Service_Component,
    Service_Type_ASC = Service_Type) 


#save into folder for all pre linkage datasets 
s3write_using(ASC_final, 
              FUN=write.csv, 
              object = "person level data sets for linkage/ASC_pre_linkage.csv", 
              bucket = project_bucket)

