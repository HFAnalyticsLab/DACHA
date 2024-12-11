## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: derived_variables_ambulance.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:Clean and derive variables for MDS from ambulance data and save a
# version where we have one row per person 
#  

# Inputs:
# Ambulance activity data 

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


ambulance_clean <- s3read_using(read.csv, object = "Ambulance_clean.csv", bucket = project_bucket) %>%
  mutate (
    time_call_connected = ymd_hms(time_call_connected),
    time_call_answered = ymd_hms (time_call_answered), 
    time_resource_arrived_on_scene = ymd_hms (time_resource_arrived_on_scene)
  ) # need to change from character to date/time structure 

# Begin derived variables
ambulance <- ambulance_clean %>%
  mutate (out_of_hours = hour(time_call_connected) >=18 | hour(time_call_connected) < 8,
          ambulance_conveyance_ED = receiving_location_type_cad == "ED",                  
                                                                              
          ambulance_attendance_fall = chief_complaint_call_triage_code == 28 )       

ambulance$ambulance_conveyance_ED[is.na(ambulance$ambulance_conveyance_ED)] <- FALSE # Included as the NAs were playing havoc with the summing below 

check_ooh <- ambulance %>%
  select (out_of_hours, time_call_connected) %>% 
  mutate(check = case_when((out_of_hours == TRUE & (hour(time_call_connected) < 18 & hour(time_call_connected) > 8)) ~ 'CHECK',
                           TRUE ~ 'OK'))

index_CH_dates <- s3read_using(readRDS, object = 'residents_all_linkage.rds', bucket = project_bucket)

ambulance_index <- ambulance %>%
  left_join(index_CH_dates, by= c("Der_Pseudo_NHS_Number" = "pseudonhsno"))

ambulance_index<- ambulance_index %>%
  mutate (receiving_location_type_cad= if_else(
    receiving_location_type_cad == "ZZZZZ",
    NA,
    receiving_location_type_cad
  )) # change invalid codes to missing 


#ambulance data is only going forward in time from index date 1, not backwards so dont have prev year data variables
ambulance_person_level_wave <- ambulance_index %>%
  group_by (Der_Pseudo_NHS_Number)%>%
  filter (ascotwave1 ==1 & ascotwave2 ==1) %>% # this means there is a true index date for both 
  filter (call_date >= (index_wave1) & call_date <= index_wave2) %>% #Only want calls which are between wave 1 and wave 2 index dates 
  summarise (
    n_ambulance_call_outs_wave = n(),
    n_ambulance_attendances_wave = sum(!is.na(time_resource_arrived_on_scene)),
    n_ambulance_conveyances_wave = sum(!is.na (receiving_location_type_cad)), 
    n_ambulance_attendances_OOH_wave = sum(out_of_hours == TRUE),
    n_ambulance_attendances_fall_wave = sum(ambulance_attendance_fall == TRUE), 
    n_ambulance_conveyances_ED_wave = sum(ambulance_conveyance_ED == TRUE) 
  )

ambulance_person_level_5m <- ambulance_index %>%
  group_by (Der_Pseudo_NHS_Number)%>%
  filter (call_date >=  "2023-06-01" & call_date <= "2023-10-31") %>% #Only want to see appointments which are between 1 June and 31 Oct index dates 
  summarise (
    n_ambulance_call_outs_5m = n(),
    n_ambulance_attendances_5m = sum(!is.na(time_resource_arrived_on_scene)),
    n_ambulance_conveyances_5m = sum(!is.na (receiving_location_type_cad)), 
    n_ambulance_attendances_OOH_5m = sum(out_of_hours == TRUE),
    n_ambulance_attendances_fall_5m = sum(ambulance_attendance_fall == TRUE), 
    n_ambulance_conveyances_ED_5m = sum(ambulance_conveyance_ED == TRUE) 
  )

ambulance_person_level <- ambulance_person_level_5m %>%
  full_join(ambulance_person_level_wave, by= "Der_Pseudo_NHS_Number")


#save into folder for all pre linkage datasets 
s3write_using(ambulance_person_level, 
              FUN=write.csv, 
              object = "person level data sets for linkage/ambulance_pre_linkage.csv", 
              bucket = project_bucket)



