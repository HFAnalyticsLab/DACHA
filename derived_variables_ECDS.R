## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: derived_variables_ECDS.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:Clean and derive variables for MDS from ECDS data and save a
# version where we have one row per person 
#  

# Inputs:
# ECDS activity data 

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

index_CH_dates <- s3read_using(readRDS, object = 'residents_all_linkage.rds', bucket = project_bucket) %>%
  mutate (index_wave1 =as.Date (index_wave1, format = "%d/%m/%Y"))

ECDS_raw <- s3read_using(read.csv, object = "ECDS.csv", bucket = project_bucket, na.strings = c("", "NA")) %>%
  mutate (
    Arrival_Date = dmy(arrdate_ec),
    Arrival_Time = hms::as_hms(arrtime_ec), 
    EC_Departure_Date = dmy(departdate_ec),
    EC_Departure_Time = hms::as_hms(departtime_ec),
    EC_Injury_Date = dmy (injdate_ec), # need to change from character to date/time structur
    EC_Injury_Time = hms::as_hms(injtime_ec),
    long_attendance = ifelse (tnrdur_ec >=720,1,0) # in minutes so 12 hours is 720
  ) 

# notes about EC_Arrival_Mode_SNOMED_CT
# 1047991000000102  - arrival by prison transport
# 1048001000000106 - arrival by police transport
# 1048021000000102  - arrival by non-emergency road ambulance
# 1048031000000100  - arrival by emergency road ambulance
# 1048041000000109  - arrival by emergency road ambulance with medical escort
# 1048051000000107  - arrival by helicopter air ambulance
# 1048061000000105  - arrival by public transport
# 1048071000000103  - arrival by own transport

# Based on ECDS_ETOS_v4.0.6 for all times the word 'fall' is mentioned, but could do more complete snomed code selection
# 161898004 - falls (finding)
# 430576002 - at increased risk for injury due to fall (finding)
# 54670004 - slipping (event)
# 75941004 - tripping (event)
# 240871000000104 - fall through height of less than one metre (event)
# 429482004 - fall from high place (event)

# Person level data set

ECDS_person_level <- ECDS_final %>%
  group_by (pseudonhsno) %>%
  filter(cntnondupe==1) %>%  
  filter (Arrival_Date >= (index_wave1 - 365) & Arrival_Date <= index_wave1) %>% # only want to see appts which were between the index date and a year before
  summarise (
    n_ED_attendances = n(), 
    n_ED_unplanned = sum(cntunplan==1),
    n_ED_seenunplan = sum(cntseenunplan==1),
    n_out_of_hours_ED_attendances = sum (ifelse (Arrival_Time >hms ("18:00:00")| Arrival_Time <hms ("08:00:00"), 1,0)),
    n_long_ED_attendances = sum (long_attendance ==1), 
    n_ED_attendances_via_ambulance = sum (ifelse (arrmode_ec %in% c ("1048031000000100","1048021000000102", "1048041000000109", "1048051000000107"),1,0)),
    n_ED_attendances_for_falls = sum (ifelse (chief_ec %in% c ("161898004", "430576002", "54670004", "75941004", "240871000000104", "429482004") | injmech_ec %in% c("54670004", "75941004", "240871000000104", "429482004"),1,0)),
    n_ED_attendances_ngproc = sum(ngproc_ec==1)
              )


ECDS_person_level_wave <- ECDS_final %>%
  group_by (pseudonhsno) %>%
  filter(cntnondupe==1) %>% 
  filter (ascotwave1 ==1 & ascotwave2 ==1) %>% # this means there is a true index date for both  
  filter (Arrival_Date >= (index_wave1) & Arrival_Date <= index_wave2) %>% #Only want to see appointments which are between wave 1 and wave 2 index dates 
  summarise (
    n_ED_attendances_wave = n_distinct (uid), 
    n_ED_unplanned_wave = sum(cntunplan==1),
    n_ED_seenunplan_wave = sum(cntseenunplan==1),
    n_out_of_hours_ED_attendances_wave = sum (ifelse (Arrival_Time >hms ("18:00:00")| Arrival_Time <hms ("08:00:00"), 1,0)),
    n_long_ED_attendances_wave = sum (long_attendance ==1), 
    n_ED_attendances_via_ambulance_wave = sum (ifelse (arrmode_ec %in% c ("1048031000000100","1048021000000102", "1048041000000109", "1048051000000107"),1,0)),
    n_ED_attendances_for_falls_wave = sum (ifelse (chief_ec %in% c ("161898004", "430576002", "54670004", "75941004", "240871000000104", "429482004") | injmech_ec %in% c("54670004", "75941004", "240871000000104", "429482004"),1,0))
  )

ECDS_person_level_5m <- ECDS_final %>%
  group_by (pseudonhsno) %>%
  filter(cntnondupe==1) %>%  
  filter (Arrival_Date >=  "2023-06-01" & Arrival_Date <= "2023-10-31") %>% #Only want to see appointments which are between 1 June and 31 Oct index dates 
  summarise (
    n_ED_attendances_5m = n_distinct (uid), 
    n_ED_unplanned_5m = sum(cntunplan==1),
    n_ED_seenunplan_5m = sum(cntseenunplan==1),
    n_out_of_hours_ED_attendances_5m = sum (ifelse (Arrival_Time >hms ("18:00:00")| Arrival_Time <hms ("08:00:00"), 1,0)),
    n_long_ED_attendances_5m = sum (long_attendance ==1), 
    n_ED_attendances_via_ambulance_5m = sum (ifelse (arrmode_ec %in% c ("1048031000000100","1048021000000102", "1048041000000109", "1048051000000107"),1,0)),
    n_ED_attendances_for_falls_5m = sum (ifelse (chief_ec %in% c ("161898004", "430576002", "54670004", "75941004", "240871000000104", "429482004") | injmech_ec %in% c("54670004", "75941004", "240871000000104", "429482004"),1,0))
  )

# now join all 3 data sets together

ECDS_person_level_final <- ECDS_person_level %>%
  full_join(ECDS_person_level_5m, by = "pseudonhsno") 

ECDS_person_level_final <- ECDS_person_level_final %>%
  full_join(ECDS_person_level_wave, by = "pseudonhsno") 


#save into folder for all pre linkage datasets 
s3write_using(ECDS_person_level_final, 
              FUN=write.csv, 
              object = "person level data sets for linkage/ECDS.csv", 
              bucket = project_bucket)

