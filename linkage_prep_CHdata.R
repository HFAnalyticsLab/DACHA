## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: linkage_prep_CHdata.R

# Corresponding author: Liz Crellin (liz.crellin@health.org.uk)

# Description: Prepare digital carehome record by adding necessary pseudo
# identifiers


# Inputs: List of all residents with IDs and cleaned CH data
# 

# Outputs:
# Digital care home record information ready for linkage in MDS

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

# Source relevant scripts -------------------------------------------------


library(aws.s3)
library(dplyr)
library(tidyverse)
library(lubridate)

project_bucket <- '' # assign project directory



#Latest list of residents with all ids
resident_ids <- s3read_using(readRDS, 
                    object = 'residents_all_linkage.rds', 
                    bucket = project_bucket)

#import the cleaned CH data
ch_dataset <-  s3read_using(read.csv,
                              object = 'Final_resident_dataset_update.csv', 
                              bucket = project_bucket,
                              na.strings = c("", "NA")
)  


#Aims: 
#add pseudonhsnos and pseudocqcids to the CH dataset so can be linked (based on client ids)
#one row - wave 1 only
#any changes for us in r/for linkage


#check waves
ch_dataset %>% 
  group_by(wave) %>% 
  tally() 

#check provider
ch_dataset %>% 
  filter(wave==1) %>% 
  group_by(provider) %>% 
  tally()

resident_ids %>% 
  group_by(Provider) %>% 
  tally() 

#wave 1 and wave 2 cuts
ch_dataset_w1 <- ch_dataset %>% 
  filter(wave==1)
ch_dataset_w2 <- ch_dataset %>% 
  filter(wave==2)

column_names_old <- colnames(ch_dataset_w2)
columns_names_new <- paste0(column_names_old, "_w2")
colnames(ch_dataset_w2) <- columns_names_new
         
         
#separate by provider and then join back together afterwards

ch_dataset_nw1 <- ch_dataset_w1 %>% 
  filter(provider==1)

ch_dataset_pw1 <- ch_dataset_w1 %>% 
  filter(provider==2) %>% 
  mutate(hashedid = toupper(hashedid))

ch_dataset_nw2 <- ch_dataset_w2 %>% 
  filter(provider_w2==1)

ch_dataset_pw2 <- ch_dataset_w2 %>% 
  filter(provider_w2==2) %>% 
  mutate(hashedid_w2 = toupper(hashedid_w2))

#check distinct ids
ch_dataset_nw1 %>% 
  select(clientid) %>% 
  n_distinct()
ch_dataset_nw2 %>% 
  select(clientid_w2) %>% 
  n_distinct()
ch_dataset_pw1 %>% 
  select(hashedid) %>% 
  n_distinct()
ch_dataset_pw2 %>% 
  select(hashedid_w2) %>% 
  n_distinct()

#join to get pseudonhsnos and pseudocqcids
ch_dataset_n_linked <- resident_ids %>% 
  filter(Provider=="N") %>% 
  left_join(ch_dataset_nw1, by = c("ClientID_N" = "clientid")) %>% 
  select(-provider, -wave, -ch_admission, -index_wave1, -index_wave2, -ascotwave1, -ascotwave2, -hashedid, -ClientID_PCS ) %>% 
  left_join(ch_dataset_nw2, by = c("ClientID_N" = "clientid_w2")) %>% 
  select(-provider_w2, -wave_w2, -ClientID_N, -residentid_w2, -hashedid_w2, -both_waves_w2) %>% 
  relocate(pseudonhsno, pseudocqcid) 

ch_dataset_p_linked <- resident_ids %>% 
  filter(Provider=="P") %>% 
  left_join(ch_dataset_pw1, by = c("ClientID_P" = "hashedid")) %>% 
  select(-provider, -ch_admission, -index_wave1, -index_wave2, -ascotwave1, -ascotwave2, -clientid, -ClientID_N) %>% 
  left_join(ch_dataset_pw2, by = c("ClientID_P" = "hashedid_w2")) %>% 
  select(-provider_w2, -wave_w2, -ClientID_P, -residentid_w2, -both_waves_w2) %>% 
  relocate(pseudonhsno, pseudocqcid) 

#Append them together again
ch_dataset_linkage <- bind_rows(ch_dataset_n_linked, ch_dataset_p_linked)

#double check the numbers
ch_dataset_linkage %>% 
  group_by(Provider) %>% 
  tally() 



############################
# save to S3
s3saveRDS(x = ch_dataset_linkage
          ,object = 'ch_dataset_linkage.rds'
          ,bucket = project_bucket
          ,multipart=TRUE)

#Export as csv for review with Yannis (wrt final output to WP5)
s3write_using(ch_dataset_linkage, 
              FUN=write.csv, 
              object = "ch_dataset_linkage.csv", 
              bucket = project_bucket,
              row.names = FALSE,
              quote=FALSE)


