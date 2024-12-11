## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: linkage_prep_CHdata.R

# Corresponding author: Liz Crellin (liz.crellin@health.org.uk)

# Descripton: this will bring all the care home level data sets together 

# Inputs:
# All the outputs of the previous scripts

# Outputs:
# One row per person data set with carehome level info included

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

# Source relevant scripts ------------------------------------------------

# Set up ####

library(aws.s3)
library(dplyr)
library(tidyverse)
library(lubridate)

## Define the project bucket
project_bucket <- '' # assign project directory



#CH id spine
pseudocqcids <- s3read_using(read.csv,
                             object = 'CH level data/ch_pseudo_spine.csv',   
                             #this file was generated in SAS direct from the cqc location ids provided by CH software providers
                             bucket = proj_bucket,
                             na.strings = c("", "NA")
)



#import the workforce questionnaire data
workforce <- s3read_using(read.csv,
                             object = 'CH level data/workforce_questionnaire/clean_pseudo/workforce_questionnaire.csv',   
                             #this file was generated in SAS. Standardized to most recent cqc id and then pseudonymised.
                             bucket = proj_bucket,
                             na.strings = c("", "NA")
)
#check one row per pseudocqcid
workforce %>% 
  select(pseudocqcid) %>% 
  n_distinct()
#rename recorded date for clarity on source
workforce <- workforce %>% 
  rename(RecordedDate_WF = RecordedDate)

#import the cleaned CQC data
cqcclean <- s3read_using(read.csv,
                          object = 'CH level data/CQC publicly available data/cqc_data_cleaned.csv',   
                          #cleaned file from 'CQC_data_prep_R'
                          bucket = proj_bucket,
                          na.strings = c("", "NA")
)
#check one row per pseudocqcid
cqcclean %>% 
  select(pseudocqcid) %>% 
  n_distinct()

#check some vars
cqcclean %>% 
  group_by(Care_home_) %>% 
  tally() 
cqcclean %>% 
  group_by(Location_Type_Sector) %>% 
  tally() 
cqcclean %>% 
  group_by(Location_Inspection_Directorate) %>% 
  tally() 
cqcclean %>% 
  group_by(Location_Primary_Inspection_Cate) %>% 
  tally() 
#Location_City - remove as too detailed
cqcclean %>% 
  group_by(Location_Type) %>% 
  tally() 

#remove unneeded cols and rename
cqcclean <- cqcclean %>% 
  select(!c(X, Care_home_, Location_Type_Sector, Location_Inspection_Directorate, 
            Location_Primary_Inspection_Cate, Location_City, Location_Type)) %>% 
  rename(Location_HSCA_start_date_CQC = Location_HSCA_start_date,
         Care_homes_beds_CQC = Care_homes_beds,
         Publication_Date_CQC = Publication_Date,
         Location_Region_CQC = Location_Region,
         Location_NHS_Region_CQC = Location_NHS_Region,
         Location_Local_Authority_CQC = Location_Local_Authority,
         Location_ONSPD_CCG_CQC = Location_ONSPD_CCG,
         Service_Type_CQC = service_type,
         #ratings can stay as they are as we know they are from CQC
         Years_Since_Reg_CQC = years_since_registration,
         Serv_user_dementia_CQC = serv_user_dementia
  )

#sort dates
cqcclean <- cqcclean %>%
  mutate(Location_HSCA_start_date_CQC = ymd(Location_HSCA_start_date_CQC),
         Publication_Date_CQC = dmy(Publication_Date_CQC))


#import the cleaned CH residency table
CHR <- s3read_using(read.csv,
                         object = 'person level data sets for linkage/CHR_pre_linkage.csv',   
                         #cleaned file from 'linkage_prep_CHR.R'
                         bucket = proj_bucket,
                         na.strings = c("", "NA")
)
CHR <- CHR %>% 
  select(-X)
CHR %>%
  select(Der_CQC_Location_CHR, Der_CQC_Service_Type_CHR, CCG_Of_Residence_CHR, CareHomeIndicator_CHR) %>% 
  n_distinct()
#latest record per care home
relevant_record_chr <- CHR %>%
  select(Der_Start_Date_CHR, Der_CQC_Location_CHR, Der_CQC_Service_Type_CHR, CCG_Of_Residence_CHR, CareHomeIndicator_CHR) %>% 
  group_by (Der_CQC_Location_CHR) %>%
  arrange(Der_CQC_Location_CHR, Der_Start_Date_CHR) %>%
  fill (everything (), .direction = "down") %>% # fills values above where there is NA 
  slice_tail(n=1) %>%  # take most recent record for the CH
  select(-Der_Start_Date_CHR) %>% 
  ungroup () %>%
  distinct () 
relevant_record_chr %>% 
  select(Der_CQC_Location_CHR) %>% 
  n_distinct()
#this has one row per pseudocqcid.
relevant_record_chr <- relevant_record_chr %>% 
  rename(Der_CQC_Location_CHRCH = Der_CQC_Location_CHR,
         Der_CQC_Service_Type_CHRCH = Der_CQC_Service_Type_CHR,
         CCG_Of_Residence_CHRCH = CCG_Of_Residence_CHR,
         CareHomeIndicator_CHRCH = CareHomeIndicator_CHR)


#then join

linked_ch_level <- pseudocqcids %>%
  left_join(workforce, by = "pseudocqcid") %>% 
  left_join(cqcclean, by = c("pseudocqcid"))%>%
  left_join(relevant_record_chr, by = c( "pseudocqcid" = "Der_CQC_Location_CHRCH"))


#output csv
s3write_using(linked_ch_level, 
              FUN=write.csv, 
              object = "Linked data sets/MDS_CH_level.csv", 
              bucket = proj_bucket)


#Now look to link with the person level MDS
#import it
linked_personlevel <- s3read_using(read.csv,
                     object = 'Linked data sets/MDS_personlevel.csv',   
                     bucket = proj_bucket,
                     na.strings = c("", "NA")
)
linked_personlevel <- linked_personlevel %>% 
  select(-X)


linked_MDS <- linked_personlevel %>% 
  left_join(linked_ch_level, by = "pseudocqcid")


# now generate new variables - registered bed capacity, years of service registration 
linked_MDS <- linked_MDS %>%
  mutate(
    registered_bed_capacity = case_when (
      is.na (Care_homes_beds_CQC) ~ "Missing", 
      Care_homes_beds_CQC <=24 ~ "0-24", 
      Care_homes_beds_CQC >= 25 & Care_homes_beds_CQC <=49 ~ "25-49",
      TRUE ~ "50+"),
    service_reg_yrs = case_when (
      is.na(Years_Since_Reg_CQC) ~ "Missing",
      Years_Since_Reg_CQC <=10 ~ "Less than 10 years",
      TRUE ~ "More than 10 years")
    
    )
      


#output csv
s3write_using(linked_MDS, 
              FUN=write.csv, 
              object = "Linked data sets/MDS.csv", 
              bucket = proj_bucket)

MDS_variables <- colnames(linked_MDS) %>%
 as.data.frame()

s3write_using(MDS_variables, FUN=write.csv, object = "wp4/Linked data sets/MDS_variables.csv", bucket = project_bucket)


#how is linkage looking for workforce survey? 
linkcheck <- linked_personlevel %>% 
  left_join(linked_ch_level, by = "pseudocqcid")
linkcheck %>% 
  filter(!is.na(RecordedDate_WF)) %>% 
  n_distinct()

