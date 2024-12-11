## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: linkage_prep_SUS.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description: This will be the final script to prepare a pre linkage data set 
# for all SUS data sets, bringing together all the variables we said we would 
# include across the three (OP, APC, ECDS)

# Inputs:
# Individual one row per person datasets for OP, APC and ECDS 
# (come from derived_variables_x scripts)

# Outputs:
# One row per person dataset with all SUS data

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

# Source relevant scripts -------------------------------------------------

# Set up 

library (aws.s3)
library (tidyverse)

project_bucket <- '' # assign project directory

# Import APC data set 

APC_clean <- s3read_using(read.csv, object = "APC.csv", bucket = project_bucket, na.strings = c("", "NA"))%>%
  mutate (
          activity_date = dmy(activity_date) ,
          cip_end = dmy(cip_end) )

test <- APC_clean %>%
  distinct (pseudonhsno)

# Select variables from cleaned APC

APC <- APC_clean %>%
  select (c(
          pseudonhsno, # pseudo id
          adm_em_12h, # count of emergency adm in year pre index date
          adm_el_12h, # count of elective adm in year pre index date
          adm_avoid_12h, # count of avoidable emergency adm in year pre index date
          bedsore_12h,  # count of bedsore adm in year pre index date
          lrti_12h, # count of adm with lrti diag in year pre index date
          urti_12h, # count of adm with urti diag in year pre index date
          destch_12h # flag for any discharge to a care home in year pre index date
          # not adding death_hosp, this comes from derived_variables_APC script
           ),
          starts_with ("c_"), # charlson variables
          starts_with ("e_"), # elixhauser variables
          starts_with ("ec_"), # variables in charlson and elixhauser
          starts_with ("f_"), # not sure this one 
          starts_with ("nr_elix_")
  ) %>%
  rename(adm_bedsore_12h = bedsore_12h,
         adm_lrti_12h = lrti_12h,
         adm_urti_12h = urti_12h,
         discharge_ch_12h = destch_12h) %>% 
  distinct () 

# Read in APC derived variables

APC_CH <- s3read_using(read.csv, object = "person level data sets for linkage/APC_CH.csv", bucket = project_bucket, na.strings = c("", "NA"))
APC_CH <- APC_CH %>% 
  select(-X)

# Read in OP derived variables - nothing else to get from OP raw data set as covered in SUS demographics

OP <- s3read_using(read.csv, object = "person level data sets for linkage/outpatients.csv", bucket = project_bucket, na.strings = c("", "NA"))
OP <- OP %>% 
  select(-X)

# Read in ECDS derived variables - nothing else to get from ECDS eaw data set as covered in SUS demographics 

ECDS <- s3read_using(read.csv, object = "person level data sets for linkage/ECDS.csv", bucket = project_bucket, na.strings = c("", "NA"))
ECDS <- ECDS %>% 
  select(-X)

# Read in SUS demographics file

SUS_demographics <-s3read_using(read.csv, object = "demographics.csv", bucket = project_bucket, na.strings = c("", "NA")) 
# demographic information from SUS data sets, derived by combining info across SUS data sets

# Save APC pre-linkage dataset

SUS_pre_linkage <- APC %>%
  full_join(APC_CH, by = "pseudonhsno" ) %>%
  full_join(ECDS, by = "pseudonhsno" ) %>%
  full_join(OP, by = "pseudonhsno" ) %>%
  full_join(SUS_demographics, by= "pseudonhsno") %>%
  mutate ( # need to have some combined variables re anaemia, diabetes and hypertension for in text table
    diabetes_combined = ifelse(e_diab_comp_h36 ==1 |e_diab_uncomp_h36 ==1 ,1,0),
    anaemia_combined = ifelse( e_anaemia_bloodloss_h36 ==1 | e_anaemia_deficiency_h36 ==1,1,0),
    hypertension_combined = ifelse (e_ht_comp_h36 ==1 | e_ht_uncomp_h36 ==1, 1,0),
    ngproc_combined = ifelse(n_ED_attendances_ngproc==1 | n_ngproc_op ==1, 1,0)
          )


#save prelinkage data set 
s3write_using(SUS_pre_linkage, FUN=write.csv, object = "person level data sets for linkage/SUS_combo_pre_linkage.csv", bucket = project_bucket)

