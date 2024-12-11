## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: ch_admission_flag.R

# Corresponding author:Freya Tracey (freya.tracey@health.org.uk)

# Description:Set a flag to show if the person was admitted to carehome within
# the year before the index date
#  

# Inputs:
# list of residents for linkage - residents_all_linkage.rds

# Outputs:
# dataset wtih IDs and flag to link in later 

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##

# Source relevant scripts -------------------------------------------------

library(tidyverse)
library (aws.s3)

project_bucket <- '' # assign project directory

index_CH_dates <- s3read_using(readRDS, object = 'residents_all_linkage.rds', bucket = project_bucket)

ch_flags <- index_CH_dates %>%
  mutate (
  CH_admission_flag = ch_admission >= index_wave1 - 365 & ch_admission <= index_wave1,
  wave  = ascotwave1 ==1 & ascotwave2 ==1 
  ) %>%
  select (pseudonhsno, CH_admission_flag, wave)# create a flag to show whether the CH admission is between index date and a year before the index date

s3write_using(ch_flags, FUN=write.csv, object = "person level data sets for linkage/ch_flags.csv", bucket = project_bucket)


