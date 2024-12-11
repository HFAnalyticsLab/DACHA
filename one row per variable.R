## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: one row per varaible.R

# Corresponding author: Freya Tracey (freya.tracey@health.org.uk)

# Descripton: generate tables for paper which have one row per variable summary 

# Inputs:
# MDS 

# Outputs:
# Series of tables for reporting

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##
library (tidyverse)
library (dplyr)
library (aws.s3)
library (gtsummary)
library (ggplot2)

project_bucket <- '' # assign project directory

# Import data sets

MDS <- s3read_using(read.csv, object = "MDS.csv", bucket = project_bucket, na.strings = c("", "NA"))  %>%
  select (-X) %>%
  mutate (
    #barthel needs to be multiplied by 5 to be from 0-20 score to 0-100 score
    barthel_score = ifelse (!is.na (barthel_score), barthel_score*5, NA)
  ) 

MDS_variables_filters <- s3read_using(read.csv, object = "MDS_variables.csv", bucket = project_bucket, na.strings = c("", "NA")) 

variables_to_exclude <- MDS_variables_filters %>%
  filter (is.na(Filter.1)) %>%
  select (
 -Filter.1, -Filter.2, -Filter.3
  ) %>%
  as.list()

frailty_conditions <- c ("f_anxdep_h36",
                          "f_dependence_h36",
                          "f_fallsfract_h36",
                          "f_incont_h36",
                          "f_mobprob_h36",
                          "f_pulcers_h36",
                          "f_senility_h36",
                          "cogimp_sus"
)

MDS_for_table <- MDS %>%
  # select (-(any_of (variables_to_exclude[[1]]))) %>%
  # need to change NAs to be missing for categorical variables so they are included in %
  # remove NAs for all continuous variables - want everything to have the same denominator, but not for diagnoses 
 mutate (IMD_quintile = coalesce(IMD_quintile, "Missing"),
         DiscussedPreferredDeathLocation_Indicator = coalesce(DiscussedPreferredDeathLocation_Indicator, "Missing"),
         DeathLocationPreferred_Type = coalesce(DeathLocationPreferred_Type, "Missing"),
         Client_Funding_Status_ASC = coalesce(Client_Funding_Status_ASC, "Missing"),
         Der_CQC_Service_Type_CHR = coalesce(Der_CQC_Service_Type_CHR, "Missing"),
         cogimp_sus = ifelse(
           rowSums(select (., c(f_delirium_h36, f_dementia_h36, f_senility_h36)))>0, 1,0)) %>%
  mutate (
         n_frailty_conditions = rowSums (select(., all_of(frailty_conditions)))
 )

#conditions 
conditions_n <- MDS_for_table %>%
  select (
    starts_with ("c_"), # charlson variables
    starts_with ("e_"), # elixhauser variables
    starts_with ("ec_"), # variables in charlson and elixhauser
    starts_with ("f_"), # frailty syndromes 
  ) %>%
  tbl_summary(
    statistic = list(
      all_categorical() ~ "{n}" # need small n suppression
    ),
    digits = all_continuous() ~2
  ) %>% 
  add_n() %>%
  modify_header (label ~ "Variable") %>%
  as.data.frame() %>%
  filter (!grepl( "Unknown", Variable)) %>%
  select (- "**N**") %>%
  rename (n= "**N = 727**")

conditions_pct <- MDS_for_table %>%
  select (
    starts_with ("c_"), # charlson variables
    starts_with ("e_"), # elixhauser variables
    starts_with ("ec_"), # variables in charlson and elixhauser
    starts_with ("f_"), # frailty syndromes 
  ) %>%
  tbl_summary(
    statistic = list(
      all_categorical() ~ "{p}%" # need small n suppression
    ),
    digits = all_continuous() ~2
  ) %>% 
  add_n() %>%
  modify_header (label ~ "Variable") %>%
  as.data.frame() %>%
  filter (!grepl( "Unknown", Variable)) 
  
conditions <- conditions_n %>%
  left_join(conditions_pct, by= "Variable") %>%
  rename (pct= "**N = 727**")

# have removed unknowns - still need to have top elixhauser bit 

conditions_overview_1 <- MDS_for_table %>%
  select (
    nr_elix_h36,
  ) %>%
  tbl_summary(
    statistic = list(
      all_continuous() ~ "{mean} ({sd})"
    ),
    digits = all_continuous() ~2
  ) %>% 
  add_n() %>%
  modify_header (label ~ "Variable") %>%
  as.data.frame() 
  
conditions_overview_2 <- MDS_for_table %>%
  select (
    nr_elix_2_h36,
  ) %>%
  tbl_summary(
    statistic = list(
      all_continuous() ~ "{mean} ({sd})"
    ),
    digits = all_continuous() ~2
  ) %>% 
  add_n() %>%
  modify_header (label ~ "Variable") %>%
  as.data.frame() %>%
  filter (!grepl( "Unknown", Variable))


conditions_final <- conditions_overview_1 %>%
  bind_rows(
    conditions_overview_2
  ) %>%
  bind_rows(
    conditions
  )

### all continuous numbers 

#to report a table which shows n with value of 1 or more, N with non missing value and % with an event need to turn into a categorical variable 

MDS_for_table_binary <- MDS_for_table %>%
  select (
    "adm_em_12h",
    "adm_el_12h",
    "adm_avoid_12h",
    "adm_lrti_12h",
"adm_urti_12h",
"ngproc_combined",
    starts_with("n_")
  ) %>%
  mutate (across (everything (),
                  ~case_when(
                    . >=1 ~1,
                    TRUE ~0
                  ),
                  .names = "{.col}_binary")) %>%

  select (
    ends_with("_binary")
  )


# so now need to use this data set to report 3 columns - CHANGE HERE 

percent <- MDS_for_table_binary %>%
  pivot_longer(
    cols=everything(),
    names_to = "category",
    values_to = "counts"
  ) %>%
  group_by(category) %>%
  na.omit() %>%
  summarise(
    total_n = n(),
    happened = sum(counts)
  ) %>%
  mutate(
    percent =(happened/ total_n)
  )



# now second part of table which is total activity, total N and mean and sd
all_counts_means <- MDS_for_table %>%
  select (
    "adm_em_12h",
    "adm_el_12h",
    "adm_avoid_12h",
    "adm_lrti_12h",
    "adm_urti_12h",
    "ngproc_combined",
    starts_with("n_")
  ) %>%
  mutate_all(~ifelse (is.na(.),0,.)) %>% # want everything to be a 0 if its a NA
  tbl_summary(
    type =  c(
      "adm_em_12h",
      "adm_el_12h",
      "adm_avoid_12h",
      "adm_lrti_12h",
      "adm_urti_12h",
      "ngproc_combined",
      starts_with("n_")) ~ "continuous",
    statistic = list(
      all_continuous() ~ "{mean} ({sd})"
    ),
    digits = all_continuous() ~2
  ) %>%
  add_n()%>%
  modify_header (label ~ "Variable") %>%
  as.data.frame()
# need to add in having two columns, one with n/N and one with mean

col_sums <- MDS_for_table %>%
  summarise (across (where(is.numeric), ~sum(., na.rm = TRUE), .names = "{.col}_colsum")) %>%
  select (ends_with("_colsum")) %>%
  pivot_longer(cols = everything (), names_to = "Variable", values_to = "Value") %>%
  mutate (Value = round (Value)) %>%
  mutate (Variable = str_remove (Variable, "_colsum")) 


all_counts_activity_means_final <- all_counts_means%>%
  left_join (col_sums, by = "Variable") %>%
  filter (Variable != "Unknown") %>% 
  relocate (
    Variable, Value, "**N**", "**N = 727**"
   ) %>%
  rename ("Total activity" = "Value")
  

# now have to do the other parts of the table which have been missed 

categorical_variables_n <- MDS_for_table %>%
  select (
   -adm_em_12h,
   -adm_el_12h,
   -adm_avoid_12h,
   -starts_with("n_"),
   -starts_with ("c_"), 
   -starts_with ("e_"), 
   -starts_with ("ec_"), 
   -starts_with ("f_"),
   -nr_elix_2_h36,
   -nr_elix_h36
            )%>%
  tbl_summary(
    statistic = list(
      all_categorical() ~ "{n}"
    ),
    digits = all_continuous() ~2
  ) %>% 
  as.data.frame()


categorical_variables_N <- MDS_for_table %>%
  select (
    -adm_em_12h,
    -adm_el_12h,
    -adm_avoid_12h,
    -starts_with("n_"),
    -starts_with ("c_"), 
    -starts_with ("e_"), 
    -starts_with ("ec_"), 
    -starts_with ("f_"),
    -nr_elix_2_h36,
    -nr_elix_h36
  )%>%
  tbl_summary(
    statistic = list(
      all_categorical() ~ "{N}"
    ),
    digits = all_continuous() ~2
  ) %>% 
  as.data.frame()



categorical_variables_pct <- MDS_for_table %>%
  select (
    -adm_em_12h,
    -adm_el_12h,
    -adm_avoid_12h,
    -starts_with("n_"),
    -starts_with ("c_"), 
    -starts_with ("e_"), 
    -starts_with ("ec_"), 
    -starts_with ("f_"),
    -nr_elix_2_h36,
    -nr_elix_h36
  )%>%
  tbl_summary(
    statistic = list(
      all_categorical() ~ "{p}%"
    ),
    digits = all_continuous() ~2
  ) %>% 
  as.data.frame()

# continuous variables from care home record

continuous_chvariables <- MDS_for_table %>%
  select (
    waterlow_recent,
    braden_recent,
    iaged_score,
    barthel_score,
    ascot_scrqol,
    icecap_qol,
    eq5d_vas,
    UK_crosswalk,
    ascot_mascore_p
  ) %>%
  tbl_summary(
    type = everything () ~"continuous",
    statistic = list(
      all_continuous() ~ "{mean} ({sd})"
    ),
    digits = all_continuous() ~2
  ) %>%
  add_n()%>%
  modify_header (label ~ "Variable") %>%
  as.data.frame()

los <- MDS%>%
  select (pseudonhsno,  lengthofstay)%>%
  mutate (lengthofstay = ifelse (lengthofstay >14000, NA, lengthofstay)) %>% #change record w >14000 to be missing
  summarise (
    mean = (mean (lengthofstay, na.rm = TRUE)),
    sd = (sd(lengthofstay, na.rm = TRUE)))


CH_level_data <- MDS_for_table %>%
  select (starts_with("Q3_"))%>%
  summarise (across (everything (), list (
    mean = ~round(mean (.x, na.rm = TRUE),2),
    sd = ~round(sd(.x, na.rm = TRUE),2),
    na_count= ~sum (is.na (.))
    ))) 

s3write_using(CH_level_data, FUN=write.csv, object = "Linked data sets/CH_level_data.csv", bucket = project_bucket)





s3write_using(continuous_chvariables, FUN=write.csv, object = "Linked data sets/continuous_chvariables.csv", bucket = project_bucket)


s3write_using(all_counts_activity_means_final, FUN=write.csv, object = "Linked data sets/all_counts_activity_means_final.csv", bucket = project_bucket)
s3write_using(percent, FUN=write.csv, object = "Linked data sets/all_counts_percent_final.csv", bucket = project_bucket)
s3write_using(conditions_final, FUN=write.csv, object = "Linked data sets/conditions_final.csv", bucket = project_bucket)

#cant join these together as >1 row with label unknown 
s3write_using(categorical_variables_n, FUN=write.csv, object = "Linked data sets/categorical_variables_n.csv", bucket = project_bucket)
s3write_using(categorical_variables_N, FUN=write.csv, object = "Linked data sets/categorical_variables_N.csv", bucket = project_bucket)
s3write_using(categorical_variables_pct, FUN=write.csv, object = "Linked data sets/categorical_variables_pct.csv", bucket = project_bucket)




