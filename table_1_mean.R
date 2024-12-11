## ==========================================================================##
# Project: DACHA

# Team: Improvement Analytics Unit (IAU) at the Health Foundation

# Script: table_1_mean.R

# Corresponding author: Freya Tracey (freya.tracey@health.org.uk)

# Descripton: generate tables for paper which summarises outcomes by groups 

# Inputs:
# MDS 

# Outputs:
# Series of tables for reporting

# Notes: To use, need to adjust locations of R scripts and csv files

## ==========================================================================##




library(aws.s3)
library(tidyverse)
library(gtsummary)
library(lubridate)
library(eeptools)

project_bucket <- '' # assign project directory
results_bucket <- '' # assign project directory for results


# Import data sets

ld <- s3read_using(read.csv, object = "Linked data sets/MDS.csv", bucket = project_bucket, na.strings = c("", "NA")) %>%
  #change care home dementia from yes/ no to 1/0 
  mutate (
    dementia = case_when(
      dementia  == "Yes" ~ 1, 
      dementia == "No" ~ 0,
    TRUE ~ NA
    )) %>% # create an additive dementia variable
  mutate (
    dementia_combo = case_when(
      is.na(f_dementia_h36) & is.na(c_dementia_h36) & is.na(dementia) ~ NA,
     f_dementia_h36 ==1 | c_dementia_h36 ==1 | dementia ==1 ~ 1,
      TRUE ~0
          
    ))

      
test <- ld %>%
  select (dementia, dementia_combo, f_dementia_h36, c_dementia_h36)




inputs <- s3read_using(read.csv, object = "Linked data sets/MDS_variables_filters_for_tables.csv", bucket = project_bucket, na.strings = c("", "NA"))

colnames(inputs) <- c("label", "variables", "filter1", "filter2", "filter3") 

subjects <- inputs %>% select(variables) %>% pull()
table_labels <- inputs %>% select(label) %>% pull()

vars_select <- c("sex_final", "nur_res", "dementia_combo", "IMD_quintile")


ages <- ld %>%
  filter(!is.na(index_date) & !is.na(dob_final)) %>%
  mutate(age = round(age_calc(as.Date(dob_final, format="%d/%m/%Y"),
                              as.Date(index_date), units="years"), 0),
         age_cat = case_when(
           age <65 ~ "<65",
           age %in% c(65:79) ~ "65-79",
           age >=80 ~ ">=80"
         )
  ) %>%
  select(pseudonhsno, age_cat)


# select and prepare relevant data
ld_variables <-  
  ld %>%
  select(pseudonhsno, all_of(vars_select), all_of(subjects), nr_elix_h36) %>%
  left_join(select(ages, pseudonhsno, age_cat)) %>%
  select(-pseudonhsno) %>%
  mutate(sex_final = as.factor(sex_final),
         nur_res = as.factor(nur_res),
         dementia_combo = as.factor(dementia_combo),
         IMD_quintile = as.factor(IMD_quintile),
         age_cat = fct_relevel(age_cat, c("<65", "65-79", ">=80")),
         IMD_quintile = fct_relevel(IMD_quintile, c("Most deprived fifth","Second most deprived fifth", "Middle fifth", "Second least deprived fifth", "Least deprived fifth"))
  ) %>%
  mutate_if(is.factor, fct_explicit_na)


create_table <- function(subject, labels) {
  
  ld_df <- ld_variables
  
  
  # create overall table
  tab_overall <- ld_df %>%
    select (!!rlang::sym(subject)) %>%
    tbl_summary(
      label = subject ~ labels,
      type =  c(subject ~ "continuous"), 
      statistic = list(
        all_continuous() ~ "{mean} ({sd})"
      ),
      digits = all_continuous() ~2
    ) %>% 
    add_n()%>%
    modify_header (label ~ "Variable")
    
  vars <- c("sex_final", "age_cat", "nur_res", "dementia_combo", "IMD_quintile")
  
  # create joined up table - needs to be a loop or a function
  tab_list <- lapply(vars, function(y) {
    
    ld_tab_1 <- ld_df %>%
      select(!!rlang::sym(subject), y) %>%
      tbl_summary(by=y,
                  label = subject ~ labels,
                  type =  c(subject ~ "continuous"), 
                  statistic = list(
                    all_continuous() ~ "{mean} ({sd})"
                  ),
                  digits = all_continuous() ~2
      ) %>% 
      add_n() %>%
      modify_header (label ~ "Variable")
    
    
  }) # close both function and lapply
  
  all_tables = c(tab_list, list(tab_overall))
  
  tbl_merge(tbls=all_tables, tab_spanner=c("Sex","Age","Nursing resident","Dementia","IMD quintile", "Overall"))
  
} # end create_tables()


summary_tables <- mapply(FUN=create_table, subject=subjects, labels=table_labels)

## functions to extract the relevant bits for editing

get_table_body <- function(subject) {
  summary_test <- summary_tables[,subject]
  summary_body <- summary_test$table_body
}

table_bodies <- lapply(subjects, get_table_body)
table_bodies_df <- as.data.frame(do.call(rbind, table_bodies))

get_table_header <- function(subject) {
  summary_test <- summary_tables[,subject]
  summary_header <- summary_test$table_styling$header
  Ns <- summary_header %>%
    mutate(table_subject = subject) %>%
    select(table_subject,spanning_header, modify_stat_n, modify_stat_level)
}

table_headers <- lapply(subjects, get_table_header)
table_headers_df <- as.data.frame(do.call(rbind, table_headers))


get_table_image <- function(subject) {
  summary_test <- summary_tables[,subject]
  image <- tbl_merge(tbls = summary_test$tbls, tab_spanner = c("Sex", "Age", "Nursing resident", 
                                                               "Dementia", "IMD quintile", "Overall"))
}

table_images <- lapply(subjects, get_table_image)


s3write_using(table_bodies_df, FUN=write.csv, object = "table_body.csv", bucket = results_bucket)
s3write_using(table_headers_df, FUN=write.csv, object = "table_head.csv", bucket = results_bucket)


#need to see what number of people are in each of the events

test <- ld_variables %>%
  select (starts_with("n_")) %>%
  summarise_all (~sum(. >0)) %>%
  bind_rows(ld_variables %>%
              group_by(IMD_quintile
                  ) %>%
              summarise (across (starts_with ("n_"), ~sum (. >0)))) %>%
  mutate (Label = ifelse (is.na(IMD_quintile), "Overall", IMD_quintile)) 
    
    
  

