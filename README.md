# DACHA

Order for running scripts
Preamble - dealing with clear data/salt keys:

/resident_ids/create_client_ids.R (unlikely we'll need to re-run this - brings in client ids prior to work in SAS)
SAS scripts to generate resident pseudo ids file
SAS script to pseudonymise CQC public data (CH level)
SAS script to pseudonymise workforce questionnaire data (CH level)


R scripts:

Get key data from DCRs:

/resident_ids/residentids_add_dates.R - RUN


ANY SCRIPTS LABELLED DATA CLEANING OR DATA PREP

all in /Data cleaning/  - RUN
CQC data prep    - RUN
csds_cleaning - RUN - need to merge as on separate branch
csds_referrals_cleaning -RUN - need to merge as on separate branch


ANY SCRIPTS LABELLED DERIVED VARIABLES:

ambulance - RUN
APC - RUN
ASC - RUN
CSDS - RUN
ECDS - RUN
OP - RUN
PDS - RUN



ch_admission_flag - RUN


SCRIPTS LABELLED LINKAGE PREP:

linkage prep SUS - RUN
linkage prep CH data - RUN - NB linkage_prep_CH_data.R is important if any work has been done on ids.
linkage prep CH residency - RUN


LINKAGE

linking data sets.R  - RUN
linking CH level data sets.R  - RUN


OUTPUTS

one row per variable - RUN
Table 1 mean - RUN
