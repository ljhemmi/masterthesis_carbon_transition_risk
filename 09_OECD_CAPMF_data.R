
# 09: OECD CAPMF data

# Project: Master Thesis
# Author: Lucas Jan Hemmi

# reset environment
rm(list = ls());gc()

# =============================================================================.
# Libraries ----
# =============================================================================.

# this section downloads and loads all required R packages as defined in the
# "packages" vector

# all packages that are needed
packages <- c(
  "data.table",
  "arrow",
  "stringi",
  "glue",
  "lubridate",
  "DBI",
  "dplyr",
  "readr",
  "readxl",
  "jsonlite",
  "httr",
  "countrycode",
  "tidyr",
  "corrplot",
  "ggrepel",
  "fixest",
  "zoo",
  "rsdmx"
)



# install missing packages
installed <- packages %in% installed.packages()[,"Package"]
if (any(!installed)) {
  install.packages(packages[!installed])
}

# load all the packages
lapply(packages, library, character.only = TRUE)

# =============================================================================.
# functions ----
# =============================================================================.



# =============================================================================.
# Data Gathering ----
# =============================================================================.

## 1) Calculate average score across Level 3 variables: ----
capmf <- as.data.frame(
  readSDMX("https://sdmx.oecd.org/public/rest/data/OECD.ENV.EPI,DSD_CAPMF@DF_CAPMF,1.0/AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+ARG+BRA+BGR+CHN+HRV+IND+IDN+MLT+PER+ROU+RUS+SAU+ZAF+AUS.A.POL_STRINGENCY.LEV3_BAN_PHOUT_COAL+LEV3_RENEWABLE_EXP+LEV3_EMIS_STD+LEV3_ETS_I+LEV3_CARBONTAX_I+LEV3_FFS_I+LEV3_EXCISETAX_I+LEV3_FIN_MECH_I+LEV3_MEPS_MOTOR+LEV3_EE_MANDATE+LEV3_ETS_B+LEV3_CARBONTAX_B+LEV3_FFS_B+LEV3_EXCISETAX_B+LEV3_FIN_MECH_B+LEV3_MEPS_APPL+LEV3_BC+LEV3_BAN_PHOUT_HEAT+LEV3_LABEL_APPL+LEV3_CONG_CHARG+LEV3_ETS_T+LEV3_CARBONTAX_T+LEV3_FFS_T+LEV3_EXCISETAX_T+LEV3_MEPS_T+LEV3_LABEL_CAR+LEV3_EXP_RAIL+LEV3_SPEED+LEV3_BAN_PHOUT_ICE+LEV3_NDC+LEV3_NZ+LEV3_RDD_EE+LEV3_RDD_CCS+LEV3_RDD_RES+LEV3_RDD_NUC+LEV3_RDD_HYDR+LEV3_RDD_OTHER+LEV3_BAN_PHOUT_EXTRAC+LEV3_FFS_PRODUCER+LEV3_METHAN+LEV3_AB+LEV3_TREATY+LEV3_INT_INIT+LEV3_PR_AV_MAR+LEV3_BAN_CREDIT+LEV3_BAN_FF_ABROAD+LEV3_EVAL_BR+LEV3_UNFCCC+LEV3_GHG_ACC+LEV3_ETS_E+LEV3_CARBONTAX_E+LEV3_FFS_E+LEV3_EXCISETAX_E+LEV3_FIT+LEV3_AUCTION+LEV3_RECS.0_TO_10?startPeriod=2009&endPeriod=2023&dimensionAtObservation=AllDimensions"
))

setDT(capmf)

capmf <- capmf[,.(TIME_PERIOD,REF_AREA,CLIM_ACT_POL,obsValue)]

capmf_wide <- dcast(
  capmf,
  TIME_PERIOD + REF_AREA ~ CLIM_ACT_POL,
  value.var = "obsValue"
)

# 2) Rename columns
setnames(capmf_wide,
         old = c("TIME_PERIOD", "REF_AREA"),
         new = c("year",       "iso3_country"))

# 3) Convert year to integer (optional but recommended)
capmf_wide[, year := as.integer(as.character(year))]

# 1) Identify the policy columns (all non-ID columns)
policy_cols <- setdiff(names(capmf_wide), c("year", "iso3_country"))
# or, more defensively:
# policy_cols <- grep("^LEV3_", names(capmf_wide), value = TRUE)

# 2) Compute the (non-lagged) average policy stringency per country-year
capmf_wide[, capmf_raw := rowMeans(.SD, na.rm = TRUE), .SDcols = policy_cols]

# If a row has all-NA policies, rowMeans() returns NaN; turn that into NA
capmf_wide[is.nan(capmf_raw), capmf_raw := NA_real_]

# 3) Standardise across the full panel (all countries x all years)
mu_capmf <- capmf_wide[, mean(capmf_raw, na.rm = TRUE)]
sd_capmf <- capmf_wide[, sd(capmf_raw, na.rm = TRUE)]

capmf_wide[, capmf_std := (capmf_raw - mu_capmf) / sd_capmf]

# capmf_std is CAPMF_ct (non-lagged), with mean 0 and sd 1 over the sample

# after capmf_std is created, before lagging
setorder(capmf_wide, iso3_country, year)

# 1) Define years including 2024
years_full <- seq(min(capmf_wide$year), max(capmf_wide$year) + 1L)  # e.g. 2009–2024

# 2) Create a balanced panel (all countries x all years)
panel_full <- CJ(
  iso3_country = unique(capmf_wide$iso3_country),
  year         = years_full
)

# 3) Merge your data onto the full panel  *** FIX IS HERE ***
capmf_wide_full <- capmf_wide[panel_full, on = .(iso3_country, year)]

# (optional) quick check: do we see 2024 now?
range(capmf_wide_full$year)      # should show ... 2024
capmf_wide_full[year == 2024][1] # inspect one example

# 4) Recompute the lag on the expanded panel
setorder(capmf_wide_full, iso3_country, year)
capmf_wide_full[, capmf_std_lag1 := shift(capmf_std, n = 1L, type = "lag"),
                by = iso3_country]

# 5) Select the variables you need
capmf_agg_wide <- capmf_wide_full[, .(year, iso3_country,
                                      capmf_raw, capmf_std, capmf_std_lag1)]
setorder(capmf_agg_wide, iso3_country, year)

# (optional) verify the lag for one country:
capmf_agg_wide[iso3_country == "AUT", .(year, capmf_std, capmf_std_lag1)]



## 2) direct from website ----
## 2) direct from website ----
capmf <- as.data.frame(
  readSDMX("https://sdmx.oecd.org/public/rest/data/OECD.ENV.EPI,DSD_CAPMF@DF_CAPMF,1.0/AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EU27_2020+ARG+BRA+BGR+CHN+HRV+IND+IDN+MLT+PER+ROU+RUS+SAU+ZAF+AUS.A.POL_STRINGENCY.LEV2_CROSS_SEC_GHGTAR+LEV2_CROSS_SEC_FFPP+LEV1_SEC+LEV1_INT+LEV1_CROSS_SEC.0_TO_10?startPeriod=2009&endPeriod=2025&dimensionAtObservation=AllDimensions")
)

setDT(capmf)

capmf <- capmf[, .(TIME_PERIOD, REF_AREA, CLIM_ACT_POL, obsValue)]

capmf_wide <- dcast(
  capmf,
  TIME_PERIOD + REF_AREA ~ CLIM_ACT_POL,
  value.var = "obsValue"
)

# ID variables
setnames(capmf_wide,
         old = c("TIME_PERIOD", "REF_AREA"),
         new = c("year",       "iso3_country"))

capmf_wide[, year := as.integer(as.character(year))]

# Rename Level 1/2 CAPMF vars
setnames(
  capmf_wide,
  old = c(
    "LEV1_SEC",
    "LEV1_CROSS_SEC",
    "LEV2_CROSS_SEC_GHGTAR",
    "LEV2_CROSS_SEC_FFPP",
    "LEV1_INT"
  ),
  new = c(
    "capmf_sectoral_policies",
    "capmf_crosssectoral_policies",
    "capmf_ghg_emission_targets",
    "capmf_fossil_fuel_production_policies",
    "capmf_international_policies"
  )
)

setorder(capmf_wide, iso3_country, year)

# ------------------------------------------------------------------
# 1) STANDARDISE each Step 2 variable over the observed sample
# ------------------------------------------------------------------

policy_cols2 <- c(
  "capmf_sectoral_policies",
  "capmf_crosssectoral_policies",
  "capmf_ghg_emission_targets",
  "capmf_fossil_fuel_production_policies",
  "capmf_international_policies"
)

# Standardise like in Part 1
for (v in policy_cols2) {
  mu_v <- capmf_wide[, mean(get(v), na.rm = TRUE)]
  sd_v <- capmf_wide[, sd(get(v), na.rm = TRUE)]
  capmf_wide[, paste0(v, "_std") := (get(v) - mu_v) / sd_v]
}

# ------------------------------------------------------------------
# 2) EXTEND by one extra year (last available year + 1)
# ------------------------------------------------------------------

# Last available year in Step 2 data (currently 2025)
last_year  <- max(capmf_wide$year, na.rm = TRUE)
first_year <- min(capmf_wide$year, na.rm = TRUE)

years_full2 <- seq(first_year, last_year + 1L)  # e.g. 2009–2026

panel_full2 <- CJ(
  iso3_country = unique(capmf_wide$iso3_country),
  year         = years_full2
)

# Left-join original data onto this expanded panel
capmf_wide_full <- capmf_wide[panel_full2, on = .(iso3_country, year)]

setorder(capmf_wide_full, iso3_country, year)

# ------------------------------------------------------------------
# 3) LAG the standardised variables within country
#     => in year (last_year + 1) you get lag = value from last_year
# ------------------------------------------------------------------

std_cols <- paste0(policy_cols2, "_std")

capmf_wide_full[, (paste0(std_cols, "_lag1")) :=
                  lapply(.SD, shift, n = 1L, type = "lag"),
                by = iso3_country,
                .SDcols = std_cols]

# Now, for each iso3_country:
#   - year == last_year + 1 has *_std_lag1 equal to year == last_year's *_std
#   - all extra rows (first_year .. last_year) behave like normal lags

# ------------------------------------------------------------------
# 4) MERGE with the Part 1 aggregate panel (capmf_agg_wide)
#     capmf_agg_wide goes up to 2024; join keeps that support.
# ------------------------------------------------------------------

setkey(capmf_wide_full, year, iso3_country)
setkey(capmf_agg_wide,   year, iso3_country)

capmf_merged <- capmf_wide_full[capmf_agg_wide]


write_parquet(capmf_merged, "../01_data/00_R_outputs/oecd_capmf.parquet")
