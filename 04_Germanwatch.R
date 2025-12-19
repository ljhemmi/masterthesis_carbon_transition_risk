# Prep & Load in Data ----

rm(list = ls());gc()

# all packages that are needed
packages <- c("tidyr",
              "dplyr",
              "writexl",
              "readxl",
              "data.table",
              "bit64",
              "arrow",
              "lubridate"
)

# install required packages
# install missing packages
installed <- packages %in% installed.packages()[,"Package"]
if (any(!installed)) {
  install.packages(packages[!installed])
}


# load all the packages
lapply(packages, library, character.only = TRUE)


# =============================================================================.
# Settings & Variables ----
# =============================================================================.


# =============================================================================.
# Data Load ----
# =============================================================================.

## I: Germanwatch / CCPI data
ccpi <- read_excel("../01_data/02_germanwatch/01_data/CCPI Policy Rating Nat. + Internat. Aggregated Grades_2007-2025.xlsx",
                  sheet = 2)

# assume `ccpi` is already read; make it a data.table
setDT(ccpi)

# keep only ID cols + the "YYYY_International/National" cols
year_cols <- grep("^\\d{4}_(International|National)$", names(ccpi), value = TRUE)
DT <- ccpi[, c("iso_country_3", "Country", year_cols), with = FALSE]

# wide -> long
long <- melt(
  DT,
  id.vars       = c("iso_country_3", "Country"),
  variable.name = "year_type",
  value.name    = "score",
  variable.factor = FALSE
)

# split into year + type
long[, c("year", "type") := tstrsplit(year_type, "_", fixed = TRUE)]
long[, year := as.integer(year)]
long[, year_type := NULL]

# rename ID columns
setnames(long, c("iso_country_3", "Country"), c("iso3_country", "country"))

# now pivot long -> wide so we get:
#   national_pol_score
#   international_pol_score
wide <- dcast(
  long,
  year + country + iso3_country ~ type,
  value.var = "score"
)

# rename to requested column names
setnames(
  wide,
  c("International", "National"),
  c("internat_ccpi_score", "nat_ccpi_score")
)

# final ordering
setcolorder(
  wide,
  c("year", "country", "iso3_country",
    "nat_ccpi_score", "internat_ccpi_score")
)
setorder(wide, country, year)



# Standardize CCPI Scores per Year (0–10) ----
# Create a copy so the original 'wide' is preserved
wide_std <- copy(wide)

# Standardize national scores per year: 0 = lowest, 10 = highest
wide_std[, nat_ccpi_std := {
  minv <- min(nat_ccpi_score, na.rm = TRUE)
  maxv <- max(nat_ccpi_score, na.rm = TRUE)
  
  # If there is meaningful variation, scale 0–10; otherwise return NA
  if (is.finite(minv) && is.finite(maxv) && maxv > minv) {
    (nat_ccpi_score - minv) / (maxv - minv) * 10
  } else {
    NA_real_
  }
}, by = year]

# Standardize international scores per year: 0 = lowest, 10 = highest
wide_std[, internat_ccpi_std := {
  minv <- min(internat_ccpi_score, na.rm = TRUE)
  maxv <- max(internat_ccpi_score, na.rm = TRUE)
  
  # If there is meaningful variation, scale 0–10; otherwise return NA
  if (is.finite(minv) && is.finite(maxv) && maxv > minv) {
    (internat_ccpi_score - minv) / (maxv - minv) * 10
  } else {
    NA_real_
  }
}, by = year]

# Combined standardized score (sum of national + international; range 0–20)
wide_std[, combined_ccpi_std := nat_ccpi_std + internat_ccpi_std]

# Separate Table with Standardized Scores Only ----
std_scores <- wide_std[
  ,
  .(
    year,
    country,
    iso3_country,
    nat_ccpi_std,
    internat_ccpi_std,
    combined_ccpi_std
  )
]

# Order for readability
setorder(std_scores, country, year)

# save parquet
write_parquet(wide, "../01_data/00_R_outputs/germanwatch_output.parquet")
write_parquet(std_scores, "../01_data/00_R_outputs/germanwatch_output_std.parquet")
