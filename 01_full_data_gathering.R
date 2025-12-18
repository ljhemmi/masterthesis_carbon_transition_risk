
# 01: combined table data gathering 

# Project: Master Thesis
# Author: Lucas Jan Hemmi

# reset environment
rm(list = ls());gc()

# restart R session to connect to wrds
.rs.restartR()

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
  "countrycode"
)

# install missing packages
installed <- packages %in% installed.packages()[,"Package"]
if (any(!installed)) {
  install.packages(packages[!installed])
}

# load all the packages
lapply(packages, library, character.only = TRUE)

# =============================================================================.
# Settings ----
# =============================================================================.

# This section includes all variable settings for time-frame and section toggles.
# It also contains helper functions used across the script.

## 1) dates and filter toggles ----
# trucost (for emissions data)
trucost_min_date <- '2007-01-01'
trucost_max_date <- '2025-09-30'
trucost_toggle <- TRUE

# Compustat FX rates (for FX data)
comp_fx_min_date <- '2007-01-01'
comp_fx_max_date <- '2025-10-17'
comp_fx_toggle <- TRUE

# Compustat combined global & NA firm & security (for primary shares)
comp_g_NA_firm_sec_trucost_filter_toggle <- TRUE
comp_scope <- "Both" # alternative are "NorthAmerica" and "Global"

# Compustat combined annual fundamentals data firm (for balance sheet / accounting data)
comp_funda_trucost_filter_toggle <- TRUE
comp_funda_scope <- "Both" # alternative are "NorthAmerica" and "Global"
comp_funda_min_date <- '2007-01-01'
comp_funda_max_date <- '2025-09-01'

# compustat NA monthly (for return and shares outstanding (NA))
comp_NA_monthly_trucost_filter_toggle <- TRUE

# compustat daily (for return and shares outstanding (non-NA))
comp_daily_min_date <- '2007-01-01'
comp_daily_max_date <- '2025-09-01'
comp_daily_toggle <- FALSE
comp_daily_trucost_filter_toggle <- TRUE

## 2) main helper functions ----
msg_keep <- function(tag, before, after) {
  pct <- if (before > 0) round((after / before) * 100, 3) else NA_real_
  message(glue("{tag}: kept {after} of {before} rows ({pct}%)."))
}

collapse_spaces <- function(x) {
  stri_trim_both(stri_replace_all_regex(x, "\\s+", " "))
}

pad <- function(x) {
  stri_c(" ", x, " ")
}

pad_collapse <- function(x) {
  pad(collapse_spaces(x))
}

to_num_safe <- function(x) {
  suppressWarnings(as.integer(sub("^([0-9]+).*$", "\\1", as.character(x))))
}

regex_any <- function(v) {
  paste0("(?i)(", paste(v, collapse="|"), ")")
}

rm_names_exact <- function(x, n1, n2) {
  # normalize targets
  x2  <- pad(collapse_spaces(fifelse(is.na(x),  "", x)))
  # normalize patterns (no padding yet)
  n1n <- stringi::stri_trim_both(collapse_spaces(fifelse(is.na(n1), "", n1)))
  n2n <- stringi::stri_trim_both(collapse_spaces(fifelse(is.na(n2), "", n2)))
  
  # replace CONM
  i1 <- !is.na(n1n) & stringi::stri_length(n1n) > 2L
  if (any(i1)) {
    pat1 <- paste0(" ", n1n[i1], " ")  # add exactly one space boundary
    x2[i1] <- stringi::stri_replace_all_fixed(
      x2[i1], pat1, " ", vectorize_all = TRUE
    )
  }
  
  # replace CONML
  i2 <- !is.na(n2n) & stringi::stri_length(n2n) > 2L
  if (any(i2)) {
    pat2 <- paste0(" ", n2n[i2], " ")
    x2[i2] <- stringi::stri_replace_all_fixed(
      x2[i2], pat2, " ", vectorize_all = TRUE
    )
  }
  
  pad(collapse_spaces(x2))
}

wb_to_long <- function(dt, value_name) {
  year_cols <- setdiff(names(dt), id_cols)
  long <- melt(
    dt,
    id.vars       = c("Country Name", "Country Code"),
    measure.vars  = year_cols,
    variable.name = "year",
    value.name    = value_name
  )
  long[, year := as.integer(gsub("^X", "", year))]
  long
}


limited_ffill_months <- function(DT,
                                 value_cols,
                                 date_col,
                                 by_cols,
                                 max_gap_months = 11L) {
  
  stopifnot(inherits(DT, "data.table"))
  
  # Standardize column names
  date_col  <- deparse(substitute(date_col))
  
  # Convert value_cols to character vector
  if (is.character(value_cols)) {
    value_cols_chr <- value_cols
  } else {
    value_cols_chr <- vapply(substitute(value_cols)[-1L], deparse, character(1L))
  }
  
  # Convert by_cols to character vector
  if (is.character(by_cols)) {
    by_cols_chr <- by_cols
  } else {
    by_cols_chr <- vapply(substitute(by_cols)[-1L], deparse, character(1L))
  }
  
  # Temporary columns
  tmp_last <- ".__last_obs_date_tmp__"
  tmp_gap  <- ".__gap_months_tmp__"
  
  # Order by grouping + date
  setorderv(DT, c(by_cols_chr, date_col))
  
  # 1) Compute last observed date (same for all value_cols)
  DT[
    ,
    (tmp_last) := {
      v <- get(value_cols_chr[1])  # use first col for NA structure
      d <- get(date_col)
      nafill(
        fifelse(!is.na(v), d, as.Date(NA_integer_)),
        type = "locf"
      )
    },
    by = by_cols_chr
  ]
  
  # 2) Compute month gaps once
  DT[
    ,
    (tmp_gap) :=
      12L * (year(get(date_col)) - year(get(tmp_last))) +
      (month(get(date_col)) - month(get(tmp_last)))
  ]
  
  # 3) Apply forward fill for each column in value_cols
  for (col in value_cols_chr) {
    tmp_ff <- paste0(col, "_ff_candidate")
    
    # forward fill within groups
    DT[, (tmp_ff) := nafill(get(col), type = "locf"), by = by_cols_chr]
    
    # apply limited fill
    DT[
      ,
      (col) := fifelse(
        get(tmp_gap) <= max_gap_months,
        get(tmp_ff),
        as.numeric(NA)
      )
    ]
    
    # cleanup
    DT[, (tmp_ff) := NULL]
  }
  
  # Remove shared temp columns
  DT[, c(tmp_last, tmp_gap) := NULL][]
  
  invisible(DT)
}


limited_ffill_months_character_robust <- function(DT,
                                                  value_cols,
                                                  date_col,
                                                  by_cols,
                                                  max_gap_months = 11L) {
  
  stopifnot(inherits(DT, "data.table"))
  
  # --- helper: forward-fill any vector (numeric, Date, character, factor, ...)
  ffill_any <- function(x) {
    if (is.numeric(x) || is.logical(x)) {
      # fast path
      return(nafill(x, type = "locf"))
    } else {
      # generic LOCF for non-numeric (Date, character, factor, etc.)
      res <- x
      last_val <- NA
      for (i in seq_along(x)) {
        if (!is.na(x[i])) {
          last_val <- x[i]
        } else if (!is.na(last_val)) {
          res[i] <- last_val
        }
      }
      res
    }
  }
  
  # --- standardize names
  date_col <- deparse(substitute(date_col))
  
  if (is.character(value_cols)) {
    value_cols_chr <- value_cols
  } else {
    value_cols_chr <- vapply(substitute(value_cols)[-1L], deparse, character(1L))
  }
  
  if (is.character(by_cols)) {
    by_cols_chr <- by_cols
  } else {
    by_cols_chr <- vapply(substitute(by_cols)[-1L], deparse, character(1L))
  }
  
  # --- temp column names
  tmp_last <- ".__last_obs_date_tmp__"
  tmp_gap  <- ".__gap_months_tmp__"
  
  # --- 0) order by group + date
  setorderv(DT, c(by_cols_chr, date_col))
  
  # --- 1) compute last observed date based on the FIRST value column
  DT[
    ,
    (tmp_last) := {
      v <- get(value_cols_chr[1L])
      d <- get(date_col)
      
      # build vector: d where v is non-NA, NA otherwise (as Date)
      base <- rep(as.Date(NA), length(d))
      idx  <- which(!is.na(v))
      if (length(idx)) base[idx] <- d[idx]
      
      # forward fill dates
      ffill_any(base)
    },
    by = by_cols_chr
  ]
  
  # --- 2) compute gap in months once
  DT[
    ,
    (tmp_gap) :=
      12L * (year(get(date_col)) - year(get(tmp_last))) +
      (month(get(date_col)) - month(get(tmp_last)))
  ]
  
  # --- 3) for each value column: forward fill, then blank out too-large gaps
  for (col in value_cols_chr) {
    tmp_ff <- paste0(col, "_ff_candidate")
    
    # forward fill within groups (type-aware)
    DT[
      ,
      (tmp_ff) := ffill_any(get(col)),
      by = by_cols_chr
    ]
    
    # first assign the filled values
    DT[, (col) := get(tmp_ff)]
    
    # then erase values where the gap is too large
    DT[get(tmp_gap) > max_gap_months, (col) := NA]
    
    # drop temp ff column
    DT[, (tmp_ff) := NULL]
  }
  
  # --- 4) clean up shared temp columns
  DT[, c(tmp_last, tmp_gap) := NULL][]
  
  invisible(DT)
}

# =============================================================================.
# WRDS access & data cleaning ----
# =============================================================================.

## I: Trucost data ----

# this section collects the emissions data from trucost, fills missing reported values
# with disclosed ones, labels disclosed and estimated based on text descriptions, 
# calculates intensites, YoY growth rates and saves output to parquet

### 1) Pull Trucost data ----
if (trucost_toggle) {
  
  message("- Downloading Trucost full dataset ...")
  
  #TODO: decide if we only need gvkey and company name from wc.
  # Full Trucost extract
  query_trucost_full <- sprintf("
    SELECT DISTINCT
        -- wrds_companies (wc)
        wc.gvkey,
        wc.institutionid,
        wc.companyname,
        wc.companytype,
        wc.country,
        wc.incorporation_country,
        wc.simpleindustry,
        wc.status,

        -- wrds_environment (we)
        we.periodenddate,
        we.fiscalyear,
        we.periodid,
        we.reportedcurrencyisocode,
        we.tcprimarysectorid,

        -- di_* fields
        -- pre-calculated intensities (tCO2e/US$ mn Revenues)
        we.di_319407, -- Intensity: GHG Scope 1
        we.di_319408, -- Intensity: GHG Scope 2 Location-based
        we.di_319409, -- Intensity: GHG Scope 3 Upstream
        
        -- scope 1
        we.di_319413, -- Absolute: GHG Scope 1
        we.di_319403_text, -- Scope 1 Carbon Disclosure
        NULL::text AS ghg_scope1_class, -- placeholder to fill in R
        we.di_376883, -- Absolute: GHG Scope 1 As Reported
        we.di_377148_text, -- Scope 1 GHG Disclosure As Reported
        NULL::text AS ghg_scope1_reported_class, -- placeholder to fill in R
        
        -- scope 2
        we.di_319414, -- Absolute: GHG Scope 2 Location-based
        we.di_329689_text, -- Scope 2 Location-based Carbon Dioxide Disclosure
        NULL::text AS ghg_scope2_loc_class, -- placeholder to fill in R
        we.di_376884, -- Absolute: GHG Scope 2 Location-based As Reported
        we.di_377149_text, -- Scope 2 Location-based GHG Disclosure As Reported
        NULL::text AS ghg_scope2_loc_reported_class, -- placeholder to fill in R
        
        -- scope 3
        we.di_319415, -- Absolute: GHG Scope 3 Upstream Total
        we.di_368758_text, -- Scope 3 GHG Upstream Total Disclosure
        NULL::text AS ghg_scope3_up_class, -- placeholder to fill in R
        
        -- additional trucost info
        we.di_319522, -- Trucost Total Revenue
        -- we.di_319416, -- Weighted Disclosure: GHG (Scope 1)
        we.di_323618_text, -- Effective Date of Revenue Calculation
        we.di_323615_text -- Effective date of Environmental Cost Calculation
        
        -- TBD
        -- we.di_326737, -- Absolute: GHG Scope 3 Downstream Total (not used)
        -- we.di_326738, -- Intensity: GHG Scope 3 Downstream Total (not used)
        -- we.di_368314, -- Intensity: GHG Scope 2 Market-based (not used)
        -- we.di_367750, -- Absolute: GHG Scope 2 Market-based (not used)
        -- we.di_376886 -- Absolute: GHG Scope 2 Market-based As Reported
        

    FROM trucost_common.wrds_companies wc
    JOIN trucost_environ.wrds_environment we
      ON we.institutionid = wc.institutionid
    WHERE wc.gvkey <> ''
      AND we.periodenddate BETWEEN '%s' AND '%s' -- in time-frame
      AND (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL) -- non full NA scope 1
      AND (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL) -- non full NA scope 2
    ORDER BY wc.gvkey, we.periodenddate
  ", trucost_min_date, trucost_max_date)
  
  trucost_full <- DBI::dbGetQuery(wrds, query_trucost_full)
  
  # variable name mapping
  message("- Renaming di_ columns ...")
  di_map <- c(
    # intensities
    di_319407       = "ghg_scope1_intensity",
    di_319408       = "ghg_scope2_loc_intensity",
    di_319409       = "ghg_scope3_upstream_intensity",
    # scope 1
    di_319413       = "ghg_scope1_absolute",
    di_319403_text  = "ghg_scope1_disclosure",
    di_376883       = "ghg_scope1_abs_reported",
    di_377148_text  = "ghg_scope1_disclosure_reported",
    # scope 2
    di_319414       = "ghg_scope2_loc_absolute",
    di_329689_text  = "ghg_scope2_loc_disclosure",
    di_376884       = "ghg_scope2_loc_abs_reported",
    di_377149_text  = "ghg_scope2_loc_disclosure_reported",
    # scope 3
    di_319415       = "ghg_scope3_upstream_absolute",
    di_368758_text  = "ghg_scope3_upstream_disclosure",
    # additional info
    di_319522       = "trucost_total_revenue_usd",
    di_319416       = "weighted_ghg_scope1",
    di_323618_text  = "revenue_effective_date",
    di_323615_text  = "env_cost_effective_date",
    # TBD
    di_326737       = "ghg_scope3_down_absolute",
    di_326738       = "ghg_scope3_down_intensity",
    di_368314       = "ghg_scope2_mkt_intensity",
    di_367750       = "ghg_scope2_mkt_absolute",
    di_376886       = "ghg_scope2_mkt_abs_reported"
  )
  
  setDT(trucost_full)
  
  # Apply the mapping
  old_names <- intersect(names(di_map), names(trucost_full))
  new_names <- di_map[old_names]
  setnames(trucost_full, old = old_names, new = new_names)
  
  ### Unique gvkeys
  message("- Extracting unique Trucost gvkeys ...")
  query_trucost_gvkeys <- sprintf("
    SELECT DISTINCT wc.gvkey
    FROM   trucost_common.wrds_companies wc
    JOIN   trucost_environ.wrds_environment we
      ON we.institutionid = wc.institutionid
    WHERE  wc.gvkey <> ''
      AND  we.periodenddate BETWEEN '%s' AND '%s'
      AND  (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL)
      AND  (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL)
  ", trucost_min_date, trucost_max_date)
  
  trucost_gvkeys <- DBI::dbGetQuery(wrds, query_trucost_gvkeys)
  
  message("trucost data loaded successfully.")
  
} else {
  
  message("Skipping Trucost data download (toggle set to FALSE).")
  trucost_full   <- NULL
  trucost_gvkeys <- NULL
}

### 2) Estimated vs. disclosed labeling----
# labeling rules according to Aswani et al. (2024) (p. 85-86)
trucost_disclosure_label_map <- c(
  "Estimate derived by applying revenue-based intensity factors to revenue data" = "estimated",
  "Value derived from data provided in Environmental/CSR" = "disclosed",
  "Exact Value from CDP" = "disclosed",
  "Extrapolated from prior year" = "estimated",
  "Exact Value from Environmental/CSR" = "disclosed",
  "Exact Value from personal communication" = "disclosed",
  "Value derived from data provided in CDP" = "disclosed",
  "Estimate based on partial data disclosure in Environmental/CSR" = "estimated",
  "Value summed up from data provided in Environmental/CSR" = "disclosed",
  "Disclosure rejected as data does not cover consolidated operations" = "estimated",
  "Value split from data provided in Environmental/CSR" = "disclosed",
  "Value derived from data provided in Annual Report/10K/Financial Accounts Disclosure" = "disclosed",
  "Value derived from fuel use provided in Environmental/CSR" = "disclosed",
  "Estimate based on partial data disclosure in CDP" = "estimated",
  "Value derived from data provided in personal communication" = "disclosed",
  "Estimate based on partial data disclosure in personal communication" = "estimated",
  "Data approximated from chart/graph in Environmental/CSR Report/Website" = "disclosed", # here maybe disclosed
  "Value derived from fuel use provided in Annual Report/10K/Financial Accounts Disclosure" = "disclosed",
  "Exact Value from Annual Report/10K/Financial Accounts Disclosure" = "disclosed",
  "Value summed up from data provided in CDP" = "disclosed",
  "Value derived from fuel use provided in CDP" = "disclosed",
  "Value split from data provided in personal communication" = "disclosed",
  "Disclosure rejected as data is normalised and no aggregating factor is available" = "estimated",
  "Data approximated from chart/graph in Annual Report/10K/Financial Accounts Disclosure" = "disclosed", # here maybe disclosed
  "Value split from data provided in CDP" = "disclosed",
  "Value summed up from data provided in personal communication" = "disclosed",
  "Value split from data provided in Annual Report/10K/Financial Accounts Disclosure" = "disclosed",
  "Value summed up from data provided in Annual Report/10K/Financial Accounts Disclosure" = "disclosed",
  "Value derived from fuel use provided in personal communication" = "disclosed",
  "Estimate based on partial data disclosure in Annual Report/10K/Financial Accounts Disclosure" = "estimated",
  "Estimate derived based on supplementary company information" = "estimated",
  "Estimate derived by applying revenue-based intensity factors to revenue data (for water utilities)" = "estimated"
)

# data source disclosure columns to fill
disclosure_cols <- c(
  "ghg_scope1_disclosure",
  "ghg_scope2_loc_disclosure",
  "ghg_scope3_upstream_disclosure",
  "ghg_scope1_disclosure_reported",
  "ghg_scope2_loc_disclosure_reported"
)
class_cols <- c(
  "ghg_scope1_class",
  "ghg_scope2_loc_class",
  "ghg_scope3_up_class",
  "ghg_scope1_reported_class",
  "ghg_scope2_loc_reported_class"
)

stopifnot(data.table::is.data.table(trucost_full))

# Fast vectorized lookup: map each disclosure text -> {estimated|disclosed|unknown}, preserve NAs
trucost_full[, (class_cols) :=
               lapply(.SD, function(x) {
                 out <- unname(trucost_disclosure_label_map[x])     # lookup
                 unmapped <- is.na(out) & !is.na(x)                 # only non-NA inputs that didn’t match
                 out[unmapped] <- "unknown"                         # mark truly unmapped
                 out                                               # keep NA for genuine missing
               }),
             .SDcols = disclosure_cols
]

### --- 3) Final scope 1–3 selection ------------------------------------

# Rejection reasons for reported values
rejection_texts <- c(
  "Disclosure rejected as data does not cover consolidated operations",
  "Disclosure rejected as data is normalised and no aggregating factor is available"
)

# helper function to set *_final, *_final_class, *_final_source for scope 1 to 3
set_final <- function(dt,
                      abs_col, abs_class_col, abs_disc_col,
                      rep_col, rep_class_col, rep_disc_col,
                      out_prefix) {
  dt[, (paste0(out_prefix, "_final")) :=
       data.table::fcase(
         !is.na(get(abs_col)), get(abs_col),
         !is.na(get(rep_col)) & !(get(rep_disc_col) %in% rejection_texts), get(rep_col),
         default = NA_real_
       )]
  
  dt[, (paste0(out_prefix, "_final_class")) :=
       data.table::fcase(
         !is.na(get(abs_col)), get(abs_class_col),
         !is.na(get(rep_col)) & !(get(rep_disc_col) %in% rejection_texts), get(rep_class_col),
         default = NA_character_
       )]
  
  dt[, (paste0(out_prefix, "_final_source")) :=
       data.table::fcase(
         !is.na(get(abs_col)), get(abs_disc_col),
         !is.na(get(rep_col)) & !(get(rep_disc_col) %in% rejection_texts), get(rep_disc_col),
         default = NA_character_
       )]
  invisible(NULL)
}

# Scope 1
set_final(trucost_full,
          abs_col        = "ghg_scope1_absolute",
          abs_class_col  = "ghg_scope1_class",
          abs_disc_col   = "ghg_scope1_disclosure",
          rep_col        = "ghg_scope1_abs_reported",
          rep_class_col  = "ghg_scope1_reported_class",
          rep_disc_col   = "ghg_scope1_disclosure_reported",
          out_prefix     = "ghg_scope1")

# Scope 2 (location-based)
set_final(trucost_full,
          abs_col        = "ghg_scope2_loc_absolute",
          abs_class_col  = "ghg_scope2_loc_class",
          abs_disc_col   = "ghg_scope2_loc_disclosure",
          rep_col        = "ghg_scope2_loc_abs_reported",
          rep_class_col  = "ghg_scope2_loc_reported_class",
          rep_disc_col   = "ghg_scope2_loc_disclosure_reported",
          out_prefix     = "ghg_scope2_loc")

# Scope 3 (upstream) — only one absolute, so direct assign
trucost_full[, `:=`(
  ghg_scope3_up_final        = ghg_scope3_upstream_absolute,
  ghg_scope3_up_final_class  = ghg_scope3_up_class,
  ghg_scope3_up_final_source = ghg_scope3_upstream_disclosure
)]

### 4) filtering the trucost data ----
#### (a) filter to rows where both final Scope 1 & Scope 2 (loc) are non-NA ----
trucost_filtered <- trucost_full[
  !is.na(ghg_scope1_final) & !is.na(ghg_scope2_loc_final)
]

msg_keep("Scope 1 and 2 NA filter:", nrow(trucost_full), nrow(trucost_filtered))

# Define columns to drop (helper/intermediate columns)
drop_cols <- c(
  # disclosure text and class helpers
  "ghg_scope1_disclosure", "ghg_scope2_loc_disclosure", "ghg_scope3_upstream_disclosure",
  "ghg_scope1_disclosure_reported", "ghg_scope2_loc_disclosure_reported",
  "ghg_scope1_class", "ghg_scope2_loc_class", "ghg_scope3_up_class",
  "ghg_scope1_reported_class", "ghg_scope2_loc_reported_class",
  # raw absolute and reported values
  "ghg_scope1_absolute", "ghg_scope1_abs_reported",
  "ghg_scope2_loc_absolute", "ghg_scope2_loc_abs_reported",
  "ghg_scope3_upstream_absolute",
  # any other intermediate columns you don’t want to keep
  "ghg_scope1_disclosure_reported", "ghg_scope2_loc_disclosure_reported",
  "ghg_scope1_abs_reported", "ghg_scope2_loc_abs_reported"
)

# Drop those columns if they exist
trucost_filtered[, (intersect(drop_cols, names(trucost_filtered))) := NULL]

# Optional ordering for readability
if (all(c("gvkey","periodenddate") %in% names(trucost_filtered))) {
  data.table::setorder(trucost_filtered, gvkey, periodenddate)
}

### 5) calculate final intensities with trucost total revenue ----
# helper: safe division (tCO2e / $M)
safe_intensity <- function(level, revenue_musd) {
  data.table::fifelse(
    !is.na(level) & !is.na(revenue_musd) & revenue_musd > 0,
    level / revenue_musd,
    NA_real_
  )
}

# compute final intensities
trucost_filtered[, `:=`(
  ghg_scope1_final_intensity     = safe_intensity(ghg_scope1_final,     trucost_total_revenue_usd),
  ghg_scope2_loc_final_intensity = safe_intensity(ghg_scope2_loc_final, trucost_total_revenue_usd),
  ghg_scope3_up_final_intensity  = safe_intensity(ghg_scope3_up_final,  trucost_total_revenue_usd)
)]

trucost_filtered[, (intersect(
  c("ghg_scope1_intensity",
    "ghg_scope2_loc_intensity",
    "ghg_scope3_upstream_intensity"),
  names(trucost_filtered))) := NULL]

### 6) remove duplicates ----
#### (a) remove double entries for gvkey insitutionid, fiscalyear pairs ----
### 1. Identify duplicate keys
dups_keys <- trucost_filtered[
  , .N, by = .(gvkey, institutionid, fiscalyear)
][N > 1L]

# Subsample containing only duplicate records
dups_dt <- trucost_filtered[
  dups_keys,
  on = .(gvkey, institutionid, fiscalyear)
]

#2. Apply prioritisation rules on the duplicate subsample

# Columns to count NA values (adjust this list if needed)
cols_for_na <- c(
  "trucost_total_revenue_usd",
  "ghg_scope1_final", "ghg_scope2_loc_final", "ghg_scope3_up_final",
  "ghg_scope1_final_intensity", "ghg_scope2_loc_final_intensity",
  "ghg_scope3_up_final_intensity"
)

dups_dt[
  # Compute rule variables
  , `:=`(
    # Priority 1: NA count
    na_count = rowSums(is.na(.SD)),
    
    # Priority 2: scope 1 class preference
    scope1_rank = fifelse(
      grepl("^disclosed", ghg_scope1_final_class, ignore.case = TRUE), 0L,
      fifelse(grepl("^estimated", ghg_scope1_final_class, ignore.case = TRUE), 1L, 2L)
    ),
    
    # Priority 3: scope 2 class preference
    scope2_rank = fifelse(
      grepl("^disclosed", ghg_scope2_loc_final_class, ignore.case = TRUE), 0L,
      fifelse(grepl("^estimated", ghg_scope2_loc_final_class, ignore.case = TRUE), 1L, 2L)
    ),
    
    # Priority 4: older env_cost_effective_date is preferred
    env_date_rank = as.POSIXct(env_cost_effective_date, format = "%b %d %Y %I:%M%p")
  ),
  .SDcols = cols_for_na
][
  # Apply ordered selection within groups
  , selected := {
    o <- order(
      na_count,       # priority 1: least NAs
      scope1_rank,    # priority 2: scope1 disclosed > estimated
      scope2_rank,    # priority 3: scope2 disclosed > estimated
      env_date_rank   # priority 4: older env_cost_effective_date
    )
    seq_len(.N) == o[1L]   # mark only the chosen row
  },
  by = .(gvkey, institutionid, fiscalyear)
]

# Keep only the selected duplicate rows
dups_selected <- dups_dt[selected == TRUE]

# Optionally drop helper columns
dups_selected[
  , c("na_count", "scope1_rank", "scope2_rank", "env_date_rank", "selected") := NULL
]

# 3. Add back non-duplicates and create a deduplicated table

# All non-duplicate rows from the original table
nondups <- trucost_filtered[
  !dups_keys,
  on = .(gvkey, institutionid, fiscalyear)
]

# Final deduplicated dataset:
trucost_filtered <- rbind(nondups, dups_selected, use.names = TRUE, fill = TRUE)

# (Optional) Set key for convenience
setkey(trucost_filtered, gvkey, institutionid, fiscalyear)

#### (b) Operating Status filter ----
trucost_filtered[
  , `:=`(
    n_gv_fy  = .N,
    any_oper = any(status == "Operating")
  ),
  by = .(gvkey, fiscalyear)
]

# 2) Filter:
#    - If there are duplicates (n_gv_fy > 1) AND at least one "Operating" row in that group,
#      then DROP rows where status != "Operating".
#    - Otherwise (no duplicates, or no Operating in the group), keep all rows as they are.
trucost_filtered <- trucost_filtered[
  !(n_gv_fy > 1 & any_oper == TRUE & status != "Operating")
]

# 3) (Optional) clean up helper columns
trucost_filtered[
  , c("n_gv_fy", "any_oper") := NULL
]

#### (c) revenue filter ----
trucost_filtered[
  , avg_rev := mean(trucost_total_revenue_usd, na.rm = TRUE),
  by = .(gvkey, institutionid)
]

## 2) Identify remaining duplicates by (gvkey, fiscalyear)
dups_keys <- trucost_filtered[
  , .N, by = .(gvkey, fiscalyear)
][N > 1L]

## Subsample: only duplicate records
dups_dt <- trucost_filtered[
  dups_keys,
  on = .(gvkey, fiscalyear)
]

## 3) Within each (gvkey, fiscalyear) duplicate group, select rows
##    belonging to the institutionid with the largest avg_rev
dups_dt[
  , selected_rev := {
    # treat NA avg_rev as -Inf so they never "win"
    tmp <- fifelse(is.na(avg_rev), -Inf, avg_rev)
    tmp == max(tmp)
  },
  by = .(gvkey, fiscalyear)
]

## Keep only the selected institution(s) within duplicate groups
dups_selected <- dups_dt[selected_rev == TRUE]

## 4) Re-add all non-duplicate rows from trucost_filtered
nondups <- trucost_filtered[
  !dups_keys,
  on = .(gvkey, fiscalyear)
]

trucost_filtered <- rbind(
  nondups,
  dups_selected,
  use.names = TRUE,
  fill = TRUE
)

## 5) Optional cleanup
trucost_filtered[
  , c("avg_rev", "selected_rev","N","i.N") := NULL
]

### 7) calculate YoY growth: levels and intensities (grouped by gvkey) ----

# ensure within-group ordering by year
data.table::setkey(trucost_filtered, gvkey, fiscalyear)

trucost_filtered[
  , c("ghg_scope1_yoy",
      "ghg_scope2_loc_yoy",
      "ghg_scope3_up_yoy",
      "ghg_scope1_yoy_class",
      "ghg_scope2_loc_yoy_class",
      "ghg_scope3_up_yoy_class",
      "ghg_scope1_intensity_yoy",
      "ghg_scope2_loc_intensity_yoy",
      "ghg_scope3_up_intensity_yoy") :=
    with(.SD, {
      fy <- fiscalyear
      consec <- (fy - data.table::shift(fy)) == 1L
      
      # --- Levels
      s1  <- ghg_scope1_final
      s1c <- ghg_scope1_final_class
      s1p <- data.table::shift(s1)
      s1cp<- data.table::shift(s1c)
      
      s2  <- ghg_scope2_loc_final
      s2c <- ghg_scope2_loc_final_class
      s2p <- data.table::shift(s2)
      s2cp<- data.table::shift(s2c)
      
      s3  <- ghg_scope3_up_final
      s3c <- ghg_scope3_up_final_class
      s3p <- data.table::shift(s3)
      s3cp<- data.table::shift(s3c)
      
      # level growths
      s1_yoy <- data.table::fifelse(consec & !is.na(s1) & !is.na(s1p) & s1p != 0, (s1 - s1p) / s1p, NA_real_)
      s2_yoy <- data.table::fifelse(consec & !is.na(s2) & !is.na(s2p) & s2p != 0, (s2 - s2p) / s2p, NA_real_)
      s3_yoy <- data.table::fifelse(consec & !is.na(s3) & !is.na(s3p) & s3p != 0, (s3 - s3p) / s3p, NA_real_)
      
      s1_class <- data.table::fcase(
        !is.na(s1_yoy) & s1c == "disclosed" & s1cp == "disclosed", "disclosed",
        !is.na(s1_yoy) & s1c == "estimated" & s1cp == "estimated", "estimated",
        !is.na(s1_yoy), "estimated",
        default = NA_character_
      )
      s2_class <- data.table::fcase(
        !is.na(s2_yoy) & s2c == "disclosed" & s2cp == "disclosed", "disclosed",
        !is.na(s2_yoy) & s2c == "estimated" & s2cp == "estimated", "estimated",
        !is.na(s2_yoy), "estimated",
        default = NA_character_
      )
      s3_class <- data.table::fcase(
        !is.na(s3_yoy) & s3c == "disclosed" & s3cp == "disclosed", "disclosed",
        !is.na(s3_yoy) & s3c == "estimated" & s3cp == "estimated", "estimated",
        !is.na(s3_yoy), "estimated",
        default = NA_character_
      )
      
      # --- Intensities (same provenance logic, no separate source columns)
      i1  <- ghg_scope1_final_intensity
      i1p <- data.table::shift(i1)
      i2  <- ghg_scope2_loc_final_intensity
      i2p <- data.table::shift(i2)
      i3  <- ghg_scope3_up_final_intensity
      i3p <- data.table::shift(i3)
      
      i1_yoy <- data.table::fifelse(consec & !is.na(i1) & !is.na(i1p) & i1p != 0, (i1 - i1p) / i1p, NA_real_)
      i2_yoy <- data.table::fifelse(consec & !is.na(i2) & !is.na(i2p) & i2p != 0, (i2 - i2p) / i2p, NA_real_)
      i3_yoy <- data.table::fifelse(consec & !is.na(i3) & !is.na(i3p) & i3p != 0, (i3 - i3p) / i3p, NA_real_)
      
      list(s1_yoy, s2_yoy, s3_yoy,
           s1_class, s2_class, s3_class,
           i1_yoy, i2_yoy, i3_yoy)
    }),
  by = gvkey,
  .SDcols = c("fiscalyear",
              "ghg_scope1_final","ghg_scope1_final_class",
              "ghg_scope2_loc_final","ghg_scope2_loc_final_class",
              "ghg_scope3_up_final","ghg_scope3_up_final_class",
              "ghg_scope1_final_intensity",
              "ghg_scope2_loc_final_intensity",
              "ghg_scope3_up_final_intensity")
]

### 8) Save to parquet ----
write_parquet(trucost_filtered, "./01_data_output/trucost_filtered.parquet")


## II: Compustat FX Rates ----

# this sections pulls the fx rates for the defined time-frame that are used to 
# convert all non-USD values to USD. It saves the output dataframe to parquet.

### 1) Pull FX rates ----
if (comp_fx_toggle) {
  message(sprintf(
    "- Running Compustat FX query for %s -> %s",
    comp_fx_min_date, comp_fx_max_date
  ))
  
  fx_sql <- sprintf("
    SELECT
        curd,
        datadate,
        exrattpd,
        exratd_fromusd,
        exratd_tousd
    FROM comp_global_daily.wrds_g_exrate
    WHERE datadate BETWEEN '%s' AND '%s'
    ORDER BY datadate, curd
  ", comp_fx_min_date, comp_fx_max_date)
  
  library(data.table)
  comp_fx_global <- DBI::dbGetQuery(wrds, fx_sql)
  
  message("FX data loaded successfully.")
} else {
  message("Skipping Compustat FX query (toggle set to FALSE).")
  comp_fx_global <- NULL
}

### 2) Save to parquet ----
# this is used to filter duplicates in the combined NA Global Annual fundamentals
write_parquet(comp_fx_global, "./01_data_output/comp_fx_global.parquet")


## III: Compustat Combined Comp & Security ----

# this section pulls either "Global", "North America" or "Both" from Compustat 
# Company and Security databases to perform common stock filtering (NAICS, Busdesc, 
# DSCI, Major Exchange, primary issue) to derive a final matching from company 
# (gvkey) to one specific security (iid). Saves the primary_gvkey_iid_exchange 
# mapping as well as the company-level information (loc, fic, NAICS, GICS, etc).

### 1) Pull Compustat NA + Global with parameterized SQL ----
if (comp_scope == "NorthAmerica") {
  message("- Running North America Compustat Firm & Security dataset ...")
  
  qry <- glue_sql("
    WITH base_na AS (
      SELECT
        s.gvkey, s.iid, s.tic, s.isin, s.cusip, s.sedol, s.secstat, s.dsci, s.tpci, s.exchg, s.excntry,
        c.busdesc, c.conm, c.conml, c.loc, c.fic, c.prican, c.prirow, c.priusa, c.gsubind, c.gind, c.ggroup, c.naics,
        e.exchgdesc, e.exchgcd, cn.isocntrydesc,
        EXISTS (
          SELECT 1
          FROM   trucost_common.wrds_companies wc
          JOIN   trucost_environ.wrds_environment we
            ON we.institutionid = wc.institutionid
          WHERE  wc.gvkey = s.gvkey
            AND  wc.gvkey <> ''
            AND  we.periodenddate BETWEEN {trucost_min_date} AND {trucost_max_date}
            AND  (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL)
            AND  (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL)
        ) AS in_trucost,
        'comp_na' AS source
      FROM comp_na_daily_all.security AS s
      LEFT JOIN comp_na_daily_all.company AS c ON c.gvkey = s.gvkey
      LEFT JOIN comp_na_daily_all.r_ex_codes AS e ON e.exchgcd = s.exchg
      LEFT JOIN comp_na_daily_all.r_country AS cn ON cn.isocntrycd = s.excntry
      WHERE s.tpci = '0'
    )
    SELECT * FROM base_na
    ORDER BY gvkey, iid;
  ", .con = wrds)
  DT <- as.data.table(DBI::dbGetQuery(wrds, qry))
  message("NA Firm & Stock data loaded successfully.")
} else if (comp_scope == "Global") {
  message("- Running Global Compustat Firm & Security dataset ...")
  
  qry <- glue_sql("
    WITH base_g AS (
      SELECT
        s.gvkey, s.iid, s.tic, s.isin, s.cusip, s.sedol, s.secstat, s.dsci, s.tpci, s.exchg, s.excntry,
        c.busdesc, c.conm, c.conml, c.loc, c.fic, c.prican, c.prirow, c.priusa, c.gsubind, c.gind, c.ggroup, c.naics,
        e.exchgdesc, e.exchgcd, cn.isocntrydesc,
        EXISTS (
          SELECT 1
          FROM   trucost_common.wrds_companies wc
          JOIN   trucost_environ.wrds_environment we
            ON we.institutionid = wc.institutionid
          WHERE  wc.gvkey = s.gvkey
            AND  wc.gvkey <> ''
            AND  we.periodenddate BETWEEN {trucost_min_date} AND {trucost_max_date}
            AND  (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL)
            AND  (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL)
        ) AS in_trucost,
        'comp_global' AS source
      FROM comp.g_security AS s
      LEFT JOIN comp.g_company  AS c  ON c.gvkey = s.gvkey
      LEFT JOIN comp.r_ex_codes AS e  ON e.exchgcd = s.exchg
      LEFT JOIN comp.r_country  AS cn ON cn.isocntrycd = s.excntry
      WHERE s.tpci = '0'
    )
    SELECT * FROM base_g
    ORDER BY gvkey, iid;
  ", .con = wrds)
  DT <- as.data.table(DBI::dbGetQuery(wrds, qry))
  message("Global Firm & Stock data loaded successfully.")
} else if (comp_scope == "Both") {
  message("- Running Combined Global & NA Compustat Firm & Security full dataset ...")
  
  qry <- glue_sql("
    WITH base_na AS (
      SELECT
        s.gvkey, s.iid, s.tic, s.isin, s.cusip, s.sedol, s.secstat, s.dsci, s.tpci, s.exchg, s.excntry,
        c.busdesc, c.conm, c.conml, c.loc, c.fic, c.prican, c.prirow, c.priusa, c.gsubind, c.gind, c.ggroup, c.naics,
        e.exchgdesc, e.exchgcd, cn.isocntrydesc,
        EXISTS (
          SELECT 1
          FROM   trucost_common.wrds_companies wc
          JOIN   trucost_environ.wrds_environment we
            ON we.institutionid = wc.institutionid
          WHERE  wc.gvkey = s.gvkey
            AND  wc.gvkey <> ''
            AND  we.periodenddate BETWEEN {trucost_min_date} AND {trucost_max_date}
            AND  (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL)
            AND  (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL)
        ) AS in_trucost,
        'comp_na' AS source
      FROM comp_na_daily_all.security AS s
      LEFT JOIN comp_na_daily_all.company AS c ON c.gvkey = s.gvkey
      LEFT JOIN comp_na_daily_all.r_ex_codes AS e ON e.exchgcd = s.exchg
      LEFT JOIN comp_na_daily_all.r_country AS cn ON cn.isocntrycd = s.excntry
      WHERE s.tpci = '0'
    ),
    base_g AS (
      SELECT
        s.gvkey, s.iid, s.tic, s.isin, s.cusip, s.sedol, s.secstat, s.dsci, s.tpci, s.exchg, s.excntry,
        c.busdesc, c.conm, c.conml, c.loc, c.fic, c.prican, c.prirow, c.priusa, c.gsubind, c.gind, c.ggroup, c.naics,
        e.exchgdesc, e.exchgcd, cn.isocntrydesc,
        EXISTS (
          SELECT 1
          FROM   trucost_common.wrds_companies wc
          JOIN   trucost_environ.wrds_environment we
            ON we.institutionid = wc.institutionid
          WHERE  wc.gvkey = s.gvkey
            AND  wc.gvkey <> ''
            AND  we.periodenddate BETWEEN {trucost_min_date} AND {trucost_max_date}
            AND  (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL)
            AND  (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL)
        ) AS in_trucost,
        'comp_global' AS source
      FROM comp.g_security AS s
      LEFT JOIN comp.g_company  AS c  ON c.gvkey = s.gvkey
      LEFT JOIN comp.r_ex_codes AS e  ON e.exchgcd = s.exchg
      LEFT JOIN comp.r_country  AS cn ON cn.isocntrycd = s.excntry
      WHERE s.tpci = '0'
    )
    SELECT * FROM base_na
    UNION ALL
    SELECT * FROM base_g
    ORDER BY gvkey, iid;
  ", .con = wrds)
  DT <- as.data.table(DBI::dbGetQuery(wrds, qry))
  message("Combined NA Global Firm & Stock data loaded successfully.")
} else {
  stop("Invalid comp_scope value. Choose 'NA', 'Global', or 'Both'.")
}

### 2) Trucost gvkey filter ----
if (isTRUE(comp_g_NA_firm_sec_trucost_filter_toggle)) {
  message("Filtering according to Trucost gvkey...")
  b <- nrow(DT); DT <- DT[in_trucost == TRUE]; a <- nrow(DT); msg_keep("Trucost filter", b, a)
} else {
  message("No filtering by Trucost gvkey.")
}

### 3) Normalize frequently used fields ----
setindexv(DT, c("gvkey","excntry","gsubind","gind","naics","exchg","exchgdesc","dsci"))

DT[, `:=`(
  dsci_u = stri_trans_toupper(fifelse(is.na(dsci), "", dsci)),
  busdesc_l = stri_trans_tolower(fifelse(is.na(busdesc), "", busdesc)),
  conm_l    = stri_trans_tolower(fifelse(is.na(conm),    "", conm)),
  conml_l   = stri_trans_tolower(fifelse(is.na(conml),   "", conml))
)]

### 4) DSCI-based “common stock” cleaning ----
kw_dupes       <- c("DUPLICATE","DUPL","\\bDUP\\b","DUPE","DULP","DUPLI","1000DUPL","XSQ","XET")
kw_depository  <- c("\\bADR\\b","\\bGDR\\b")
kw_pref        <- c("PREFERRED","\\bPF\\b","\\bPFD\\b","\\bPREF\\b","'PF'")
kw_warrants    <- c("WARRANTS?","\\bWTS\\b","\\bWTS2\\b","WARRT")
kw_debt        <- c("\\bDEB\\b","\\bDB\\b","\\bDCB\\b","DEBT","DEBENTURES?")
kw_unit_phr    <- c("RLST IT","INVESTMENT TRUST","INV TST","UNIT TRUST","UNT TST","TST UNITS",
                    "TRUST UNITS","TST UNIT","UNITS","TRUST","TRUST UNIT")
kw_unit_tok    <- c("\\bUT\\b","\\.IT\\b","\\bIT\\.")
kw_misc <- c("\\b500\\b","\\bBOND\\b","DEFER","\\bDEP\\b","DEPY","ELKS","\\bETF\\b","\\bFUND\\b",
             "\\bFD\\b","\\bIDX\\b","\\bINDEX\\b","\\bLP\\b","MIPS","MITS","MITT","\\bMPS\\b",
             "NIKKEI","\\bNOTE\\b","PERQS","PINES","PRTF","PTNS","PTSHP","QUIBS","QUIDS","\\bRATE\\b",
             "RCPTS","RECEIPTS","\\bREIT\\b","RETUR","SCORE","\\bSPDR\\b","STRYPES","TOPRS",
             "\\bUNIT\\b","\\bUNT\\b","\\bUTS\\b","\\bWTS\\b","YIELD","\\bYLD\\b","XXXXX")
kw_expired <- c("EXPIRED","EXPD","EXPIRY","EXPY")
kw_universal_extra <- c("UNTS INVESTMENT","UNTS TRUST")


# build a single, case-insensitive pattern string
RX_ALL <- regex_any(c(
  kw_dupes, kw_depository, kw_pref, kw_warrants, kw_debt,
  kw_unit_phr, kw_unit_tok, kw_misc, kw_expired, kw_universal_extra
))

# now use stri_detect_regex normally
DT[, rm_pct := stri_detect_fixed(dsci_u, "%", opts_fixed = stri_opts_fixed(case_insensitive = FALSE))]
DT[, rm_kw  := stri_detect_regex(dsci_u, RX_ALL)]

# country-specific keywords
DT[, rm_country := FALSE]
DT[excntry == "CAN", rm_country := stri_detect_regex(dsci_u, "(RESTRICTD|\\bNVTG\\b|\\bSVTG\\b|NON-VTG|INC\\.FD\\.)", 
                                                     opts_regex = stri_opts_regex(case_insensitive=TRUE))]
DT[excntry == "LKA", rm_country := stri_detect_regex(dsci_u, "(RTS|RIGHTS|\\bNON\\s*VTG\\b|\\bNON\\s*VOTING\\b|\\(NON-VTG\\)|\\(NON-VOTING\\))", 
                                                     opts_regex = stri_opts_regex(case_insensitive=TRUE))]
DT[excntry == "PER", rm_country := stri_detect_regex(dsci_u, "(\\bINVERSION\\b|\\bINVN\\b|\\bINV\\b|\\bINVT\\s+SHS\\b)",, 
                                                     opts_regex = stri_opts_regex(case_insensitive=TRUE))]
DT[excntry == "AUS", rm_country := stri_detect_regex(dsci_u, "(AUSTRALIAN\\s+EQUITIES\\s+STRONG\\s+B|RTS|DEF|DFD|DEFF|PAID|PRF)",
                                                     opts_regex = stri_opts_regex(case_insensitive=TRUE))]
DT[excntry == "BEL", rm_country := stri_detect_regex(dsci_u, "(AFV|CONV|VVPR|STRIP)", 
                                                     opts_regex = stri_opts_regex(case_insensitive=TRUE))]
DT[excntry == "ITA", rm_country := stri_detect_regex(dsci_u, "(RSP|RNC|RIGHTS|PV|RP)",
                                                     opts_regex = stri_opts_regex(case_insensitive=TRUE))]
DT[excntry == "BRA", rm_country := stri_detect_regex(dsci_u,"(PN|PNA|PNB|PNC|PND|PNE|PNF|PNG|RCSA|RCTB|PNDEAD|PNADEAD|PNBDEAD|PNCDEAD|PNDDEAD|PNEDEAD|PNFDEAD|PNGDEAD)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "COL", rm_country := stri_detect_regex(dsci_u,"(PFCL|PRIVILEGIADAS|PRVLG)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "GRC", rm_country := stri_detect_regex(dsci_u,"(PR|PB)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "HUN", rm_country := stri_detect_regex(dsci_u,"(OE)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "IDN", rm_country := stri_detect_regex(dsci_u,"(FB|FBDEAD|RTS|RIGHTS)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "IND", rm_country := stri_detect_regex(dsci_u,"(XNH)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "ISR", rm_country := stri_detect_regex(dsci_u,"(P1)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "KOR", rm_country := stri_detect_regex(dsci_u,"(1P|2P|3P|1PB|2PB|3PB|4PB|5PB|6PB|1PFD|1PF|PF2|2PF)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "LTU", rm_country := stri_detect_regex(dsci_u,"(PREFERENCE)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "MEX", rm_country := stri_detect_regex(dsci_u,"(ACP|BCP|CPO|NO\\s+VOTING|C|L|O)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "MYS", rm_country := stri_detect_regex(dsci_u,"(\\bA\\b|'A'|FB|FBDEAD|XCO|XCODEAD|SES|RIGHTS)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "PHL", rm_country := stri_detect_regex(dsci_u,"(PDR)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "PRT", rm_country := stri_detect_regex(dsci_u,"(\\bR\\b|'R')",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "ZAF", rm_country := stri_detect_regex(dsci_u,"(N|CPF|OPTS)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "SGP", rm_country := stri_detect_regex(dsci_u,"(NCPS|NCPS100|NRFD|FB|FBDEAD)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "TWN", rm_country := stri_detect_regex(dsci_u,"(TDR)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "THA", rm_country := stri_detect_regex(dsci_u,"(FB|FBDEAD)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "DEU", rm_country := stri_detect_regex(dsci_u,"(GENUSSCHEINE|GSH)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "NLD", rm_country := stri_detect_regex(dsci_u,"(CERT|CERTS|STK)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "NZL", rm_country := stri_detect_regex(dsci_u,"(RTS)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "AUT", rm_country := stri_detect_regex(dsci_u,"(PC|GSH|GENUSSSCHEINE)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "SWE", rm_country := stri_detect_regex(dsci_u,"(VXX|CONVERTED|CONV)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "CHE", rm_country := stri_detect_regex(dsci_u,"(CONVERTED|CONV|CONVERSION)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "DNK", rm_country := stri_detect_regex(dsci_u,"(VXX|CSE)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "FRA", rm_country := stri_detect_regex(dsci_u,"(ADP|CI|CIP|ORA|ORCI|OBSA|OPCSM|SGP|SICAV|FCP|FCPR|FCPE|FCPI|FCPIMT|OPCVM)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]
DT[excntry == "GBR", rm_country := stri_detect_regex(dsci_u,"(RANKING\\s+FOR\\s+DIVIDEND|PAID|NV)",
                                                     opts_regex = stri_opts_regex(case_insensitive = TRUE))]


DT[, to_remove := fifelse(is.na(rm_pct), FALSE, rm_pct) | fifelse(is.na(rm_kw), FALSE, rm_kw) | fifelse(is.na(rm_country), FALSE, rm_country)]
b <- nrow(DT); DT <- DT[!DT$to_remove]; a <- nrow(DT); msg_keep("DSCI filter", b, a)

### 5) GICS REIT filter ----
DT[, is_REIT := fifelse(gsubind %in% c("40401010") | gind %in% c("404020","601010","402040"), TRUE, FALSE, na = FALSE)]
b <- nrow(DT); DT <- DT[!DT$is_REIT]; a <- nrow(DT); msg_keep("REIT filter", b, a)

# drop helper columns
DT[, c("rm_pct", "rm_kw","rm_country","to_remove","is_REIT") := NULL]

### 6) Busdesc filter (Steps 7–12 condensed & vectorized) ----
# cleanup punctuation
DT[, `:=`(
  busdesc_l = collapse_spaces(stri_replace_all_regex(busdesc_l, "[\\.,;]", " ")),
  conm_l    = collapse_spaces(stri_replace_all_regex(conm_l,    "[\\.,;]", " ")),
  conml_l   = collapse_spaces(stri_replace_all_regex(conml_l,   "[\\.,;]", " "))
)]

# --- Step 2: remove raw CONM/CONML from BUSDESC ---
DT[, busdesc_l := pad(busdesc_l)]
DT[, busdesc_l := rm_names_exact(busdesc_l, conm_l, conml_l)]

# --- Step 3: "&" -> "and" in all three, then repeat Step 2 ---
DT[, `:=`(
  busdesc_l = collapse_spaces(stringi::stri_replace_all_fixed(busdesc_l, "&", " and ")),
  conm_l    = collapse_spaces(stringi::stri_replace_all_fixed(conm_l,    "&", " and ")),
  conml_l   = collapse_spaces(stringi::stri_replace_all_fixed(conml_l,   "&", " and "))
)]
DT[, busdesc_l := rm_names_exact(busdesc_l, conm_l, conml_l)]  # repeat Step 2

# --- company token list ---
corp_tokens <- c(
  "tel aviv","ltd","inc","corp","plc ici","plc","sa","limited","berhad","ab","tbk","ag","co","as",
  "bhd","spa","pcl","nv","asa","corporation","pjsc","s.a.","se","group","oyj","a/s","a.s.","(publ)",
  "cv","holdings","s.a","(pt)","ltd.","saog","nl","kk","akcyjna","inc.","s.p.a.","sirketi","kgaa",
  "pt","jsc","s.p.a","n.v.","(the)","bruxelles","sas","modaraba","saa","c.v.","ojsc","co.ltd.",
  "madrid","lima","a s","s  a","oy","london","sca","holding","milano","incorporated","c v","n v",
  "b v","s a b","s. a. b","torino","a.s","roma","berlin","muenchen","anonyme","stockholm","wien",
  "n.v","zuerich","hamburg","zug","psc","sab","warszawa","augsburg","bv","lp","na","s.a.a","sa/nv",
  "schaffhausen","stuttgart","tas","gmbh","llc","incorporation","p.s.c.","(bbva)","abp","coltd",
  "corp.","helsinki","porto","santiago","vevey","enterprises","duesseldorf","casablanca","groupe",
  "aktiebolag","aktiengesellschaft","bern","bilbao","bologna","cagliari","baar","essen","frankfurt",
  "cva","hldg","hldgs","k.k","k k","ptc","s.a.a.","s/a","esp","sarl","(pakistan)","marseille",
  "geneve","s.a.s.","c.v","(bo)","(bs)","(gbr)","(new)","(re)","-old","lld","ltd)","grundbesitz-ag",
  "corp)","co.ltd","s.a.o.g.","saog","p.l.c","grp","lt","ind"
)

strip_tokens_fixed <- local({
  toks <- unique(stringi::stri_trim_both(stringi::stri_trans_tolower(corp_tokens)))
  toks <- toks[order(nchar(toks), decreasing = TRUE)]
  function(x) {
    x2 <- pad_collapse(stringi::stri_trans_tolower(fifelse(is.na(x), "", x)))
    for (tk in toks) x2 <- stringi::stri_replace_all_fixed(x2, pad(tk), " ", vectorize_all = FALSE)
    pad_collapse(x2)
  }
})

DT[, `:=`(
  conm2  = strip_tokens_fixed(conm_l),
  conml2 = strip_tokens_fixed(conml_l)
)]

# --- Step 5: remove CONM2/CONML2 from BUSDESC ---
DT[, busdesc_l := rm_names_exact(busdesc_l, conm2, conml2)]

# --- Step 6: '-' -> ' ' in CONM2/CONML2, then repeat Step 5 ---
DT[, `:=`(
  conm2  = collapse_spaces(stringi::stri_replace_all_fixed(fifelse(is.na(conm2),  "", as.character(conm2)),  "-", " ")),
  conml2 = collapse_spaces(stringi::stri_replace_all_fixed(fifelse(is.na(conml2), "", as.character(conml2)), "-", " "))
)]

DT[, busdesc_l := rm_names_exact(busdesc_l, conm2, conml2)]

# --- Step 7: strip parentheses in CONM/CONML/BUSDESC before name keyword test ---
DT[, `:=`(
  conm_l    = collapse_spaces(stri_replace_all_regex(conm_l,    "[()]", " ")),
  conml_l   = collapse_spaces(stri_replace_all_regex(conml_l,   "[()]", " ")),
  busdesc_l = collapse_spaces(stri_replace_all_regex(busdesc_l, "[()]", " "))
)]

# keywords in names
name_kw <- c("fund","trust","venture capital trust","vct","reit")
name_kw_rx <- stri_c("(?:^|\\s+)(",
                     stri_join(stri_replace_all_fixed(name_kw, " ", "\\s+"), collapse="|"),
                     ")\\b")
flag_step7 <- stri_detect_regex(DT$conm_l,  name_kw_rx) |
  stri_detect_regex(DT$conml_l, name_kw_rx)


# --- Step 8 remove expressions in busdesc ---
rm_exprs <- c(
  "fund advisors","fund managers","fund benchmarks","fund raisings","fund administrations",
  "fund transfers","fund services","fund products","fund sponsors","fund plan sponsors",
  "fund corps","fund companies","fund groups","trust advisors","trust banks","trust managers",
  "trust sponsors","reit managers","fund advisor","fund manager","fund benchmark",
  "fund raising","fund administration","fund transfer","fund service","fund product",
  "fund sponsor","fund plan sponsor","fund corp","fund company","fund group","trust advisor",
  "trust bank","trust manager","trust sponsor","reit manager","feeder","multi-asset","multi asset",
  "balanced","fixed income","self-managed","public","publicly owned","publicly-owned",
  "closed ended","closed end","closed-ended","closed-end","close ended","close end","closeended",
  "close-end","opened ended","opened end","opened-ended","opened-end","open ended","open end",
  "open-ended","open-end"
)

rm_exprs_rx <- stri_c("(?:^|\\s+)(",
                      stri_join(stri_replace_all_fixed(rm_exprs, " ", "\\s+"), collapse="|"),
                      ")\\b")
DT[, busdesc_l := pad(collapse_spaces(stri_replace_all_regex(busdesc_l, rm_exprs_rx, " ")))]

# Step 9 sequences
str1 <- c(" is a "," was a "," as a "," is an "," was an "," as an "," specializes in "," operates as a "," operates as an ")
str2 <- c(
  "property trust","property investment trust","property fund","property investment fund",
  "private equity trust","private equity investment trust","private equity fund",
  "private equity investment fund","venture capital trust","venture capital investment trust",
  "venture capital fund","venture capital investment fund","real estate trust",
  "real estate investment trust","real estate fund","real estate investment fund",
  "interval fund","investment trust","etf","reit","vct","unit trust","unit investment trust",
  "split capital fund","split investment fund","split capital trust","split capital investment trust",
  "exchange traded fund","exchange-traded fund","exchange-traded-fund","exchange traded-fund",
  "currency fund","fund","mutual fund","equity fund","equity investment fund","equity mutual fund",
  "hedge fund","equity hedge fund","traded fund"
)
rx_seq <- stri_c("(?i)(?:^|\\s+)(", stri_join(stri_replace_all_fixed(str1, " ", "\\s+"), collapse="|"),
                 ")\\b.*?(", stri_join(stri_replace_all_fixed(str2, " ", "\\s+"), collapse="|"), ")\\b")
flag_step9  <- stri_detect_regex(DT$busdesc_l, rx_seq)

# Step 10 “fund/trust invests/operates” phrases
step10_phr <- c(
  "fund invests","fund prefers to invest","fund engages","fund operates","fund employs",
  "fund was formerly known","fund replicates","fund seeks to invest",
  "trust invests","trust prefers to invest","trust engages","trust operates","trust employs",
  "trust was formerly known","trust replicates","trust seeks to invest"
)
rx_step10 <- stri_c("(?:^|\\s+)(", stri_join(stri_replace_all_fixed(step10_phr, " ", "\\s+"), collapse="|"), ")\\b")
flag_step10 <- stri_detect_regex(DT$busdesc_l, rx_step10)

# Step 11: first 101 chars rule
first101   <- stri_sub(DT$busdesc_l, 1L, pmin(stri_length(DT$busdesc_l), 101L))
flag_pos   <- stri_detect_regex(first101, "( reit\\b| an\\s+investment\\s+trust\\b| real\\s+estate\\s+investment\\s+trust\\b)")
flag_neg   <- stri_detect_regex(first101, "(by|for|of|to|through)\\s+(reit|an\\s+investment\\s+trust|real\\s+estate\\s+investment\\s+trust)")
flag_step11 <- (flag_pos & !flag_neg)

DT[, is_fund_trust_busdesc := as.logical(flag_step7 | flag_step9 | flag_step10 | flag_step11)]

# Step 12: remove funds/trusts EXCEPT banks

nz <- function(x) fifelse(is.na(x), "", x)
edge <- "(?i)^bank\\b|\\bbank$"

# check for bank indication
has_bank_edge <- stri_detect_regex(nz(DT$conm_l), edge) | stri_detect_regex(nz(DT$conml_l), edge)
has_bank_phr  <- stri_detect_regex(nz(DT$conm_l),  "(?i)trust\\s*(?:&|and)\\s*banking|securities\\s+co\\b") |
  stri_detect_regex(nz(DT$conml_l), "(?i)trust\\s*(?:&|and)\\s*banking|securities\\s+co\\b")
has_ggroup_bank <- fcoalesce(DT$ggroup == "4010", FALSE)

is_bank <- fcoalesce(has_bank_edge | has_bank_phr |
                       (has_ggroup_bank & fcoalesce(DT$is_fund_trust_busdesc, FALSE)), FALSE)


drop_busdesc_funds <- fcoalesce(DT$is_fund_trust_busdesc & !is_bank, FALSE)

b <- nrow(DT)
DT <- DT[!drop_busdesc_funds]
a <- nrow(DT); msg_keep("BUSDESC filter", b, a)

# remove working cols
DT[, c("busdesc_l","conm_l","conml_l","conm2","conml2",
       "is_fund_trust_busdesc","dsci_u") := NULL]


### 7) NAICS 525 filter ----
DT[, naics := as.character(naics)]
b <- nrow(DT); DT <- DT[ !stri_detect_regex(naics, "^525") ]; a <- nrow(DT); msg_keep("NAICS 525 filter", b, a)


### 8) Major Exchange filter (by descriptions) ----
# Goal: only include stocks on major stock exchanges per country.
# Alves et al. (2025) include an exchange if it is either in 
# Bessembinder et al. (2019) or in Chaieb et al. (2021)

qry_ex_codes_ref <- "
SELECT *
FROM comp.r_ex_codes
ORDER BY exchgdesc, exchgcd;
"

ex_codes_ref <- DBI::dbGetQuery(wrds, qry_ex_codes_ref)
# Ensure data.table
ex_codes_ref <- as.data.table(ex_codes_ref)

# Stock Exchanges either in Bessembinder et al. (2019) or Chaieb et al. (2021)
# Changes to their paper: NYSE -> New York Stock Exchange, NYSE Arca -> NYSEArca,
# Amex -> NYSE American, Nasdaq -> Nasdaq Stock Market

major_exchanges <- c(
  "Abu Dhabi Securities Exchange",
  "ASX All Markets",
  "Athens Exchange SA Cash Market",
  "BM and F Bovespa SA Bolsa De Valores Mercadorias E Futuros",
  "Bolsa De Comercio De Buenos Aires",
  "Bolsa De Madrid",
  "Bolsa De Valores De Colombia",
  "Bolsa Mexicana De Valores Mexican Stock Exchange",
  "Borsa Italiana Electronic Share Market",
  "BSE Ltd",
  "Bursa Malaysia",
  "Deutsche Boerse AG",
  "Dubai Financial Market",
  "Hong Kong Exchanges and Clearing Ltd",
  "Indonesia Stock Exchange",
  "Istanbul Stock Exchange",
  "Johannesburg Stock Exchange",
  "Korea Exchange KOSDAQ",
  "Korea Exchange Stock Market",
  "London Stock Exchange",
  "MICEX Stock Exchange",
  "Moscow Stock Exchange",
  "NASDAQ OMX Helsinki Ltd",
  "NASDAQ OMX Nordic",
  "Nasdaq Stock Market",
  "National Stock Exchange of India",
  "New York Stock Exchange",
  "New Zealand Exchange Ltd",
  "Nigerian Stock Exchange",
  "NYSE American",
  "NYSE Euronext Amsterdam",
  "NYSE Euronext Brussels",
  "NYSE Euronext Lisbon",
  "NYSE Euronext Paris",
  "NYSEArca",
  "OMX Nordic Exchange Copenhagen AS",
  "Osaka Securities Exchange",
  "Oslo Bors ASA",
  "Paris",
  "Rio de Janeiro",
  "Saudi Stock Exchange",
  "Shanghai Stock Exchange",
  "Shenzhen Stock Exchange",
  "Singapore Exchange",
  "Stock Exchange of Thailand",
  "Swiss Exchange",
  "TAIPEI EXCHANGE",
  "Taiwan Stock Exchange",
  "Tel Aviv Stock Exchange",
  "Tokyo Stock Exchange",
  "Toronto Stock Exchange",
  "TSX Venture Exchange",
  "Warsaw Stock Exchange",
  "Wellington",
  "Wiener Boerse AG",
  "XETRA",
  "Zurich"
)

# apply to compustat firm stock dataset
DT[, major_exch_flag := fcoalesce(exchgdesc %chin% major_exchanges, FALSE) ]


b <- nrow(DT); DT <- DT[major_exch_flag == TRUE]; a <- nrow(DT); msg_keep("Exchange filter", b, a)


### 9) Primary issue selection ----
DT[, `:=`(
  iid    = trimws(as.character(iid)),
  priusa = trimws(as.character(priusa)),
  prican = trimws(as.character(prican)),
  prirow = trimws(as.character(prirow))
)]

#TODO check for loc vs.fic
DT[, home_flag := !is.na(excntry) & !is.na(loc) & excntry == loc]

DT[, primary_flag := (!is.na(iid) & (
  (!is.na(priusa) & excntry == "USA" & iid == priusa) |
    (!is.na(prican) & excntry == "CAN" & iid == prican) |
    (!is.na(prirow) & !(excntry %chin% c("USA","CAN")) & iid == prirow)
))]

DT[, `:=`(
  secstat_active = as.integer(secstat == "A"),
  class_penalty  = as.integer(stri_detect_regex(fifelse(is.na(dsci), "", toupper(dsci)), "(^|[^A-Z])(CL|SER)([^A-Z]|$)")),
  iid_num        = to_num_safe(iid),
  has_isin_ord   = as.integer(!is.na(isin) & nzchar(isin)),
  primary_ord    = as.integer(primary_flag),
  home_ord       = as.integer(home_flag),
  major_ord      = as.integer(major_exch_flag)
)]

setorder(DT, gvkey, -primary_ord, -home_ord, -major_ord, -has_isin_ord,
         -secstat_active, class_penalty, iid_num, iid)

DT[, main_primary := seq_len(.N) == 1L, by = gvkey]

b <- nrow(DT)
DT <- DT[main_primary == TRUE]
a <- nrow(DT)
msg_keep("Primary share filter", b, a)

# save main iids to separate table
primary_iid_exchg_g_NA <- DT[main_primary == TRUE, .(gvkey, primary_iid = iid, exchg, conm)]

# drop temp columns
DT[, c("major_exch_flag",
       "home_flag",
       "primary_flag",
       "secstat_active",
       "class_penalty",
       "iid_num",
       "has_isin_ord",
       "primary_ord",
       "home_ord",
       "major_ord",
       "main_primary") := NULL]

### 10) Save to parquet ----

# this is used to filter duplicates in the combined NA Global Annual fundamentals
write_parquet(primary_iid_exchg_g_NA, "./01_data_output/primary_iid_exchg_g_NA.parquet")

# this is the security to company mapping file with required information
write_parquet(DT, "./01_data_output/comp_sec_info_g_NA.parquet")



## IV: Compustat Annual Fundamentals info (with optional Trucost gvkey filter) ----

# This section collects the annual fundamentals data (balance sheet) from compustat
# combined for Global and North American data set. This data is used to calculate
# control variables (like B/M ratios, Earnings growth, Leverage, Invest/A).
# Saves the final output to parquet.

### 1) Pull Compustat Data ----
message("- Running Global Annual Compustat Fundamentals dataset ...")

qry <- glue_sql("
WITH global_f AS (
  SELECT
    f.gvkey,
    f.datadate,
    'comp_global' AS source,
    f.fyear,
    f.pdate,
    f.fdate,
    f.final,
    f.consol,
    f.indfmt,
    f.datafmt,
    f.costat,
    f.acctstd,
    f.curcd,
    f.exchg,
    
    -- to double check match with comp_g_NA
    c.conm,
    c.loc,
    c.fic,
    
    -- required financials
    f.capx,     -- Capital Expenditures
    f.ceq,      -- Common Equity (book value of equity)
    f.seq,      -- Stockholders’ Equity
    f.teq,      -- Total Equity
    f.pstk,     -- Preferred Stock
    f.epsexcon,    -- Earnings Per Share (Basic) - Excluding Extraordinary Items - Consolidated
    
    f.revt,     -- Revenue (Total)
    f.sale,     -- Sales/Turnover
    f.cogs,     -- Cost of Goods Sold
    f.nicon,    -- Net Income (Consolidated)
    f.ebit,     -- Earnings Before Interest and Taxes
    f.ebitda,   -- Earnings Before Interest, Taxes, Depreciation & Amortization
    
    f.at,       -- Total Assets
    
    f.lt,       -- Total Liabilities
    f.dlc,      -- Debt, Current
    f.dltt,     -- Debt, Long-Term
    
    f.oiadp,    -- Operating Income After Depreciation
    
    f.ppent,    -- Property, Plant & Equipment - Net
    f.txditc,   -- Deferred Taxes & Investment Tax Credit
    f.xrd,      -- Research & Development Expense
    
    /* Trucost linkage indicator */
    EXISTS (
      SELECT 1
      FROM   trucost_common.wrds_companies wc
      JOIN   trucost_environ.wrds_environment we
             ON we.institutionid = wc.institutionid
      WHERE  wc.gvkey = f.gvkey
        AND  wc.gvkey <> ''
        AND  we.periodenddate BETWEEN {trucost_min_date} AND {trucost_max_date}
        AND  (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL)  -- Scope 1 data
        AND  (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL)  -- Scope 2 (location-based)
    ) AS in_trucost

  FROM comp_global_daily.g_funda AS f
  LEFT JOIN comp_global_daily.g_company AS c
    ON c.gvkey = f.gvkey
  WHERE f.consol  = 'C'
    AND f.indfmt  IN ('INDL','FS')
    AND f.datafmt = 'HIST_STD'
    AND f.costat  IN ('A','I')
    AND f.datadate BETWEEN {comp_funda_min_date} AND {comp_funda_max_date}
),

na_f AS (
  SELECT
    f.gvkey,
    f.datadate,
    'comp_na' AS source,
    f.fyear,
    f.pdate,
    f.fdate,
    f.final,
    f.consol,
    f.indfmt,
    f.datafmt,
    f.costat,
    f.acctstd,
    f.curcd,
    f.exchg,
    
    -- to double check match with comp_g_NA
    c.conm,
    c.loc,
    c.fic,
    
    -- required financials
    f.capx,     -- Capital Expenditures
    f.ceq,      -- Common Equity (book value of equity)
    f.seq,      -- Stockholders’ Equity
    f.teq,      -- Total Equity
    f.pstk,     -- Preferred Stock
    f.epspx AS epsexcon,    -- Earnings Per Share (Basic) - Excluding Extraordinary Items - Consolidated
    
    f.revt,     -- Revenue (Total)
    f.sale,     -- Sales/Turnover
    f.cogs,     -- Cost of Goods Sold
    f.ni AS nicon,   -- ni = nicon in NorthAm (also niadj available)
    f.ebit,     -- Earnings Before Interest and Taxes
    f.ebitda,   -- Earnings Before Interest, Taxes, Depreciation & Amortization
    
    f.at,       -- Total Assets
    
    f.lt,       -- Total Liabilities
    f.dlc,      -- Debt, Current
    f.dltt,     -- Debt, Long-Term
    
    f.oiadp,    -- Operating Income After Depreciation
    
    f.ppent,    -- Property, Plant & Equipment - Net
    f.txditc,   -- Deferred Taxes & Investment Tax Credit
    f.xrd,      -- Research & Development Expense
    
    /* Trucost linkage indicator (same gvkey logic) */
    EXISTS (
      SELECT 1
      FROM   trucost_common.wrds_companies wc
      JOIN   trucost_environ.wrds_environment we
             ON we.institutionid = wc.institutionid
      WHERE  wc.gvkey = f.gvkey
        AND  wc.gvkey <> ''
        AND  we.periodenddate BETWEEN {trucost_min_date} AND {trucost_max_date}
        AND  (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL)
        AND  (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL)
    ) AS in_trucost

  FROM comp_na_daily_all.funda AS f
  LEFT JOIN comp_na_daily_all.company AS c
    ON c.gvkey = f.gvkey
  WHERE f.consol  = 'C'
    AND f.indfmt  IN ('INDL','FS')
    AND f.datafmt = 'STD'
    AND f.costat  IN ('A','I')
    AND f.datadate BETWEEN {comp_funda_min_date} AND {comp_funda_max_date}
)

SELECT *
FROM (
  SELECT * FROM global_f
  UNION ALL
  SELECT * FROM na_f
) u
ORDER BY gvkey, datadate, source;
", .con = wrds)

comp_funda <- as.data.table(DBI::dbGetQuery(wrds, qry))


### 2) Trucost gvkey filter ----
if (isTRUE(comp_funda_trucost_filter_toggle)) {
  message("Filtering according to Trucost gvkey...")
  b <- nrow(comp_funda); comp_funda <- comp_funda[in_trucost == TRUE]; a <- nrow(comp_funda); msg_keep("Trucost filter", b, a)
} else {
  message("No filtering by Trucost gvkey.")
}

### 3) duplicate data entry filtering ----
# Load primary gvkey <-> exchg mapping
primary_iid_exchg_g_NA <- read_parquet("./01_data_output/primary_iid_exchg_g_NA.parquet")
pri <- as.data.table(primary_iid_exchg_g_NA)[, .(gvkey, exchg)]
pri <- unique(pri, by = c("gvkey","exchg"))

# Flag rows in comp_funda whose (gvkey, exchg) exists in the primary file
comp_funda[pri, on = .(gvkey, exchg), exchg_in_primary := !is.na(i.gvkey)]
comp_funda[, exchg_in_primary := fifelse(is.na(exchg_in_primary), FALSE, exchg_in_primary)]

# Build priority flags
comp_funda[, indl_pref := (indfmt == "INDL")]

# Apply the 2-step preference within each gvkey + datadate group:
#    - First: exchg_in_primary == TRUE
#    - Second: indl_pref == TRUE (i.e., INDL over FS)
setorder(comp_funda, gvkey, datadate, -exchg_in_primary, -indl_pref)

before_n <- nrow(comp_funda)
comp_funda <- comp_funda[, .SD[1], by = .(gvkey, datadate)]
after_n  <- nrow(comp_funda)

msg_keep("Duplicate Removal", before_n, after_n)

# Optional: drop helper columns
comp_funda[, c("exchg_in_primary","indl_pref") := NULL]


### 4) all values to USD via FX rates table ----
comp_fx_global <- read_parquet("./01_data_output/comp_fx_global.parquet")
setDT(comp_fx_global)

# match on datadate and curcd (comp_funda) & curd (comp_fx_global)

# Normalize keys
comp_fx_global[, `:=`(
  datadate = as.Date(datadate),
  curd     = toupper(curd)
)]
comp_funda[, `:=`(
  datadate = as.Date(datadate),
  curcd    = toupper(curcd)
)]

# Add a helper flag for sorting (TRUE if non-NA FX rate to USD)
comp_fx_global[, has_tousd := !is.na(exratd_tousd)]

# Order to prefer rows with valid exratd_tousd values
setorder(comp_fx_global, datadate, curd, -has_tousd)

# Keep only one row per (datadate, curd)
comp_fx_unique <- comp_fx_global[
  , .SD[1], by = .(datadate, curd)
][
  , .(
    datadate,
    curcd       = curd,
    fx_tousd    = exratd_tousd
    #fx_fromusd  = exratd_fromusd,
    #fx_tpd      = exrattpd
  )
]

# Ensure USD rows exist with rate = 1 for every datadate in comp_funda
usd_rows <- unique(comp_funda[, .(datadate)])
usd_rows[, `:=`(curcd = "USD", fx_tousd = 1, fx_fromusd = 1, fx_tpd = 1)]
comp_fx_unique <- unique(rbind(comp_fx_unique, usd_rows, fill = TRUE),
                         by = c("datadate","curcd"))

# Add FX columns to comp_funda by (datadate, currency)
setkey(comp_funda,   datadate, curcd)
setkey(comp_fx_unique, datadate, curcd)

comp_funda[comp_fx_unique,
           `:=`(
             fx_tousd   = i.fx_tousd
             #fx_fromusd = i.fx_fromusd,
             #fx_tpd     = i.fx_tpd
           ),
           on = .(datadate, curcd)
]

setorder(comp_funda, gvkey, datadate)

### 5) convert all fundamental data to USD ----
# define cols that get translated to USD
fund_cols <- c(
  "capx",
  "ceq",
  "seq",
  "teq",
  "pstk",
  "epsexcon",
  "revt",
  "sale",
  "cogs",
  "nicon",
  "ebit",
  "ebitda",
  "at",
  "lt",
  "dlc",
  "dltt",
  "oiadp",
  "ppent",
  "txditc",
  "xrd"
)

# create USD copy
comp_funda_usd <- copy(comp_funda)[
  , `:=`(
    orig_curcd = curcd,
    curcd = "USD"
  )
][
  , (fund_cols) := lapply(.SD, function(x) x * fx_tousd),
  .SDcols = fund_cols
]

### 6) Construct variables ----

#### Book Value of Equity ----
comp_funda_usd[, book_equity := fifelse(
  !is.na(seq) & !is.na(txditc),
  seq + txditc - fcoalesce(pstk, 0),                # precise BE
  fifelse(!is.na(at) & !is.na(lt), at - lt, NA_real_) # fallback: AT - LT, else NA
)]

#### Leverage ----
# according to Alves et al. (2025):
# allow either DLC or DLTT to be missing but not both
# negative values are set to missing
comp_funda_usd[
  !(is.na(dltt) & is.na(dlc)) & !is.na(at) & at != 0,
  leverage := ifelse(
    (!is.na(dltt) & dltt < 0) | (!is.na(dlc) & dlc < 0),
    NA_real_,
    (fcoalesce(dltt, 0) + fcoalesce(dlc, 0)) / at
  )
]

#### ROE ----
# according to Bolton & Kacperczyk (2023):
comp_funda_usd[
  book_equity != 0 & !is.na(book_equity) & !is.na(nicon),
  ROE := nicon / book_equity
]

#### Invest/A ----
# according to Bolton & Kacperczyk (2023):
comp_funda_usd[
  at != 0 & !is.na(at) & !is.na(capx),
  inv_over_at := capx / at
]

#### Investment ----
# according to Alves et al. (2025):
comp_funda_usd[
  order(gvkey, datadate),
  inv_perc := {
    fy <- fyear                # or extract from datadate if fiscal year variable not present
    consec <- (fy - shift(fy)) == 1L
    ifelse(
      consec & !is.na(at) & !is.na(shift(at)) & shift(at) != 0,
      (at / shift(at)) - 1,
      NA_real_
    )
  },
  by = gvkey
]

#### Sales Growth ----
# according to Hambel & Van der Sanden (2025):
# REVT or if not available Sales
comp_funda_usd[
  order(gvkey, datadate),
  sales_growth := {
    fy <- fyear
    consec <- (fy - shift(fy)) == 1L
    sales_var <- fcoalesce(revt, sale)
    ifelse(
      consec & !is.na(sales_var) & !is.na(shift(sales_var)) & shift(sales_var) != 0,
      (sales_var / shift(sales_var)) - 1,
      NA_real_
    )
  },
  by = gvkey
]

#### Log PPE ----
# according to Bolton & Kacperczyk (2023)
comp_funda_usd[
  !is.na(ppent) & ppent > 0,
  log_ppe := log(ppent)
]

#### ROS ----
# according to Aswani et al. (2024)
comp_funda_usd[
  !is.na(oiadp) & !is.na(fcoalesce(revt, sale)) & fcoalesce(revt, sale) != 0,
  ROS := oiadp / fcoalesce(revt, sale)
]

#### ROA ----
# according to Aswani et al. (2024)
comp_funda_usd[
  !is.na(oiadp) & !is.na(at) & at != 0,
  ROA := oiadp / at
]

#### EBIT Margin ----
# according to Aswani et al. (2024)
comp_funda_usd[
  !is.na(ebit) & !is.na(fcoalesce(revt, sale)) & fcoalesce(revt, sale) != 0,
  ebit_margin := ebit / fcoalesce(revt, sale)
]

#### EBITDA Margin ----
# according to Aswani et al. (2024)
comp_funda_usd[
  !is.na(ebitda) & !is.na(fcoalesce(revt, sale)) & fcoalesce(revt, sale) != 0,
  ebitda_margin := ebitda / fcoalesce(revt, sale)
]

#### EPS Growth ----
# according to Aswani et al. (2024)
comp_funda_usd[
  order(gvkey, datadate),
  eps_growth := {
    fy <- fyear
    consec <- (fy - shift(fy)) == 1L
    ifelse(
      consec & !is.na(epsexcon) & !is.na(shift(epsexcon)) & shift(epsexcon) != 0,
      (epsexcon / shift(epsexcon)) - 1,
      NA_real_
    )
  },
  by = gvkey
]

#### Log Sales ----
# according to Aswani et al. (2024)
comp_funda_usd[
  !is.na(fcoalesce(revt, sale)) & fcoalesce(revt, sale) > 0,
  log_sales := log(fcoalesce(revt, sale))
]

#### Gross Profitability ----
# according to Alves et al. (2025)
comp_funda_usd[
  !is.na(fcoalesce(revt, sale)) & !is.na(cogs) & !is.na(at) & at != 0,
  gross_profit := (fcoalesce(revt, sale) - cogs) / at
]

#### Tangibility ----
# according to Alves et al. (2025)
comp_funda_usd[
  !is.na(ppent) & !is.na(at) & at != 0,
  tang := ppent / at
]

#### R&D ----
# according to Alves et al. (2025)
comp_funda_usd[
  !is.na(at) & at != 0,
  rd_intensity := fcoalesce(xrd, 0) / at
]


### 7) Save to parquet ----
write_parquet(comp_funda_usd, "./01_data_output/comp_funda_usd.parquet")



## V: Compustat NA Monthly security data (with optional Trucost gvkey filter) ----

# This section gathers the North American monthly security data that are used
# as a sanity check with the manually calculated total returns since here we have 
# a trt1m column that pre-calculates them. The monthly data are used as a backfill
# for section VI whenever the trfd value is missing.

### 1) Pull Compustat Data ----

begin_date <- comp_daily_min_date
end_date <- comp_daily_max_date

qry <- glue_sql("
WITH base_na AS (
  SELECT
    s.gvkey,
    s.iid,
    s.datadate,
    s.tic,
    s.exchg,
    c.conm,
    c.loc,
    c.fic,
    s.curcdm,
    s.prccm,
    s.ajexm,
    s.trfm,
    s.trt1m,
    s.cshom,
    1.0::numeric AS qunit,
    /* Trucost linkage flag */
    EXISTS (
      SELECT 1
      FROM   trucost_common.wrds_companies wc
      JOIN   trucost_environ.wrds_environment we
             ON we.institutionid = wc.institutionid
      WHERE  wc.gvkey = s.gvkey
        AND  wc.gvkey <> ''
        AND  we.periodenddate BETWEEN {trucost_min_date} AND {trucost_max_date}
        AND  (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL)
        AND  (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL)
    ) AS in_trucost,
    'comp_na'::text AS source
  FROM comp_na_daily_all.secm s
  LEFT JOIN comp_na_daily_all.company c
    ON c.gvkey = s.gvkey
  WHERE s.tpci = '0'
    AND s.datadate BETWEEN {begin_date} AND {end_date}
)
SELECT
  gvkey, iid, datadate, source, tic, exchg,
  conm, loc, fic, curcdm,
  prccm, ajexm, trfm, trt1m, cshom,
  qunit, in_trucost
FROM base_na
ORDER BY gvkey, iid, datadate
", .con = wrds)

comp_NA_monthly <- as.data.table(DBI::dbGetQuery(wrds, qry))

### 2) Convert to USD ----
comp_fx_global <- read_parquet("./01_data_output/comp_fx_global.parquet")
setDT(comp_fx_global)

# Normalize keys
comp_fx_global[, `:=`(
  datadate = as.Date(datadate),
  curd     = toupper(curd)
)]
comp_NA_monthly[, `:=`(
  datadate = as.Date(datadate),
  curcdm   = toupper(curcdm)
)]

# Prefer rows with valid exratd_tousd, then keep one per (datadate, currency)
comp_fx_global[, has_tousd := !is.na(exratd_tousd)]
setorder(comp_fx_global, datadate, curd, -has_tousd)

comp_fx_unique <- comp_fx_global[
  , .SD[1], by = .(datadate, curd)
][
  , .(datadate,
      curcd = curd,
      fx_tousd = exratd_tousd)
]

# Ensure USD=1 for all dates in NA monthly
usd_rows <- unique(comp_NA_monthly[, .(datadate)])
usd_rows[, `:=`(curcd = "USD", fx_tousd = 1)]
comp_fx_unique <- rbind(comp_fx_unique, usd_rows, fill = TRUE)

# RENAME to match comp_NA_monthly's column 'curcdm' and set keys
setnames(comp_fx_unique, "curcd", "curcdm")
setkey(comp_fx_unique,   datadate, curcdm)
setkey(comp_NA_monthly,  datadate, curcdm)

# Join FX -> NA monthly
comp_NA_monthly[comp_fx_unique,
                `:=`(fx_tousd = i.fx_tousd),
                on = .(datadate, curcdm)
]

setorder(comp_NA_monthly, gvkey, iid, datadate)

### 3) Recalc returns with USD prices ----

for (col in c("prccm","qunit","ajexm","cshom","trfm")){
  set(comp_NA_monthly, j = col, value = as.numeric(comp_NA_monthly[[col]]))
}

comp_NA_monthly[is.na(fx_tousd) | is.na(qunit) | qunit == 0 | is.na(ajexm) | ajexm == 0,
                c("prcd_adj_usd","mktcap_usd") := .(NA_real_, NA_real_)]
comp_NA_monthly[is.na(prcd_adj_usd), prcd_adj_usd := (prccm * fx_tousd) / (qunit * ajexm)]
comp_NA_monthly[is.na(mktcap_usd), mktcap_usd   := prcd_adj_usd * (cshom * ajexm)]

setorder(comp_NA_monthly, gvkey, iid, datadate)

comp_NA_monthly[, price_trfd_eom := prcd_adj_usd * trfm]

comp_NA_monthly[
  order(gvkey, iid, datadate),
  ret_m := {
    ym     <- year(datadate) * 12L + month(datadate)
    ym_lag <- shift(ym)
    
    consec <- (ym - ym_lag) == 1L
    p      <- price_trfd_eom
    p_lag  <- shift(p)
    
    ifelse(
      consec & !is.na(p) & !is.na(p_lag) & p_lag != 0,
      (p / p_lag) - 1,
      NA_real_
    )
  },
  by = .(gvkey, iid)
]


### 4) Trucost gvkey filter ----
if (isTRUE(comp_NA_monthly_trucost_filter_toggle)) {
  message("Filtering according to Trucost gvkey...")
  b <- nrow(comp_NA_monthly); comp_NA_monthly <- comp_NA_monthly[in_trucost == TRUE]; a <- nrow(comp_NA_monthly); msg_keep("Trucost filter", b, a)
} else {
  message("No filtering by Trucost gvkey.")
}

### 5) primary gvkey–iid filter ----
# load (or reuse) the primary map
primary_map <- as.data.table(read_parquet("./01_data_output/primary_iid_exchg_g_NA.parquet"))[
  , .(gvkey = as.character(gvkey), primary_iid = as.character(primary_iid))
][!is.na(primary_iid)]
setkey(primary_map, gvkey, primary_iid)

# normalize types/keys on comp_NA_monthly
comp_NA_monthly[, `:=`(gvkey = as.character(gvkey), iid = as.character(iid))]
setkey(comp_NA_monthly, gvkey, iid)

# apply filter (semi-join)
b <- nrow(comp_NA_monthly)
comp_NA_monthly <- comp_NA_monthly[primary_map, on = .(gvkey, iid = primary_iid), nomatch = 0L]
a <- nrow(comp_NA_monthly); msg_keep("Primary issue filter (NA monthly)", b, a)

# (optional) re-sort after filtering
setorder(comp_NA_monthly, gvkey, iid, datadate)

### 6) Save to parquet ----
write_parquet(comp_NA_monthly, "./01_data_output/comp_NA_monthly.parquet")



## VI: Compustat Global & NA Daily prices (with optional Trucost gvkey filter) ----

# This section loads both Global & North America Daily security data (prices,
# adjustment factors, shares outstanding). Due to the size of the dataset, this part
# performs the data gathering into smaller chunks (see '1)' below). Within each step
# the adjusted prices, market caps and shares outstanding are calculated in USD
# and only the end-of-month (EOM) values are saved to save data and RAM usage.
# In a subsequent step the chunks are joined back to one data.table which is used
# to calculate the USD returns.

### 1) load chunks and turn to monthly ----
# inputs
start_all <- as.IDate(comp_daily_min_date)
end_all   <- as.IDate(comp_daily_max_date)
brks <- seq(start_all, end_all, by = "2 years")
if (tail(brks, 1L) < end_all) brks <- c(brks, end_all + 1)

# FX
comp_fx_global <- read_parquet("./01_data_output/comp_fx_global.parquet")
setDT(comp_fx_global)
comp_fx_global[, datadate := as.IDate(datadate)]
setkey(comp_fx_global, curd, datadate)

# primary iid map
primary_map <- as.data.table(read_parquet("./01_data_output/primary_iid_exchg_g_NA.parquet"))[
  , .(gvkey, primary_iid = as.character(primary_iid))
][!is.na(primary_iid)]
setkey(primary_map, gvkey, primary_iid)


# Build once for the whole run (full horizon, not per chunk)
trucost_full_min_date <- format(start_all, "%Y-%m-%d")
trucost_full_max_date <- format(end_all,   "%Y-%m-%d")

fetch_combined <- function(d1, d2, use_trucost,
                           fx_dt = comp_fx_global,
                           primary_map_dt = primary_map) {
  d1s <- format(d1, "%Y-%m-%d")
  d2s <- format(d2, "%Y-%m-%d")
  
  if (use_trucost) {
    sql <- sprintf("
      WITH trucost_gvkeys AS (
        SELECT DISTINCT wc.gvkey
        FROM   trucost_common.wrds_companies wc
        JOIN   trucost_environ.wrds_environment we
               ON we.institutionid = wc.institutionid
        WHERE  wc.gvkey <> ''
          AND  we.periodenddate BETWEEN '%s' AND '%s'  -- FULL horizon
          AND  (we.di_319413 IS NOT NULL OR we.di_376883 IS NOT NULL)
          AND  (we.di_319414 IS NOT NULL OR we.di_376884 IS NOT NULL)
      )
      SELECT * FROM (
        -- Global
        SELECT
          'comp_global'::text AS source,
          s.fic,                -- from g_secd
          s.loc,                -- from g_secd
          s.gvkey,
          s.datadate,
          s.conm,
          s.exchg,
          s.secstat,
          gc.dlrsn,
          s.ajexdi,
          s.cshoc,
          s.curcdd,
          s.prccd,
          s.prcstd,
          s.qunit,
          s.tpci,
          s.iid,
          s.trfd
        FROM comp_global_daily.g_secd s
        LEFT JOIN comp_global_daily.g_company gc
          ON gc.gvkey = s.gvkey
        WHERE s.datadate BETWEEN '%s' AND '%s'
          AND s.tpci = '0'
          AND s.prcstd IN (3,10)
          AND EXISTS (
            SELECT 1
            FROM trucost_gvkeys t
            WHERE t.gvkey = s.gvkey
          )

        UNION ALL

        -- North America (no qunit -> 1)
        SELECT
          'comp_na'::text AS source,
          s.fic,                -- from NA secd
          c.loc,                -- from comp.company
          s.gvkey,
          s.datadate,
          s.conm,
          s.exchg,
          s.secstat,
          c.dlrsn,
          s.ajexdi,
          s.cshoc,
          s.curcdd,
          s.prccd,
          s.prcstd,
          1.0::numeric AS qunit,
          s.tpci,
          s.iid,
          s.trfd
        FROM comp_na_daily_all.secd s
        LEFT JOIN comp.company c
          ON c.gvkey = s.gvkey
        WHERE s.datadate BETWEEN '%s' AND '%s'
          AND s.tpci = '0'
          AND s.prcstd IN (3,10)
          AND EXISTS (
            SELECT 1
            FROM trucost_gvkeys t
            WHERE t.gvkey = s.gvkey
          )
      ) u
      ORDER BY gvkey, iid, datadate
    ",
                   trucost_full_min_date, trucost_full_max_date,
                   d1s, d2s, d1s, d2s)
    
  } else {
    sql <- sprintf("
      SELECT * FROM (
        -- Global
        SELECT
          'comp_global'::text AS source,
          s.fic,                -- from g_secd
          s.loc,                -- from g_secd
          s.gvkey,
          s.datadate,
          s.conm,
          s.exchg,
          s.secstat,
          gc.dlrsn,
          s.ajexdi,
          s.cshoc,
          s.curcdd,
          s.prccd,
          s.prcstd,
          s.qunit,
          s.tpci,
          s.iid,
          s.trfd
        FROM comp_global_daily.g_secd s
        LEFT JOIN comp_global_daily.g_company gc
          ON gc.gvkey = s.gvkey
        WHERE s.datadate BETWEEN '%s' AND '%s'
          AND s.tpci = '0'
          AND s.prcstd IN (3,10)

        UNION ALL

        -- North America
        SELECT
          'comp_na'::text AS source,
          s.fic,                -- from NA secd
          c.loc,                -- from comp.company
          s.gvkey,
          s.datadate,
          s.conm,
          s.exchg,
          s.secstat,
          c.dlrsn,
          s.ajexdi,
          s.cshoc,
          s.curcdd,
          s.prccd,
          s.prcstd,
          1.0::numeric AS qunit,
          s.tpci,
          s.iid,
          s.trfd
        FROM comp_na_daily_all.secd s
        LEFT JOIN comp.company c
          ON c.gvkey = s.gvkey
        WHERE s.datadate BETWEEN '%s' AND '%s'
          AND s.tpci = '0'
          AND s.prcstd IN (3,10)
      ) u
      ORDER BY gvkey, iid, datadate
    ",
                   d1s, d2s, d1s, d2s)
  }
  
  dt <- as.data.table(DBI::dbGetQuery(wrds, sql))
  if (!nrow(dt)) return(NULL)
  
  dt[, `:=`(
    datadate = as.IDate(datadate),
    gvkey    = as.character(gvkey),
    iid      = as.character(iid)
  )]
  setkey(dt, gvkey, iid)
  dt <- dt[primary_map_dt, on = .(gvkey, iid = primary_iid), nomatch = 0L]
  if (!nrow(dt)) return(NULL)
  
  setorder(dt, gvkey, iid, datadate)
  
  fx_sub <- fx_dt[datadate >= d1 & datadate <= d2]
  dt[fx_sub, on = .(curcdd = curd, datadate), exratd_tousd := i.exratd_tousd]
  
  for (col in c("prccd","qunit","ajexdi","cshoc","trfd"))
    set(dt, j = col, value = as.numeric(dt[[col]]))
  
  dt[is.na(exratd_tousd) | is.na(qunit) | qunit == 0 |
       is.na(ajexdi) | ajexdi == 0,
     c("prcd_adj_usd","mktcap_usd") := .(NA_real_, NA_real_)]
  dt[is.na(prcd_adj_usd),
     prcd_adj_usd := (prccd * exratd_tousd) / (qunit * ajexdi)]
  dt[is.na(mktcap_usd),
     mktcap_usd   := prcd_adj_usd * (cshoc * ajexdi)]
  
  dt[, month := as.IDate(lubridate::floor_date(datadate, "month"))]
  EOM <- dt[, .SD[.N], by = .(gvkey, iid, month)]
  setorder(EOM, gvkey, iid, month)
  EOM[, price_trfd_eom := prcd_adj_usd * trfd]
  
  EOM[, .(gvkey, iid, month, datadate, source,
          fic, loc, conm, exchg, secstat, dlrsn, curcdd, prcstd,
          prccd, ajexdi, qunit, cshoc, trfd, exratd_tousd,
          prcd_adj_usd, mktcap_usd, price_trfd_eom)]
}


# chunk loop: write EOM only
if (isTRUE(comp_daily_toggle)) {
  message("Loading Compustat Daily Global & North America")
  for (i in seq_len(length(brks) - 1L)) {
    d1 <- brks[i]
    d2 <- min(brks[i + 1L] - 1, end_all)
    
    message(sprintf("Chunk %d: %s -> %s", i, d1, d2))
    eom_chunk <- fetch_combined(d1, d2, comp_daily_trucost_filter_toggle)
    if (is.null(eom_chunk) || !nrow(eom_chunk)) next
    
    out_file <- sprintf("./01_data_output/01_chunks_prices/comp_g_daily_EOM_%s_%s.parquet",
                        format(d1, "%Y-%m-%d"), format(d2, "%Y-%m-%d"))
    write_parquet(eom_chunk, out_file)
    rm(eom_chunk); gc()
  }
  
  message("Done. Chunks saved as EOM rows with adjusted USD price and market cap.")
}else{
  message("Compustat Daily Toggle is set to FALSE")
}


### 2) merge and calculate monthly returns ----
# discover EOM chunk files
chunk_files <- list.files(
  "./01_data_output/01_chunks_prices/",
  pattern = "^comp_g_daily_EOM_\\d{4}-\\d{2}-\\d{2}_\\d{4}-\\d{2}-\\d{2}\\.parquet$",
  full.names = TRUE
)

# 2) read + bind (schema-safe), fix types
dt_list <- lapply(chunk_files, function(f) {
  x <- read_parquet(f); setDT(x)
  if (!inherits(x$datadate, "IDate")) x[, datadate := as.IDate(datadate)]
  if (!inherits(x$month, "IDate"))   x[, month    := as.IDate(month)]
  x[]
})
eom_all <- rbindlist(dt_list, use.names = TRUE, fill = TRUE)

# 3) sort
setorder(eom_all, gvkey, iid, month, datadate)

# calculate the end-of-month total returns
eom_all[
  order(gvkey, iid, month),
  ret_m := {
    # convert month to numeric index (year-month)
    ym     <- year(month) * 12L + month(month)
    ym_lag <- shift(ym)
    
    consec <- (ym - ym_lag) == 1L
    p      <- price_trfd_eom
    p_lag  <- shift(p)
    
    ifelse(
      consec & !is.na(p) & !is.na(p_lag) & p_lag != 0,
      (p / p_lag) - 1,
      NA_real_
    )
  },
  by = .(gvkey, iid)
]

# 4) Save to parquet
# write_parquet(eom_all, "./01_data_output/comp_daily_EOM_consolidated.parquet")

### 3) fill missing values from monthly comp NA ----
# Load monthly NA (from V)
comp_NA_monthly <- as.data.table(read_parquet("./01_data_output/comp_NA_monthly.parquet"))
if (!inherits(comp_NA_monthly$datadate, "IDate")) comp_NA_monthly[, datadate := as.IDate(datadate)]
comp_NA_monthly[, `:=`(gvkey = as.character(gvkey), iid = as.character(iid))]
comp_NA_monthly[, month := as.IDate(lubridate::floor_date(datadate, "month"))]
setkey(comp_NA_monthly, gvkey, iid, month)

# Ensure VI table keyed
if (!inherits(eom_all$datadate, "IDate")) eom_all[, datadate := as.IDate(datadate)]
if (!inherits(eom_all$month, "IDate"))   eom_all[, month    := as.IDate(month)]
if (!"trt1m" %in% names(eom_all)) eom_all[, trt1m := as.numeric(NA)]
eom_all[, `:=`(gvkey = as.character(gvkey), iid = as.character(iid))]
setkey(eom_all, gvkey, iid, month)

# Prepare join view with mapped/compatible fields from V
v_join <- comp_NA_monthly[, .(
  gvkey, iid, month,
  fic_v      = fic,
  loc_v      = loc,
  conm_v     = conm,
  exchg_v    = exchg,
  prccd_v    = prccm,
  curcdd_v   = curcdm,          # currency code
  ajexdi_v   = ajexm,           # adj factor monthly -> daily name
  qunit_v    = qunit,
  cshoc_v    = cshom,           # shares outstanding
  trfd_v     = trfm,            # total return factor (monthly)
  exratd_tousd_v = fx_tousd,    # USD FX
  prcd_adj_usd_v = prcd_adj_usd,
  mktcap_usd_v   = mktcap_usd,
  price_trfd_eom_v = price_trfd_eom,
  ret_m_v        = ret_m,
  trt1m_v        = trt1m
)]

# Fill ONLY where VI has trfd == NA and source == "comp_na"
eom_all[v_join, on = .(gvkey, iid, month),
        `:=`(
          fic            = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.fic_v),            i.fic_v,            fic),
          loc            = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.loc_v),            i.loc_v,            loc),
          conm           = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.conm_v),           i.conm_v,           conm),
          exchg          = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.exchg_v),          i.exchg_v,          exchg),
          curcdd         = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.curcdd_v),         i.curcdd_v,         curcdd),
          prccd          = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.prccd_v),         i.prccd_v,         prccd),
          ajexdi         = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.ajexdi_v),         i.ajexdi_v,         ajexdi),
          qunit          = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.qunit_v),          i.qunit_v,          qunit),
          cshoc          = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.cshoc_v),          i.cshoc_v,          cshoc),
          trfd           = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.trfd_v),           i.trfd_v,           trfd),
          exratd_tousd   = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.exratd_tousd_v),   i.exratd_tousd_v,   exratd_tousd),
          prcd_adj_usd   = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.prcd_adj_usd_v),   i.prcd_adj_usd_v,   prcd_adj_usd),
          mktcap_usd     = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.mktcap_usd_v),     i.mktcap_usd_v,     mktcap_usd),
          price_trfd_eom = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.price_trfd_eom_v), i.price_trfd_eom_v, price_trfd_eom),
          ret_m          = fifelse(source == "comp_na" & is.na(trfd) & !is.na(i.ret_m_v),          i.ret_m_v,          ret_m),
          trt1m          = fifelse(source == "comp_na" & is.na(trt1m) & !is.na(i.trt1m_v),                  i.trt1m_v, trt1m), # safety check to compare trt1m column with my calcs
          source         = fifelse(source == "comp_na" & is.na(trfd),                              "comp_NA_monthly",  source)
        )
]

setorder(eom_all, gvkey, iid, month, datadate)

#write_parquet(eom_all, "./01_data_output/comp_daily_EOM_consolidated_filled.parquet")

### 4) Data Cleaning ----
####  4.1 Detection of inactive securities and adding delisting return ---- 
# based on Alves et al. (2025) and Bessembinder et al. (2019)

# identify flat-price active stocks in the LAST 12 MONTHS of the sample
sample_end   <- max(eom_all$month, na.rm = TRUE)
sample_start <- sample_end %m-% months(11L)   # inclusive 12-month window

last12 <- eom_all[month >= sample_start & month <= sample_end]

flat_last12 <- last12[
  secstat == "A",
  {
    p <- prccd
    n <- sum(!is.na(p))
    if (n == 12L && length(unique(p[!is.na(p)])) == 1L) .SD else NULL
  },
  by = .(gvkey, iid)
]

# 2) remove those 12 "inactive" months from eom_all
b <- nrow(eom_all)
eom_all <- eom_all[!flat_last12, on = .(gvkey, iid, month)]
a <- nrow(eom_all)

msg_keep("Inactive removal", b, a)

# 3) set delisting return (-30%) in last remaining month for these stocks
delist_keys <- unique(flat_last12[, .(gvkey, iid)])

delist_months <- eom_all[
  delist_keys,
  on = .(gvkey, iid),
  allow.cartesian = TRUE
][
  , .SD[which.max(month)], by = .(gvkey, iid)
]

eom_all[
  delist_months,
  on = .(gvkey, iid, month),
  ret_m := -0.30
]


### 5) additional control variables ----

#### Momentum ----
# According to Alves et al. (2025)
eom_all[
  order(gvkey, iid, datadate),
  mom := {
    n <- 11L
    r <- shift(ret_m, 2L)
    
    # how many non-missing returns in the window?
    valid <- frollapply(!is.na(r), n, sum, align = "right")
    
    # rolling compounded return
    val <- exp(frollsum(log1p(r), n, align = "right", na.rm = TRUE)) - 1
    
    # compute span in calendar months between the current date and
    # the *earliest* date in the n-length window
    # earliest index in the window is i - (n - 1)
    span_months <- {
      d <- datadate
      d_earliest <- shift(d, n - 1L)   # date associated with earliest obs in window
      12L * (year(d) - year(d_earliest)) +
        (month(d) - month(d_earliest))
    }
    
    # for perfectly monthly data, span_months will be 10 for an 11-return window
    fifelse(valid >= 9 & span_months <= 12L, val, NA_real_)
  },
  by = .(gvkey, iid)
]

#### Volatility ----
# rolling standard deviation of monthly returns over past 12 months (excl. t)
# requires at least 9 valid months, as in Aswani / Fama-French style
eom_all[
  order(gvkey, iid, datadate),
  vola := {
    n <- 12L
    r <- shift(ret_m, 1L)
    valid <- frollapply(!is.na(r), n, sum, align = "right")
    raw_vol <- frollapply(r, n, sd, align = "right", na.rm = TRUE)
    span_months <- {
      d <- datadate
      d0 <- shift(d, n - 1L)
      12L * (year(d) - year(d0)) + (month(d) - month(d0))
    }
    fifelse(valid >= 9 & span_months <= 12L, raw_vol, NA_real_)
  },
  by = .(gvkey, iid)
]

#### Logsize ----
# according to Alves et al. (2025)
# natural logarithm of firm i’s market capitalization 
# (price times shares outstanding) as of June of year t in USD
eom_all[
  month(month) == 6 & !is.na(mktcap_usd) & mktcap_usd > 0,
  logsize := log(mktcap_usd)
]

eom_all[
  order(gvkey, iid, month),
  `:=`(
    logsize_ff_candidate = nafill(logsize, type = "locf"),
    last_obs_month = nafill(
      fifelse(!is.na(logsize), month, as.IDate(NA_integer_)),
      type = "locf"
    )
  ),
  by = .(gvkey, iid)
]

eom_all[, gap_months :=
          12L * (year(month) - year(last_obs_month)) +
          (month(month) - month(last_obs_month))]

eom_all[, logsize :=
          ifelse(gap_months <= 11L, logsize_ff_candidate, NA_real_)]

eom_all[, c("logsize_ff_candidate", "last_obs_month", "gap_months") := NULL]


#### Market value of Equity (December) ----
eom_all[
  month(month) == 12 & !is.na(mktcap_usd) & mktcap_usd > 0,
  ME_dec := mktcap_usd
]


### 6) only keep selected columns ----
eom_all_key_cols <- eom_all[, .(
  gvkey,
  iid,
  datadate,
  conm,
  month,
  source,
  loc,
  fic,
  secstat,
  dlrsn,
  exchg,
  curcdd,
  mktcap_usd,
  ret_m,
  mom,
  vola,
  logsize,
  ME_dec
)]

### 7) Save to parquet ----
write_parquet(eom_all_key_cols, "./01_data_output/comp_EOM_ret_market_cap.parquet")


# =============================================================================.
# External data sources ----
# =============================================================================.

## VII: Monthly Risk factors & market beta ----
### 1) helper function ----
read_french_monthly <- function(url, annual_start_words = "Annual Factors") {
  
  # 1) Download the zip
  zf <- tempfile(fileext = ".zip")
  download.file(url, zf, mode = "wb")
  
  # 2) Identify the TXT or CSV file inside the zip
  zip_listing <- unzip(zf, list = TRUE)
  inner_idx <- which(grepl("\\.(txt|csv)$", zip_listing$Name, ignore.case = TRUE))[1]
  stopifnot(length(inner_idx) == 1)
  inner <- zip_listing$Name[inner_idx]
  
  # 3) Read all lines to locate section boundaries
  con <- unz(zf, inner, open = "rb")
  lines <- readLines(con, warn = FALSE, encoding = "UTF-8")
  close(con)
  
  # 4) Find header line of the monthly table and the annual section
  header_line <- which(grepl("^,?Mkt-?RF,SMB,HML,RF\\s*$", lines))[1]
  annual_start <- which(grepl(annual_start_words, lines, ignore.case = TRUE))[1]
  
  stopifnot(!is.na(header_line), !is.na(annual_start), header_line < annual_start)
  
  # 5) Compute bounds for monthly rows
  skip_rows <- header_line
  n_max <- annual_start - header_line - 2
  
  # 6) Read just the monthly block
  monthly <- readr::read_delim(
    file = unz(zf, inner),
    delim = ",",
    skip = skip_rows,
    n_max = n_max,
    col_names = c("date","Mkt_RF","SMB","HML","RF"),
    col_types = readr::cols(
      date   = readr::col_integer(),
      Mkt_RF = readr::col_double(),
      SMB    = readr::col_double(),
      HML    = readr::col_double(),
      RF     = readr::col_double()
    ),
    na = "-99.99",
    trim_ws = TRUE,
    locale = readr::locale(decimal_mark = "."),
    show_col_types = TRUE
  )
  
  # Drop any empty rows
  monthly <- monthly[!is.na(monthly$date), ]
  return(setDT(monthly))
}


read_french_monthly_ff5 <- function(url, annual_start_words = "Annual Factors") {
  
  # 1) Download the zip
  zf <- tempfile(fileext = ".zip")
  download.file(url, zf, mode = "wb")
  
  # 2) Identify the TXT or CSV file inside the zip
  zip_listing <- unzip(zf, list = TRUE)
  inner_idx <- which(grepl("\\.(txt|csv)$", zip_listing$Name, ignore.case = TRUE))[1]
  stopifnot(length(inner_idx) == 1)
  inner <- zip_listing$Name[inner_idx]
  
  # 3) Read all lines to locate section boundaries
  con <- unz(zf, inner, open = "rb")
  lines <- readLines(con, warn = FALSE, encoding = "UTF-8")
  close(con)
  
  # 4) Find header line of the monthly table and the annual section
  header_line <- which(grepl("^,?Mkt-?RF,SMB,HML,RMW,CMA,RF\\s*$", lines))[1]
  annual_start <- which(grepl(annual_start_words, lines, ignore.case = TRUE))[1]
  
  stopifnot(!is.na(header_line), !is.na(annual_start), header_line < annual_start)
  
  # 5) Compute bounds for monthly rows
  skip_rows <- header_line
  n_max <- annual_start - header_line - 2
  
  # 6) Read just the monthly block
  monthly <- readr::read_delim(
    file = unz(zf, inner),
    delim = ",",
    skip = skip_rows,
    n_max = n_max,
    col_names = c("date","Mkt_RF","SMB","HML","RMW","CMA","RF"),
    col_types = readr::cols(
      date   = readr::col_integer(),
      Mkt_RF = readr::col_double(),
      SMB    = readr::col_double(),
      HML    = readr::col_double(),
      RMW    = readr::col_double(),
      CMA    = readr::col_double(),
      RF     = readr::col_double()
    ),
    na = "-99.99",
    trim_ws = TRUE,
    locale = readr::locale(decimal_mark = "."),
    show_col_types = FALSE
  )
  
  # Drop any empty rows and convert to data.table
  monthly <- monthly[!is.na(monthly$date), ]
  return(data.table::setDT(monthly))
}


### 2) download developed 3 factors ----
url <- "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Developed_3_Factors_CSV.zip"

FF_dev_3 <- read_french_monthly(url)
FF_dev_3[, month := as.Date(paste0(substr(date, 1, 4), "-", substr(date, 5, 6), "-01"))]
FF_dev_3[, c("Mkt_RF", "RF", "SMB", "HML") := lapply(.SD, function(x) x / 100),
         .SDcols = c("Mkt_RF", "RF", "SMB", "HML")]

head(FF_dev_3, 3)

### 3) download developed ex US 3 factors ----
url <- "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Developed_ex_US_3_Factors_CSV.zip"

FF_dev_ex_US_3 <- read_french_monthly(url)
FF_dev_ex_US_3[, month := as.Date(paste0(substr(date, 1, 4), "-", substr(date, 5, 6), "-01"))]
FF_dev_ex_US_3[, c("Mkt_RF", "RF", "SMB", "HML") := lapply(.SD, function(x) x / 100),
               .SDcols = c("Mkt_RF", "RF", "SMB", "HML")]


head(FF_dev_ex_US_3, 3)

### 4) download US 3 factors ----
url <- "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip"

FF_US_3 <- read_french_monthly(url)
FF_US_3[, month := as.Date(paste0(substr(date, 1, 4), "-", substr(date, 5, 6), "-01"))]
FF_US_3[, c("Mkt_RF", "RF", "SMB", "HML") := lapply(.SD, function(x) x / 100),
        .SDcols = c("Mkt_RF", "RF", "SMB", "HML")]

head(FF_US_3, 3)
tail(FF_US_3, 3)

### 5) download emerging markets 5 factors ----

# The emerging markets countries currently include:
# Brazil, Chile, China, Colombia, Czech Republic, Egypt, Greece, Hungary, India, 
# Indonesia, Malaysia, Mexico, Kuwait, Peru, Philippines, Poland, Qatar, Saudi Arabia, 
# South Africa, South Korea, Taiwan, Thailand, Turkey, United Arab Emirates.

url <- "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Emerging_5_Factors_CSV.zip"

FF_emerg_5 <- read_french_monthly_ff5(url)
FF_emerg_5[, month := as.Date(paste0(substr(date, 1, 4), "-", substr(date, 5, 6), "-01"))]
FF_emerg_5[, c("Mkt_RF", "RF", "SMB", "HML","RMW","CMA") := lapply(.SD, function(x) x / 100),
           .SDcols = c("Mkt_RF", "RF", "SMB", "HML","RMW","CMA")]


head(FF_emerg_5)
tail(FF_emerg_5, 3)

### 6) rolling market betas ----
# Data load
# note that all FF tables use the 1-month US T-bill yield as the RF

# --- get monthly returns
comp_EOM_ret_market_cap <- read_parquet("./01_data_output/comp_EOM_ret_market_cap.parquet")
setDT(comp_EOM_ret_market_cap)

# subset to only needed cols
comp_EOM_ret_market_cap <- comp_EOM_ret_market_cap[, .(gvkey, iid, conm, exchg, datadate, month, mktcap_usd, ret_m, fic)]

# --- get HQ location
comp_sec_info_g_NA <- read_parquet("./01_data_output/comp_sec_info_g_NA.parquet")
setDT(comp_sec_info_g_NA)

# subset to only needed cols
comp_sec_info_g_NA <- comp_sec_info_g_NA[, .(gvkey, iid, loc)]

# --- join loc on returns
comp_EOM_ret_market_cap <- left_join(comp_EOM_ret_market_cap,comp_sec_info_g_NA, 
                                     by = c("gvkey", "iid"))

#### Country Specific Market Returns ----
# this part is calcualated based on the full sample of securities in the compustat
# data universe. Instead of the same primary_iid mapping used in chapter V, it
# selects unique gvkey-iid-month observations based on the largest market cap.

full_country_vwret_loc <- read_parquet("./01_data_output/full_country_vwret_loc.parquet")
setDT(full_country_vwret_loc)
#comp_EOM_ret_market_cap <- comp_EOM_ret_market_cap[
#  full_country_vwret_loc, on = .(loc, month), nomatch = 0L
#]
comp_EOM_ret_market_cap <- full_country_vwret_loc[
  comp_EOM_ret_market_cap, on = .(loc, month)
]


full_country_vwret_fic <- read_parquet("./01_data_output/full_country_vwret_fic.parquet")
setDT(full_country_vwret_fic)
#comp_EOM_ret_market_cap <- comp_EOM_ret_market_cap[
#  full_country_vwret_fic, on = .(fic, month), nomatch = 0L
#]
comp_EOM_ret_market_cap <- full_country_vwret_fic[
  comp_EOM_ret_market_cap, on = .(fic, month)
]


#### MSCI All Country World Index ----
# --- add MSCI All Country World Index (combined emerging and developed returns)
# Load data
#msci_acwi <- fread("./02_data_input/msci_acwi_index.csv")
#
## --- 2. Parse and clean columns ---
#msci_acwi[, Dates := as.Date(Dates, format = "%b-%d-%Y")]
#msci_acwi[, MSCI_ACWI := as.numeric(MSCI_ACWI)]
#
## --- 3. Get end-of-month (EOM) prices ---
## For each year-month, keep only the last available trading day
#msci_acwi_eom <- msci_acwi[, .SD[.N], by = .(year = year(Dates), month = month(Dates))]
#
## --- 4. Compute simple monthly returns ---
#setorder(msci_acwi_eom, year, month)
#msci_acwi_eom[, ACWI_Rm := MSCI_ACWI / shift(MSCI_ACWI) - 1]
#
## --- 5. Create a standard 'month' column for merging later ---
#msci_acwi_eom[, month := as.Date(paste0(year, "-", month, "-01"))]
#
## --- 6. Keep only relevant columns ---
#msci_acwi_eom <- msci_acwi_eom[, .(month, ACWI_Rm)]
#
## --- join on eom returns
##comp_EOM_ret_market_cap <- comp_EOM_ret_market_cap[msci_acwi_eom, on = "month", nomatch = 0]
#comp_EOM_ret_market_cap <- msci_acwi_eom[comp_EOM_ret_market_cap, on = "month"]

# --- select key cols from FF data
FF_dev_3 <- FF_dev_3[, .(month, Mkt_RF)]
setnames(FF_dev_3,
         old = setdiff(names(FF_dev_3), "month"),
         new = paste0(setdiff(names(FF_dev_3), "month"), "_dev_3"))


FF_emerg_5 <- FF_emerg_5[, .(month, Mkt_RF)]
setnames(FF_emerg_5,
         old = setdiff(names(FF_emerg_5), "month"),
         new = paste0(setdiff(names(FF_emerg_5), "month"), "_emerg_5"))


FF_US_3 <- FF_US_3[, .(month, Mkt_RF, RF)]
setnames(FF_US_3,
         old = setdiff(names(FF_US_3), "month"),
         new = paste0(setdiff(names(FF_US_3), "month"), "_US_3"))


# join them 
comp_EOM_ret_market_cap <- FF_dev_3[comp_EOM_ret_market_cap, on = "month"]
comp_EOM_ret_market_cap <- FF_emerg_5[comp_EOM_ret_market_cap, on = "month"]
comp_EOM_ret_market_cap <- FF_US_3[comp_EOM_ret_market_cap, on = "month"]

setnames(comp_EOM_ret_market_cap,
         old = "RF_US_3",
         new = "RF")


# calculate excess returns
comp_EOM_ret_market_cap[
  !is.na(ret_m) & !is.na(RF),
  ret_m_RF := ret_m - RF
]

#comp_EOM_ret_market_cap[
#  !is.na(ACWI_Rm) & !is.na(RF),
#  ACWI_Rm_RF := ACWI_Rm - RF
#]

comp_EOM_ret_market_cap[
  !is.na(Rm_country_loc) & !is.na(RF),
  Rm_country_loc_RF := Rm_country_loc - RF
]

comp_EOM_ret_market_cap[
  !is.na(Rm_country_fic) & !is.na(RF),
  Rm_country_fic_RF := Rm_country_fic - RF
]


#### Beta ----
# Rolling CAPM beta via rolling moments (fast, vectorized)
# y = excess stock return (Ri - Rf)
# x = market excess return (Rm - Rf)
rolling_beta_fast_lagged <- function(dt,
                                     id_cols = c("gvkey","iid"),
                                     time_col = "month",
                                     y_col = "ret_m_RF",
                                     x_col = "comb_Mkt_RF",
                                     window = 24,
                                     min_non_na = 12,
                                     out_col = paste0("beta_", window, "m_lag1")) {
  stopifnot(is.data.table(dt))
  setorderv(dt, c(id_cols, time_col))
  dt[, (out_col) := {
    # shift inputs by 1 so the rolling window ends at t-1
    x <- shift(get(x_col), 1L)
    #x <- get(x_col)
    y <- shift(get(y_col), 1L)
    #y <- get(y_col)
    
    ok  <- !(is.na(x) | is.na(y))
    xx  <- fifelse(ok, x, 0.0)
    yy  <- fifelse(ok, y, 0.0)
    xy  <- fifelse(ok, x*y, 0.0)
    x2  <- fifelse(ok, x*x, 0.0)
    cnt <- as.integer(ok)
    
    sx  <- frollsum(xx,  window, align = "right")
    sy  <- frollsum(yy,  window, align = "right")
    sxy <- frollsum(xy,  window, align = "right")
    sx2 <- frollsum(x2,  window, align = "right")
    n   <- frollsum(cnt, window, align = "right")
    
    mx <- sx / n; my <- sy / n
    cov_xy <- (sxy / n) - (mx * my)
    var_x  <- (sx2 / n) - (mx * mx)
    
    beta <- cov_xy / var_x
    beta[n < min_non_na] <- NA_real_
    beta[var_x <= 0 | is.na(var_x)] <- NA_real_
    beta
  }, by = id_cols]
  invisible(dt)
}


# applied to the data

# 24 months version
# version with MSCI ACWI index for all countries
#rolling_beta_fast_lagged(comp_EOM_ret_market_cap,
#                         id_cols = c("gvkey","iid"),
#                         time_col = "month",
#                         y_col = "ret_m_RF",
#                         x_col = "ACWI_Rm_RF",
#                         window = 24,
#                         min_non_na = 12,
#                         out_col = "beta_24m_lag1_ACWI")

# version with country specific market return (LOC grouped)
rolling_beta_fast_lagged(comp_EOM_ret_market_cap,
                         id_cols = c("gvkey","iid"),
                         time_col = "month",
                         y_col = "ret_m_RF",
                         x_col = "Rm_country_loc_RF",
                         window = 24,
                         min_non_na = 12,
                         out_col = "beta_24m_lag1_loc")

# version with country specific market return (FIC grouped)
rolling_beta_fast_lagged(comp_EOM_ret_market_cap,
                         id_cols = c("gvkey","iid"),
                         time_col = "month",
                         y_col = "ret_m_RF",
                         x_col = "Rm_country_fic_RF",
                         window = 24,
                         min_non_na = 12,
                         out_col = "beta_24m_lag1_fic")

# 12 months version
# version with MSCI ACWI index for all countries
#rolling_beta_fast_lagged(comp_EOM_ret_market_cap,
#                         id_cols = c("gvkey","iid"),
#                         time_col = "month",
#                         y_col = "ret_m_RF",
#                         x_col = "ACWI_Rm_RF",
#                         window = 12,
#                         min_non_na = 12,
#                         out_col = "beta_12m_lag1_ACWI")

# version with country specific market return (LOC grouped)
rolling_beta_fast_lagged(comp_EOM_ret_market_cap,
                         id_cols = c("gvkey","iid"),
                         time_col = "month",
                         y_col = "ret_m_RF",
                         x_col = "Rm_country_loc_RF",
                         window = 12,
                         min_non_na = 12,
                         out_col = "beta_12m_lag1_loc")

# version with country specific market return (FIC grouped)
rolling_beta_fast_lagged(comp_EOM_ret_market_cap,
                         id_cols = c("gvkey","iid"),
                         time_col = "month",
                         y_col = "ret_m_RF",
                         x_col = "Rm_country_fic_RF",
                         window = 12,
                         min_non_na = 12,
                         out_col = "beta_12m_lag1_fic")


### 7) save to parquet ----
market_betas <- comp_EOM_ret_market_cap[
  , .(
    gvkey, 
    iid, 
    conm, 
    datadate,
    month,
    fic, 
    loc, 
    n_stocks_full_comp_fic, 
    n_stocks_full_comp_loc, 
    #beta_24m_lag1_ACWI,
    beta_24m_lag1_loc,
    beta_24m_lag1_fic,
    #beta_12m_lag1_ACWI,
    beta_12m_lag1_loc,
    beta_12m_lag1_fic)
]

write_parquet(market_betas, "./01_data_output/market_betas.parquet")


## VIII: Climate Attention & Concern Measures ----
### MCCC & UMC Index (Ardia et al., 2022) ----
#### 1) Download MCCC ----
url <- "https://www.dropbox.com/scl/fi/uucc6401uje293ofc3ahq/Sentometrics_US_Media_Climate_Change_Index.xlsx?dl=1&rlkey=jvgb6xg9w4ctdz5cdl6qun5md"

tmp <- tempfile(fileext = ".xlsx")
download.file(url, destfile = tmp, mode = "wb")

mccc <- read_excel(tmp, sheet = "2025 update monthly", skip = 6)
setDT(mccc)

mccc[, Date := as.Date(Date)]
mccc[, month := as.Date(floor_date(Date, unit = "month"))]

# select key columns
mccc <- mccc[, .(Date, month, Aggregate)]

#### 2) Calculate UMC (simple AR(1) ----
setorder(mccc, month)   # ensure chronological order
compute_AR1 <- function(
    dt,
    date_col   = "month",
    value_col  = "Aggregate",
    window     = 36L,
    output_col = "UMC"
) {
  # Work on a copy to preserve user data
  out <- as.data.table(copy(dt))
  
  # Basic checks
  if (!date_col %in% names(out)) {
    stop("date_col '", date_col, "' not found. Available columns: ",
         paste(names(out), collapse = ", "))
  }
  if (!value_col %in% names(out)) {
    stop("value_col '", value_col, "' not found. Available columns: ",
         paste(names(out), collapse = ", "))
  }
  
  # Ensure numeric
  out[, (value_col) := as.numeric(get(value_col))]
  
  # Sort chronologically
  out <- out[order(out[[date_col]])]
  
  # Lagged value
  out[, lag_value := shift(get(value_col), 1L)]
  
  # Add forecast + output shock col
  out[, PRED := NA_real_]
  out[, (output_col) := NA_real_]
  
  n <- nrow(out)
  
  if (n > window + 1L) {
    for (t in (window + 1L):n) {
      
      idx <- (t - window):(t - 1L)
      
      sub <- out[idx, .(
        y     = get(value_col),
        y_lag = lag_value
      )]
      
      if (anyNA(sub)) next
      
      # AR(1)
      fit <- lm(y ~ y_lag, data = sub)
      
      # Predicted value at time t
      pred_t <- predict(fit, newdata = data.frame(y_lag = out$lag_value[t]))
      out$PRED[t] <- pred_t
      
      # Shock
      out[[output_col]][t] <- out[[value_col]][t] - pred_t
    }
  }
  
  # Remove temp column
  out[, lag_value := NULL]
  out[, PRED := NULL]
  
  return(out)
}

mccc_UMC <- compute_AR1(
  dt        = mccc,
  date_col  = "month",
  value_col = "Aggregate",
  window    = 36L,
  output_col = "UMC"
)

#### 3) Calculate UMC (ARX) ----
#calculated offline in 08_UMC_ARX.R

#### 4) save to parquet----
# write_parquet(mccc_UMC, "./01_data_output/mccc_UMC.parquet")


### TRI & PRI: Transition Risk Index (Bua et al., 2024) ----
### Transition Risk Index (Bua et al., 2024) ---- 
#### 1) Download TRI ---- 
url <- "https://c/media/Climate_Risk_Index.xlsx" 
tmp <- tempfile(fileext = ".xlsx") 

download.file(url, destfile = tmp, mode = "wb") 

tri_pri <- read_excel(tmp, range = "A9:E5351", col_names = c("Date", "Transition_concern", "TRI", "Physical_concern", "PRI")) 
setDT(tri_pri) 
tri_pri[, Date := as.Date(Date)] 
tri_pri[, month := as.Date(format(Date, "%Y-%m-01"))] 

#### 2) monthly scores ---- 
# Create monthly averages with Date set to the first of each month 
tri_pri_monthly <- tri_pri[ , .( TRI = mean(TRI, na.rm = TRUE), 
                                 PRI = mean(PRI, na.rm = TRUE), 
                                 Transition_concern = mean(Transition_concern, na.rm = TRUE), 
                                 Physical_concern = mean(Physical_concern, na.rm = TRUE) ), 
                            by = .(month) 
][order(month)] 

#### 3) alternative calculation ---- 
TR_monthly <- compute_AR1(dt = tri_pri_monthly, 
                          date_col = "month", 
                          value_col = "Transition_concern", 
                          window = 36L, 
                          output_col = "TRI_monthly_roll" ) 

PR_monthly <- compute_AR1(dt = tri_pri_monthly, 
                          date_col = "month", 
                          value_col = "Physical_concern", 
                          window = 36L, 
                          output_col = "PRI_monthly_roll" ) 

setDT(tri_pri_monthly) 
setDT(TR_monthly) 
setDT(PR_monthly) 

# Add TRI_monthly_roll 
tri_pri_monthly[TR_monthly, TRI_monthly_roll := i.TRI_monthly_roll, on = "month" ] 

# Add PRI_monthly_roll 
tri_pri_monthly[PR_monthly, PRI_monthly_roll := i.PRI_monthly_roll, on = "month" ]


#### 4) save to parquet----
write_parquet(tri_pri_monthly, "./01_data_output/tri_pri_monthly.parquet")


### CAI: Climate Attention Index (Arteaga-Garavito et al., 2025) ----
#### 1) Download CAI (monthly by country, wide) ----
url <- "https://raw.githubusercontent.com/majoarteaga/ClimateAttentionACCY/refs/heads/main/11_CCV_index_monthly_16012023_bycountry.csv"
cai <- read.csv(url)
setDT(cai)

cai[, X := NULL]                    # drop index column from CSV
cai[, date := as.Date(date)]        # ensure Date type

# Country columns are everything except 'date'
country_cols <- setdiff(names(cai), "date")

# ISO2 to ISO3 mapping for CAI countries
iso_map <- c(
  AR = "ARG",
  AU = "AUS",
  BR = "BRA",
  CA = "CAN",
  CL = "CHL",
  HK = "HKG",
  CN = "CHN",
  CO = "COL",
  FR = "FRA",
  DE = "DEU",
  IN = "IND",
  IT = "ITA",
  JP = "JPN",
  KR = "KOR",
  MX = "MEX",
  NZ = "NZL",
  NO = "NOR",
  PT = "PRT",
  SA = "SAU",
  ZA = "ZAF",
  ES = "ESP",
  SE = "SWE",
  CH = "CHE",
  UK = "GBR",
  US = "USA"
)

#### 2) Get per-country GDP from Worldbank ----
url_gdp_tot <- "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.CD?downloadformat=csv"
zip_gdp_tot <- "gdp_tot.zip"

download.file(url_gdp_tot, destfile = zip_gdp_tot, mode = "wb")
unzip(zip_gdp_tot)

files_gdp_tot <- list.files(pattern = "API_NY.GDP.MKTP.CD.*\\.csv$")
gdp_tot_raw   <- read_csv(files_gdp_tot[1], skip = 4)
setDT(gdp_tot_raw)

# Common WB structure
id_cols <- c("Country Name", "Country Code", "Indicator Name", "Indicator Code")

# Total GDP (current USD) to long
gdp_tot_long <- wb_to_long(gdp_tot_raw, "gdp_usd")
gdp_tot_long <- gdp_tot_long[year >= 2007 & year <= 2025]

setnames(gdp_tot_long, "Country Code", "iso3")
gdp_tot_long[, `Country Name` := NULL]

setcolorder(
  gdp_tot_long,
  c("iso3", "year", "gdp_usd")
)

# Turn CAI to long format and attach ISO codes, year
cai_long <- melt(
  cai,
  id.vars       = "date",
  measure.vars  = country_cols,
  variable.name = "iso2_country",
  value.name    = "value"
)

cai_long[, month := as.Date(format(date, "%Y-%m-01"))]
cai_long[, year  := as.integer(format(month, "%Y"))]

# Map ISO2 to ISO3
cai_long[, iso3_country := iso_map[iso2_country]]

# Keep only rows with a mapped ISO3 (drop any stray / non-country columns)
cai_long <- cai_long[!is.na(iso3_country)]

#### 3) Merge GDP into CAI and compute yearly GDP weights ----
# Attach GDP level (country-year)
cai_long[gdp_tot_long,
         on = .(iso3_country = iso3, year),
         gdp_usd := i.gdp_usd]

# Collapse to unique country-year rows for weight computation
gdp_weights <- unique(
  cai_long[!is.na(gdp_usd),
           .(iso3_country, year, gdp_usd)]
)

# GDP share among CAI countries per year
gdp_weights[, gdp_weight := gdp_usd / sum(gdp_usd, na.rm = TRUE),
            by = year]

# Merge weights back to full panel (repeated across months within a year)
cai_long[gdp_weights,
         on = .(iso3_country, year),
         gdp_weight := i.gdp_weight]

#### 4) Compute GDP-weighted & equal-weighted Global CAI per month ----
# 4a) GDP-weighted Global CAI
cai_global_gdp <- cai_long[!is.na(gdp_weight),
                           .(value = sum(value * gdp_weight, na.rm = TRUE)),
                           by = month]

cai_global_gdp[, `:=`(
  date         = month,
  year         = as.integer(format(month, "%Y")),
  iso2_country = "Global",
  iso3_country = "GLB",
  gdp_usd      = NA_real_,
  gdp_weight   = NA_real_
)]

# 4b) Equal-weighted Global CAI (simple average across countries)
cai_global_eq <- cai_long[!is.na(iso3_country) & !is.na(value),
                          .(value = mean(value, na.rm = TRUE)),
                          by = month]

cai_global_eq[, `:=`(
  date         = month,
  year         = as.integer(format(month, "%Y")),
  iso2_country = "Global_EQ",
  iso3_country = "GLB_EQ",
  gdp_usd      = NA_real_,
  gdp_weight   = NA_real_
)]

# Bind Global series back to CAI panel and tidy 
cai_long <- rbindlist(
  list(
    cai_long,
    cai_global_gdp,
    cai_global_eq
  ),
  use.names = TRUE,
  fill = TRUE
)

setorder(cai_long, iso3_country, month)

#### 5) Split into two tables: ----
#     (i) Global aggregates (equal- and GDP-weighted)
#     (ii) Country-level CAI only
cai_global_panel  <- cai_long[iso3_country %in% c("GLB", "GLB_EQ")]
cai_country_panel <- cai_long[!iso3_country %in% c("GLB", "GLB_EQ")]

#### 6) Reuse AR(1) calculation for CAI ----

# Helper: run compute_AR1() by panel (country or global)
compute_AR1_panel <- function(dt,
                              group_cols,
                              date_col   = "month",
                              value_col  = "value",
                              window     = 12L,
                              output_col = "UCAI") {
  dt[, compute_AR1(
    dt        = .SD,
    date_col  = date_col,
    value_col = value_col,
    window    = window,
    output_col = output_col
  ),
  by = group_cols]
}

# 6a) AR(1) shocks for GDP- and equal-weighted global CAI
cai_global_AR <- compute_AR1_panel(
  dt         = cai_global_panel,
  group_cols = c("iso3_country", "iso2_country"),
  date_col   = "month",
  value_col  = "value",
  window     = 12L,
  output_col = "UCAI"    # AR(1) error term (analogous to UMC)
)

# 6b) AR(1) shocks for country-level CAI
cai_country_AR <- compute_AR1_panel(
  dt         = cai_country_panel,
  group_cols = c("iso3_country", "iso2_country"),
  date_col   = "month",
  value_col  = "value",
  window     = 12L,
  output_col = "UCAI"    # same definition, but per country
)

# Select key cols
cai_global_AR <- cai_global_AR[, .(iso3_country, date, month, year, UCAI)]
cai_country_AR <- cai_country_AR[, .(iso3_country, iso2_country, date, value, month, year, UCAI)]

# Reshape from long (GLB / GLB_EQ) to wide
cai_global_AR <- dcast(
  cai_global_AR,
  month + year + date ~ iso3_country,
  value.var = "UCAI"
)

# Rename columns to readable global shock names
setnames(
  cai_global_AR,
  old = c("GLB", "GLB_EQ"),
  new = c("UCAI_global_GDP", "UCAI_global_EQ")
)

setorder(cai_global_AR, month)


#### 7) combine all country measures for analysis ----
# indexes
setnames(mccc_UMC,       "Aggregate",          "mccc_monthly")
setnames(tri_pri_monthly,"Transition_concern", "Transition_concern_monthly")
setnames(tri_pri_monthly,"Physical_concern",   "Physical_concern_monthly")
setnames(cai_global_gdp, "value",              "cai_global_gdp_monthly")
setnames(cai_global_eq,  "value",              "cai_global_eq_monthly")

dt_mccc <- mccc_UMC[, .(month, mccc_monthly)]
dt_tri  <- tri_pri_monthly[, .(month,
                               Transition_concern_monthly,
                               Physical_concern_monthly)]
dt_gdp  <- cai_global_gdp[, .(month, cai_global_gdp_monthly)]
dt_eq   <- cai_global_eq[, .(month, cai_global_eq_monthly)]

monthly_merged_attention <- Reduce(
  function(x, y) merge(x, y, by = "month", all = TRUE),
  list(dt_mccc, dt_tri, dt_gdp, dt_eq)
)

# AR(1) series
dt_umc <- mccc_UMC[, .(month, UMC)]
dt_tri <- tri_pri_monthly[, .(month,TRI_monthly_roll, TRI)]
dt_ucai <- cai_global_AR[,. (month,UCAI_global_GDP)]

monthly_merged_unexpected_attention <- Reduce(
  function(x, y) merge(x, y, by = "month", all = TRUE),
  list(dt_umc,dt_tri,dt_ucai)
)

# delete temp tables
rm(cai, country_cols, iso_map, gdp, gdp_list,
   cai_long, cai_global_gdp, cai_global_eq,
   cai_global_panel, cai_country_panel, gdp_weights,
   dt_mccc, dt_tri, dt_gdp, dt_eq,
   dt_umc,dt_tri,dt_ucai)


#### 8) save to parquet ----
write_parquet(cai_global_AR,  "./01_data_output/cai_global_AR.parquet")
write_parquet(cai_country_AR, "./01_data_output/cai_country_AR.parquet")
write_parquet(monthly_merged_attention, "./01_data_output/monthly_merged_attention.parquet")
write_parquet(monthly_merged_unexpected_attention, "./01_data_output/monthly_merged_unexpected_attention.parquet")

## IX: Country-level Policy measures ----

### Preferences for Action Index (PAI) (Galanis et al., 2022) ----
# construct an indicator from the unweighted sums of four variables: 

#### (i) World Bank GDP indicators ----
# (i.a) GDP per capita in current USD (World Bank)
url_gdp_pc <- "https://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.CD?downloadformat=csv"
zip_gdp_pc <- "gdp_pc.zip"

download.file(url_gdp_pc, destfile = zip_gdp_pc, mode = "wb")
unzip(zip_gdp_pc)

files_gdp_pc <- list.files(pattern = "API_NY.GDP.PCAP.CD.*\\.csv$")
gdp_pc_raw   <- read_csv(files_gdp_pc[1], skip = 4)
setDT(gdp_pc_raw)

# (i.b) Total GDP in current USD (World Bank)
url_gdp_tot <- "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.CD?downloadformat=csv"
zip_gdp_tot <- "gdp_tot.zip"

download.file(url_gdp_tot, destfile = zip_gdp_tot, mode = "wb")
unzip(zip_gdp_tot)

files_gdp_tot <- list.files(pattern = "API_NY.GDP.MKTP.CD.*\\.csv$")
gdp_tot_raw   <- read_csv(files_gdp_tot[1], skip = 4)
setDT(gdp_tot_raw)

# Common WB structure
id_cols <- c("Country Name", "Country Code", "Indicator Name", "Indicator Code")

# GDP per capita (current USD) to long
gdp_pc_long <- wb_to_long(gdp_pc_raw, "gdp_pc")
gdp_pc_long <- gdp_pc_long[year >= 2007 & year <= 2025]
setcolorder(
  gdp_pc_long,
  c("Country Name", "Country Code", "year", "gdp_pc")
)

# Total GDP (current USD) to long
gdp_tot_long <- wb_to_long(gdp_tot_raw, "gdp_tot")
gdp_tot_long <- gdp_tot_long[year >= 2007 & year <= 2025]
setcolorder(
  gdp_tot_long,
  c("Country Name", "Country Code", "year", "gdp_tot")
)

#### (ii) Sum of oil, gas, coal rents as % of GDP ----
# World Bank API URLs
url_oil_rents     <- "https://api.worldbank.org/v2/en/indicator/NY.GDP.PETR.RT.ZS?downloadformat=csv"
url_nat_gas_rents <- "https://api.worldbank.org/v2/en/indicator/NY.GDP.NGAS.RT.ZS?downloadformat=csv"
url_coal_rents    <- "https://api.worldbank.org/v2/en/indicator/NY.GDP.COAL.RT.ZS?downloadformat=csv"

# Helper: download + unzip + read CSV, then convert to data.table
read_wb_indicator <- function(url, zip_name_pattern, file_pattern) {
  
  # remove any stale data from previous runs
  old_files <- list.files(pattern = file_pattern, full.names = TRUE)
  old_meta  <- list.files(pattern = "Metadata", full.names = TRUE)
  file.remove(c(old_files, old_meta))
  
  zip_file <- paste0(zip_name_pattern, ".zip")
  download.file(url, destfile = zip_file, mode = "wb")
  unzip(zip_file)
  
  files <- list.files(pattern = file_pattern)
  files <- files[!grepl("Metadata", files, ignore.case = TRUE)]
  
  if (length(files) != 1) {
    stop("Expected exactly one data CSV for pattern ", file_pattern,
         " but found: ", paste(files, collapse = ", "))
  }
  
  dt <- as.data.table(read_csv(files[1], skip = 4))
  dt
}

# Load rent indicators (wide)
oil_raw  <- read_wb_indicator(url_oil_rents,     "oil_rents",     "API_NY.GDP.PETR.RT.ZS.*\\.csv$")
gas_raw  <- read_wb_indicator(url_nat_gas_rents, "nat_gas_rents", "API_NY.GDP.NGAS.RT.ZS.*\\.csv$")
coal_raw <- read_wb_indicator(url_coal_rents,    "coal_rents",    "API_NY.GDP.COAL.RT.ZS.*\\.csv$")

# wide to long using the same helper
oil_long  <- wb_to_long(oil_raw,  "oil_rents")
gas_long  <- wb_to_long(gas_raw,  "gas_rents")
coal_long <- wb_to_long(coal_raw, "coal_rents")

# Merge the three rent series
fossil_rents <- Reduce(
  function(x, y) merge(
    x, y,
    by  = c("Country Name", "Country Code", "year"),
    all = TRUE
  ),
  list(oil_long, gas_long, coal_long)
)

setDT(fossil_rents)

# Sum rents with NA rule: if all three are NA, keep NA; otherwise sum available
fossil_rents[
  ,
  fossil_rents := {
    vals <- c(oil_rents, gas_rents, coal_rents)
    if (all(is.na(vals))) NA_real_ else sum(vals, na.rm = TRUE)
  },
  by = .(`Country Name`, `Country Code`, year)
]

fossil_rents <- fossil_rents[year >= 2007 & year <= 2025]

setcolorder(
  fossil_rents,
  c("Country Name", "Country Code", "year",
    "oil_rents", "gas_rents", "coal_rents", "fossil_rents")
)


#### (iii) Control of corruption index (WGI, World Bank) ----
url_cc <- "https://api.worldbank.org/v2/en/indicator/CC.EST?downloadformat=csv"

cc_raw <- read_wb_indicator(
  url              = url_cc,
  zip_name_pattern = "control_corruption",
  file_pattern     = "API_CC.EST.*\\.csv$"
)

control_corruption <- wb_to_long(cc_raw, "control_corruption")
control_corruption <- control_corruption[year >= 2007 & year <= 2025]

setcolorder(
  control_corruption,
  c("Country Name", "Country Code", "year", "control_corruption")
)


#### (iv) Combine WB indicators into wb_indicators ----
wb_indicators <- Reduce(
  function(x, y) merge(
    x, y,
    by  = c("Country Name", "Country Code", "year"),
    all = TRUE
  ),
  list(
    gdp_pc_long,
    gdp_tot_long,
    fossil_rents,
    control_corruption
  )
)

setDT(wb_indicators)

setnames(
  wb_indicators,
  old = c("Country Name", "Country Code"),
  new = c("country", "iso3_country")
)

setcolorder(
  wb_indicators,
  c(
    "country", "iso3_country", "year",
    "gdp_pc", "gdp_tot",
    "oil_rents", "gas_rents", "coal_rents", "fossil_rents",
    "control_corruption"
  )
)


#### (v) Vulnerability Index (Notre Dame-Global) ----
vulnerability <- read_csv("./02_data_input/vulnerability.csv")
setDT(vulnerability)

vul_long <- melt(
  vulnerability,
  id.vars       = c("ISO3", "Name"),
  variable.name = "year",
  value.name    = "vulnerability"
)

# Convert year from character to integer
vul_long[, year := as.integer(as.character(year))]

# Keep only years overlapping with wb_indicators
vul_long <- vul_long[year >= 2007 & year <= 2025]

# Rename ISO3 to match wb_indicators' iso3_country
setnames(vul_long, "ISO3", "iso3_country")

# Drop country name column from this file (we already have from WB)
vul_long[, c("Name") := NULL]

# Left join onto wb_indicators (adds vulnerability column)
wb_indicators <- merge(
  wb_indicators,
  vul_long,
  by = c("iso3_country", "year"),
  all.x = TRUE
)

# Optional: final column order (including vulnerability)
setcolorder(
  wb_indicators,
  c(
    "country", "iso3_country", "year",
    "gdp_pc", "gdp_tot",
    "oil_rents", "gas_rents", "coal_rents", "fossil_rents",
    "control_corruption",
    "vulnerability"
  )
)

#### (vi) EI and EIA total fossil fuel production USD ----
EI_EIA_total_ff_USD <- read_parquet("./02_data_input/EIA_EI_total_ff_prod_USD.parquet")

wb_indicators <- merge(
  wb_indicators,
  EI_EIA_total_ff_USD,
  by = c("iso3_country", "year"),
  all.x = TRUE
)

wb_indicators[ , total_ff_produced_per_gdp := total_ff_USD / gdp_tot]

# get the locations that are in the final sample
final_sample_countries <- read_parquet("./01_data_output/comp_EOM_ret_market_cap.parquet",
                                       col_select = c("gvkey", "iid", "loc","fic"))
setDT(final_sample_countries)
final_locs <- unique(final_sample_countries$loc)
final_fics <- unique(final_sample_countries$fic)

rm(final_sample_countries); gc()

#filter to only include locs
wb_indicators <- wb_indicators[iso3_country %in% final_locs]

wb_indicators <- wb_indicators[,.(year,
                                  iso3_country,
                                  country,
                                  gdp_pc,
                                  gdp_tot,
                                  fossil_rents,
                                  total_ff_produced_per_gdp,
                                  vulnerability,
                                  control_corruption)]


# The variables are z-transformed to make them comparable.
vars_to_z <- c(
  "gdp_pc",
  "fossil_rents",
  "total_ff_produced_per_gdp",
  "vulnerability",
  "control_corruption"
)

for (v in vars_to_z) {
  wb_indicators[
    !is.na(get(v)),
    paste0(v, "_z") := (get(v) - mean(get(v), na.rm = TRUE)) /
      sd(get(v),   na.rm = TRUE)
  ]
}

wb_indicators[, PAI := gdp_pc_z + vulnerability_z + control_corruption_z - total_ff_produced_per_gdp_z]


### Share of climate forcing and climate vulnerable assets ----
# idea to use the vulnerability score as a proxy for CVAs
# for CFA: value of fossil fuel production to GDP

wb_indicators[ , cfa_cva_ratio := total_ff_produced_per_gdp / vulnerability]


pai_cfa_cva <- wb_indicators[,.(year,iso3_country,PAI,cfa_cva_ratio)]
write_parquet(pai_cfa_cva, "./01_data_output/pai_cfa_cva.parquet")
write_parquet(wb_indicators, "./01_data_output/wb_indicators.parquet")

# =============================================================================.
# Combined Dataset ----
# =============================================================================.

# This section creates the final combined "panel" data set with the highest
# frequency being monthly returns and market caps. Each row represents a security
# year-month.

rm(combined)

### 1) Load year-month returns ----
combined <- read_parquet("./01_data_output/comp_EOM_ret_market_cap.parquet")
setDT(combined)

combined[, year := year(datadate)]

### 2) Join company info ----
comp_sec_info_g_NA <- read_parquet("./01_data_output/comp_sec_info_g_NA.parquet")
setDT(comp_sec_info_g_NA)

# drop temp columns
comp_sec_info_g_NA[, c(#"gvkey",
  #"iid" ,
  "tic",
  "isin",
  "cusip",
  "sedol" ,
  "secstat",
  "dsci",
  "tpci" ,
  "exchg" ,
  "excntry",
  "busdesc" ,
  "conm",
  "conml",
  #"loc" ,
  "fic",
  "prican",
  "prirow" ,
  "priusa",
  #"gsubind",
  #"gind",
  #"ggroup" ,
  #"naics" ,
  "exchgdesc",
  "exchgcd" ,
  "isocntrydesc" ,
  "in_trucost" ,
  "source" 
) := NULL]

# join on gvkey & iid
combined <- left_join(combined,comp_sec_info_g_NA, 
                      by = c("gvkey", "iid"),
                      suffix = c("", "_info"))


rm(comp_sec_info_g_NA)


### (2.1) add GIND names from MSCI ----
url <- "https://www.msci.com/documents/1296102/23c8ec04-fd1c-3518-e04c-4aa37027889d"
tmp <- tempfile(fileext = ".xlsx")

download.file(url, destfile = tmp, mode = "wb")

gics <- read_excel(tmp, sheet = "Effective close of Mar 17 2023", range = "E4:F337")
setDT(gics)
setnames(gics, old = c(names(gics)[1], names(gics)[2]), new = c("gind", "gics_industry"))
gics <- gics[!(is.na(gind) & is.na(gics_industry))]
gics[,gind := as.character(gind)]

gics[, gics_industry := gsub("\\s*\\(.*?\\)", "", gics_industry)]

combined <- left_join(combined,gics, 
                      by = c("gind"))



### 3) Join yearly USD fundamentals ----
comp_funda_usd <- read_parquet("./01_data_output/comp_funda_usd.parquet")
setDT(comp_funda_usd)

# drop temp columns
comp_funda_usd[, c("final",
                   "exchg",
                   "consol",
                   "indfmt",
                   "datafmt",
                   "costat",
                   "acctstd",
                   "conm",
                   "loc",
                   "fic", 
                   "in_trucost",
                   "fx_tousd",
                   "orig_curcd",
                   "curcd") := NULL]

# create month variable
comp_funda_usd[, month := as.Date(format(datadate, "%Y-%m-01"))]

# join on month
combined <- left_join(combined,comp_funda_usd, 
                      by = c("gvkey", "month"),
                      suffix = c("", "_funda"))

rm(comp_funda_usd)

setDT(combined)
# drop temp columns from fundamentals
combined[, c(
  "capx",
  "ceq",
  "seq",
  "teq",
  "pstk",
  "epsexcon",
  "revt",
  #"sale",
  "cogs",
  "nicon",
  "ebit",
  "ebitda",
  #"at",
  "lt",
  "dlc",
  "dltt",
  "oiadp",
  "ppent",
  "txditc",
  "xrd"
) := NULL]

setDT(combined)

# na_summary <- melt(
#   combined[, lapply(.SD, function(x) mean(is.na(x)) * 100)],
#   variable.name = "column", value.name = "pct_NA"
# )[order(-pct_NA)]
# 
# print(na_summary)

#### (i) B/M ----
# --- forward fill the last available Book Equity
# Ensure proper ordering
setorder(combined, gvkey, iid, datadate)

## Forward fill within each gvkey + iid but account for year gaps
limited_ffill_months(
  DT = combined,
  value_cols = c("book_equity"),
  date_col   = month,
  by_cols    = c("gvkey", "iid"),
  max_gap_months = 11L
)

# need to adjust mktcap to millions
combined[
  !is.na(ME_dec) & !is.na(book_equity) & ME_dec != 0,
  bm := (book_equity)/(ME_dec/(10^6))
]

combined[
  !is.na(ME_dec) & !is.na(book_equity) & ME_dec > 0 & book_equity > 0,
  log_bm := log((book_equity)/(ME_dec/(10^6)))
]


#### (ii) forward fill all fundamental variables ----
limited_ffill_months(
  DT = combined,
  value_cols = c("leverage", 
                 "ROE", 
                 "inv_over_at",
                 "inv_perc",
                 "sales_growth",
                 "log_ppe",
                 "ROS",
                 "ROA",
                 "ebit_margin",
                 "ebitda_margin",
                 "eps_growth",
                 "log_sales",
                 "gross_profit",
                 "tang",
                 "rd_intensity",
                 "bm",
                 "log_bm",
                 "at",
                 "sale"),
  date_col   = month,
  by_cols    = c("gvkey", "iid"),
  max_gap_months = 11L
)

### 4) Join market beta ----
market_betas <- read_parquet("./01_data_output/market_betas.parquet")
setDT(market_betas)

# drop temp columns
market_betas[, c(
  #"gvkey",
  #"iid",
  "conm",
  "datadate",
  #"month",
  "fic",
  "loc",
  "n_stocks_full_comp_fic",
  #"n_stocks_full_comp_loc",
  "beta_24m_lag1_ACWI",
  #"beta_24m_lag1_loc",
  #"beta_24m_lag1_fic",
  "beta_12m_lag1_ACWI"
  #"beta_12m_lag1_loc",
  #"beta_12m_lag1_fic"
) := NULL]

# join on month
combined <- left_join(combined,market_betas, 
                      by = c("gvkey", "iid","month")
)
rm(market_betas)

### 5) Join trucost emissions data ----
trucost_filtered <- read_parquet("./01_data_output/trucost_filtered.parquet")
setDT(trucost_filtered)

# create month variable
trucost_filtered[, month := as.Date(format(periodenddate, "%Y-%m-01"))]

# drop temp columns
trucost_filtered[, c(#"gvkey",
  "institutionid",
  "companyname",
  "companytype",
  "country",
  "incorporation_country",
  "simpleindustry",
  "status",
  #"periodenddate",
  #"fiscalyear",
  "periodid",
  "reportedcurrencyisocode",
  #"tcprimarysectorid",
  #"trucost_total_revenue_usd",
  #"revenue_effective_date",
  "env_cost_effective_date",
  #"ghg_scope1_final",
  #"ghg_scope1_final_class",
  "ghg_scope1_final_source",
  #"ghg_scope2_loc_final",
  #"ghg_scope2_loc_final_class",
  "ghg_scope2_loc_final_source",
  #"ghg_scope3_up_final",
  #"ghg_scope3_up_final_class",
  "ghg_scope3_up_final_source"
  #"ghg_scope1_final_intensity",
  #"ghg_scope2_loc_final_intensity",
  #"ghg_scope3_up_final_intensity",
  #"ghg_scope1_yoy",
  #"ghg_scope2_loc_yoy",
  #"ghg_scope3_up_yoy",
  #"ghg_scope1_yoy_class",
  #"ghg_scope2_loc_yoy_class",
  #"ghg_scope3_up_yoy_class",
  #"ghg_scope1_intensity_yoy",
  #"ghg_scope2_loc_intensity_yoy",
  #"ghg_scope3_up_intensity_yoy" 
) := NULL]


# join on month
combined <- left_join(combined,trucost_filtered, 
                      by = c("gvkey", "month"),
                      suffix = c("", "_tc"))

rm(trucost_filtered)

#### (i) forward fill all trucost data ----
setDT(combined)

# for character cols
limited_ffill_months_character_robust(
  DT = combined,
  value_cols = c(
    "periodenddate",
    "tcprimarysectorid",
    "ghg_scope1_final_class",
    "ghg_scope2_loc_final_class",
    "ghg_scope3_up_final_class",
    "ghg_scope1_yoy_class",
    "ghg_scope2_loc_yoy_class",
    "ghg_scope3_up_yoy_class"
  ),
  date_col   = month,
  by_cols    = c("gvkey", "iid"),
  max_gap_months = 11L
)

# for non character cols
setDT(combined)

limited_ffill_months(
  DT = combined,
  value_cols = c(
    #"periodenddate",
    "fiscalyear",
    "trucost_total_revenue_usd",
    "ghg_scope1_final",
    "ghg_scope2_loc_final",
    "ghg_scope3_up_final",
    "ghg_scope1_final_intensity",
    "ghg_scope2_loc_final_intensity",
    "ghg_scope3_up_final_intensity",
    "ghg_scope1_yoy",
    "ghg_scope2_loc_yoy",
    "ghg_scope3_up_yoy",
    "ghg_scope1_intensity_yoy",
    "ghg_scope2_loc_intensity_yoy",
    "ghg_scope3_up_intensity_yoy"
  ),
  date_col   = month,
  by_cols    = c("gvkey", "iid"),
  max_gap_months = 11L
)

### 6) Join climate attention and concern indexes ----
#### (i) UMC ----
mccc_UMC <- read_parquet("./02_data_input/mccc_UMC_AR1_ARX.parquet")
setDT(mccc_UMC)

# drop temp columns
mccc_UMC[, c(
  "Date",
  "Aggregate"
) := NULL]

# join on month
combined <- left_join(combined,mccc_UMC, 
                      by = c("month"),
                      suffix = c("", "_UMC"))

rm(mccc_UMC)


#### (ii) TRI / PRI ----
tri_pri_monthly <- read_parquet("./01_data_output/tri_pri_monthly.parquet")
setDT(tri_pri_monthly)

# drop temp columns
tri_pri_monthly[, c(
  "Transition_concern",
  "Physical_concern"
) := NULL]

# join on month
combined <- left_join(combined,tri_pri_monthly, 
                      by = c("month"))

rm(tri_pri_monthly)


#### (iii) CAI ----
##### (a) country level ----
cai_country_AR <- read_parquet("./01_data_output/cai_country_AR.parquet")
setDT(cai_country_AR)

# drop temp columns
cai_country_AR[, c(
  "iso2_country",
  "date",
  "year",
  "value"
) := NULL]

setnames(cai_country_AR, old = "iso3_country", new = "country")
setnames(cai_country_AR, old = "UCAI", new = "UCAI_country")

# join on month & country = loc
combined <- left_join(combined,cai_country_AR, 
                      by = c("month" = "month", "loc" = "country"))

rm(cai_country_AR)
##### (b) global aggregate ----
cai_global_AR <- read_parquet("./01_data_output/cai_global_AR.parquet")
setDT(cai_global_AR)

cai_global_AR[, c(
  "date",
  "year"
) := NULL]

# join on month
combined <- left_join(combined,cai_global_AR, 
                      by = c("month"))

rm(cai_global_AR)

### 7) add country-level policy measures ----
#### Germanwatch CCPI ----
germanwatch <- read_parquet("./02_data_input/germanwatch_output.parquet")
setDT(germanwatch)

germanwatch[, c(
  "country"
) := NULL]

setDT(combined)
# join on month
combined <- left_join(combined,germanwatch, 
                      by = c("year" = "year","loc" = "iso3_country"))

rm(germanwatch)

#### PAI & CFA/CVA ----
pai_cfa_cva <- read_parquet("./01_data_output/pai_cfa_cva.parquet")
setDT(pai_cfa_cva)

setDT(combined)
# join on month
combined <- left_join(combined,pai_cfa_cva, 
                      by = c("year" = "year","loc" = "iso3_country"))

rm(pai_cfa_cva)


### 8) dropping non-needed cols ----
setDT(combined)

combined[, c(
  "source",
  "dlrsn",
  "exchg",
  "curcdd",
  "mktcap_usd",
  "ME_dec",
  "loc_info",
  "gsubind",
  "ggroup",
  "source_funda",
  "pdate",
  "fdate",
  "book_equity",
  "beta_24m_lag1_fic",
  "beta_12m_lag1_fic"
) := NULL]

write_parquet(combined, "./01_data_output/final_combined.parquet")


### 9) lagging data ----
# following Hambel & van der Sanden (2025) lag emissions data at least as much 
# as accounting variables
combined <- read_parquet("./01_data_output/final_combined.parquet")

# functions
lag_months_fast <- function(DT, cols, n, max_gap_months = 1L) {
  n              <- as.integer(n)
  max_gap_months <- as.integer(max_gap_months)
  
  setDT(DT)
  setkey(DT, gvkey, iid, month)   # assumes month is a Date
  
  # max allowed difference between desired lag month and actual data (in days)
  roll_days <- 31L * max_gap_months
  
  # Names of new lagged columns
  new_names <- paste0(cols, "_lag", n, "m")
  
  # Do ONE rolling join that returns all requested lag columns at once
  lagged_vals <- DT[
    .(gvkey, iid, month %m-% months(n)),       # desired lag month per row
    on   = .(gvkey, iid, month),
    roll = roll_days,
    .SD,                                      # return only the columns in `cols`
    .SDcols = cols
  ]
  
  # Assign all new columns in one go
  DT[, (new_names) := lagged_vals]
  
  invisible(DT)
}

lag_months <- function(DT, cols, n, max_gap_months = 1L) {
  n              <- as.integer(n)
  max_gap_months <- as.integer(max_gap_months)
  
  setDT(DT)
  setkey(DT, gvkey, iid, month)   # assumes month is a Date
  
  # max allowed difference between desired lag month and actual data (in days)
  roll_days <- 31L * max_gap_months   # conservative upper bound
  
  for (col in cols) {
    new_name <- paste0(col, "_lag", n, "m")
    
    DT[, (new_name) :=
         DT[
           .(gvkey, iid, month %m-% months(n)),    # desired lag month
           on   = .(gvkey, iid, month),
           roll = roll_days,                       # limit how far we can roll back
           get(col)
         ]
    ]
  }
  
  invisible(DT)
}

# standard lag of 6 months on accounting variables
setDT(combined)

lag_months_fast(
  combined,
  cols = c(
    "leverage",
    "ROE", 
    "sales_growth",
    "inv_over_at",
    "log_ppe",
    "ROS",
    "ROA",
    "ebit_margin",
    "ebitda_margin",
    "eps_growth",
    "log_sales",
    "at"
  ),
  n    = 6,
  max_gap_months = 2
)

# standard lag of 6 months on emissions variables
lag_months_fast(
  combined,
  cols = c(
    "ghg_scope1_final",
    "ghg_scope1_final_class",
    "ghg_scope2_loc_final",
    "ghg_scope2_loc_final_class",
    "ghg_scope3_up_final",
    "ghg_scope3_up_final_class",
    "ghg_scope1_final_intensity",
    "ghg_scope2_loc_final_intensity",
    "ghg_scope3_up_final_intensity",
    "ghg_scope1_yoy",
    "ghg_scope2_loc_yoy",
    "ghg_scope3_up_yoy",
    "ghg_scope1_yoy_class",
    "ghg_scope2_loc_yoy_class",
    "ghg_scope3_up_yoy_class",
    "ghg_scope1_intensity_yoy",
    "ghg_scope2_loc_intensity_yoy",
    "ghg_scope3_up_intensity_yoy"
  ),
  n    = 6,
  max_gap_months = 2
)

# lag UMC_AR1 and UMC_ARX by one month
lag_months_fast(
  combined,
  cols = c(
    "UMC_AR1",
    "UMC_ARX"
  ),
  n    = 1,
  max_gap_months = 2
)


### 10) time-frame filter 2010 to 2025
# only keep observations newer than January 01 2010
combined <- combined[datadate >= "2009-01-01"]

# drop all observations where no returns are available
combined <- combined[!is.na(ret_m)]

### X) save to parquet ----
write_parquet(combined, "./01_data_output/final_combined.parquet")


# =============================================================================.
# Appendix ----
# =============================================================================.
