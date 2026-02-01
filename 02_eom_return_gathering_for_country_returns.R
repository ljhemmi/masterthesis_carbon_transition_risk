# =============================================================================.
# Libraries ----
# =============================================================================.

# reset environment
rm(list = ls());gc()

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
  "readr"
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
  pct <- if (before > 0) round((after / before) * 100, 2) else NA_real_
  message(glue("{tag}: kept {after} of {before} rows ({pct}%)."))
}




## VI: Compustat Global & NA Daily prices (no Trucost filter, no primary_iid prefilter) ----

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

# --- simplified fetch (no Trucost, no primary_iid join) ---
fetch_combined <- function(d1, d2, fx_dt = comp_fx_global) {
  d1s <- format(d1, "%Y-%m-%d"); d2s <- format(d2, "%Y-%m-%d")
  
  sql <- sprintf("
  SELECT * FROM (
    -- Global
    SELECT
      'comp_global'::text AS source,
      s.fic,
      gc.loc AS loc,                     -- <- from company table
      s.gvkey, s.datadate, s.conm, s.exchg, s.secstat,
      gc.dlrsn, s.ajexdi, s.cshoc, s.curcdd, s.prccd, s.prcstd,
      s.qunit, s.tpci, s.iid, s.trfd
    FROM comp_global_daily.g_secd s
    LEFT JOIN comp_global_daily.g_company gc ON gc.gvkey = s.gvkey
    WHERE s.datadate BETWEEN '%s' AND '%s'
      AND s.tpci = '0'
      AND s.prcstd IN (3,10)

    UNION ALL

    -- North America (qunit = 1)
    SELECT
      'comp_na'::text AS source,
      s.fic,
      c.loc AS loc,                      -- <- from comp.company
      s.gvkey, s.datadate, s.conm, s.exchg, s.secstat,
      c.dlrsn, s.ajexdi, s.cshoc, s.curcdd, s.prccd, s.prcstd,
      1.0::numeric AS qunit, s.tpci, s.iid, s.trfd
    FROM comp_na_daily_all.secd s
    LEFT JOIN comp.company c ON c.gvkey = s.gvkey
    WHERE s.datadate BETWEEN '%s' AND '%s'
      AND s.tpci = '0'
      AND s.prcstd IN (3,10)
  ) u
  ORDER BY gvkey, iid, datadate
", d1s, d2s, d1s, d2s)
  
  dt <- as.data.table(DBI::dbGetQuery(wrds, sql))
  if (!nrow(dt)) return(NULL)
  
  # types / keys
  dt[, `:=`(datadate = as.IDate(datadate),
            gvkey = as.character(gvkey),
            iid   = as.character(iid))]
  setorder(dt, gvkey, iid, datadate)
  
  # FX merge
  fx_sub <- fx_dt[datadate >= d1 & datadate <= d2]
  dt[fx_sub, on = .(curcdd = curd, datadate), exratd_tousd := i.exratd_tousd]
  
  # numeric
  for (col in c("prccd","qunit","ajexdi","cshoc","trfd"))
    set(dt, j = col, value = as.numeric(dt[[col]]))
  
  # adjusted USD price and market cap
  dt[is.na(exratd_tousd) | is.na(qunit) | qunit == 0 | is.na(ajexdi) | ajexdi == 0,
     c("prcd_adj_usd","mktcap_usd") := .(NA_real_, NA_real_)]
  dt[is.na(prcd_adj_usd),
     prcd_adj_usd := (prccd * exratd_tousd) / (qunit * ajexdi)]
  dt[is.na(mktcap_usd),
     mktcap_usd   := prcd_adj_usd * (cshoc * ajexdi)]
  
  # end-of-month rows
  dt[, month := as.IDate(lubridate::floor_date(datadate, "month"))]
  EOM <- dt[, .SD[.N], by = .(gvkey, iid, month)]
  setorder(EOM, gvkey, iid, month)
  EOM[, price_trfd_eom := prcd_adj_usd * trfd]
  
  # --- select primary security = largest market cap per gvkey-month ---
  # ties broken by first in order; adjust if you want deterministic tie-breakers
  EOM_primary <- EOM[!is.na(mktcap_usd) & mktcap_usd > 0]
  setorder(EOM_primary, gvkey, month, -mktcap_usd, iid)
  EOM_primary <- EOM_primary[, .SD[1L], by = .(gvkey, month)]
  
  EOM_primary[, .(gvkey, iid, month, datadate, source,
                  fic, loc, conm, exchg, secstat, dlrsn, curcdd, prcstd,
                  prccd, ajexdi, qunit, cshoc, trfd, exratd_tousd,
                  prcd_adj_usd, mktcap_usd, price_trfd_eom)]
}

# chunk loop: write EOM (primary by largest mktcap) only
if (isTRUE(comp_daily_toggle)) {
  message("Loading Compustat Daily Global & North America (no Trucost, primary by largest mktcap)")
  for (i in seq_len(length(brks) - 1L)) {
    d1 <- brks[i]
    d2 <- min(brks[i + 1L] - 1, end_all)
    
    message(sprintf("Chunk %d: %s -> %s", i, d1, d2))
    eom_chunk <- fetch_combined(d1, d2)
    if (is.null(eom_chunk) || !nrow(eom_chunk)) next
    
    out_file <- sprintf("./01_data_output/02_chunk_prices_test_global/comp_g_daily_EOM_%s_%s.parquet",
                        format(d1, "%Y-%m-%d"), format(d2, "%Y-%m-%d"))
    write_parquet(eom_chunk, out_file)
    rm(eom_chunk); gc()
  }
  message("Done. Chunks saved as EOM rows with adjusted USD price and market cap (primary by largest mktcap).")
} else {
  message("Compustat Daily Toggle is set to FALSE")
}

### 2) merge and calculate monthly returns ----
# discover EOM chunk files
chunk_files <- list.files(
  "./01_data_output/02_chunk_prices_test_global/",
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
setorder(eom_all, gvkey, month, datadate)

# calculate the end-of-month total returns
eom_all[
  order(gvkey, month),
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
  by = .(gvkey)
]


# Value-weighted country returns from EOM panel ----
# 0) Ensure uniqueness and chronological order
#stopifnot(eom_all[, uniqueN(.SD), by = .(gvkey, iid, month)][, all(V1 == 1L)])
setorderv(eom_all, c("gvkey","iid","month"))

# 1) Lag market cap (t-1) per security to avoid look-ahead in weights
eom_all[, mktcap_lag := shift(mktcap_usd, 1L), by = .(gvkey, iid)]

# 2) Country (loc) value-weighted return per month
country_vwret_loc <- eom_all[
  !is.na(ret_m) & !is.na(mktcap_lag) & mktcap_lag > 0,
  .(Rm_country_loc = weighted.mean(ret_m, w = mktcap_lag, na.rm = TRUE),
    n_stocks_full_comp_loc   = .N),
  by = .(loc, month)
]

country_vwret_fic <- eom_all[
  !is.na(ret_m) & !is.na(mktcap_lag) & mktcap_lag > 0,
  .(Rm_country_fic = weighted.mean(ret_m, w = mktcap_lag, na.rm = TRUE),
    n_stocks_full_comp_fic   = .N),
  by = .(fic, month)
]


write_parquet(country_vwret_loc, "./01_data_output/full_country_vwret_loc.parquet")
write_parquet(country_vwret_fic, "./01_data_output/full_country_vwret_fic.parquet")
