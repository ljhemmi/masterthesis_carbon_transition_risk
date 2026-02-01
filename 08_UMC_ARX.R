
# 08: UMC ARX version

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
  "zoo"
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
# UMC Calculation ----
# =============================================================================.

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
  output_col = "UMC_AR1"
)

#### 3) ARX Version ----
##### Data load ----

###### US EPU ----
EPU <- read_xlsx("../01_data/12_ARX_UMC_datasources/US_Policy_Uncertainty_Data.xlsx")
setDT(EPU)
setorder(EPU, Year, Month)
EPU$date <- as.Date(paste0("01","/",EPU$Month,"/",EPU$Year),format = "%d/%m/%Y")
EPU[, month := as.Date(floor_date(date, unit = "month"))]

EPU_lagged = EPU[,.(News_Based_Policy_Uncert_Index,date,month)]
EPU_lagged[, News_Based_Policy_Uncert_Index_lag := shift(News_Based_Policy_Uncert_Index, n = 1)]

EPU_lagged[, News_Based_Policy_Uncert_Index_lag := News_Based_Policy_Uncert_Index_lag/100]

EPU_lagged <- EPU_lagged[date >= "2000-01-01"]

###### VIX ----
VIX <- read.csv("../01_data/12_ARX_UMC_datasources/VIXCLS.csv")
setDT(VIX)
VIX[, observation_date := as.Date(observation_date)]
VIX[, VIXCLS := as.numeric(VIXCLS)]
VIX[, VIXCLS := na.locf(VIXCLS, na.rm = FALSE)]

VIX[, month := as.Date(format(observation_date, "%Y-%m-01"))]
VIX_monthly <- VIX[ , .SD[.N], by = month]

VIX_monthly[, VIXCLS := na.locf(VIXCLS, na.rm = FALSE)]

VIX_monthly[, VIXCLS_lag := shift(VIXCLS, 1)]

VIX_monthly <- VIX_monthly[month >= "2000-01-01"]

###### WTI crude oil price ----
WTI <- read.csv("../01_data/12_ARX_UMC_datasources/MCOILWTICO.csv")
setDT(WTI)
WTI[, observation_date := as.Date(observation_date)]
WTI[, MCOILWTICO := as.numeric(MCOILWTICO)]
WTI[, MCOILWTICO := na.locf(MCOILWTICO, na.rm = FALSE)]

WTI[, month := as.Date(format(observation_date, "%Y-%m-01"))]

setorder(WTI, month)
WTI[, MCOILWTICO_return := MCOILWTICO / shift(MCOILWTICO, 1) - 1]
WTI[, MCOILWTICO_return_lag := shift(MCOILWTICO_return, 1)]

WTI <- WTI[month >= "2000-01-01"]

###### Mont Belvieu Texas price ----
PROPANE <- read.csv("../01_data/12_ARX_UMC_datasources/MPROPANEMBTX.csv")
setDT(PROPANE)

PROPANE[, observation_date := as.Date(observation_date)]
PROPANE[, MPROPANEMBTX := as.numeric(MPROPANEMBTX)]
PROPANE[, MPROPANEMBTX := na.locf(MPROPANEMBTX, na.rm = FALSE)]

PROPANE[, month := as.Date(format(observation_date, "%Y-%m-01"))]

setorder(PROPANE, month)
PROPANE[, MPROPANEMBTX_return := MPROPANEMBTX / shift(MPROPANEMBTX, 1) - 1]
PROPANE[, MPROPANEMBTX_return_lag := shift(MPROPANEMBTX_return, 1)]

PROPANE <- PROPANE[month >= "2000-01-01"]

###### Henry hub natural gas spot price ----
NG <- read.csv("../01_data/12_ARX_UMC_datasources/MHHNGSP.csv")
setDT(NG)

NG[, observation_date := as.Date(observation_date)]
NG[, MHHNGSP := as.numeric(MHHNGSP)]
NG[, MHHNGSP := na.locf(MHHNGSP, na.rm = FALSE)]

NG[, month := as.Date(format(observation_date, "%Y-%m-01"))]

setorder(NG, month)
NG[, MHHNGSP_return := MHHNGSP / shift(MHHNGSP, 1) - 1]
NG[, MHHNGSP_return_lag := shift(MHHNGSP_return, 1)]

NG <- NG[month >= "2000-01-01"]

###### Term and default factor ----
BAA <- read.csv("../01_data/12_ARX_UMC_datasources/BAA.csv")
setDT(BAA)
BAA[, BAA := na.locf(BAA, na.rm = FALSE)]
BAA[, observation_date := as.Date(observation_date)]
BAA[, month := as.Date(floor_date(observation_date, unit = "month"))]
BAA_monthly <- BAA[, .SD[.N], by = month][, .(month, BAA)]

GS10 <- read.csv("../01_data/12_ARX_UMC_datasources/GS10.csv")
setDT(GS10)
GS10[, GS10 := na.locf(GS10, na.rm = FALSE)]
GS10[, observation_date := as.Date(observation_date)]
GS10[, month := as.Date(floor_date(observation_date, unit = "month"))]
GS10_monthly <- GS10[, .SD[.N], by = month][, .(month, GS10)]

TERM_DFLT <- merge(GS10_monthly, BAA_monthly, by = "month", all = TRUE)
setorder(TERM_DFLT, month)

# Fill missing
TERM_DFLT[, GS10 := na.locf(GS10, na.rm = FALSE)]
TERM_DFLT[, BAA  := na.locf(BAA,  na.rm = FALSE)]

# TERM = monthly change in GS10
TERM_DFLT[, TERM := GS10 - shift(GS10, 1)]

# DFLT = monthly change in (BAA - GS10)
TERM_DFLT[, spread := BAA - GS10]
TERM_DFLT[, DFLT := spread - shift(spread, 1)]

# Lags needed for ARX
TERM_DFLT[, TERM_lag := shift(TERM, 1)]
TERM_DFLT[, DFLT_lag := shift(DFLT, 1)]

# Clean result
TERM_DFLT_final <- TERM_DFLT[, .(month, GS10, BAA, TERM, TERM_lag, DFLT, DFLT_lag)]
TERM_DFLT_final <- TERM_DFLT_final[month >= "2000-01-01"]

###### S&P Green Bond Index ----
sp_green_bond_index <- read_xlsx("../01_data/12_ARX_UMC_datasources/spgruss_green_bond_index.xlsx")
setDT(sp_green_bond_index)

setnames(sp_green_bond_index,
         old = c("Date", ".SPGRUSS (TRDPRC_1)"),
         new = c("date_raw", "price_raw"))

sp_green_bond_index <- sp_green_bond_index[!(date_raw == "NA" | price_raw == "Close")]
sp_green_bond_index[, date := as.Date(substr(date_raw, 1, 10))]

sp_green_bond_index[, price := as.numeric(gsub("[^0-9.]", "", price_raw))]
setorder(sp_green_bond_index, date)

sp_green_bond_index[, logret := log(price) - log(shift(price, 1))]

# lagged log return: logret_{t-1}
sp_green_bond_index[, logret_lag := shift(logret, 1)]


###### Fama French Factors ----
ff_raw <- read_csv(
  "../01_data/12_ARX_UMC_datasources/F-F_Research_Data_5_Factors_2x3.csv",
  skip = 3,
  col_names = TRUE,
  show_col_types = FALSE
)
ff_raw <- ff_raw[!grepl("Annual", ff_raw[[1]]), ]
setDT(ff_raw)
setnames(ff_raw, old = names(ff_raw)[1], new = "date")
ff_raw[, date := as.Date(paste0(substr(date,1,4),"-",substr(date,5,6),"-01"))]
num_cols <- c("Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF")
ff_raw[, (num_cols) := lapply(.SD, function(x) as.numeric(x)/100), .SDcols = num_cols]
ff_5_factors <- ff_raw[, .(date, `Mkt-RF`, SMB, HML, RMW, CMA, RF)]
ff_5_factors <- ff_5_factors[!is.na(date)]

raw_lines <- readLines("../01_data/12_ARX_UMC_datasources/F-F_Momentum_Factor.csv")
header_line <- grep(",Mom$", raw_lines)[1]
data_lines <- raw_lines[(header_line+1):length(raw_lines)]
data_lines <- data_lines[grepl("^[0-9]{6},", data_lines)]
ff_mom <- fread(text = data_lines, header = FALSE)
setnames(ff_mom, c("date_ym", "UMD_raw"))
ff_mom[, date := as.Date(
  sprintf("%s-%s-01", substr(date_ym, 1, 4), substr(date_ym, 5, 6))
)]
ff_mom[, UMD := as.numeric(UMD_raw)]
ff_mom[UMD <= -99, UMD := NA_real_]
ff_mom[, UMD := UMD / 100]
ff_mom <- ff_mom[, .(date, UMD)]

setkey(ff_5_factors, date)
setkey(ff_mom, date)

ff_all <- merge(ff_5_factors, ff_mom, by = "date", all = TRUE)

factor_cols <- setdiff(names(ff_all), "date")   # all non-date cols
ff_all[, paste0(factor_cols, "_lag") :=
         lapply(.SD, shift, n = 1),
       .SDcols = factor_cols]

ff_all <- ff_all[date >= as.Date("2000-01-01")]

##### Into covariate matrix X ----

## 1) Prepare individual (month, variable) tables
## Base time index from your UMC sample
start_month <- as.Date("2000-01-01")

# Upper bound: e.g. max month in mccc_UMC (or in your covariates)
end_month <- max(mccc_UMC$month, na.rm = TRUE)

# Pure monthly calendar index
x_base <- data.table(
  month = seq(from = start_month, to = end_month, by = "month")
)

## EPU (already lagged & /100)
EPU_x <- EPU_lagged[
  , .(month,
      EPU = News_Based_Policy_Uncert_Index_lag)
][month >= as.Date("2000-01-01")]

## VIX (monthly, lagged level)
VIX_x <- VIX_monthly[
  , .(month,
      VIX = VIXCLS_lag)
][month >= as.Date("2000-01-01")]

## WTI (monthly return, lagged)
WTI_x <- WTI[
  , .(month,
      WTI = MCOILWTICO_return_lag)
][month >= as.Date("2000-01-01")]

## PROPANE (monthly return, lagged)
PROPANE_x <- PROPANE[
  , .(month,
      PROPANE = MPROPANEMBTX_return_lag)
][month >= as.Date("2000-01-01")]

## NG (monthly return, lagged)
NG_x <- NG[
  , .(month,
      NG = MHHNGSP_return_lag)
][month >= as.Date("2000-01-01")]

## TERM & DFLT (lagged changes)
TERM_DFLT_x <- TERM_DFLT_final[
  , .(month,
      TERM = TERM_lag,
      DFLT = DFLT_lag)
][month >= as.Date("2000-01-01")]

## Green bond (lagged log return)
sp_green_bond_index[, month := as.Date(floor_date(date, "month"))]
GreenBond_x <- sp_green_bond_index[
  , .(month,
      GreenBond = logret_lag)
][month >= as.Date("2000-01-01")]

## Fama–French factors + momentum (use lagged versions from ff_all)
## ff_all currently has: date, Mkt-RF, SMB, HML, RMW, CMA, RF, UMD, and *_lag
ff_factors_x <- ff_all[
  date >= as.Date("2000-01-01"),
  .(
    month   = date,
    Mkt.RF  = `Mkt-RF_lag`,
    SMB     = SMB_lag,
    HML     = HML_lag,
    RMW     = RMW_lag,
    CMA     = CMA_lag,
    UMD     = UMD_lag
    # RF_lag exists too if you want it; often not used in X
  )
]


## 2) Merge everything into a single X data.table
X_dt <- merge(x_base, EPU_x,       by = "month", all.x = TRUE)
X_dt <- merge(X_dt,   ff_factors_x,by = "month", all.x = TRUE)
X_dt <- merge(X_dt,   NG_x,        by = "month", all.x = TRUE)
X_dt <- merge(X_dt,   PROPANE_x,   by = "month", all.x = TRUE)
X_dt <- merge(X_dt,   WTI_x,       by = "month", all.x = TRUE)
X_dt <- merge(X_dt,   VIX_x,       by = "month", all.x = TRUE)
X_dt <- merge(X_dt,   TERM_DFLT_x, by = "month", all.x = TRUE)
X_dt <- merge(X_dt,   GreenBond_x, by = "month", all.x = TRUE)

## Optional: ensure final restriction to >= 2000-01-01 (already enforced above)
X_dt <- X_dt[month >= as.Date("2000-01-01")]


## 3) Turn into X matrix for ARX
# Order by month
setorder(X_dt, month)


##### 4) ARX-based UMC (UMC_ARX) with flexible covariate inclusion ----

# 4.1 Align MCCC (y) with X by month
# Use the MCCC series with the AR(1) UMC already computed; Aggregate is the level
mccc_arx <- mccc_UMC[month >= as.Date("2000-01-01")]
setorder(mccc_arx, month)

# Match X rows to MCCC months
X_dt_aligned <- X_dt[match(mccc_arx$month, month)]

# Carry forward missing values column-wise (like original code)
reg_cols <- setdiff(names(X_dt_aligned), "month")
X_dt_aligned[, (reg_cols) := lapply(.SD, zoo::na.locf, na.rm = FALSE), .SDcols = reg_cols]

# Build numeric X matrix
X_full <- as.matrix(X_dt_aligned[, ..reg_cols])
rownames(X_full) <- as.character(X_dt_aligned$month)

# Dependent variable: MCCC level (Aggregate)
y_vec <- as.numeric(mccc_arx$Aggregate)

# 4.2 ARX helper: AR(1) + subset of xreg columns, dropping any with NA in window
f_arx_fit <- function(y, xreg) {
  n <- length(y)
  if (n <= 2L) return(NA_real_)  # not enough data
  
  # y_t for t = 2, ..., n-1
  y_ <- y[2:(n - 1L)]
  
  # AR(1) term (y_{t-1}) and contemporaneous regressors x_t
  X_ <- cbind(AR = y[1:(n - 2L)], xreg[2:(n - 1L), , drop = FALSE])
  
  # If y_ has NA, or all regressors dropped, just return NA
  if (anyNA(y_) || ncol(X_) == 0L || anyNA(X_)) {
    return(NA_real_)
  }
  
  fit <- lm(y_ ~ 1 + ., data = as.data.frame(X_))
  
  # One-step-ahead prediction for time n
  X_pred <- cbind(AR = y[n - 1L], xreg[n, , drop = FALSE])
  y_pred <- as.numeric(predict(fit, newdata = as.data.frame(X_pred)))
  
  # Prediction error at time n
  y[n] - y_pred
}

# 4.3 Rolling ARX: drop any regressors that have NA in the estimation window
window_arx <- 36L          # estimation window length (months)
n_obs      <- length(y_vec)
UMC_ARX    <- rep(NA_real_, n_obs)

if (n_obs > window_arx) {
  for (t in window_arx:n_obs) {
    # Window indices: (t - window_arx + 1) ... t
    idx_window <- (t - window_arx + 1L):t
    
    y_win  <- y_vec[idx_window]
    X_win0 <- X_full[idx_window, , drop = FALSE]
    
    # Drop any regressor with at least one NA in this window
    if (ncol(X_win0) > 0L) {
      keep_cols <- apply(X_win0, 2L, function(z) !any(is.na(z)))
    } else {
      keep_cols <- logical(0L)
    }
    
    if (!any(keep_cols)) {
      # No valid exogenous regressors → pure AR(1)
      X_win <- matrix(, nrow = length(idx_window), ncol = 0L)
    } else {
      X_win <- X_win0[, keep_cols, drop = FALSE]
    }
    
    UMC_ARX[t] <- f_arx_fit(y = y_win, xreg = X_win)
  }
}

# 4.4 Attach ARX-based UMC to mccc_UMC and keep AR(1)-based UMC as well
mccc_arx[, UMC_ARX := UMC_ARX]

mccc_UMC <- merge(
  mccc_UMC,
  mccc_arx[, .(month, UMC_ARX)],
  by = "month",
  all.x = TRUE,
  sort = TRUE
)

write_parquet(mccc_UMC, "../01_data/00_R_outputs/mccc_UMC_AR1_ARX.parquet")
