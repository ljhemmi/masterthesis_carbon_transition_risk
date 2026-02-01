
# 07: regressions

# Project: Master Thesis
# Author: Lucas Jan Hemmi

# reset environment
rm(list = ls());gc()

# =============================================================================.
# Libraries ----
# =============================================================================.

# this section downloads and loads all required R packages as defined in the
# "packages" vector

# define required packages
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
  "ggplot2",
  "tidyr",
  "corrplot",
  "ggrepel",
  "fixest",
  "patchwork",
  "scales",
  "ggdist",
  "rcartocolor",
  "officer",
  "flextable",
  "devEMF",
  "modelsummary",
  "broom",
  "ggh4x"
)

# install missing packages
installed <- packages %in% installed.packages()[,"Package"]
if (any(!installed)) {
  install.packages(packages[!installed])
}

# load all the packages
lapply(packages, library, character.only = TRUE)

# =============================================================================.
# Functions ----
# =============================================================================.

## 1) General functions ----
winsorize_pct <- function(x, pct = 1) {
  # pct = 1 means 1% in each tail
  stopifnot(pct >= 0, pct <= 50)
  
  cutoff <- pct / 100 
  qs <- quantile(x, probs = c(cutoff, 1 - cutoff), na.rm = TRUE)
  
  x[x < qs[1]] <- qs[1]
  x[x > qs[2]] <- qs[2]
  x
}

sum_or_na <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  sum(x, na.rm = TRUE)
}

fmt <- function(x, digits = 3) {
  format(round(x, digits), nsmall = digits, scientific = FALSE, trim = TRUE)
}

# function that creates a header row
make_header_row <- function(group_name, cols) {
  hdr <- as.list(rep("", length(cols)))
  names(hdr) <- cols
  hdr$Variable <- group_name
  hdr
}

add_event_line <- function(p, show_label = FALSE) {
  p <- p +
    geom_vline(
      data     = event_dt,
      aes(xintercept = ym_date),
      linetype  = "dotted",
      linewidth = 0.8,
      colour    = "grey20",
      alpha     = 1
    )
  
  if (show_label) {
    p <- p +
      geom_text(
        data  = event_dt,
        aes(x = ym_date, y = Inf, label = label),
        angle = 0,
        vjust = 1.1,
        hjust = -0.08,
        size  = 3,
        colour = "grey20"
      ) +
      coord_cartesian(clip = "off")  # allow label in margin
  }
  
  p
}

add_country_year_controls <- function(
    dt,
    df_country_year,
    ym_col = "ym",
    loc_col = "loc",
    year_col = "year",
    control_vars = CY_CONTROL_VARS
) {
  dt <- copy(as.data.table(dt))
  
  # derive year from ym once, locally
  dt[, (year_col) := as.integer(substr(get(ym_col), 1L, 4L))]
  
  # minimal country-year table (only needed cols)
  cy <- as.data.table(df_country_year)[
    ,
    c(loc_col, year_col, control_vars),
    with = FALSE
  ]
  cy[, (year_col) := as.integer(get(year_col))]
  
  # left join: keep all rows of dt
  dt[cy, on = c(loc_col, year_col), (control_vars) := mget(paste0("i.", control_vars))]
  dt
}

add_country_year_controls <- function(
    dt,
    df_country_year,
    ym_col = "ym",
    loc_col = "loc",
    year_col = "year",
    control_vars = CY_CONTROL_VARS
) {
  library(data.table)
  dt <- copy(as.data.table(dt))
  
  # Ensure year exists
  if (!year_col %in% names(dt)) {
    if (ym_col %in% names(dt)) {
      dt[, (year_col) := as.integer(substr(as.character(get(ym_col)), 1L, 4L))]
    } else {
      stop("dt must contain either '", year_col, "' or '", ym_col, "'.")
    }
  } else {
    dt[, (year_col) := as.integer(get(year_col))]
  }
  
  # minimal country-year table
  cy <- as.data.table(df_country_year)[, c(loc_col, year_col, control_vars), with = FALSE]
  cy[, (year_col) := as.integer(get(year_col))]
  
  # join
  dt[cy, on = c(loc_col, year_col), (control_vars) := mget(paste0("i.", control_vars))]
  dt
}

add_region <- function(dt, loc_col = "loc") {
  dt <- copy(as.data.table(dt))
  dt[, region := fcase(
    get(loc_col) %in% europe,       "Europe",
    get(loc_col) %in% americas,     "Americas",
    get(loc_col) %in% asia_pacific, "Asia-Pacific",
    get(loc_col) %in% middle_east,  "Middle East & Africa",
    default = "Other"
  )]
  dt
}

mk_col_labels <- function(model_names, dv = "Ret") {
  setNames(paste0("(", seq_along(model_names), ")\n", dv), model_names)
}

get_group <- function(nm) sub("_.*$", "", nm)  # "Global_m10" -> "Global"

build_group_header <- function(model_names, group_titles) {
  groups <- get_group(model_names)
  grp_levels <- intersect(names(group_titles), unique(groups))
  groups <- factor(groups, levels = grp_levels)
  
  list(
    values    = c("", unname(group_titles[grp_levels])),
    colwidths = c(1, as.integer(table(groups)))
  )
}

## 2) Regression functions ----
run_ghg_specs <- function(df,
                          ghg_measure = c("log", "intensity"),
                          lag6m = FALSE,
                          ghg_class = c("full", "estimated", "disclosed"),
                          industry_var = c("gind", "naics", "tcprimarysectorid"),
                          loc_filter_toggle = FALSE,
                          loc_selection = character(0),
                          time_frame_toggle = FALSE,
                          datadate_min = NULL,
                          datadate_max = NULL,
                          ret_col = "R_m",
                          controls = character(0),
                          display_controls = character(0),
                          country_fe_toggle = FALSE,
                          country_fe_variable = "loc") {
  
  ghg_measure  <- match.arg(ghg_measure)
  ghg_class    <- match.arg(ghg_class)
  industry_var <- match.arg(industry_var)
  
  suf <- if (lag6m) "_lag6m" else ""
  
  # 1. LOCATION FILTER (if activated)
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  # 2. DATE FILTER (if activated)
  # Requires the df to have a variable 'datadate' that can be compared numerically or as Date.
  if (time_frame_toggle) {
    if (!is.null(datadate_min)) {
      df <- df[datadate >= datadate_min]
    }
    if (!is.null(datadate_max)) {
      df <- df[datadate <= datadate_max]
    }
  }
  
  # 3. Scope-specific class columns
  class_cols <- list(
    S1 = paste0("ghg_scope1_class", suf),
    S2 = paste0("ghg_scope2_loc_class", suf),
    S3 = paste0("ghg_scope3_up_class", suf)
  )
  
  # 4. Scope variable selection (log vs intensity)
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1", suf),
      S2 = paste0("log_ghg_scope2_loc", suf),
      S3 = paste0("log_ghg_scope3_up", suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity", suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity", suf)
    )
  }
  
  # 5. Controls
  ctrl_none <- character(0)
  ctrl_size <- controls[controls == "logsize"]
  ctrl_full <- controls
  
  control_sets <- list(
    none = ctrl_none,
    size = ctrl_size,
    full = ctrl_full
  )
  
  # 6. Fixed effects (base)
  FE_ym          <- "ym"
  FE_industry_ym <- paste0(industry_var, " + ym")
  
  # 6b. Add country fixed effects if toggled on
  if (country_fe_toggle) {
    FE_ym          <- paste(FE_ym,          country_fe_variable, sep = " + ")
    FE_industry_ym <- paste(FE_industry_ym, country_fe_variable, sep = " + ")
  }
  
  # 7. MODEL BUILDER
  make_model <- function(scope_var, ctrl_vec, fe_str, class_col) {
    
    df_filt <- df
    
    # Scope-specific class filtering
    if (ghg_class != "full") {
      df_filt <- df_filt[get(class_col) == ghg_class]
    }
    
    rhs <- paste(c(scope_var, ctrl_vec), collapse = " + ")
    fml <- as.formula(paste0(ret_col, " ~ ", rhs, " | ", fe_str))
    
    feols(
      fml,
      data    = df_filt,
      cluster = ~ gvkey + ym
    )
  }
  
  # 8. MODEL ORDER (m1–m12)
  models <- list()
  
  spec_list <- list(
    list(ctrl = control_sets$none, fe = FE_ym),          # m1–m3
    list(ctrl = control_sets$none, fe = FE_industry_ym), # m4–m6
    list(ctrl = control_sets$size, fe = FE_industry_ym), # m7–m9
    list(ctrl = control_sets$full, fe = FE_industry_ym)  # m10–m12
  )
  
  idx <- 1
  for (spec in spec_list) {
    models[[paste0("m", idx)]] <- make_model(scope_vars$S1, spec$ctrl, spec$fe, class_cols$S1); idx <- idx + 1
    models[[paste0("m", idx)]] <- make_model(scope_vars$S2, spec$ctrl, spec$fe, class_cols$S2); idx <- idx + 1
    models[[paste0("m", idx)]] <- make_model(scope_vars$S3, spec$ctrl, spec$fe, class_cols$S3); idx <- idx + 1
  }
  
  # 9. Controls suppressed from etable
  if (identical(display_controls, "all")) {
    drop_vars <- character(0)
  } else {
    drop_vars <- setdiff(controls, display_controls)
  }
  
  list(
    models        = models,
    drop_controls = drop_vars
  )
}

run_ghg_specs_inv_pref <- function(df,
                                   ghg_measure = c("log", "intensity"),
                                   lag6m = FALSE,
                                   ghg_class = c("full", "estimated", "disclosed"),
                                   industry_var = c("gind", "naics", "tcprimarysectorid"),
                                   loc_filter_toggle = FALSE,
                                   loc_selection = character(0),
                                   time_frame_toggle = FALSE,
                                   datadate_min = NULL,
                                   datadate_max = NULL,
                                   ret_col = "R_m",
                                   controls = character(0),
                                   display_controls = character(0),
                                   country_fe_toggle = FALSE,
                                   country_fe_variable = "loc",
                                   inv_pref_var = NULL,   # length 0, 1, or 2
                                   include_scope2 = TRUE,
                                   include_scope3 = TRUE) {
  
  ghg_measure  <- match.arg(ghg_measure)
  ghg_class    <- match.arg(ghg_class)
  industry_var <- match.arg(industry_var)
  
  # interaction variable normalization
  if (is.null(inv_pref_var) || length(inv_pref_var) == 0) {
    inv_pref_var <- NULL
  } else {
    inv_pref_var <- inv_pref_var[nzchar(inv_pref_var)]
    if (length(inv_pref_var) == 0) {
      inv_pref_var <- NULL
    } else if (length(inv_pref_var) > 2) {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  suf <- if (lag6m) "_lag6m" else ""
  
  # 1. LOCATION FILTER
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  # 2. DATE FILTER
  if (time_frame_toggle) {
    if (!is.null(datadate_min)) df <- df[datadate >= datadate_min]
    if (!is.null(datadate_max)) df <- df[datadate <= datadate_max]
  }
  
  # 3. Scope classes
  class_cols <- list(
    S1 = paste0("ghg_scope1_class", suf),
    S2 = paste0("ghg_scope2_loc_class", suf),
    S3 = paste0("ghg_scope3_up_class", suf)
  )
  
  # 4. Scope variable selection
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1", suf),
      S2 = paste0("log_ghg_scope2_loc", suf),
      S3 = paste0("log_ghg_scope3_up", suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity", suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity", suf)
    )
  }
  
  # 5. Controls
  ctrl_full <- controls
  control_sets <- list(full = ctrl_full)
  
  # 6. Fixed effects
  FE_ym          <- "ym"
  FE_industry_ym <- paste0(industry_var, " + ym")
  
  if (country_fe_toggle) {
    FE_ym          <- paste(FE_ym,          country_fe_variable, sep = " + ")
    FE_industry_ym <- paste(FE_industry_ym, country_fe_variable, sep = " + ")
  }
  
  # Helper: build interaction structure
  build_scope_term <- function(scope_var, inv_pref_var) {
    if (is.null(inv_pref_var)) {
      scope_var
    } else if (length(inv_pref_var) == 1) {
      paste0(scope_var, " * ", inv_pref_var[1])
    } else if (length(inv_pref_var) == 2) {
      paste0(scope_var, " * ", inv_pref_var[1], " * ", inv_pref_var[2])
    } else {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  # 7. MODEL BUILDER
  make_model <- function(scope_var, ctrl_vec, fe_str, class_col) {
    
    df_filt <- df
    if (ghg_class != "full") {
      df_filt <- df_filt[get(class_col) == ghg_class]
    }
    
    scope_term <- build_scope_term(scope_var, inv_pref_var)
    rhs <- paste(c(scope_term, ctrl_vec), collapse = " + ")
    fml <- as.formula(paste0(ret_col, " ~ ", rhs, " | ", fe_str))
    
    feols(fml, data = df_filt, cluster = ~ gvkey + ym)
  }
  
  # 8. Model blocks
  models <- list()
  spec_list <- list(
    list(ctrl = control_sets$full, fe = FE_ym),
    list(ctrl = control_sets$full, fe = FE_industry_ym)
  )
  
  idx <- 1
  for (spec in spec_list) {
    
    models[[paste0("m", idx)]] <-
      make_model(scope_vars$S1, spec$ctrl, spec$fe, class_cols$S1)
    idx <- idx + 1
    
    if (include_scope2) {
      models[[paste0("m", idx)]] <-
        make_model(scope_vars$S2, spec$ctrl, spec$fe, class_cols$S2)
      idx <- idx + 1
    }
    
    if (include_scope3) {
      models[[paste0("m", idx)]] <-
        make_model(scope_vars$S3, spec$ctrl, spec$fe, class_cols$S3)
      idx <- idx + 1
    }
  }
  
  # 9. Controls suppressed in etable
  drop_vars <- if (identical(display_controls, "all")) character(0)
  else setdiff(controls, display_controls)
  
  
  # 10. Specification description text
  loc_text <- if (loc_filter_toggle) {
    paste0("on (loc in {", paste(loc_selection, collapse = ", "), "})")
  } else "off"
  
  tf_text <- if (time_frame_toggle) {
    paste0("on (",
           if (!is.null(datadate_min)) datadate_min else "-inf",
           " to ",
           if (!is.null(datadate_max)) datadate_max else "+inf",
           ")")
  } else "off"
  
  ctrl_full_text <- if (length(ctrl_full) == 0) "none"
  else paste(ctrl_full, collapse = ", ")
  
  country_fe_text <- if (country_fe_toggle) {
    paste0("on (", country_fe_variable, ")")
  } else "off"
  
  inv_pref_text <- if (is.null(inv_pref_var)) {
    "none"
  } else {
    paste(inv_pref_var, collapse = " * ")
  }
  
  scope_text <- paste(
    "Included scopes: ",
    paste(
      c("S1",
        if (include_scope2) "S2" else NULL,
        if (include_scope3) "S3" else NULL),
      collapse = ", "
    )
  )
  
  spec_text <- paste0(
    "Model specification choices:\n",
    "- GHG measure: ", ghg_measure, " (suffix '", suf, "')\n",
    "- GHG class filter: ", ghg_class, "\n",
    "- Industry variable for FE: ", industry_var, "\n",
    "- Return column: ", ret_col, "\n",
    "- Location filter: ", loc_text, "\n",
    "- Time frame filter: ", tf_text, "\n",
    "- Country fixed effects: ", country_fe_text, "\n",
    "- Full controls: ", ctrl_full_text, "\n",
    "- Investor preference interaction: ", inv_pref_text, "\n",
    "- ", scope_text, "\n",
    "\nBlock structure:\n",
    "- Block 1: FE = ", FE_ym, " (no industry FE), scopes S1",
    if (include_scope2) ", S2" else "",
    if (include_scope3) ", S3" else "", "\n",
    "- Block 2: FE = ", FE_industry_ym, " (with industry FE), scopes S1",
    if (include_scope2) ", S2" else "",
    if (include_scope3) ", S3" else "", "\n"
  )
  
  # RETURN
  list(
    models        = models,
    drop_controls = drop_vars,
    spec_text     = spec_text
  )
}

run_ghg_specs_inv_pref_only_inter <- function(df,
                                   ghg_measure = c("log", "intensity"),
                                   lag6m = FALSE,
                                   ghg_class = c("full", "estimated", "disclosed"),
                                   industry_var = c("gind", "naics", "tcprimarysectorid"),
                                   loc_filter_toggle = FALSE,
                                   loc_selection = character(0),
                                   time_frame_toggle = FALSE,
                                   datadate_min = NULL,
                                   datadate_max = NULL,
                                   ret_col = "R_m",
                                   controls = character(0),
                                   display_controls = character(0),
                                   country_fe_toggle = FALSE,
                                   country_fe_variable = "loc",
                                   inv_pref_var = NULL,   # length 0, 1, or 2
                                   include_scope2 = TRUE,
                                   include_scope3 = TRUE) {
  
  ghg_measure  <- match.arg(ghg_measure)
  ghg_class    <- match.arg(ghg_class)
  industry_var <- match.arg(industry_var)
  
  # interaction variable normalization
  if (is.null(inv_pref_var) || length(inv_pref_var) == 0) {
    inv_pref_var <- NULL
  } else {
    inv_pref_var <- inv_pref_var[nzchar(inv_pref_var)]
    if (length(inv_pref_var) == 0) {
      inv_pref_var <- NULL
    } else if (length(inv_pref_var) > 2) {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  suf <- if (lag6m) "_lag6m" else ""
  
  # 1. LOCATION FILTER
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  # 2. DATE FILTER
  if (time_frame_toggle) {
    if (!is.null(datadate_min)) df <- df[datadate >= datadate_min]
    if (!is.null(datadate_max)) df <- df[datadate <= datadate_max]
  }
  
  # 3. Scope classes
  class_cols <- list(
    S1 = paste0("ghg_scope1_class", suf),
    S2 = paste0("ghg_scope2_loc_class", suf),
    S3 = paste0("ghg_scope3_up_class", suf)
  )
  
  # 4. Scope variable selection
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1", suf),
      S2 = paste0("log_ghg_scope2_loc", suf),
      S3 = paste0("log_ghg_scope3_up", suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity", suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity", suf)
    )
  }
  
  # 5. Controls
  ctrl_full <- controls
  control_sets <- list(full = ctrl_full)
  
  # 6. Fixed effects
  FE_ym          <- "ym"
  FE_industry_ym <- paste0(industry_var, " + ym")
  
  if (country_fe_toggle) {
    FE_ym          <- paste(FE_ym,          country_fe_variable, sep = " + ")
    FE_industry_ym <- paste(FE_industry_ym, country_fe_variable, sep = " + ")
  }
  
  # Helper: build interaction structure
  build_scope_term <- function(scope_var, inv_pref_var) {
    if (is.null(inv_pref_var)) {
      scope_var
    } else if (length(inv_pref_var) == 1) {
      # main effect of scope + interaction only (NO main effect of inv_pref_var)
      paste0(scope_var, " + ", scope_var, ":", inv_pref_var[1])
    } else if (length(inv_pref_var) == 2) {
      # simplest: main scope + three-way interaction only
      # (you can expand pairwise terms if you want them)
      paste0(scope_var, " + ",
             scope_var, ":", inv_pref_var[1], ":", inv_pref_var[2])
    } else {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  # 7. MODEL BUILDER
  make_model <- function(scope_var, ctrl_vec, fe_str, class_col) {
    
    df_filt <- df
    if (ghg_class != "full") {
      df_filt <- df_filt[get(class_col) == ghg_class]
    }
    
    scope_term <- build_scope_term(scope_var, inv_pref_var)
    rhs <- paste(c(scope_term, ctrl_vec), collapse = " + ")
    fml <- as.formula(paste0(ret_col, " ~ ", rhs, " | ", fe_str))
    
    feols(fml, data = df_filt, cluster = ~ gvkey + ym)
  }
  
  # 8. Model blocks
  models <- list()
  spec_list <- list(
    list(ctrl = control_sets$full, fe = FE_ym),
    list(ctrl = control_sets$full, fe = FE_industry_ym)
  )
  
  idx <- 1
  for (spec in spec_list) {
    
    models[[paste0("m", idx)]] <-
      make_model(scope_vars$S1, spec$ctrl, spec$fe, class_cols$S1)
    idx <- idx + 1
    
    if (include_scope2) {
      models[[paste0("m", idx)]] <-
        make_model(scope_vars$S2, spec$ctrl, spec$fe, class_cols$S2)
      idx <- idx + 1
    }
    
    if (include_scope3) {
      models[[paste0("m", idx)]] <-
        make_model(scope_vars$S3, spec$ctrl, spec$fe, class_cols$S3)
      idx <- idx + 1
    }
  }
  
  # 9. Controls suppressed in etable
  drop_vars <- if (identical(display_controls, "all")) character(0)
  else setdiff(controls, display_controls)
  
  
  # 10. Specification description text
  loc_text <- if (loc_filter_toggle) {
    paste0("on (loc in {", paste(loc_selection, collapse = ", "), "})")
  } else "off"
  
  tf_text <- if (time_frame_toggle) {
    paste0("on (",
           if (!is.null(datadate_min)) datadate_min else "-inf",
           " to ",
           if (!is.null(datadate_max)) datadate_max else "+inf",
           ")")
  } else "off"
  
  ctrl_full_text <- if (length(ctrl_full) == 0) "none"
  else paste(ctrl_full, collapse = ", ")
  
  country_fe_text <- if (country_fe_toggle) {
    paste0("on (", country_fe_variable, ")")
  } else "off"
  
  inv_pref_text <- if (is.null(inv_pref_var)) {
    "none"
  } else {
    paste(inv_pref_var, collapse = " * ")
  }
  
  scope_text <- paste(
    "Included scopes: ",
    paste(
      c("S1",
        if (include_scope2) "S2" else NULL,
        if (include_scope3) "S3" else NULL),
      collapse = ", "
    )
  )
  
  spec_text <- paste0(
    "Model specification choices:\n",
    "- GHG measure: ", ghg_measure, " (suffix '", suf, "')\n",
    "- GHG class filter: ", ghg_class, "\n",
    "- Industry variable for FE: ", industry_var, "\n",
    "- Return column: ", ret_col, "\n",
    "- Location filter: ", loc_text, "\n",
    "- Time frame filter: ", tf_text, "\n",
    "- Country fixed effects: ", country_fe_text, "\n",
    "- Full controls: ", ctrl_full_text, "\n",
    "- Investor preference interaction: ", inv_pref_text, "\n",
    "- ", scope_text, "\n",
    "\nBlock structure:\n",
    "- Block 1: FE = ", FE_ym, " (no industry FE), scopes S1",
    if (include_scope2) ", S2" else "",
    if (include_scope3) ", S3" else "", "\n",
    "- Block 2: FE = ", FE_industry_ym, " (with industry FE), scopes S1",
    if (include_scope2) ", S2" else "",
    if (include_scope3) ", S3" else "", "\n"
  )
  
  # RETURN
  list(
    models        = models,
    drop_controls = drop_vars,
    spec_text     = spec_text
  )
}

run_ghg_specs_dummy <- function(df,
                                ghg_measure = c("log", "intensity"),
                                lag6m = FALSE,
                                ghg_class = c("full", "estimated", "disclosed"),
                                industry_var = c("gind", "naics", "tcprimarysectorid"),
                                loc_filter_toggle = FALSE,
                                loc_selection = character(0),
                                time_frame_toggle = FALSE,
                                datadate_min = NULL,
                                datadate_max = NULL,
                                ret_col = "R_m",
                                controls = character(0),
                                display_controls = character(0),
                                country_fe_toggle = FALSE,
                                country_fe_variable = "loc",
                                dummy_toggle = FALSE,
                                dummy_col = NULL) {
  
  ghg_measure  <- match.arg(ghg_measure)
  ghg_class    <- match.arg(ghg_class)
  industry_var <- match.arg(industry_var)
  
  suf <- if (lag6m) "_lag6m" else ""
  
  # Sanity check for dummy
  if (dummy_toggle) {
    if (is.null(dummy_col) || !dummy_col %in% names(df)) {
      stop("dummy_toggle = TRUE but 'dummy_col' is NULL or not found in 'df'.")
    }
  }
  
  # 1. LOCATION FILTER (if activated)
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  # 2. DATE FILTER (if activated)
  if (time_frame_toggle) {
    if (!is.null(datadate_min)) {
      df <- df[datadate >= datadate_min]
    }
    if (!is.null(datadate_max)) {
      df <- df[datadate <= datadate_max]
    }
  }
  
  # 3. Scope-specific class columns
  class_cols <- list(
    S1 = paste0("ghg_scope1_class", suf),
    S2 = paste0("ghg_scope2_loc_class", suf),
    S3 = paste0("ghg_scope3_up_class", suf)
  )
  
  # 4. Scope variable selection (log vs intensity)
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1", suf),
      S2 = paste0("log_ghg_scope2_loc", suf),
      S3 = paste0("log_ghg_scope3_up", suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity", suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity", suf)
    )
  }
  
  # 5. Controls
  ctrl_none <- character(0)
  ctrl_size <- controls[controls == "logsize"]
  ctrl_full <- controls
  
  control_sets <- list(
    none = ctrl_none,
    size = ctrl_size,
    full = ctrl_full
  )
  
  # 6. Fixed effects (base)
  FE_ym          <- "ym"
  FE_industry_ym <- paste0(industry_var, " + ym")
  
  # 6b. Add country fixed effects if toggled on
  if (country_fe_toggle) {
    FE_ym          <- paste(FE_ym,          country_fe_variable, sep = " + ")
    FE_industry_ym <- paste(FE_industry_ym, country_fe_variable, sep = " + ")
  }
  
  # 7. MODEL BUILDER
  make_model <- function(scope_var, ctrl_vec, fe_str, class_col) {
    
    df_filt <- df
    
    # Scope-specific class filtering
    if (ghg_class != "full") {
      df_filt <- df_filt[get(class_col) == ghg_class]
    }
    
    # RHS: main GHG variable + controls
    rhs_terms <- c(scope_var, ctrl_vec)
    
    # Add interaction with dummy, if requested:
    # Only the interaction, not the main dummy effect, to avoid collinearity with FEs
    if (dummy_toggle) {
      inter_term <- paste0(scope_var, ":", dummy_col)
      rhs_terms  <- c(rhs_terms, inter_term)
    }
    
    rhs <- paste(rhs_terms, collapse = " + ")
    fml <- as.formula(paste0(ret_col, " ~ ", rhs, " | ", fe_str))
    
    feols(
      fml,
      data    = df_filt,
      cluster = ~ gvkey + ym
    )
  }
  
  # 8. MODEL ORDER (m1–m12)
  models <- list()
  
  spec_list <- list(
    list(ctrl = control_sets$none, fe = FE_ym),          # m1–m3
    list(ctrl = control_sets$none, fe = FE_industry_ym), # m4–m6
    list(ctrl = control_sets$size, fe = FE_industry_ym), # m7–m9
    list(ctrl = control_sets$full, fe = FE_industry_ym)  # m10–m12
  )
  
  idx <- 1
  for (spec in spec_list) {
    models[[paste0("m", idx)]] <- make_model(scope_vars$S1, spec$ctrl, spec$fe, class_cols$S1); idx <- idx + 1
    models[[paste0("m", idx)]] <- make_model(scope_vars$S2, spec$ctrl, spec$fe, class_cols$S2); idx <- idx + 1
    models[[paste0("m", idx)]] <- make_model(scope_vars$S3, spec$ctrl, spec$fe, class_cols$S3); idx <- idx + 1
  }
  
  # 9. Controls suppressed from etable
  if (identical(display_controls, "all")) {
    drop_vars <- character(0)
  } else {
    drop_vars <- setdiff(controls, display_controls)
  }
  
  # 10. Text summary of model choices
  loc_text <- if (loc_filter_toggle) {
    paste0("on (loc in {", paste(loc_selection, collapse = ", "), "})")
  } else {
    "off"
  }
  
  tf_text <- if (time_frame_toggle) {
    paste0("on (", if (!is.null(datadate_min)) datadate_min else "-inf",
           " to ",
           if (!is.null(datadate_max)) datadate_max else "+inf", ")")
  } else {
    "off"
  }
  
  ctrl_pool_text <- if (length(controls) > 0) {
    paste(controls, collapse = ", ")
  } else {
    "none"
  }
  
  ctrl_size_text <- if (length(ctrl_size) > 0) {
    paste(ctrl_size, collapse = ", ")
  } else {
    "none"
  }
  
  ctrl_full_text <- if (length(ctrl_full) > 0) {
    paste(ctrl_full, collapse = ", ")
  } else {
    "none"
  }
  
  country_fe_text <- if (country_fe_toggle) {
    paste0("on (", country_fe_variable, ")")
  } else {
    "off"
  }
  
  dummy_text <- if (dummy_toggle) {
    paste0("on (interaction of each scope variable with ", dummy_col, ")")
  } else {
    "off"
  }
  
  spec_text <- paste0(
    "Model specification choices:\n",
    "- GHG measure: ", ghg_measure, " (suffix '", suf, "')\n",
    "- GHG class filter: ", ghg_class, "\n",
    "- Industry variable for FE: ", industry_var, "\n",
    "- Return column: ", ret_col, "\n",
    "- Location filter: ", loc_text, "\n",
    "- Time frame filter: ", tf_text, "\n",
    "- Country fixed effects: ", country_fe_text, "\n",
    "- Controls pool: ", ctrl_pool_text, "\n",
    "- Dummy interaction: ", dummy_text, "\n",
    "\nBlock structure (m1–m12):\n",
    "- m1–m3: scopes S1–S3, controls: none, FE: ", FE_ym, "\n",
    "- m4–m6: scopes S1–S3, controls: none, FE: ", FE_industry_ym, "\n",
    "- m7–m9: scopes S1–S3, controls: size-only (", ctrl_size_text, "), FE: ", FE_industry_ym, "\n",
    "- m10–m12: scopes S1–S3, controls: full (", ctrl_full_text, "), FE: ", FE_industry_ym, "\n"
  )
  
  list(
    models        = models,
    drop_controls = drop_vars,
    spec_text     = spec_text
  )
}

run_ghg_specs_dummy_full_spec <- function(df,
                                     ghg_measure = c("log", "intensity"),
                                     lag6m = FALSE,
                                     ghg_class = c("full", "estimated", "disclosed"),
                                     industry_var = c("gind", "naics", "tcprimarysectorid"),
                                     loc_filter_toggle = FALSE,
                                     loc_selection = character(0),
                                     time_frame_toggle = FALSE,
                                     datadate_min = NULL,
                                     datadate_max = NULL,
                                     ret_col = "R_m",
                                     controls = character(0),
                                     display_controls = character(0),
                                     country_fe_toggle = FALSE,
                                     country_fe_variable = "loc",
                                     # DUMMY INTERACTIONS (no main effects)
                                     dummy_toggle = FALSE,
                                     dummy_cols = character(0),
                                     # GENERIC INTERACTIONS (optionally with main effects)
                                     interact_toggle = FALSE,
                                     interact_with_vars = character(0),
                                     interact_include_main_effect = TRUE,
                                     # SCOPE TOGGLES
                                     toggle_S1 = TRUE,
                                     toggle_S2 = TRUE,
                                     toggle_S3 = TRUE) {
  
  ghg_measure  <- match.arg(ghg_measure)
  ghg_class    <- match.arg(ghg_class)
  industry_var <- match.arg(industry_var)
  
  suf <- if (lag6m) "_lag6m" else ""
  
    if (!any(toggle_S1, toggle_S2, toggle_S3)) {
    stop("All scope toggles are FALSE. At least one of toggle_S1, toggle_S2, toggle_S3 must be TRUE.")
  }
  
    if (dummy_toggle) {
    if (length(dummy_cols) == 0L) {
      stop("dummy_toggle = TRUE but 'dummy_cols' is empty.")
    }
    if (!all(dummy_cols %in% names(df))) {
      missing_cols <- dummy_cols[!dummy_cols %in% names(df)]
      stop("Some 'dummy_cols' not found in 'df': ", paste(missing_cols, collapse = ", "))
    }
  }
  
    if (interact_toggle) {
    if (length(interact_with_vars) == 0L) {
      stop("interact_toggle = TRUE but 'interact_with_vars' is empty.")
    }
    if (!all(interact_with_vars %in% names(df))) {
      missing_cols <- interact_with_vars[!interact_with_vars %in% names(df)]
      stop("Some 'interact_with_vars' not found in 'df': ", paste(missing_cols, collapse = ", "))
    }
  }
  
  # 1. LOCATION FILTER (if activated)
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  # 2. DATE FILTER (if activated)
  if (time_frame_toggle) {
    if (!is.null(datadate_min)) {
      df <- df[datadate >= datadate_min]
    }
    if (!is.null(datadate_max)) {
      df <- df[datadate <= datadate_max]
    }
  }
  
  # 3. Scope-specific class columns
  class_cols <- list(
    S1 = paste0("ghg_scope1_class", suf),
    S2 = paste0("ghg_scope2_loc_class", suf),
    S3 = paste0("ghg_scope3_up_class", suf)
  )
  
  # 4. Scope variable selection (log vs intensity)
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1", suf),
      S2 = paste0("log_ghg_scope2_loc", suf),
      S3 = paste0("log_ghg_scope3_up", suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity", suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity", suf)
    )
  }
  
  # 5. Controls: always full spec
  ctrl_full <- controls
  
  # 6. Fixed effects: we treat FE_industry_ym as the "full" FE spec
  FE_industry_ym <- paste0(industry_var, " + ym")
  
  if (country_fe_toggle) {
    FE_industry_ym <- paste(FE_industry_ym, country_fe_variable, sep = " + ")
  }
  
  # 7. MODEL BUILDER
  make_model <- function(scope_var, ctrl_vec, fe_str, class_col) {
    
    df_filt <- df
    
    # Scope-specific class filtering
    if (ghg_class != "full") {
      df_filt <- df_filt[get(class_col) == ghg_class]
    }
    
    # RHS: main GHG variable + controls
    rhs_terms <- c(scope_var, ctrl_vec)
    
    # 7a. Add interactions with dummies (no main effects)
    if (dummy_toggle && length(dummy_cols) > 0L) {
      inter_terms_dummy <- paste0(scope_var, ":", dummy_cols)
      rhs_terms         <- c(rhs_terms, inter_terms_dummy)
    }
    
    # 7b. Add generic interactions (with optional main effects)
    if (interact_toggle && length(interact_with_vars) > 0L) {
      if (interact_include_main_effect) {
        rhs_terms <- c(rhs_terms, interact_with_vars)
      }
      inter_terms_generic <- paste0(scope_var, ":", interact_with_vars)
      rhs_terms           <- c(rhs_terms, inter_terms_generic)
    }
    
    # Avoid duplicates if a variable appears both as control and in interactions
    rhs_terms <- unique(rhs_terms)
    
    rhs <- paste(rhs_terms, collapse = " + ")
    fml <- as.formula(paste0(ret_col, " ~ ", rhs, " | ", fe_str))
    
    feols(
      fml,
      data    = df_filt,
      cluster = ~ gvkey + ym
    )
  }
  
  # 8. MODELS: one full-spec model per included scope
  models <- list()
  idx <- 1
  
  if (toggle_S1) {
    models[[paste0("m", idx)]] <- make_model(scope_vars$S1, ctrl_full, FE_industry_ym, class_cols$S1)
    idx <- idx + 1
  }
  
  if (toggle_S2) {
    models[[paste0("m", idx)]] <- make_model(scope_vars$S2, ctrl_full, FE_industry_ym, class_cols$S2)
    idx <- idx + 1
  }
  
  if (toggle_S3) {
    models[[paste0("m", idx)]] <- make_model(scope_vars$S3, ctrl_full, FE_industry_ym, class_cols$S3)
    idx <- idx + 1
  }
  
  # 9. Controls suppressed from etable
  if (identical(display_controls, "all")) {
    drop_vars <- character(0)
  } else {
    drop_vars <- setdiff(controls, display_controls)
  }
  
  # 10. Text summary of model choices
  
  # scopes text
  scope_included <- c(
    if (toggle_S1) "S1" else NULL,
    if (toggle_S2) "S2" else NULL,
    if (toggle_S3) "S3" else NULL
  )
  scope_text <- if (length(scope_included) > 0L) {
    paste(scope_included, collapse = ", ")
  } else {
    "none"
  }
  
  loc_text <- if (loc_filter_toggle) {
    paste0("on (loc in {", paste(loc_selection, collapse = ", "), "})")
  } else {
    "off"
  }
  
  tf_text <- if (time_frame_toggle) {
    paste0("on (", if (!is.null(datadate_min)) datadate_min else "-inf",
           " to ",
           if (!is.null(datadate_max)) datadate_max else "+inf", ")")
  } else {
    "off"
  }
  
  ctrl_full_text <- if (length(ctrl_full) > 0) {
    paste(ctrl_full, collapse = ", ")
  } else {
    "none"
  }
  
  country_fe_text <- if (country_fe_toggle) {
    paste0("on (", country_fe_variable, ")")
  } else {
    "off"
  }
  
  dummy_text <- if (dummy_toggle && length(dummy_cols) > 0L) {
    paste0(
      "on (interaction of each scope variable with dummies: ",
      paste(dummy_cols, collapse = ", "),
      "; main dummy effects omitted)"
    )
  } else {
    "off"
  }
  
  interact_text <- if (interact_toggle && length(interact_with_vars) > 0L) {
    paste0(
      "on (interaction of each scope variable with: ",
      paste(interact_with_vars, collapse = ", "),
      "; main effects ",
      if (interact_include_main_effect) "included" else "omitted",
      ")"
    )
  } else {
    "off"
  }
  
  spec_text <- paste0(
    "Model specification choices (FULL SPEC ONLY):\n",
    "- GHG measure: ", ghg_measure, " (suffix '", suf, "')\n",
    "- Scopes included: ", scope_text, "\n",
    "- GHG class filter: ", ghg_class, "\n",
    "- Industry variable for FE: ", industry_var, "\n",
    "- Return column: ", ret_col, "\n",
    "- Location filter: ", loc_text, "\n",
    "- Time frame filter: ", tf_text, "\n",
    "- Country fixed effects: ", country_fe_text, "\n",
    "- Controls (full): ", ctrl_full_text, "\n",
    "- Dummy interactions: ", dummy_text, "\n",
    "- Generic interactions: ", interact_text, "\n",
    "\nFE used in all models: ", FE_industry_ym, "\n"
  )
  
  list(
    models        = models,
    drop_controls = drop_vars,
    spec_text     = spec_text
  )
}

run_pai_cfa_decomp_model <- function(y,
                              type_model   = c("pai", "cfa", "decomp_structural", "decomp_full"),
                              fixed_effect = c("loc_year", "year"),
                              lag          = TRUE,
                              z_rhs        = TRUE,
                              data         = df_country_year,
                              cluster      = "loc") {
  
  type_model   <- match.arg(type_model)
  fixed_effect <- match.arg(fixed_effect)
  
  # --- helper: build RHS variable name
  # lag: TRUE  -> use *_lag1 (if available)
  # z_rhs: TRUE -> use *_z (and *_z_lag1 when lag=TRUE)
  pick_x <- function(base) {
    if (isTRUE(lag) && isTRUE(z_rhs)) return(paste0(base, "_z_lag1"))
    if (isTRUE(lag) && !isTRUE(z_rhs)) return(paste0(base, "_lag1"))
    if (!isTRUE(lag) && isTRUE(z_rhs)) return(paste0(base, "_z"))
    base
  }
  
  # --- choose regressors by model type
  x_vars <- switch(
    type_model,
    "pai" = c(pick_x("PAI")),
    "cfa" = c(pick_x("cfa_cva_ratio")),
    "decomp_structural" = c(
      pick_x("total_ff_produced_per_gdp"),
      pick_x("vulnerability")
    ),
    "decomp_full" = c(
      pick_x("total_ff_produced_per_gdp"),
      pick_x("vulnerability"),
      pick_x("gdp_pc"),
      pick_x("control_corruption")
    )
  )
  
  # --- FE part
  fe_part <- if (fixed_effect == "loc_year") "loc + year" else "year"
  
  # --- build and run model
  fml <- as.formula(paste0(y, " ~ ", paste(x_vars, collapse = " + "), " | ", fe_part))
  
  # sanity check: fail fast if columns are missing
  needed <- unique(c(y, x_vars, "year", if (fixed_effect == "loc_year") "loc" else "loc"))
  missing <- setdiff(needed, names(data))
  if (length(missing) > 0L) {
    stop("Missing columns in data: ", paste(missing, collapse = ", "))
  }
  
  feols(
    fml,
    cluster = as.formula(paste0("~", cluster)),
    data = data
  )
}

run_ghg_yearly_betas <- function(
    df,
    ghg_measure        = c("log", "intensity"),
    scope              = c("S1", "S2", "S3"),
    lag6m              = FALSE,
    ghg_class          = c("full", "estimated", "disclosed"),
    loc_filter_toggle  = FALSE,
    loc_selection      = character(0),
    time_frame_toggle  = FALSE,
    datadate_min       = NULL,
    datadate_max       = NULL,
    ret_col            = "R_m",
    controls           = character(0),
    min_firms          = 30L,
    group_by_loc       = FALSE,
    inv_pref_var       = NULL,   # length 0, 1, or 2
    fixed_effect       = NULL,
    cluster_vars       = c("gvkey", "gind")  # NEW: cluster within-year pooled regressions
) {
  library(data.table)
  library(fixest)
  
  ghg_measure <- match.arg(ghg_measure)
  ghg_class   <- match.arg(ghg_class)
  scope       <- match.arg(scope, several.ok = TRUE)
  
  # --- normalize interaction variable ---
  if (is.null(inv_pref_var) || length(inv_pref_var) == 0) {
    inv_pref_var <- NULL
  } else {
    inv_pref_var <- inv_pref_var[nzchar(inv_pref_var)]
    if (length(inv_pref_var) == 0) {
      inv_pref_var <- NULL
    } else if (length(inv_pref_var) > 2) {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  # --- normalize fixed_effect argument ---
  if (is.null(fixed_effect) || length(fixed_effect) == 0) {
    fixed_effect <- NULL
  } else {
    fixed_effect <- fixed_effect[nzchar(fixed_effect)]
    if (length(fixed_effect) == 0) {
      fixed_effect <- NULL
    }
  }
  
  # --- normalize cluster vars ---
  if (is.null(cluster_vars) || length(cluster_vars) == 0) {
    cluster_vars <- NULL
  } else {
    cluster_vars <- cluster_vars[nzchar(cluster_vars)]
    if (length(cluster_vars) == 0) cluster_vars <- NULL
  }
  
  suf <- if (lag6m) "_lag6m" else ""
  
  df <- as.data.table(df)
  
  # 0) Ensure a yearly index exists
  if (!"year" %in% names(df)) {
    if ("datadate" %in% names(df)) {
      df[, year := as.integer(format(as.IDate(datadate), "%Y"))]
    } else if ("month" %in% names(df)) {
      df[, year := as.integer(format(as.IDate(month), "%Y"))]
    } else if ("ym" %in% names(df)) {
      df[, year := as.integer(substr(ym, 1, 4))]
    } else {
      stop("run_ghg_yearly_betas: need 'year' or one of ('datadate','month','ym') to construct year.")
    }
  }
  
  # 1) Location filter
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  # 2) Date filter (requires datadate)
  if (time_frame_toggle) {
    if (!"datadate" %in% names(df)) {
      stop("time_frame_toggle=TRUE requires a 'datadate' column in df.")
    }
    if (!is.null(datadate_min)) df <- df[datadate >= datadate_min]
    if (!is.null(datadate_max)) df <- df[datadate <= datadate_max]
  }
  
  # 3) Class columns
  class_cols <- list(
    S1 = paste0("ghg_scope1_class",     suf),
    S2 = paste0("ghg_scope2_loc_class", suf),
    S3 = paste0("ghg_scope3_up_class",  suf)
  )
  
  # 4) Scope variables
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1",     suf),
      S2 = paste0("log_ghg_scope2_loc", suf),
      S3 = paste0("log_ghg_scope3_up",  suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity",     suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity",  suf)
    )
  }
  
  # --- helper to build interaction term (carbon * inv_pref_var) ---
  build_scope_term <- function(scope_var, inv_pref_var) {
    if (is.null(inv_pref_var)) {
      scope_var
    } else if (length(inv_pref_var) == 1) {
      paste0(scope_var, " * ", inv_pref_var[1])
    } else if (length(inv_pref_var) == 2) {
      paste0(scope_var, " * ", inv_pref_var[1], " * ", inv_pref_var[2])
    } else {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  # 5) Helper to run pooled-within-year regressions for one scope
  run_scope <- function(scope_label) {
    scope_var <- scope_vars[[scope_label]]
    class_col <- class_cols[[scope_label]]
    
    if (!scope_var %in% names(df)) {
      warning("Scope variable not found in df: ", scope_var, ". Skipping scope ", scope_label, ".")
      return(NULL)
    }
    
    df_scope <- copy(df)
    
    # Filter by ghg_class if requested
    if (ghg_class != "full") {
      if (class_col %in% names(df_scope)) {
        df_scope <- df_scope[get(class_col) == ghg_class]
      } else {
        warning("ghg_class='", ghg_class, "' requested but class column missing: ", class_col,
                ". Proceeding without class filtering.")
      }
    }
    
    # Keep only rows where key columns exist (carbon + return)
    df_scope <- df_scope[!is.na(get(scope_var)) & !is.na(get(ret_col))]
    
    if (nrow(df_scope) == 0L) {
      warning("No observations left for scope ", scope_label, " after filtering.")
      return(NULL)
    }
    
    # --- build RHS: (scope_var [+ interactions]) + controls ---
    scope_term <- build_scope_term(scope_var, inv_pref_var)
    rhs_terms  <- c(scope_term, controls)
    rhs_str    <- paste(rhs_terms, collapse = " + ")
    
    # --- build formula with or without fixed effects ---
    if (is.null(fixed_effect)) {
      fml <- as.formula(paste(ret_col, "~", rhs_str))
    } else {
      fe_str <- paste(fixed_effect, collapse = " + ")
      fml <- as.formula(paste0(ret_col, " ~ ", rhs_str, " | ", fe_str))
    }
    
    # Grouping: year or (loc, year)
    by_cols <- if (group_by_loc) c("loc", "year") else "year"
    
    # Build vcov formula dynamically (drop missing cluster vars)
    make_vcov <- function(d) {
      if (is.null(cluster_vars)) return(NULL)
      keep <- cluster_vars[cluster_vars %in% names(d)]
      if (length(keep) == 0) return(NULL)
      as.formula(paste("~", paste(keep, collapse = " + ")))
    }
    
    # Run pooled-within-year regressions by group (year or loc-year)
    res <- df_scope[
      ,
      {
        if (.N < min_firms) {
          list(beta_carbon = NA_real_, se_carbon = NA_real_, n_firms = .N)
        } else {
          out <- tryCatch(
            {
              vc <- make_vcov(.SD)
              mod <- if (is.null(vc)) {
                feols(fml, data = .SD)
              } else {
                feols(fml, data = .SD, vcov = vc)
              }
              
              if (nobs(mod) < min_firms) {
                return(list(beta_carbon = NA_real_, se_carbon = NA_real_, n_firms = nobs(mod)))
              }
              
              # Be explicit about vcov at summary time as well
              cf <- if (is.null(vc)) {
                summary(mod)$coeftable
              } else {
                summary(mod, vcov = vc)$coeftable
              }
              
              if (!scope_var %in% rownames(cf)) {
                list(beta_carbon = NA_real_, se_carbon = NA_real_, n_firms = nobs(mod))
              } else {
                list(
                  beta_carbon = cf[scope_var, "Estimate"],
                  se_carbon   = cf[scope_var, "Std. Error"],
                  n_firms     = nobs(mod)
                )
              }
            },
            error = function(e) {
              list(beta_carbon = NA_real_, se_carbon = NA_real_, n_firms = .N)
            }
          )
          out
        }
      },
      by = by_cols
    ]
    
    res[, scope := scope_label]
    res[]
  }
  
  # 6) Run for selected scopes
  res_list <- lapply(scope, run_scope)
  names(res_list) <- scope
  res_list <- res_list[!vapply(res_list, is.null, logical(1L))]
  
  if (length(res_list) == 0L) {
    stop("run_ghg_yearly_betas: no valid scope results produced.")
  }
  
  series_long <- rbindlist(res_list, use.names = TRUE, fill = TRUE)
  
  if (group_by_loc) {
    setorder(series_long, loc, scope, year)
  } else {
    setorder(series_long, scope, year)
  }
  
  # 7) Wide output via dcast (including n_firms -> n_*)
  if (group_by_loc) {
    series_wide <- dcast(
      series_long,
      loc + year ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms")
    )
    setorder(series_wide, loc, year)
  } else {
    series_wide <- dcast(
      series_long,
      year ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms")
    )
    setorder(series_wide, year)
  }
  
  # Rename n_firms_* columns to n_* for clarity
  n_cols <- grep("^n_firms_", names(series_wide), value = TRUE)
  if (length(n_cols) > 0L) {
    setnames(series_wide, n_cols, sub("^n_firms_", "n_", n_cols))
  }
  
  series_wide[]
}

run_ghg_monthly_betas <- function(
    df,
    ghg_measure        = c("log", "intensity"),
    scope              = c("S1", "S2", "S3"),
    lag6m              = FALSE,
    ghg_class          = c("full", "estimated", "disclosed"),
    loc_filter_toggle  = FALSE,
    loc_selection      = character(0),
    time_frame_toggle  = FALSE,
    datadate_min       = NULL,
    datadate_max       = NULL,
    ret_col            = "R_m",
    controls           = character(0),
    min_firms          = 30L,
    group_by_loc       = FALSE,
    inv_pref_var       = NULL,   # length 0, 1, or 2
    fixed_effect       = NULL
) {
  library(data.table)
  library(fixest)
  
  ghg_measure <- match.arg(ghg_measure)
  ghg_class   <- match.arg(ghg_class)
  scope       <- match.arg(scope, several.ok = TRUE)
  
  # --- normalize interaction variable ---
  if (is.null(inv_pref_var) || length(inv_pref_var) == 0) {
    inv_pref_var <- NULL
  } else {
    inv_pref_var <- inv_pref_var[nzchar(inv_pref_var)]
    if (length(inv_pref_var) == 0) {
      inv_pref_var <- NULL
    } else if (length(inv_pref_var) > 2) {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  # --- normalize fixed_effect argument ---
  if (is.null(fixed_effect) || length(fixed_effect) == 0) {
    fixed_effect <- NULL
  } else {
    fixed_effect <- fixed_effect[nzchar(fixed_effect)]
    if (length(fixed_effect) == 0) {
      fixed_effect <- NULL
    }
  }
  
  suf <- if (lag6m) "_lag6m" else ""
  
  df <- as.data.table(df)
  
  # 0) Ensure ym exists
  if (!"ym" %in% names(df)) {
    if ("month" %in% names(df)) {
      df[, ym := format(as.IDate(month), "%Y-%m")]
    } else {
      stop("run_ghg_monthly_betas: 'ym' not in df and no 'month' column to construct it.")
    }
  }
  
  # 1) Location filter
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  # 2) Date filter
  if (time_frame_toggle) {
    if (!is.null(datadate_min)) df <- df[datadate >= datadate_min]
    if (!is.null(datadate_max)) df <- df[datadate <= datadate_max]
  }
  
  # 3) Class columns
  class_cols <- list(
    S1 = paste0("ghg_scope1_class",        suf),
    S2 = paste0("ghg_scope2_loc_class",    suf),
    S3 = paste0("ghg_scope3_up_class",     suf)
  )
  
  # 4) Scope variables
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1",        suf),
      S2 = paste0("log_ghg_scope2_loc",    suf),
      S3 = paste0("log_ghg_scope3_up",     suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity",     suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity",  suf)
    )
  }
  
  # --- helper to build interaction term (carbon * inv_pref_var) ---
  build_scope_term <- function(scope_var, inv_pref_var) {
    if (is.null(inv_pref_var)) {
      scope_var
    } else if (length(inv_pref_var) == 1) {
      paste0(scope_var, " * ", inv_pref_var[1])
    } else if (length(inv_pref_var) == 2) {
      paste0(scope_var, " * ", inv_pref_var[1], " * ", inv_pref_var[2])
    } else {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  # 5) Helper to run CS regressions for one scope
  run_scope <- function(scope_label) {
    scope_var <- scope_vars[[scope_label]]
    class_col <- class_cols[[scope_label]]
    
    if (!scope_var %in% names(df)) {
      warning("Scope variable not found in df: ", scope_var, ". Skipping scope ", scope_label, ".")
      return(NULL)
    }
    
    df_scope <- copy(df)
    
    # Filter by ghg_class if requested
    if (ghg_class != "full" && class_col %in% names(df_scope)) {
      df_scope <- df_scope[get(class_col) == ghg_class]
    }
    
    # Keep only rows where key columns exist (carbon + return)
    df_scope <- df_scope[!is.na(get(scope_var)) & !is.na(get(ret_col))]
    
    if (nrow(df_scope) == 0L) {
      warning("No observations left for scope ", scope_label, " after filtering.")
      return(NULL)
    }
    
    # --- build RHS: (scope_var [+ interactions]) + controls ---
    scope_term <- build_scope_term(scope_var, inv_pref_var)
    rhs_terms  <- c(scope_term, controls)
    rhs_str    <- paste(rhs_terms, collapse = " + ")
    
    # --- build formula with or without fixed effects ---
    if (is.null(fixed_effect)) {
      fml <- as.formula(paste(ret_col, "~", rhs_str))
    } else {
      fe_str <- paste(fixed_effect, collapse = " + ")
      fml <- as.formula(paste0(ret_col, " ~ ", rhs_str, " | ", fe_str))
    }
    
    # Grouping: ym or (loc, ym)
    by_cols <- if (group_by_loc) c("loc", "ym") else "ym"
    
    # Run cross-sectional regressions by group
    res <- df_scope[
      ,
      {
        # Case 1: month not selected pre-regression (too few rows)
        if (.N < min_firms) {
          list(
            beta_carbon  = NA_real_,
            se_carbon    = NA_real_,
            n_firms      = .N,                 # nrows since model did not run
            error_reason = "pre_sample"
          )
        } else {
          out <- tryCatch(
            {
              mod <- feols(fml, data = .SD)
              
              # Case 2: model ran, but estimation sample fell below threshold
              if (nobs(mod) < min_firms) {
                return(list(
                  beta_carbon  = NA_real_,
                  se_carbon    = NA_real_,
                  n_firms      = nobs(mod),      # nobs(mod) since model ran
                  error_reason = "post_obs"
                ))
              }
              
              cf <- summary(mod)$coeftable
              
              # Model ran successfully; coefficient may or may not be present (e.g., collinearity)
              if (!scope_var %in% rownames(cf)) {
                list(
                  beta_carbon  = NA_real_,
                  se_carbon    = NA_real_,
                  n_firms      = nobs(mod),
                  error_reason = NA_character_
                )
              } else {
                list(
                  beta_carbon  = cf[scope_var, "Estimate"],
                  se_carbon    = cf[scope_var, "Std. Error"],
                  n_firms      = nobs(mod),
                  error_reason = NA_character_
                )
              }
            },
            error = function(e) {
              # Model failed to estimate; treat as "did not run" for n purposes
              list(
                beta_carbon  = NA_real_,
                se_carbon    = NA_real_,
                n_firms      = .N,               # nrows since model did not run
                error_reason = "model_error"     # change to NA_character_ if you only want pre_sample/post_obs
              )
            }
          )
          out
        }
      },
      by = by_cols
    ]
    
    res[, scope := scope_label]
    res[]
  }
  
  # 6) Run for selected scopes
  res_list <- lapply(scope, run_scope)
  names(res_list) <- scope
  res_list <- res_list[!vapply(res_list, is.null, logical(1L))]
  
  if (length(res_list) == 0L) {
    stop("run_ghg_monthly_betas: no valid scope results produced.")
  }
  
  series_long <- rbindlist(res_list, use.names = TRUE, fill = TRUE)
  
  if (group_by_loc) {
    setorder(series_long, loc, scope, ym)
  } else {
    setorder(series_long, scope, ym)
  }
  
  # 7) Wide output via dcast (including n_firms -> n_* and error_reason -> error_reason_*)
  if (group_by_loc) {
    series_wide <- dcast(
      series_long,
      loc + ym ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms", "error_reason")
    )
    setorder(series_wide, loc, ym)
  } else {
    series_wide <- dcast(
      series_long,
      ym ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms", "error_reason")
    )
    setorder(series_wide, ym)
  }
  
  # Rename n_firms_* columns to n_* for clarity
  n_cols <- grep("^n_firms_", names(series_wide), value = TRUE)
  if (length(n_cols) > 0L) {
    setnames(series_wide, n_cols, sub("^n_firms_", "n_", n_cols))
  }
  
  series_wide[]
}

run_ghg_monthly_window_betas <- function(
    df,
    ghg_measure        = c("log", "intensity"),
    scope              = c("S1", "S2", "S3"),
    lag6m              = FALSE,
    ghg_class          = c("full", "estimated", "disclosed"),
    loc_filter_toggle  = FALSE,
    loc_selection      = character(0),
    time_frame_toggle  = FALSE,
    datadate_min       = NULL,
    datadate_max       = NULL,
    ret_col            = "R_m",
    controls           = character(0),
    min_firms          = 30L,
    group_by_loc       = FALSE,
    inv_pref_var       = NULL,
    fixed_effect       = NULL,
    window             = NULL
) {
  library(data.table)
  library(fixest)
  
  ghg_measure <- match.arg(ghg_measure)
  ghg_class   <- match.arg(ghg_class)
  scope       <- match.arg(scope, several.ok = TRUE)
  
  if (!is.null(window)) {
    if (length(window) != 1 || is.na(window) || window < 1) stop("window must be a single positive integer.")
    window <- as.integer(window)
  }
  
  if (is.null(inv_pref_var) || length(inv_pref_var) == 0) {
    inv_pref_var <- NULL
  } else {
    inv_pref_var <- inv_pref_var[nzchar(inv_pref_var)]
    if (length(inv_pref_var) == 0) {
      inv_pref_var <- NULL
    } else if (length(inv_pref_var) > 2) {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  if (is.null(fixed_effect) || length(fixed_effect) == 0) {
    fixed_effect <- NULL
  } else {
    fixed_effect <- fixed_effect[nzchar(fixed_effect)]
    if (length(fixed_effect) == 0) {
      fixed_effect <- NULL
    }
  }
  
  suf <- if (lag6m) "_lag6m" else ""
  
  df <- as.data.table(df)
  
  if (!"ym" %in% names(df)) {
    if ("month" %in% names(df)) {
      df[, ym := format(as.IDate(month), "%Y-%m")]
    } else {
      stop("run_ghg_monthly_betas: 'ym' not in df and no 'month' column to construct it.")
    }
  }
  
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  if (time_frame_toggle) {
    if (!is.null(datadate_min)) df <- df[datadate >= datadate_min]
    if (!is.null(datadate_max)) df <- df[datadate <= datadate_max]
  }
  
  class_cols <- list(
    S1 = paste0("ghg_scope1_class",        suf),
    S2 = paste0("ghg_scope2_loc_class",    suf),
    S3 = paste0("ghg_scope3_up_class",     suf)
  )
  
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1",        suf),
      S2 = paste0("log_ghg_scope2_loc",    suf),
      S3 = paste0("log_ghg_scope3_up",     suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity",     suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity",  suf)
    )
  }
  
  build_scope_term <- function(scope_var, inv_pref_var) {
    if (is.null(inv_pref_var)) {
      scope_var
    } else if (length(inv_pref_var) == 1) {
      paste0(scope_var, " * ", inv_pref_var[1])
    } else if (length(inv_pref_var) == 2) {
      paste0(scope_var, " * ", inv_pref_var[1], " * ", inv_pref_var[2])
    } else {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  run_scope <- function(scope_label) {
    scope_var <- scope_vars[[scope_label]]
    class_col <- class_cols[[scope_label]]
    
    if (!scope_var %in% names(df)) {
      warning("Scope variable not found in df: ", scope_var, ". Skipping scope ", scope_label, ".")
      return(NULL)
    }
    
    df_scope <- copy(df)
    
    if (ghg_class != "full" && class_col %in% names(df_scope)) {
      df_scope <- df_scope[get(class_col) == ghg_class]
    }
    
    df_scope <- df_scope[!is.na(get(scope_var)) & !is.na(get(ret_col))]
    
    if (nrow(df_scope) == 0L) {
      warning("No observations left for scope ", scope_label, " after filtering.")
      return(NULL)
    }
    
    scope_term <- build_scope_term(scope_var, inv_pref_var)
    rhs_terms  <- c(scope_term, controls)
    rhs_str    <- paste(rhs_terms, collapse = " + ")
    
    if (is.null(fixed_effect)) {
      fml <- as.formula(paste(ret_col, "~", rhs_str))
    } else {
      fe_str <- paste(fixed_effect, collapse = " + ")
      fml <- as.formula(paste0(ret_col, " ~ ", rhs_str, " | ", fe_str))
    }
    
    if (is.null(window) || window <= 1L) {
      by_cols <- if (group_by_loc) c("loc", "ym") else "ym"
      
      res <- df_scope[
        ,
        {
          if (.N < min_firms) {
            list(
              beta_carbon  = NA_real_,
              se_carbon    = NA_real_,
              n_firms      = .N,
              error_reason = "pre_sample"
            )
          } else {
            out <- tryCatch(
              {
                mod <- feols(fml, data = .SD)
                if (nobs(mod) < min_firms) {
                  return(list(
                    beta_carbon  = NA_real_,
                    se_carbon    = NA_real_,
                    n_firms      = nobs(mod),
                    error_reason = "post_obs"
                  ))
                }
                cf <- summary(mod)$coeftable
                if (!scope_var %in% rownames(cf)) {
                  list(
                    beta_carbon  = NA_real_,
                    se_carbon    = NA_real_,
                    n_firms      = nobs(mod),
                    error_reason = NA_character_
                  )
                } else {
                  list(
                    beta_carbon  = cf[scope_var, "Estimate"],
                    se_carbon    = cf[scope_var, "Std. Error"],
                    n_firms      = nobs(mod),
                    error_reason = NA_character_
                  )
                }
              },
              error = function(e) {
                list(
                  beta_carbon  = NA_real_,
                  se_carbon    = NA_real_,
                  n_firms      = .N,
                  error_reason = "model_error"
                )
              }
            )
            out
          }
        },
        by = by_cols
      ]
      
      res[, scope := scope_label]
      return(res[])
    }
    
    if (!group_by_loc) {
      ym_vec <- sort(unique(df_scope$ym))
      res_list <- vector("list", length(ym_vec))
      
      for (i in seq_along(ym_vec)) {
        ym_i <- ym_vec[i]
        if (i < window) {
          res_list[[i]] <- data.table(
            ym           = ym_i,
            beta_carbon  = NA_real_,
            se_carbon    = NA_real_,
            n_firms      = NA_integer_,
            error_reason = "pre_window"
          )
          next
        }
        
        win_months <- ym_vec[(i - window + 1L):i]
        dt_win <- df_scope[ym %in% win_months]
        
        if (nrow(dt_win) < min_firms) {
          res_list[[i]] <- data.table(
            ym           = ym_i,
            beta_carbon  = NA_real_,
            se_carbon    = NA_real_,
            n_firms      = nrow(dt_win),
            error_reason = "pre_sample"
          )
          next
        }
        
        out <- tryCatch(
          {
            mod <- feols(fml, data = dt_win)
            if (nobs(mod) < min_firms) {
              return(data.table(
                ym           = ym_i,
                beta_carbon  = NA_real_,
                se_carbon    = NA_real_,
                n_firms      = nobs(mod),
                error_reason = "post_obs"
              ))
            }
            cf <- summary(mod)$coeftable
            if (!scope_var %in% rownames(cf)) {
              data.table(
                ym           = ym_i,
                beta_carbon  = NA_real_,
                se_carbon    = NA_real_,
                n_firms      = nobs(mod),
                error_reason = NA_character_
              )
            } else {
              data.table(
                ym           = ym_i,
                beta_carbon  = cf[scope_var, "Estimate"],
                se_carbon    = cf[scope_var, "Std. Error"],
                n_firms      = nobs(mod),
                error_reason = NA_character_
              )
            }
          },
          error = function(e) {
            data.table(
              ym           = ym_i,
              beta_carbon  = NA_real_,
              se_carbon    = NA_real_,
              n_firms      = nrow(dt_win),
              error_reason = "model_error"
            )
          }
        )
        
        res_list[[i]] <- out
      }
      
      res <- rbindlist(res_list, use.names = TRUE, fill = TRUE)
      res[, scope := scope_label]
      return(res[])
    }
    
    loc_vec <- sort(unique(df_scope$loc))
    out_all <- vector("list", length(loc_vec))
    
    for (j in seq_along(loc_vec)) {
      loc_j <- loc_vec[j]
      dt_loc <- df_scope[loc == loc_j]
      ym_vec <- sort(unique(dt_loc$ym))
      res_list <- vector("list", length(ym_vec))
      
      for (i in seq_along(ym_vec)) {
        ym_i <- ym_vec[i]
        if (i < window) {
          res_list[[i]] <- data.table(
            loc          = loc_j,
            ym           = ym_i,
            beta_carbon  = NA_real_,
            se_carbon    = NA_real_,
            n_firms      = NA_integer_,
            error_reason = "pre_window"
          )
          next
        }
        
        win_months <- ym_vec[(i - window + 1L):i]
        dt_win <- dt_loc[ym %in% win_months]
        
        if (nrow(dt_win) < min_firms) {
          res_list[[i]] <- data.table(
            loc          = loc_j,
            ym           = ym_i,
            beta_carbon  = NA_real_,
            se_carbon    = NA_real_,
            n_firms      = nrow(dt_win),
            error_reason = "pre_sample"
          )
          next
        }
        
        out <- tryCatch(
          {
            mod <- feols(fml, data = dt_win)
            if (nobs(mod) < min_firms) {
              return(data.table(
                loc          = loc_j,
                ym           = ym_i,
                beta_carbon  = NA_real_,
                se_carbon    = NA_real_,
                n_firms      = nobs(mod),
                error_reason = "post_obs"
              ))
            }
            cf <- summary(mod)$coeftable
            if (!scope_var %in% rownames(cf)) {
              data.table(
                loc          = loc_j,
                ym           = ym_i,
                beta_carbon  = NA_real_,
                se_carbon    = NA_real_,
                n_firms      = nobs(mod),
                error_reason = NA_character_
              )
            } else {
              data.table(
                loc          = loc_j,
                ym           = ym_i,
                beta_carbon  = cf[scope_var, "Estimate"],
                se_carbon    = cf[scope_var, "Std. Error"],
                n_firms      = nobs(mod),
                error_reason = NA_character_
              )
            }
          },
          error = function(e) {
            data.table(
              loc          = loc_j,
              ym           = ym_i,
              beta_carbon  = NA_real_,
              se_carbon    = NA_real_,
              n_firms      = nrow(dt_win),
              error_reason = "model_error"
            )
          }
        )
        
        res_list[[i]] <- out
      }
      
      out_all[[j]] <- rbindlist(res_list, use.names = TRUE, fill = TRUE)
    }
    
    res <- rbindlist(out_all, use.names = TRUE, fill = TRUE)
    res[, scope := scope_label]
    res[]
  }
  
  res_list <- lapply(scope, run_scope)
  names(res_list) <- scope
  res_list <- res_list[!vapply(res_list, is.null, logical(1L))]
  
  if (length(res_list) == 0L) {
    stop("run_ghg_monthly_betas: no valid scope results produced.")
  }
  
  series_long <- rbindlist(res_list, use.names = TRUE, fill = TRUE)
  
  if (group_by_loc) {
    setorder(series_long, loc, scope, ym)
  } else {
    setorder(series_long, scope, ym)
  }
  
  if (group_by_loc) {
    series_wide <- dcast(
      series_long,
      loc + ym ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms", "error_reason")
    )
    setorder(series_wide, loc, ym)
  } else {
    series_wide <- dcast(
      series_long,
      ym ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms", "error_reason")
    )
    setorder(series_wide, ym)
  }
  
  n_cols <- grep("^n_firms_", names(series_wide), value = TRUE)
  if (length(n_cols) > 0L) {
    setnames(series_wide, n_cols, sub("^n_firms_", "n_", n_cols))
  }
  
  series_wide[]
}

run_ghg_monthly_window_betas <- function(
    df,
    ghg_measure        = c("log", "intensity"),
    scope              = c("S1", "S2", "S3"),
    lag6m              = FALSE,
    ghg_class          = c("full", "estimated", "disclosed"),
    loc_filter_toggle  = FALSE,
    loc_selection      = character(0),
    time_frame_toggle  = FALSE,
    datadate_min       = NULL,
    datadate_max       = NULL,
    ret_col            = "R_m",
    controls           = character(0),
    min_firms          = 30L,
    group_by_loc       = FALSE,
    inv_pref_var       = NULL,
    fixed_effect       = NULL,
    window             = NULL
) {
  library(data.table)
  library(fixest)
  
  ghg_measure <- match.arg(ghg_measure)
  ghg_class   <- match.arg(ghg_class)
  scope       <- match.arg(scope, several.ok = TRUE)
  
  if (!is.null(window)) {
    if (length(window) != 1 || is.na(window) || window < 1) stop("window must be a single positive integer.")
    window <- as.integer(window)
  }
  
  if (is.null(inv_pref_var) || length(inv_pref_var) == 0) {
    inv_pref_var <- NULL
  } else {
    inv_pref_var <- inv_pref_var[nzchar(inv_pref_var)]
    if (length(inv_pref_var) == 0) {
      inv_pref_var <- NULL
    } else if (length(inv_pref_var) > 2) {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  if (is.null(fixed_effect) || length(fixed_effect) == 0) {
    fixed_effect <- NULL
  } else {
    fixed_effect <- fixed_effect[nzchar(fixed_effect)]
    if (length(fixed_effect) == 0) {
      fixed_effect <- NULL
    }
  }
  
  suf <- if (lag6m) "_lag6m" else ""
  
  df <- as.data.table(df)
  
  if (!"ym" %in% names(df)) {
    if ("month" %in% names(df)) {
      df[, ym := format(as.IDate(month), "%Y-%m")]
    } else {
      stop("run_ghg_monthly_window_betas: 'ym' not in df and no 'month' column to construct it.")
    }
  }
  
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  if (time_frame_toggle) {
    if (!is.null(datadate_min)) df <- df[datadate >= datadate_min]
    if (!is.null(datadate_max)) df <- df[datadate <= datadate_max]
  }
  
  class_cols <- list(
    S1 = paste0("ghg_scope1_class",        suf),
    S2 = paste0("ghg_scope2_loc_class",    suf),
    S3 = paste0("ghg_scope3_up_class",     suf)
  )
  
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1",        suf),
      S2 = paste0("log_ghg_scope2_loc",    suf),
      S3 = paste0("log_ghg_scope3_up",     suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity",     suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity",  suf)
    )
  }
  
  build_scope_term <- function(scope_var, inv_pref_var) {
    if (is.null(inv_pref_var)) {
      scope_var
    } else if (length(inv_pref_var) == 1) {
      paste0(scope_var, " * ", inv_pref_var[1])
    } else if (length(inv_pref_var) == 2) {
      paste0(scope_var, " * ", inv_pref_var[1], " * ", inv_pref_var[2])
    } else {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  run_scope <- function(scope_label) {
    scope_var <- scope_vars[[scope_label]]
    class_col <- class_cols[[scope_label]]
    
    if (!scope_var %in% names(df)) {
      warning("Scope variable not found in df: ", scope_var, ". Skipping scope ", scope_label, ".")
      return(NULL)
    }
    
    df_scope <- copy(df)
    
    if (ghg_class != "full" && class_col %in% names(df_scope)) {
      df_scope <- df_scope[get(class_col) == ghg_class]
    }
    
    df_scope <- df_scope[!is.na(get(scope_var)) & !is.na(get(ret_col))]
    
    if (nrow(df_scope) == 0L) {
      warning("No observations left for scope ", scope_label, " after filtering.")
      return(NULL)
    }
    
    scope_term <- build_scope_term(scope_var, inv_pref_var)
    rhs_terms  <- c(scope_term, controls)
    rhs_str    <- paste(rhs_terms, collapse = " + ")
    
    if (is.null(fixed_effect)) {
      fml <- as.formula(paste(ret_col, "~", rhs_str))
    } else {
      fe_str <- paste(fixed_effect, collapse = " + ")
      fml <- as.formula(paste0(ret_col, " ~ ", rhs_str, " | ", fe_str))
    }
    
    if (is.null(window) || window <= 1L) {
      by_cols <- if (group_by_loc) c("loc", "ym") else "ym"
      
      res <- df_scope[
        ,
        {
          if (.N < min_firms) {
            list(
              beta_carbon  = NA_real_,
              se_carbon    = NA_real_,
              n_firms      = .N,
              error_reason = "pre_sample"
            )
          } else {
            out <- tryCatch(
              {
                mod <- feols(fml, data = .SD)
                if (nobs(mod) < min_firms) {
                  list(
                    beta_carbon  = NA_real_,
                    se_carbon    = NA_real_,
                    n_firms      = nobs(mod),
                    error_reason = "post_obs"
                  )
                } else {
                  cf <- summary(mod)$coeftable
                  if (!scope_var %in% rownames(cf)) {
                    list(
                      beta_carbon  = NA_real_,
                      se_carbon    = NA_real_,
                      n_firms      = nobs(mod),
                      error_reason = NA_character_
                    )
                  } else {
                    list(
                      beta_carbon  = cf[scope_var, "Estimate"],
                      se_carbon    = cf[scope_var, "Std. Error"],
                      n_firms      = nobs(mod),
                      error_reason = NA_character_
                    )
                  }
                }
              },
              error = function(e) {
                list(
                  beta_carbon  = NA_real_,
                  se_carbon    = NA_real_,
                  n_firms      = .N,
                  error_reason = "model_error"
                )
              }
            )
            out
          }
        },
        by = by_cols
      ]
      return(res[])
    }
    
    if (!group_by_loc) {
      ym_vec <- sort(unique(df_scope$ym))
      res_list <- vector("list", length(ym_vec))
      
      for (i in seq_along(ym_vec)) {
        ym_i <- ym_vec[i]
        
        if (i < window) {
          res_list[[i]] <- data.table(
            ym           = ym_i,
            beta_carbon  = NA_real_,
            se_carbon    = NA_real_,
            n_firms      = NA_integer_,
            error_reason = "pre_window"
          )
          next
        }
        
        win_months <- ym_vec[(i - window + 1L):i]
        dt_win <- df_scope[ym %in% win_months]
        
        if (nrow(dt_win) < min_firms) {
          res_list[[i]] <- data.table(
            ym           = ym_i,
            beta_carbon  = NA_real_,
            se_carbon    = NA_real_,
            n_firms      = nrow(dt_win),
            error_reason = "pre_sample"
          )
          next
        }
        
        out <- tryCatch(
          {
            mod <- feols(fml, data = dt_win)
            if (nobs(mod) < min_firms) {
              data.table(
                ym           = ym_i,
                beta_carbon  = NA_real_,
                se_carbon    = NA_real_,
                n_firms      = nobs(mod),
                error_reason = "post_obs"
              )
            } else {
              cf <- summary(mod)$coeftable
              if (!scope_var %in% rownames(cf)) {
                data.table(
                  ym           = ym_i,
                  beta_carbon  = NA_real_,
                  se_carbon    = NA_real_,
                  n_firms      = nobs(mod),
                  error_reason = NA_character_
                )
              } else {
                data.table(
                  ym           = ym_i,
                  beta_carbon  = cf[scope_var, "Estimate"],
                  se_carbon    = cf[scope_var, "Std. Error"],
                  n_firms      = nobs(mod),
                  error_reason = NA_character_
                )
              }
            }
          },
          error = function(e) {
            data.table(
              ym           = ym_i,
              beta_carbon  = NA_real_,
              se_carbon    = NA_real_,
              n_firms      = nrow(dt_win),
              error_reason = "model_error"
            )
          }
        )
        
        res_list[[i]] <- out
      }
      
      res <- rbindlist(res_list, use.names = TRUE, fill = TRUE)
      return(res[])
    }
    
    loc_vec <- sort(unique(df_scope$loc))
    out_all <- vector("list", length(loc_vec))
    
    for (j in seq_along(loc_vec)) {
      loc_j <- loc_vec[j]
      dt_loc <- df_scope[loc == loc_j]
      ym_vec <- sort(unique(dt_loc$ym))
      res_list <- vector("list", length(ym_vec))
      
      for (i in seq_along(ym_vec)) {
        ym_i <- ym_vec[i]
        
        if (i < window) {
          res_list[[i]] <- data.table(
            loc          = loc_j,
            ym           = ym_i,
            beta_carbon  = NA_real_,
            se_carbon    = NA_real_,
            n_firms      = NA_integer_,
            error_reason = "pre_window"
          )
          next
        }
        
        win_months <- ym_vec[(i - window + 1L):i]
        dt_win <- dt_loc[ym %in% win_months]
        
        if (nrow(dt_win) < min_firms) {
          res_list[[i]] <- data.table(
            loc          = loc_j,
            ym           = ym_i,
            beta_carbon  = NA_real_,
            se_carbon    = NA_real_,
            n_firms      = nrow(dt_win),
            error_reason = "pre_sample"
          )
          next
        }
        
        out <- tryCatch(
          {
            mod <- feols(fml, data = dt_win)
            if (nobs(mod) < min_firms) {
              data.table(
                loc          = loc_j,
                ym           = ym_i,
                beta_carbon  = NA_real_,
                se_carbon    = NA_real_,
                n_firms      = nobs(mod),
                error_reason = "post_obs"
              )
            } else {
              cf <- summary(mod)$coeftable
              if (!scope_var %in% rownames(cf)) {
                data.table(
                  loc          = loc_j,
                  ym           = ym_i,
                  beta_carbon  = NA_real_,
                  se_carbon    = NA_real_,
                  n_firms      = nobs(mod),
                  error_reason = NA_character_
                )
              } else {
                data.table(
                  loc          = loc_j,
                  ym           = ym_i,
                  beta_carbon  = cf[scope_var, "Estimate"],
                  se_carbon    = cf[scope_var, "Std. Error"],
                  n_firms      = nobs(mod),
                  error_reason = NA_character_
                )
              }
            }
          },
          error = function(e) {
            data.table(
              loc          = loc_j,
              ym           = ym_i,
              beta_carbon  = NA_real_,
              se_carbon    = NA_real_,
              n_firms      = nrow(dt_win),
              error_reason = "model_error"
            )
          }
        )
        
        res_list[[i]] <- out
      }
      
      out_all[[j]] <- rbindlist(res_list, use.names = TRUE, fill = TRUE)
    }
    
    res <- rbindlist(out_all, use.names = TRUE, fill = TRUE)
    return(res[])
  }
  
  res_list <- lapply(scope, run_scope)
  names(res_list) <- scope
  res_list <- res_list[!vapply(res_list, is.null, logical(1L))]
  if (length(res_list) == 0L) stop("run_ghg_monthly_window_betas: no valid scope results produced.")
  
  for (k in seq_along(res_list)) {
    res_list[[k]] <- as.data.table(res_list[[k]])
    if ("scope" %in% names(res_list[[k]])) res_list[[k]][, scope := NULL]
  }
  
  series_long <- rbindlist(res_list, use.names = TRUE, fill = TRUE, idcol = "scope")
  
  if (group_by_loc) {
    setorder(series_long, loc, scope, ym)
  } else {
    setorder(series_long, scope, ym)
  }
  
  if (group_by_loc) {
    series_wide <- dcast(
      series_long,
      loc + ym ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms", "error_reason")
    )
    setorder(series_wide, loc, ym)
  } else {
    series_wide <- dcast(
      series_long,
      ym ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms", "error_reason")
    )
    setorder(series_wide, ym)
  }
  
  n_cols <- grep("^n_firms_", names(series_wide), value = TRUE)
  if (length(n_cols) > 0L) {
    setnames(series_wide, n_cols, sub("^n_firms_", "n_", n_cols))
  }
  
  series_wide[]
}

## 3) Plotting functions ----
plot_ghg_scatter_faceted <- function(data,
                                     year_select,
                                     ghg_var,
                                     size_var,
                                     gvkeys = NULL,
                                     label_gvkeys = NULL) {
  
  ghg_var_quo  <- rlang::enquo(ghg_var)
  size_var_quo <- rlang::enquo(size_var)
  
  ghg_var_name  <- rlang::quo_name(ghg_var_quo)
  size_var_name <- rlang::quo_name(size_var_quo)
  
  # Axis labels
  make_label <- function(x) {
    x <- gsub("_", " ", x)
    x <- gsub("^log ", "Log ", x)
    x
  }
  x_lab <- make_label(size_var_name)
  y_lab <- make_label(ghg_var_name)
  
  # Ensure data.table
  dt <- data.table::as.data.table(data)
  
  infer_class_col <- function(dt, ghg_var_name) {
    # If user passed a class var by accident or intentionally
    if (ghg_var_name %in% names(dt) && grepl("_class$", ghg_var_name)) {
      return(ghg_var_name)
    }
    
    candidates <- unique(c(
      paste0(ghg_var_name, "_class"),                                   # e.g., ghg_scope1 -> ghg_scope1_class
      sub("_intensity$", "_class", ghg_var_name),           # e.g., ghg_scope1_intensity -> ghg_scope1_class
      sub("_intensity$", "_class", ghg_var_name),                       # generic intensity fallback
      sub("$", "_class", ghg_var_name),                     # e.g., ghg_scope2_loc -> ghg_scope2_loc_class
      sub("_.*$", "_class", ghg_var_name)                   # broader fallback if extra suffixes appear
    ))
    
    candidates <- candidates[candidates %in% names(dt)]
    if (length(candidates) == 0L) {
      stop(
        sprintf(
          "Could not infer a *_class column for ghg_var='%s'. Tried: %s",
          ghg_var_name,
          paste(unique(c(
            paste0(ghg_var_name, "_class"),
            sub("_intensity$", "_class", ghg_var_name),
            sub("_intensity$", "_class", ghg_var_name),
            sub("$", "_class", ghg_var_name),
            sub("_.*$", "_class", ghg_var_name)
          )), collapse = ", ")
        )
      )
    }
    candidates[1]
  }
  
  ghg_var_base <- sub("^log_", "", ghg_var_name)
  
  class_var_name <- infer_class_col(dt, ghg_var_base)
  
  # Year filter
  dt_year <- dt[year == year_select]
  
  # Last observation per gvkey (requires datadate to exist)
  dt_last <- dt_year[
    order(gvkey, datadate),
    .SD[.N],
    by = gvkey
  ]
  
  # Keep only estimated / disclosed (using inferred class column)
  plot_data <- dt_last[
    get(class_var_name) %in% c("estimated", "disclosed")
  ]
  
  if (nrow(plot_data) == 0L) {
    stop("No observations found for this year and class filter.")
  }
  
  # Drop NAs in the chosen variables
  plot_data_clean <- plot_data[
    !is.na(get(size_var_name)) &
      !is.na(get(ghg_var_name))
  ]
  
  # gvkeys to label / highlight
  label_keys <- if (!is.null(label_gvkeys)) label_gvkeys else gvkeys
  
  if (!is.null(label_keys)) {
    points_to_label <- plot_data_clean[gvkey %in% label_keys]
  } else {
    points_to_label <- plot_data_clean[0]
  }
  
  # Base plot
  p <- ggplot2::ggplot(
    plot_data_clean,
    ggplot2::aes(x = !!size_var_quo, y = !!ghg_var_quo)
  ) +
    ggplot2::geom_point(
      alpha = 0.15,
      size  = 1.1,
      color = "#1f78b4"
    ) +
    ggplot2::facet_wrap(
      ggplot2::vars(!!rlang::sym(class_var_name)),
      nrow = 1,
      scales = "fixed"
    ) +
    ggplot2::labs(
      x = x_lab,
      y = y_lab,
      title = paste0(
        "Scatterplot of ", y_lab, " vs ", x_lab, " (", year_select, ")"
      )
    ) +
    ggplot2::theme_minimal(base_size = 11) +
    ggplot2::theme(
      panel.grid.minor = ggplot2::element_blank(),
      panel.grid.major = ggplot2::element_line(linewidth = 0.3, colour = "grey90"),
      panel.border     = ggplot2::element_rect(colour = "black", fill = NA, linewidth = 0.3),
      strip.text       = ggplot2::element_text(face = "bold"),
      plot.title       = ggplot2::element_text(face = "bold", hjust = 0.5),
      axis.title       = ggplot2::element_text(face = "plain"),
      axis.text        = ggplot2::element_text(colour = "black")
    )
  
  # Highlight + label selected gvkeys
  if (nrow(points_to_label) > 0L) {
    p <- p +
      ggrepel::geom_label_repel(
        data = points_to_label,
        ggplot2::aes(
          x = !!size_var_quo,
          y = !!ghg_var_quo,
          label = conm
        ),
        inherit.aes        = FALSE,
        color              = "black",
        fill               = scales::alpha("white", 0.85),
        size               = 4,
        point.padding      = 0.5,
        box.padding        = 0.8,
        min.segment.length = 0,
        seed               = 69
      ) +
      ggplot2::geom_point(
        data = points_to_label,
        ggplot2::aes(x = !!size_var_quo, y = !!ghg_var_quo),
        inherit.aes = FALSE,
        color       = "black",
        size        = 2
      )
  }
  
  p
}

plot_country_timeseries <- function(var_name,
                                    data = df_country_year,
                                    country_var = "loc",
                                    time_var = "year") {
  
  stopifnot(var_name %in% names(data))
  
  ggplot(
    data,
    aes_string(
      x = time_var,
      y = var_name,
      group = country_var,
      color = country_var
    )
  ) +
    geom_line(alpha = 0.7, linewidth = 0.6) +
    labs(
      title = paste("Time Series of", var_name, "by Country"),
      x = "Year",
      y = var_name
    ) +
    theme_minimal() +
    theme(
      legend.position = "none",
      plot.title = element_text(face = "bold")
    )
}

plot_country_timeseries_highlight <- function(var_name,
                                              highlight_countries = NULL,
                                              first_year = NULL,
                                              data = df_country_year,
                                              country_var = "loc",
                                              time_var = "year") {
  
  ## helper: filter only what is plotted (data + labels + scales), leave source `data` untouched
  .apply_plot_filter <- function(dt, var_for_na = NULL) {
    if (is.null(first_year)) return(dt)
    stopifnot(is.numeric(first_year), length(first_year) == 1L, !is.na(first_year))
    fy <- as.integer(first_year)
    out <- dt[get(time_var) >= fy]
    if (!is.null(var_for_na)) out <- out[!is.na(get(var_for_na))]
    out
  }
  
  ## original single-variable implementation
  .single_impl <- function(var_name_single) {
    
    stopifnot(var_name_single %in% names(data))
    stopifnot(country_var %in% names(data))
    stopifnot(time_var %in% names(data))
    stopifnot("country"   %in% names(data))
    
    dt <- as.data.table(data)
    
    dt[, highlight := fifelse(
      get(country_var) %in% highlight_countries,
      "highlight", "other"
    )]
    
    ## compute times on full data (unchanged behavior)
    ts_info <- dt[!is.na(get(var_name_single)),
                  .(
                    n_obs   = uniqueN(get(time_var)),
                    last_yr = max(get(time_var))
                  ),
                  by = country_var]
    
    ts_info   <- ts_info[order(-n_obs, -last_yr)]
    last_time <- as.integer(ts_info$last_yr[1])
    
    first_time <- dt[!is.na(get(var_name_single)), min(get(time_var), na.rm = TRUE)]
    first_time <- as.integer(first_time)
    
    ## plotting data filtered only for output
    dt_plot <- .apply_plot_filter(dt)
    
    ## labels should respect the plot filter (otherwise you'd label points not shown)
    label_dt <- dt_plot[
      highlight == "highlight" &
        !is.na(get(var_name_single)) &
        get(time_var) <= last_time
    ][
      order(get(time_var)),
      .SD[.N],
      by = country_var
    ]
    
    label_dt[, label_name := country]
    
    extra_space <- 3
    x_max       <- last_time + extra_space
    
    ## x-scale limits should reflect the plot filter on the left edge, but keep original last_time
    first_time_plot <- first_time
    if (!is.null(first_year)) first_time_plot <- max(first_time_plot, as.integer(first_year))
    
    x_scale <- scale_x_continuous(
      limits = c(first_time_plot, x_max),
      breaks = seq(first_time_plot, last_time, by = 1),
      expand = c(0, 0)
    )
    
    y_range  <- range(dt_plot[[var_name_single]], na.rm = TRUE)
    y_breaks <- pretty(y_range, n = 6)
    
    y_scale <- scale_y_continuous(
      breaks = y_breaks,
      expand = expansion(mult = c(0.02, 0.05))
    )
    
    highlight_levels <- sort(unique(label_dt[[country_var]]))
    n <- length(highlight_levels)
    pal_vals <- rcartocolor::carto_pal(n = n+1, name = "Bold")
    pal_vals <- pal_vals[1:n]
    
    ggplot(
      dt_plot,
      aes_string(
        x = time_var,
        y = var_name_single,
        group = country_var
      )
    ) +
      geom_segment(
        data = data.frame(y = y_breaks),
        inherit.aes = FALSE,
        aes(x = first_time_plot, xend = last_time, y = y, yend = y),
        color = "grey90",
        linewidth = 0.5
      ) +
      geom_line(
        data      = dt_plot[highlight == "other"],
        color     = "grey80",
        alpha     = 0.5,
        linewidth = 0.6
      ) +
      geom_line(
        data = dt_plot[highlight == "highlight"],
        aes(color = .data[[country_var]]),
        linewidth = 1.1
      ) +
      geom_text_repel(
        data = label_dt,
        aes(
          label = label_name,
          color = .data[[country_var]]
        ),
        direction          = "y",
        hjust              = 0,
        xlim = c(last_time + 0.6, NA),
        size               = 3.3,
        fontface           = "bold",
        segment.size       = 0.5,
        segment.alpha      = 0.7,
        segment.linetype   = "dotted",
        min.segment.length = 0,
        show.legend        = FALSE
      ) +
      geom_segment(
        inherit.aes = FALSE,
        x    = first_time_plot,
        xend = last_time,
        y    = -Inf,
        yend = -Inf,
        linewidth = 0.6
      ) +
      scale_color_manual(values = setNames(pal_vals, highlight_levels)) +
      x_scale +
      y_scale +
      labs(
        title = paste("Time Series of", var_name_single),
        x = "Year",
        y = var_name_single
      ) +
      theme_classic(base_size = 10) +
      theme(
        legend.position      = "none",
        plot.title           = element_text(hjust = 0.5, face = "bold"),
        panel.grid.major.y   = element_blank(),
        panel.grid.major.x   = element_blank(),
        panel.grid.minor     = element_blank(),
        axis.line.y          = element_line(linewidth = 0.6),
        axis.line.x          = element_blank(),
        axis.ticks           = element_line(linewidth = 0.7),
        plot.margin          = margin(6, 15, 6, 6)
      ) +
      coord_cartesian(clip = "off")
  }
  
  ## main dispatch
  if (length(var_name) == 1L) {
    return(.single_impl(var_name[1]))
  }
  
  if (length(var_name) != 2L) {
    stop("var_name must be length 1 or 2.")
  }
  
  # two variables: shared x-axis across both panels
  dt_all <- as.data.table(data)
  dt_all[, highlight := fifelse(
    get(country_var) %in% highlight_countries,
    "highlight", "other"
  )]
  
  ## compute shared x-range on full data (unchanged behavior)
  get_first <- function(v) {
    as.integer(dt_all[!is.na(get(v)), min(get(time_var), na.rm = TRUE)])
  }
  get_last <- function(v) {
    ts_info <- dt_all[!is.na(get(v)),
                      .(
                        n_obs   = uniqueN(get(time_var)),
                        last_yr = max(get(time_var))
                      ),
                      by = country_var]
    ts_info <- ts_info[order(-n_obs, -last_yr)]
    as.integer(ts_info$last_yr[1])
  }
  
  first_times <- vapply(var_name, get_first, integer(1))
  last_times  <- vapply(var_name, get_last,  integer(1))
  
  global_first <- min(first_times)
  global_last  <- max(last_times)
  
  ## apply plot-only left bound if requested
  global_first_plot <- global_first
  if (!is.null(first_year)) {
    stopifnot(is.numeric(first_year), length(first_year) == 1L, !is.na(first_year))
    global_first_plot <- max(global_first_plot, as.integer(first_year))
  }
  
  extra_space  <- 3
  x_max_global <- global_last + extra_space
  
  shared_x_scale <- scale_x_continuous(
    limits = c(global_first_plot, x_max_global),
    breaks = seq(global_first_plot, global_last, by = 1),
    expand = c(0, 0)
  )
  
  .plot_two <- function(var_name_single, show_x_axis) {
    
    vn <- var_name_single
    
    dt <- copy(dt_all)
    
    ts_info <- dt[!is.na(get(vn)),
                  .(
                    n_obs   = uniqueN(get(time_var)),
                    last_yr = max(get(time_var))
                  ),
                  by = country_var]
    
    ts_info <- ts_info[order(-n_obs, -last_yr)]
    last_time_local <- as.integer(ts_info$last_yr[1])
    
    ## plot-only filtering
    dt_plot <- .apply_plot_filter(dt)
    
    label_dt <- dt_plot[
      highlight == "highlight" &
        !is.na(get(vn)) &
        get(time_var) <= last_time_local
    ][
      order(get(time_var)),
      .SD[.N],
      by = country_var
    ]
    
    label_dt[, label_name := country]
    
    y_range  <- range(dt_plot[[vn]], na.rm = TRUE)
    y_breaks <- pretty(y_range, n = 6)
    
    y_scale <- scale_y_continuous(
      breaks = y_breaks,
      expand = expansion(mult = c(0.02, 0.05))
    )
    
    highlight_levels <- sort(unique(label_dt[[country_var]]))
    n <- length(highlight_levels)
    pal_vals <- rcartocolor::carto_pal(n = n+1, name = "Bold")
    pal_vals <- pal_vals[1:n]
    
    p <- ggplot(
      dt_plot,
      aes_string(
        x = time_var,
        y = vn,
        group = country_var
      )
    ) +
      geom_segment(
        data = data.frame(y = y_breaks),
        inherit.aes = FALSE,
        aes(x = global_first_plot, xend = global_last, y = y, yend = y),
        color = "grey90",
        linewidth = 0.5
      ) +
      geom_line(
        data      = dt_plot[highlight == "other"],
        color     = "grey80",
        alpha     = 0.5,
        linewidth = 0.6
      ) +
      geom_line(
        data = dt_plot[highlight == "highlight"],
        aes(color = .data[[country_var]]),
        linewidth = 1.1
      ) +
      geom_text_repel(
        data = label_dt,
        aes(
          label = label_name,
          color = .data[[country_var]]
        ),
        direction          = "y",
        hjust              = 0,
        xlim               = c(global_last + 0.6, NA),
        size               = 3.3,
        fontface           = "bold",
        segment.size       = 0.5,
        segment.alpha      = 0.7,
        segment.linetype   = "dotted",
        min.segment.length = 0,
        show.legend        = FALSE
      ) +
      geom_segment(
        inherit.aes = FALSE,
        x    = global_first_plot,
        xend = global_last,
        y    = -Inf,
        yend = -Inf,
        linewidth = 0.6
      ) +
      scale_color_manual(values = setNames(pal_vals, highlight_levels)) +
      shared_x_scale +
      y_scale +
      labs(
        title = paste("Time Series of", vn),
        x = if (show_x_axis) "Year" else NULL,
        y = vn
      ) +
      theme_classic(base_size = 10) +
      theme(
        legend.position      = "none",
        plot.title           = element_text(hjust = 0.5, face = "bold"),
        panel.grid.major.y   = element_blank(),
        panel.grid.major.x   = element_blank(),
        panel.grid.minor     = element_blank(),
        axis.line.y          = element_line(linewidth = 0.6),
        axis.line.x          = element_blank(),
        axis.ticks           = element_line(linewidth = 0.7),
        plot.margin          = margin(6, 15, 6, 6)
      ) +
      coord_cartesian(clip = "off")
    
    if (!show_x_axis) {
      p <- p +
        theme(
          axis.title.x = element_blank(),
          axis.text.x  = element_blank(),
          axis.ticks.x = element_blank()
        )
    }
    
    p
  }
  
  library(patchwork)
  .plot_two(var_name[1], show_x_axis = FALSE) /
    .plot_two(var_name[2], show_x_axis = TRUE)
}

plot_country_boxplots <- function(
    dt,
    value_var,
    start_year,
    end_year,
    country_col = "country",
    loc_col     = "loc",
    order_by    = c("median", "end"),
    show_points = TRUE,
    title       = NULL,
    subtitle    = NULL
) {
  
  # 0. Basic argument handling
  order_by <- match.arg(order_by)
  
  dt  <- as.data.table(dt)
  yrs <- seq.int(start_year, end_year)
  
  # Fallback labels
  if (is.null(title)) {
    title <- sprintf(
      "Distribution of %s by country (%d–%d)",
      value_var, start_year, end_year
    )
  }
  
  if (is.null(subtitle)) {
    subtitle <- if (order_by == "median") {
      "Countries ordered by median value over the selected years"
    } else {
      sprintf("Countries ordered by value in %d", end_year)
    }
  }
  
  # 1. Subset timeframe, drop NA loc / country / value
  dt_sub <- dt[
    year %in% yrs &
      !is.na(get(loc_col)) &
      !is.na(get(country_col)) &
      !is.na(get(value_var)),
    .(
      country = get(country_col),
      year,
      value  = get(value_var)
    )
  ]
  
  # Handle edge case: nothing to plot
  if (nrow(dt_sub) == 0L) {
    stop("No data available for the specified years and filters.", call. = FALSE)
  }
  
  # 2. Ordering logic
  if (order_by == "median") {
    ord <- dt_sub[
      ,
      .(med = median(value, na.rm = TRUE)),
      by = country
    ][
      order(med),
      country
    ]
  } else {
    dt_end <- dt_sub[year == end_year]
    
    if (nrow(dt_end) == 0L) {
      stop(
        sprintf("No data available for end_year = %d.", end_year),
        call. = FALSE
      )
    }
    
    ord <- dt_end[order(value), country]
  }
  
  dt_sub[, country := factor(country, levels = ord)]
  
  # 3. Plot
  p <- ggplot(dt_sub, aes(x = value, y = country)) +
    geom_boxplot(
      outlier.alpha = 0.4,
      linewidth     = 0.4,
      width         = 0.7
    ) +
    labs(
      x        = value_var,
      y        = NULL,
      title    = title,
      subtitle = subtitle
    ) +
    theme_classic(base_size = 10) +
    theme(
      #panel.grid.major.y = element_blank(),
      panel.grid.minor   = element_blank(),
      panel.grid.major.x = element_line(linewidth = 0.25, colour = "grey85"),
      axis.ticks.y       = element_blank(),
      plot.title         = element_text(face = "bold"),
      plot.subtitle      = element_text(margin = margin(b = 8))
    )
  
  # 4. Optional: overlay all yearly points (jittered)
  if (show_points) {
    p <- p +
      geom_jitter(
        height = 0.15,
        size   = 0.7,
        alpha  = 0.4
      )
  }
  
  p
}


plot_country_scatter <- function(dt,
                                 var_x,
                                 var_y,
                                 start_year,
                                 end_year,
                                 agg_fun    = c("median", "mean", "full"),
                                 loc_col    = "loc",
                                 group_col  = NULL,
                                 highlight_iso3 = NULL,
                                 group_facet_toggle = FALSE) {
  
  library(data.table)
  library(ggplot2)
  library(ggrepel)
  
  agg_fun <- match.arg(agg_fun)
  dt <- as.data.table(dt)
  
  make_label <- function(x) {
    x <- gsub("_", " ", x)
    x <- gsub("^log ", "Log ", x)
    x
  }
  x_lab <- make_label(var_x)
  y_lab <- make_label(var_y)
  
  if (!isFALSE(group_facet_toggle) && is.null(group_col)) {
    warning("group_facet_toggle is TRUE but group_col is NULL; faceting skipped.")
    group_facet_toggle <- FALSE
  }
  
  cols_needed <- c("year", loc_col, var_x, var_y, group_col)
  cols_needed <- cols_needed[!is.na(cols_needed)]
  missing_cols <- setdiff(cols_needed, names(dt))
  if (length(missing_cols) > 0L) {
    stop("Missing columns: ", paste(missing_cols, collapse = ", "))
  }
  
  dt_sub <- dt[
    year >= start_year & year <= end_year,
    ..cols_needed
  ]
  
  setnames(dt_sub, old = loc_col, new = "loc")
  setnames(dt_sub, old = var_x,   new = "x")
  setnames(dt_sub, old = var_y,   new = "y")
  if (!is.null(group_col)) setnames(dt_sub, old = group_col, new = "group")
  
  if (agg_fun %in% c("median", "mean")) {
    fun <- if (agg_fun == "median") median else mean
    
    # KEY CHANGE: restrict to rows where BOTH x and y are observed,
    # so the summary is computed on the same set of years for x and y.
    if (!is.null(group_col)) {
      dt_plot <- dt_sub[!is.na(x) & !is.na(y),
                        .(
                          x = fun(x),
                          y = fun(y)
                        ),
                        by = .(loc, group)
      ]
    } else {
      dt_plot <- dt_sub[!is.na(x) & !is.na(y),
                        .(
                          x = fun(x),
                          y = fun(y)
                        ),
                        by = loc
      ]
    }
  } else {
    # full panel: keep only complete (x,y) rows so points are comparable
    dt_plot <- dt_sub[!is.na(x) & !is.na(y)]
  }
  
  # (Optional) drop groups with no observations after filtering
  dt_plot <- dt_plot[!is.na(x) & !is.na(y)]
  
  if (!is.null(highlight_iso3)) {
    dt_plot[, highlighted := loc %in% highlight_iso3]
  } else {
    dt_plot[, highlighted := FALSE]
  }
  
  dt_plot[, label_alpha := ifelse(highlighted, 1, 0.6)]
  dt_plot[, label_font  := ifelse(highlighted, "bold", "plain")]
  
  group_colors <- c(
    "Europe"                = "#4E79A7",
    "Americas"              = "#F28E2B",
    "Asia-Pacific"          = "#E15759",
    "Middle East & Africa"  = "#76B7B2",
    "Offshore"              = "#59A14F",
    "Other"                 = "grey60"
  )
  
  p <- ggplot(dt_plot, aes(x = x, y = y)) +
    {
      if (!is.null(group_col)) {
        geom_point(aes(color = group), size = 2.3, alpha = 0.8)
      } else {
        geom_point(size = 2.3, alpha = 0.6, color = "grey60")
      }
    } +
    {
      if (!is.null(group_col)) {
        scale_color_manual(values = group_colors)
      }
    } +
    geom_smooth(method = "lm", se = FALSE, linewidth = 0.6, color = "darkred", alpha = 0.7) +
    ggrepel::geom_text_repel(
      aes(label = loc, alpha = label_alpha, fontface = label_font),
      size               = 3,
      box.padding        = 0.15,
      point.padding      = 0.05,
      segment.size       = 0.25,
      min.segment.length = 0,
      show.legend        = FALSE
    ) +
    scale_alpha_identity() +
    theme_classic() +
    theme(
      axis.line        = element_blank(),
      panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
      strip.background = element_rect(color = "black", fill = "grey90"),
      legend.position  = "top"
    ) +
    labs(
      x = x_lab,
      y = y_lab,
      title = paste0(
        "Scatterplot of ", y_lab, " vs ", x_lab,
        " (", start_year, "–", end_year, ", ", agg_fun, ", complete-case years)"
      ),
      color = if (!is.null(group_col)) "Group" else NULL
    )
  
  if (isTRUE(group_facet_toggle)) {
    p <- p +
      facet_wrap(~group) +
      guides(color = "none")
  }
  
  p
}

plot_ghg_betas <- function(
    ts_dt,                  # output of run_ghg_monthly_betas()
    scope           = "S1", # "S1", "S2", "S3"
    loc_selection   = NULL, # optional vector of locs to keep (if loc exists)
    plot_all_locs   = FALSE,# if TRUE and loc exists -> facets by loc
    plot_date_min   = NULL, # optional Date lower bound (applied AFTER MA)
    plot_date_max   = NULL, # optional Date upper bound (applied AFTER MA)
    ci_mult         = 1.96, # CI multiplier
    facet_ncol      = 4,    # number of facet columns when plot_all_locs = TRUE
    facet_scales    = "free_y",
    base_col        = "#00BFE9",  # single base color for single/facetted plots
    zero_linewidth  = 0.4,        # linewidth of y = 0 reference line
    ma_window       = NULL,       # moving-average window (months); if NULL or <= 1, no MA
    events          = NULL,       # optional named vector: names = "YYYY-MM-DD", values = labels
    election_periods = NULL       # optional data.frame with cols: start, end, label
) {
  # expects: data.table, ggplot2, zoo or data.table's frollmean
  
  dt <- as.data.table(ts_dt)
  
  # 1) Check that the requested scope exists
  beta_col <- paste0("beta_carbon_", scope)
  se_col   <- paste0("se_carbon_", scope)
  
  if (!beta_col %in% names(dt) || !se_col %in% names(dt)) {
    stop("Missing beta/se columns for scope ", scope,
         ". Expected columns: '", beta_col, "' and '", se_col, "'.")
  }
  
  # 2) Ensure we have a Date for plotting: ym_date
  if (!"ym_date" %in% names(dt)) {
    if (!"ym" %in% names(dt)) {
      stop("ts_dt must contain either 'ym' (YYYY-MM) or 'ym_date'.")
    }
    dt[, ym_date := as.Date(paste0(ym, "-01"))]
  } else {
    dt[, ym_date := as.Date(ym_date)]
  }
  
  # 3) Optional loc filtering (if loc exists)
  has_loc <- "loc" %in% names(dt)
  
  if (has_loc && !is.null(loc_selection) && length(loc_selection) > 0L) {
    dt <- dt[loc %in% loc_selection]
    if (nrow(dt) == 0L) {
      stop("No rows left after filtering on loc_selection = ",
           paste(loc_selection, collapse = ", "))
    }
  }
  
  # 4) Build plotting data for chosen scope and compute CI on FULL series
  if (has_loc) {
    plot_dt <- dt[, .(
      ym_date,
      loc,
      beta = get(beta_col),
      se   = get(se_col)
    )]
  } else {
    plot_dt <- dt[, .(
      ym_date,
      beta = get(beta_col),
      se   = get(se_col)
    )]
  }
  
  plot_dt[, `:=`(
    ci_low  = beta - ci_mult * se,
    ci_high = beta + ci_mult * se
  )]
  
  plot_dt <- plot_dt[!is.na(beta)]
  if (nrow(plot_dt) == 0L) {
    stop("No non-NA beta values available to plot for scope ", scope, ".")
  }
  
  # 5) Optional moving average (trailing, right-aligned) on FULL series
  use_ma <- !is.null(ma_window) && ma_window > 1L
  
  if (use_ma) {
    if (has_loc) {
      setorder(plot_dt, loc, ym_date)
      plot_dt[, ma_beta := frollmean(beta, n = ma_window, align = "right"), by = loc]
    } else {
      setorder(plot_dt, ym_date)
      plot_dt[, ma_beta := frollmean(beta, n = ma_window, align = "right")]
    }
  }
  
  # 6) Apply optional plotting time window (after MA)
  if (!is.null(plot_date_min)) {
    plot_dt <- plot_dt[ym_date >= as.Date(plot_date_min)]
  }
  if (!is.null(plot_date_max)) {
    plot_dt <- plot_dt[ym_date <= as.Date(plot_date_max)]
  }
  
  if (nrow(plot_dt) == 0L) {
    stop("No observations left in the requested date range.")
  }
  
  # 6b) Build events data (vertical lines), restricted to the plotting window
  event_dt <- NULL
  if (!is.null(events) && length(events) > 0L) {
    ev_dates <- as.Date(names(events))
    
    event_dt <- data.table(
      ym_date = ev_dates,
      label   = as.character(events)
    )
    
    event_dt <- event_dt[
      ym_date >= min(plot_dt$ym_date, na.rm = TRUE) &
        ym_date <= max(plot_dt$ym_date, na.rm = TRUE)
    ]
    
    if (nrow(event_dt) == 0L) {
      event_dt <- NULL
    }
  }
  
  # 7) Helpers: zero line, theme & layers
  
  add_zero_line <- function() {
    geom_hline(
      yintercept = 0,
      linewidth  = zero_linewidth,
      colour     = "grey30",
      alpha      = 0.7
    )
  }
  
  nice_theme <- function(base_size = 10, show_legend = FALSE) {
    theme_classic(base_size = base_size) +
      theme(
        plot.title        = element_text(face = "bold", hjust = 0),
        strip.text        = element_text(face = "bold", size = base_size - 1),
        axis.text.x       = element_text(size = base_size - 2),
        axis.text.y       = element_text(size = base_size - 2),
        legend.position   = if (show_legend) "bottom" else "none",
        panel.grid.minor  = element_blank(),
        panel.spacing.y   = unit(0.25, "lines"),
        plot.margin       = margin(5.5, 15, 5.5, 5.5, "pt")
      )
  }
  
  # background presidential intervals (rectangles)
  interval_rect_layer <- function() {
    if (is.null(election_periods)) return(NULL)
    
    rect_dt <- as.data.table(election_periods)
    if (!all(c("start", "end", "label") %in% names(rect_dt))) {
      stop("election_periods must have columns: 'start', 'end', 'label'.")
    }
    
    rect_dt[, `:=`(
      start = as.Date(start),
      end   = as.Date(end)
    )]
    
    rect_dt <- rect_dt[
      end   >= min(plot_dt$ym_date, na.rm = TRUE) &
        start <= max(plot_dt$ym_date, na.rm = TRUE)
    ]
    
    if (nrow(rect_dt) == 0L) return(NULL)
    
    # Colors by president: Trump = dark red, Biden = dark blue, others = grey
    rect_dt[, fill_col := fcase(
      grepl("Trump", label, ignore.case = TRUE), "#8B0000",
      grepl("Biden", label, ignore.case = TRUE), "#00008B",
      default = "grey50"
    )]
    
    geom_rect(
      data        = rect_dt,
      aes(xmin = start, xmax = end, ymin = -Inf, ymax = Inf, fill = fill_col),
      alpha       = 0.08,
      inherit.aes = FALSE,
      colour      = NA
    )
  }
  
  # foreground labels for presidential intervals (bottom)
  add_interval_labels <- function(p) {
    if (is.null(election_periods)) return(p)
    
    rect_dt <- as.data.table(election_periods)
    if (!all(c("start", "end", "label") %in% names(rect_dt))) return(p)
    
    rect_dt[, `:=`(
      start = as.Date(start),
      end   = as.Date(end)
    )]
    
    rect_dt <- rect_dt[
      end   >= min(plot_dt$ym_date, na.rm = TRUE) &
        start <= max(plot_dt$ym_date, na.rm = TRUE)
    ]
    
    if (nrow(rect_dt) == 0L) return(p)
    
    p +
      scale_fill_identity() +   # interpret fill_col hex directly
      geom_text(
        data  = rect_dt,
        aes(x = start, y = -Inf, label = label),
        vjust = -0.5,   # slightly above bottom axis
        hjust = -0.1,
        size  = 3
      )
  }
  
  # vertical event lines
  add_event_layers <- function(p) {
    if (is.null(event_dt)) return(p)
    
    p +
      geom_vline(
        data     = event_dt,
        aes(xintercept = ym_date),
        linetype  = "dashed",
        linewidth = 0.4,
        colour    = "grey20",
        alpha     = 0.7
      ) +
      geom_text(
        data  = event_dt,
        aes(x = ym_date, y = Inf, label = label),
        angle = 90,
        vjust = 1.1,
        hjust = 1,
        size  = 2.8
      )
  }
  
  # 8) Helper: single-series plot (no loc or single loc)
  make_single_plot <- function(d, loc_label = NULL) {
    
    p <- ggplot(d, aes(x = ym_date)) +
      interval_rect_layer() +   # background presidential intervals
      add_zero_line() +
      geom_ribbon(
        aes(ymin = ci_low, ymax = ci_high),
        fill      = base_col,
        alpha     = 0.1,
        linewidth = 0
      ) +
      geom_line(
        aes(y = ci_low),
        colour    = base_col,
        alpha     = 0.45,
        linewidth = 0.3
      ) +
      geom_line(
        aes(y = ci_high),
        colour    = base_col,
        alpha     = 0.45,
        linewidth = 0.3
      ) +
      geom_line(
        aes(y = beta),
        colour    = base_col,
        alpha     = 0.95,
        linewidth = 0.8
      )
    
    if (use_ma && "ma_beta" %in% names(d)) {
      p <- p +
        geom_line(
          aes(y = ma_beta),
          colour    = "grey15",
          linewidth = 1,
          linetype  = "solid"
        )
    }
    
    p <- p +
      scale_x_date(date_breaks = "1 year", date_labels = "%y") +
      labs(
        x = "Date",
        y = paste0("Carbon beta (", scope, ")"),
        title = if (is.null(loc_label)) {
          paste0("Monthly carbon beta for ", scope)
        } else {
          paste0("Monthly carbon beta for ", scope, " (loc = ", loc_label, ")")
        }
      )
    
    p <- add_event_layers(p)
    p <- add_interval_labels(p)
    
    p + nice_theme(base_size = 10, show_legend = FALSE)
  }
  
  # 9) Case A: no loc dimension -> single-series plot
  if (!has_loc) {
    return(make_single_plot(plot_dt))
  }
  
  # 10) Case B: loc exists & plot_all_locs = TRUE -> facets by loc
  if (plot_all_locs) {
    
    p <- ggplot(plot_dt, aes(x = ym_date)) +
      interval_rect_layer() +   # background
      add_zero_line() +
      geom_ribbon(
        aes(ymin = ci_low, ymax = ci_high),
        fill      = base_col,
        alpha     = 0.12,
        linewidth = 0
      ) +
      geom_line(
        aes(y = ci_low),
        colour    = base_col,
        alpha     = 0.45,
        linewidth = 0.3
      ) +
      geom_line(
        aes(y = ci_high),
        colour    = base_col,
        alpha     = 0.45,
        linewidth = 0.3
      ) +
      geom_line(
        aes(y = beta),
        colour    = base_col,
        alpha     = 0.95,
        linewidth = 0.8
      )
    
    if (use_ma && "ma_beta" %in% names(plot_dt)) {
      p <- p +
        geom_line(
          aes(y = ma_beta),
          colour    = "grey15",
          linewidth = 1,
          linetype  = "solid"
        )
    }
    
    p <- p +
      facet_wrap(~ loc, ncol = facet_ncol, scales = facet_scales) +
      scale_x_date(date_breaks = "1 year", date_labels = "%y") +
      labs(
        x = "Date",
        y = paste0("Carbon beta (", scope, ")"),
        title = paste0("Monthly carbon beta for ", scope, " by location")
      )
    
    p <- add_event_layers(p)
    p <- add_interval_labels(p)
    
    p + nice_theme(base_size = 9, show_legend = FALSE)
    
  } else {
    # 11) Case C: loc exists & plot_all_locs = FALSE
    #      - if one loc -> single-series style
    #      - if multiple locs -> combined panel colored by loc
    uniq_locs <- sort(unique(plot_dt$loc))
    
    if (length(uniq_locs) == 1L) {
      return(make_single_plot(plot_dt, loc_label = uniq_locs))
    }
    
    p <- ggplot(plot_dt,
                aes(x = ym_date, y = beta, colour = loc, fill = loc, group = loc)) +
      interval_rect_layer() +   # background
      add_zero_line() +
      geom_ribbon(
        aes(ymin = ci_low, ymax = ci_high),
        alpha     = 0.12,
        linewidth = 0
      ) +
      geom_line(linewidth = 0.8, alpha = 0.95)
    
    if (use_ma && "ma_beta" %in% names(plot_dt)) {
      p <- p +
        geom_line(
          aes(y = ma_beta),
          linewidth = 1,
          linetype  = "solid"
        )
    }
    
    p <- p +
      scale_x_date(date_breaks = "1 year", date_labels = "%y") +
      labs(
        x = "Date",
        y = paste0("Carbon beta (", scope, ")"),
        color = "Location",
        fill  = "Location",
        title = paste0("Monthly carbon beta for ", scope, " by location")
      )
    
    p <- add_event_layers(p)
    p <- add_interval_labels(p)
    
    p + nice_theme(base_size = 10, show_legend = TRUE)
  }
}

plot_ghg_betas_stacked <- function(
    ts_dt_top,
    ts_dt_bottom,
    
    # text above each plot
    text_top    = NULL,
    text_bottom = NULL,
    
    # optional global title
    title = NULL,
    
    # x-axis label (BOTTOM plot only)
    x_label = "Date",
    
    # toggle bottom event labels
    show_bottom_event_labels = FALSE,
    
    # layout controls
    text_height     = 0.10,
    collect_guides  = TRUE,
    text_align      = c("left", "center", "right"),
    text_size       = 10,
    text_fontface   = "plain",
    text_xpad_npc   = 0.02,
    
    ...
) {
  if (!requireNamespace("patchwork", quietly = TRUE)) {
    stop("Package 'patchwork' is required.")
  }
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop("Package 'ggplot2' is required.")
  }
  
  text_align <- match.arg(text_align)
  
  make_text_strip <- function(txt) {
    if (is.null(txt) || !nzchar(txt)) return(patchwork::plot_spacer())
    
    x <- switch(text_align,
                left   = text_xpad_npc,
                center = 0.5,
                right  = 1 - text_xpad_npc
    )
    just <- switch(text_align,
                   left   = "left",
                   center = "center",
                   right  = "right"
    )
    
    patchwork::wrap_elements(
      grid::textGrob(
        label = txt,
        x = grid::unit(x, "npc"),
        just = just,
        gp = grid::gpar(fontsize = text_size, fontface = text_fontface)
      )
    )
  }
  
  remove_event_label_text_layers <- function(p) {
    built <- ggplot2::ggplot_build(p)
    keep <- rep(TRUE, length(p$layers))
    
    for (i in seq_along(p$layers)) {
      if (!inherits(p$layers[[i]]$geom, "GeomText")) next
      d <- built$data[[i]]
      if (is.null(d) || nrow(d) == 0L) next
      if ("y" %in% names(d) && all(is.infinite(d$y))) {
        keep[i] <- FALSE
      }
    }
    
    p$layers <- p$layers[keep]
    p
  }
  
  # TOP plot: remove x-axis label
  p_top <- plot_ghg_betas(ts_dt = ts_dt_top, ...) +
    ggplot2::labs(title = NULL, subtitle = NULL, x = NULL)
  
  # BOTTOM plot: keep/set x-axis label
  p_bottom <- plot_ghg_betas(ts_dt = ts_dt_bottom, ...) +
    ggplot2::labs(title = NULL, subtitle = NULL, x = x_label)
  
  if (!isTRUE(show_bottom_event_labels)) {
    p_bottom <- remove_event_label_text_layers(p_bottom)
  }
  
  strip_top <- make_text_strip(text_top)
  strip_bot <- make_text_strip(text_bottom)
  
  h_top <- if (is.null(text_top) || !nzchar(text_top)) 1e-6 else text_height
  h_bot <- if (is.null(text_bottom) || !nzchar(text_bottom)) 1e-6 else text_height
  
  p <- patchwork::wrap_plots(
    strip_top, p_top, strip_bot, p_bottom,
    ncol = 1,
    heights = c(h_top, 1, h_bot, 1),
    guides = if (collect_guides) "collect" else "keep"
  )
  
  if (!is.null(title)) {
    p <- p + patchwork::plot_annotation(
      title = title,
      theme = ggplot2::theme(
        plot.title = ggplot2::element_text(face = "bold", hjust = 0)
      )
    )
  }
  
  if (collect_guides) {
    p <- p & ggplot2::theme(legend.position = "bottom")
  }
  
  p
}

plot_ghg_yearly_betas <- function(
    ts_dt,                   # output of run_ghg_yearly_betas()
    scope           = "S1",   # "S1", "S2", "S3"
    loc_selection   = NULL,  # optional vector of locs to keep (if loc exists)
    plot_all_locs   = FALSE, # if TRUE and loc exists -> facets by loc
    plot_date_min   = NULL,  # optional Date lower bound (applied AFTER MA)
    plot_date_max   = NULL,  # optional Date upper bound (applied AFTER MA)
    ci_mult         = 1.96,  # CI multiplier
    facet_ncol      = 4,     # number of facet columns when plot_all_locs = TRUE
    facet_scales    = "free_y",
    base_col        = "#00BFE9",
    zero_linewidth  = 0.4,
    ma_window       = NULL,  # moving-average window (years); if NULL or <= 1, no MA
    events          = NULL,  # optional named vector: names = "YYYY-MM-DD", values = labels
    election_periods = NULL  # optional data.frame with cols: start, end, label
) {
  library(data.table)
  library(ggplot2)
  
  dt <- as.data.table(ts_dt)
  
  # 1) Check that the requested scope exists
  beta_col <- paste0("beta_carbon_", scope)
  se_col   <- paste0("se_carbon_", scope)
  
  if (!beta_col %in% names(dt) || !se_col %in% names(dt)) {
    stop("Missing beta/se columns for scope ", scope,
         ". Expected columns: '", beta_col, "' and '", se_col, "'.")
  }
  
  # 2) Ensure we have a Date for plotting: year_date
  # Accept either 'year_date' or 'year'
  if (!"year_date" %in% names(dt)) {
    if (!"year" %in% names(dt)) {
      stop("ts_dt must contain either 'year' (YYYY) or 'year_date'.")
    }
    dt[, year_date := as.Date(paste0(as.integer(year), "-01-01"))]
  } else {
    dt[, year_date := as.Date(year_date)]
  }
  
  # 3) Optional loc filtering (if loc exists)
  has_loc <- "loc" %in% names(dt)
  
  if (has_loc && !is.null(loc_selection) && length(loc_selection) > 0L) {
    dt <- dt[loc %in% loc_selection]
    if (nrow(dt) == 0L) {
      stop("No rows left after filtering on loc_selection = ",
           paste(loc_selection, collapse = ", "))
    }
  }
  
  # 4) Build plotting data for chosen scope and compute CI on FULL series
  if (has_loc) {
    plot_dt <- dt[, .(
      year_date,
      loc,
      beta = get(beta_col),
      se   = get(se_col)
    )]
  } else {
    plot_dt <- dt[, .(
      year_date,
      beta = get(beta_col),
      se   = get(se_col)
    )]
  }
  
  plot_dt[, `:=`(
    ci_low  = beta - ci_mult * se,
    ci_high = beta + ci_mult * se
  )]
  
  plot_dt <- plot_dt[!is.na(beta)]
  if (nrow(plot_dt) == 0L) {
    stop("No non-NA beta values available to plot for scope ", scope, ".")
  }
  
  # 5) Optional moving average (trailing, right-aligned) on FULL series
  use_ma <- !is.null(ma_window) && ma_window > 1L
  if (use_ma) {
    if (has_loc) {
      setorder(plot_dt, loc, year_date)
      plot_dt[, ma_beta := frollmean(beta, n = ma_window, align = "right"), by = loc]
    } else {
      setorder(plot_dt, year_date)
      plot_dt[, ma_beta := frollmean(beta, n = ma_window, align = "right")]
    }
  }
  
  # 6) Apply optional plotting time window (after MA)
  if (!is.null(plot_date_min)) {
    plot_dt <- plot_dt[year_date >= as.Date(plot_date_min)]
  }
  if (!is.null(plot_date_max)) {
    plot_dt <- plot_dt[year_date <= as.Date(plot_date_max)]
  }
  if (nrow(plot_dt) == 0L) {
    stop("No observations left in the requested date range.")
  }
  
  # 6b) Build events data (vertical lines), restricted to the plotting window
  event_dt <- NULL
  if (!is.null(events) && length(events) > 0L) {
    ev_dates <- as.Date(names(events))
    event_dt <- data.table(
      year_date = ev_dates,
      label     = as.character(events)
    )
    
    event_dt <- event_dt[
      year_date >= min(plot_dt$year_date, na.rm = TRUE) &
        year_date <= max(plot_dt$year_date, na.rm = TRUE)
    ]
    
    if (nrow(event_dt) == 0L) event_dt <- NULL
  }
  
  # 7) Helpers: zero line, theme & layers
  add_zero_line <- function() {
    geom_hline(
      yintercept = 0,
      linewidth  = zero_linewidth,
      colour     = "grey30",
      alpha      = 0.7
    )
  }
  
  nice_theme <- function(base_size = 10, show_legend = FALSE) {
    theme_classic(base_size = base_size) +
      theme(
        plot.title        = element_text(face = "bold", hjust = 0),
        strip.text        = element_text(face = "bold", size = base_size - 1),
        axis.text.x       = element_text(size = base_size - 2),
        axis.text.y       = element_text(size = base_size - 2),
        legend.position   = if (show_legend) "bottom" else "none",
        panel.grid.minor  = element_blank(),
        panel.spacing.y   = unit(0.25, "lines"),
        plot.margin       = margin(5.5, 15, 5.5, 5.5, "pt")
      )
  }
  
  # background presidential intervals (rectangles)
  interval_rect_layer <- function() {
    if (is.null(election_periods)) return(NULL)
    
    rect_dt <- as.data.table(election_periods)
    if (!all(c("start", "end", "label") %in% names(rect_dt))) {
      stop("election_periods must have columns: 'start', 'end', 'label'.")
    }
    
    rect_dt[, `:=`(
      start = as.Date(start),
      end   = as.Date(end)
    )]
    
    rect_dt <- rect_dt[
      end   >= min(plot_dt$year_date, na.rm = TRUE) &
        start <= max(plot_dt$year_date, na.rm = TRUE)
    ]
    if (nrow(rect_dt) == 0L) return(NULL)
    
    rect_dt[, fill_col := fcase(
      grepl("Trump", label, ignore.case = TRUE), "#8B0000",
      grepl("Biden", label, ignore.case = TRUE), "#00008B",
      default = "grey50"
    )]
    
    geom_rect(
      data        = rect_dt,
      aes(xmin = start, xmax = end, ymin = -Inf, ymax = Inf, fill = fill_col),
      alpha       = 0.08,
      inherit.aes = FALSE,
      colour      = NA
    )
  }
  
  # foreground labels for presidential intervals (bottom)
  add_interval_labels <- function(p) {
    if (is.null(election_periods)) return(p)
    
    rect_dt <- as.data.table(election_periods)
    if (!all(c("start", "end", "label") %in% names(rect_dt))) return(p)
    
    rect_dt[, `:=`(
      start = as.Date(start),
      end   = as.Date(end)
    )]
    
    rect_dt <- rect_dt[
      end   >= min(plot_dt$year_date, na.rm = TRUE) &
        start <= max(plot_dt$year_date, na.rm = TRUE)
    ]
    if (nrow(rect_dt) == 0L) return(p)
    
    p +
      scale_fill_identity() +
      geom_text(
        data  = rect_dt,
        aes(x = start, y = -Inf, label = label),
        vjust = -0.5,
        hjust = -0.1,
        size  = 3
      )
  }
  
  # vertical event lines
  add_event_layers <- function(p) {
    if (is.null(event_dt)) return(p)
    
    p +
      geom_vline(
        data      = event_dt,
        aes(xintercept = year_date),
        linetype  = "dashed",
        linewidth = 0.4,
        colour    = "grey20",
        alpha     = 0.7
      ) +
      geom_text(
        data  = event_dt,
        aes(x = year_date, y = Inf, label = label),
        angle = 90,
        vjust = 1.1,
        hjust = 1,
        size  = 2.8
      )
  }
  
  # 8) Helper: single-series plot (no loc or single loc)
  make_single_plot <- function(d, loc_label = NULL) {
    
    p <- ggplot(d, aes(x = year_date)) +
      interval_rect_layer() +
      add_zero_line() +
      geom_ribbon(
        aes(ymin = ci_low, ymax = ci_high),
        fill      = base_col,
        alpha     = 0.1,
        linewidth = 0
      ) +
      geom_line(
        aes(y = ci_low),
        colour    = base_col,
        alpha     = 0.45,
        linewidth = 0.3
      ) +
      geom_line(
        aes(y = ci_high),
        colour    = base_col,
        alpha     = 0.45,
        linewidth = 0.3
      ) +
      geom_line(
        aes(y = beta),
        colour    = base_col,
        alpha     = 0.95,
        linewidth = 0.8
      )
    
    if (use_ma && "ma_beta" %in% names(d)) {
      p <- p +
        geom_line(
          aes(y = ma_beta),
          colour    = "grey15",
          linewidth = 1,
          linetype  = "solid"
        )
    }
    
    p <- p +
      scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
      labs(
        x = "Year",
        y = paste0("Carbon beta (", scope, ")"),
        title = if (is.null(loc_label)) {
          paste0("Yearly carbon beta for ", scope)
        } else {
          paste0("Yearly carbon beta for ", scope, " (loc = ", loc_label, ")")
        }
      )
    
    p <- add_event_layers(p)
    p <- add_interval_labels(p)
    
    p + nice_theme(base_size = 10, show_legend = FALSE)
  }
  
  # 9) Case A: no loc dimension -> single-series plot
  if (!has_loc) {
    return(make_single_plot(plot_dt))
  }
  
  # 10) Case B: loc exists & plot_all_locs = TRUE -> facets by loc
  if (plot_all_locs) {
    
    p <- ggplot(plot_dt, aes(x = year_date)) +
      interval_rect_layer() +
      add_zero_line() +
      geom_ribbon(
        aes(ymin = ci_low, ymax = ci_high),
        fill      = base_col,
        alpha     = 0.12,
        linewidth = 0
      ) +
      geom_line(
        aes(y = ci_low),
        colour    = base_col,
        alpha     = 0.45,
        linewidth = 0.3
      ) +
      geom_line(
        aes(y = ci_high),
        colour    = base_col,
        alpha     = 0.45,
        linewidth = 0.3
      ) +
      geom_line(
        aes(y = beta),
        colour    = base_col,
        alpha     = 0.95,
        linewidth = 0.8
      )
    
    if (use_ma && "ma_beta" %in% names(plot_dt)) {
      p <- p +
        geom_line(
          aes(y = ma_beta),
          colour    = "grey15",
          linewidth = 1,
          linetype  = "solid"
        )
    }
    
    p <- p +
      facet_wrap(~ loc, ncol = facet_ncol, scales = facet_scales) +
      scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
      labs(
        x = "Year",
        y = paste0("Carbon beta (", scope, ")"),
        title = paste0("Yearly carbon beta for ", scope, " by location")
      )
    
    p <- add_event_layers(p)
    p <- add_interval_labels(p)
    
    return(p + nice_theme(base_size = 9, show_legend = FALSE))
  }
  
  # 11) Case C: loc exists & plot_all_locs = FALSE
  uniq_locs <- sort(unique(plot_dt$loc))
  
  if (length(uniq_locs) == 1L) {
    return(make_single_plot(plot_dt, loc_label = uniq_locs))
  }
  
  p <- ggplot(plot_dt,
              aes(x = year_date, y = beta, colour = loc, fill = loc, group = loc)) +
    interval_rect_layer() +
    add_zero_line() +
    geom_ribbon(
      aes(ymin = ci_low, ymax = ci_high),
      alpha     = 0.12,
      linewidth = 0
    ) +
    geom_line(linewidth = 0.8, alpha = 0.95)
  
  if (use_ma && "ma_beta" %in% names(plot_dt)) {
    p <- p +
      geom_line(
        aes(y = ma_beta),
        linewidth = 1,
        linetype  = "solid"
      )
  }
  
  p <- p +
    scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
    labs(
      x = "Year",
      y = paste0("Carbon beta (", scope, ")"),
      color = "Location",
      fill  = "Location",
      title = paste0("Yearly carbon beta for ", scope, " by location")
    )
  
  p <- add_event_layers(p)
  p <- add_interval_labels(p)
  
  p + nice_theme(base_size = 10, show_legend = TRUE)
}

plot_beta_vs_country_vars <- function(
    beta_dt,                 # output of run_ghg_monthly_betas (group_by_loc = TRUE)
    data_df,                 # original df with country-level variables
    scope          = "S1",   # which scope: "S1", "S2", "S3"
    loc_selection  = NULL,   # optional: vector of locs to keep
    country_vars,            # character vector of variable names in data_df
    min_months_per_year = 6, # require at least this many months with non-NA beta
    agg_fun_beta    = mean,  # how to aggregate monthly betas -> yearly beta
    agg_fun_country = mean,  # how to aggregate df vars -> yearly
    facet_by_country = FALSE,# TRUE -> one panel per country
    print_plots     = TRUE,  # if TRUE, display plots
    
    # NEW: y-axis toggle
    free_y_axis     = FALSE, # if TRUE and facet_by_country=TRUE, allow y to vary by facet
    
    # label toggle (years)
    label_years     = FALSE, # if TRUE, add year labels to points
    label_size      = 3,     # text size for year labels
    max_overlaps    = 50     # ggrepel overlap control
) {
  library(data.table)
  library(ggplot2)
  library(ggrepel)
  
  # Coerce to data.table
  b <- as.data.table(beta_dt)
  d <- as.data.table(data_df)
  
  # Check beta column
  beta_col <- paste0("beta_carbon_", scope)
  if (!beta_col %in% names(b)) {
    stop("Column not found in beta_dt: ", beta_col)
  }
  if (!"loc" %in% names(b)) {
    stop("beta_dt must include a 'loc' column (run run_ghg_monthly_betas with group_by_loc = TRUE).")
  }
  
  # Create year field in beta_dt
  if (!"ym_date" %in% names(b)) {
    if (!"ym" %in% names(b)) stop("beta_dt must contain either ym or ym_date.")
    b[, ym_date := as.Date(paste0(ym, "-01"))]
  } else {
    b[, ym_date := as.Date(ym_date)]
  }
  b[, year := as.integer(format(ym_date, "%Y"))]
  
  # Optional country filter
  if (!is.null(loc_selection)) {
    b <- b[loc %in% loc_selection]
  }
  
  # Aggregate monthly betas to yearly betas
  yearly_beta <- b[
    !is.na(get(beta_col)),
    .(
      beta_year   = agg_fun_beta(get(beta_col), na.rm = TRUE),
      months_used = sum(!is.na(get(beta_col)))
    ),
    by = .(loc, year)
  ]
  
  yearly_beta <- yearly_beta[months_used >= min_months_per_year]
  
  if (nrow(yearly_beta) == 0L) {
    stop("No yearly beta observations after applying min_months_per_year filter.")
  }
  
  # Prepare country-level variables
  if (!"loc" %in% names(d)) stop("data_df must contain a 'loc' column.")
  
  # Year
  if ("datadate" %in% names(d)) {
    d[, year := as.integer(format(datadate, "%Y"))]
  } else if ("ym" %in% names(d)) {
    d[, year := as.integer(substr(ym, 1, 4))]
  } else {
    stop("data_df must contain datadate or ym to infer year.")
  }
  
  if (!is.null(loc_selection)) {
    d <- d[loc %in% loc_selection]
  }
  
  # Ensure variables exist
  missing_vars <- setdiff(country_vars, names(d))
  if (length(missing_vars) > 0) {
    stop("Missing variables in data_df: ", paste(missing_vars, collapse = ", "))
  }
  
  # Aggregate variables to yearly
  country_year <- d[
    ,
    lapply(.SD, agg_fun_country, na.rm = TRUE),
    by = .(loc, year),
    .SDcols = country_vars
  ]
  
  # Merge beta + country vars
  merged <- merge(
    yearly_beta,
    country_year,
    by = c("loc", "year"),
    all = FALSE
  )
  
  if (nrow(merged) == 0L) stop("No overlapping country-year pairs.")
  
  # Build scatterplots
  plots <- list()
  
  # Facet scale control:
  # - If free_y_axis=TRUE: both x and y can vary (scales="free")
  # - Else: only x varies (scales="free_x"), y fixed across facets
  facet_scales <- if (isTRUE(free_y_axis)) "free" else "free_x"
  
  for (v in country_vars) {
    
    base_plot <- ggplot(merged, aes(x = .data[[v]], y = beta_year))
    
    if (!facet_by_country) {
      # Multiple countries in one plot, colored by loc
      p <- base_plot +
        aes(color = loc) +
        geom_hline(yintercept = 0, color = "grey20", linewidth = 0.5, alpha = 0.8) +
        geom_point(size = 2.3, alpha = 0.8) +
        geom_smooth(method = "lm", se = FALSE, linewidth = 0.6,
                    color = "darkred", alpha = 0.7) +
        {
          if (isTRUE(label_years)) {
            geom_text_repel(
              aes(label = year, color = loc),
              size = label_size,
              box.padding = 0.15,
              point.padding = 0.05,
              segment.size = 0.25,
              min.segment.length = 0,
              max.overlaps = max_overlaps,
              show.legend = FALSE
            )
          }
        } +
        theme_classic() +
        theme(
          axis.line        = element_blank(),
          panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
          strip.background = element_rect(color = "black", fill = "grey90"),
          legend.position  = "top"
        ) +
        labs(
          x = v,
          y = paste0("Yearly ", deparse(substitute(agg_fun_beta)),
                     " carbon beta (", scope, ")"),
          color = "Country",
          title = paste0("Carbon beta vs ", v, " (country-year)")
        )
      
    } else {
      # Faceted by country
      p <- base_plot +
        geom_hline(yintercept = 0, color = "grey20", linewidth = 0.5, alpha = 0.8) +
        geom_point(size = 2.3, alpha = 0.6, color = "grey60") +
        geom_smooth(method = "lm", se = FALSE, linewidth = 0.6,
                    color = "darkred", alpha = 0.7) +
        {
          if (isTRUE(label_years)) {
            geom_text_repel(
              aes(label = year),
              size = label_size,
              box.padding = 0.15,
              point.padding = 0.05,
              segment.size = 0.25,
              min.segment.length = 0,
              max.overlaps = max_overlaps
            )
          }
        } +
        facet_wrap(~ loc, scales = facet_scales) +
        theme_classic() +
        theme(
          axis.line        = element_blank(),
          panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
          strip.background = element_rect(color = "black", fill = "grey90"),
          legend.position  = "none"
        ) +
        labs(
          x = v,
          y = paste0("Yearly ", deparse(substitute(agg_fun_beta)),
                     " carbon beta (", scope, ")"),
          title = paste0("Carbon beta vs ", v, " (country-year panels)")
        )
    }
    
    if (print_plots) print(p)
    plots[[v]] <- p
  }
  
  invisible(plots)
}

plot_beta_vs_country_vars_panel <- function(
    dt,                      # e.g., b_yearly_ts_with_controls (key: loc, year)
    var_x,                   # character vector of X variables (country-year vars)
    scope        = "S1",      # "S1","S2","S3" if var_y not provided
    var_y        = NULL,      # optional override (e.g., "beta_carbon_S1")
    start_year,
    end_year,
    loc_col      = "loc",
    year_col     = "year",
    group_col    = NULL,      # e.g., "region"
    loc_selection  = NULL,
    highlight_locs = NULL,
    facet_by_country = FALSE, # TRUE -> one panel per country
    free_y_axis     = FALSE,  # if facet_by_country=TRUE, allow y to vary by facet
    label_years     = FALSE,
    label_size      = 3,
    max_overlaps    = 50,
    weight_by_n     = FALSE,  # if TRUE, weight lm line by n_<scope> (if present)
    x_label         = NULL,
    y_label         = NULL,
    plot_title      = NULL,
    print_plots     = TRUE
) {
  library(data.table)
  library(ggplot2)
  library(ggrepel)
  
  DT <- as.data.table(dt)
  var_x <- as.character(var_x)
  
  # Resolve y column
  if (is.null(var_y)) {
    if (!scope %in% c("S1", "S2", "S3")) stop("scope must be one of: S1, S2, S3.")
    var_y <- paste0("beta_carbon_", scope)
  }
  var_y <- as.character(var_y)
  if (length(var_y) != 1L) stop("var_y must be length 1.")
  
  # Optional weight column
  w_col <- paste0("n_", scope)
  use_w <- isTRUE(weight_by_n) && (w_col %in% names(DT))
  
  make_label <- function(x) {
    x <- gsub("_", " ", x)
    x <- gsub("^log ", "Log ", x)
    x
  }
  
  x_labels_vec <- sapply(var_x, make_label, USE.NAMES = TRUE)
  if (length(var_x) == 1L && !is.null(x_label)) x_labels_vec[var_x] <- x_label
  if (is.null(y_label)) y_label <- make_label(var_y)
  
  # Check required columns
  cols_needed <- c(loc_col, year_col, var_y, var_x, group_col, if (use_w) w_col else NULL)
  cols_needed <- cols_needed[!is.na(cols_needed)]
  missing_cols <- setdiff(cols_needed, names(DT))
  if (length(missing_cols) > 0L) stop("Missing: ", paste(missing_cols, collapse = ", "))
  
  # Optional loc filter
  if (!is.null(loc_selection) && length(loc_selection) > 0L) {
    DT <- DT[get(loc_col) %in% loc_selection]
    if (nrow(DT) == 0L) stop("No rows left after loc_selection.")
  }
  
  # Subset to year range and non-NA y
  DT_sub <- DT[
    get(year_col) >= start_year & get(year_col) <= end_year & !is.na(get(var_y)),
    ..cols_needed
  ]
  if (nrow(DT_sub) == 0L) stop("No observations with non-NA y in year range.")
  
  # Standardize names internally
  rename_if_needed <- function(DT, old, new) {
    if (!is.null(old) && old != new) setnames(DT, old, new)
  }
  rename_if_needed(DT_sub, loc_col,  "loc")
  rename_if_needed(DT_sub, year_col, "year")
  rename_if_needed(DT_sub, var_y,    "y")
  if (!is.null(group_col)) rename_if_needed(DT_sub, group_col, "group")
  if (use_w) rename_if_needed(DT_sub, w_col, "w")
  
  # Melt X variables
  id_vars <- c("loc", "year", "y", if (!is.null(group_col)) "group", if (use_w) "w")
  DT_long <- melt(
    DT_sub,
    id.vars       = id_vars,
    measure.vars  = var_x,
    variable.name = "x_var",
    value.name    = "x"
  )
  
  DT_long <- DT_long[!is.na(x) & !is.na(y)]
  if (nrow(DT_long) == 0L) stop("No non-NA (x,y) pairs after filtering.")
  
  DT_long[, x_var_label := x_labels_vec[as.character(x_var)]]
  
  # Highlight logic
  if (!is.null(highlight_locs) && length(highlight_locs) > 0L) {
    DT_long[, highlighted := loc %in% highlight_locs]
  } else {
    DT_long[, highlighted := FALSE]
  }
  DT_long[, label_alpha := fifelse(highlighted, 1, 0.6)]
  DT_long[, label_font  := fifelse(highlighted, "bold", "plain")]
  
  # Facet scale control (for per-country panels)
  facet_scales <- if (isTRUE(free_y_axis)) "free" else "free_x"
  
  # Group color palette (kept from your prior function)
  group_colors <- c(
    "Europe"                = "#4E79A7",
    "Americas"              = "#F28E2B",
    "Asia-Pacific"          = "#E15759",
    "Middle East & Africa"  = "#76B7B2",
    "Offshore"              = "#59A14F",
    "Other"                 = "grey60"
  )
  
  default_title <- if (length(var_x) == 1L) {
    paste0(y_label, " vs ", x_labels_vec[var_x], " (", start_year, "–", end_year, ")")
  } else {
    paste0(y_label, " vs multiple X variables (", start_year, "–", end_year, ")")
  }
  plot_title <- if (is.null(plot_title)) default_title else plot_title
  
  plots <- list()
  
  for (v in unique(DT_long$x_var)) {
    dplot <- DT_long[x_var == v]
    
    base_plot <- ggplot(dplot, aes(x = x, y = y)) +
      geom_hline(yintercept = 0, color = "grey20", linewidth = 0.5, alpha = 0.8)
    
    # Points + smooth:
    if (!facet_by_country) {
      # all countries in one plot
      if (!is.null(group_col)) {
        base_plot <- base_plot +
          geom_point(aes(color = group), size = 2.3, alpha = 0.8) +
          scale_color_manual(values = group_colors)
      } else {
        base_plot <- base_plot +
          geom_point(size = 2.3, alpha = 0.6, color = "grey60")
      }
      
      base_plot <- base_plot +
        geom_smooth(
          method = "lm", se = FALSE, linewidth = 0.6,
          color = "darkred", alpha = 0.7,
          aes(weight = if (use_w) w else NULL)
        )
      
      if (isTRUE(label_years)) {
        base_plot <- base_plot +
          geom_text_repel(
            aes(label = year, alpha = label_alpha, fontface = label_font),
            size = label_size,
            box.padding = 0.15,
            point.padding = 0.05,
            segment.size = 0.25,
            min.segment.length = 0,
            max.overlaps = max_overlaps,
            show.legend = FALSE
          ) +
          scale_alpha_identity()
      }
      
      p <- base_plot +
        theme_classic() +
        theme(
          axis.line        = element_blank(),
          panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
          strip.background = element_rect(color = "black", fill = "grey90"),
          legend.position  = "top"
        ) +
        labs(
          x     = x_labels_vec[as.character(v)],
          y     = y_label,
          title = paste0(plot_title, " — ", x_labels_vec[as.character(v)]),
          color = if (!is.null(group_col)) "Group" else NULL
        )
      
    } else {
      # facets by country (within-country cloud)
      base_plot <- base_plot +
        geom_point(size = 2.3, alpha = 0.6, color = "grey60") +
        geom_smooth(
          method = "lm", se = FALSE, linewidth = 0.6,
          color = "darkred", alpha = 0.7,
          aes(weight = if (use_w) w else NULL)
        )
      
      if (isTRUE(label_years)) {
        base_plot <- base_plot +
          geom_text_repel(
            aes(label = year),
            size = label_size,
            box.padding = 0.15,
            point.padding = 0.05,
            segment.size = 0.25,
            min.segment.length = 0,
            max.overlaps = max_overlaps
          )
      }
      
      p <- base_plot +
        facet_wrap(~ loc, scales = facet_scales) +
        theme_classic() +
        theme(
          axis.line        = element_blank(),
          panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
          strip.background = element_rect(color = "black", fill = "grey90"),
          legend.position  = "none"
        ) +
        labs(
          x     = x_labels_vec[as.character(v)],
          y     = y_label,
          title = paste0(plot_title, " — country panels")
        )
    }
    
    if (print_plots) print(p)
    plots[[as.character(v)]] <- p
  }
  
  invisible(plots)
}


plot_panel_scatter <- function(dt,
                               var_x,
                               var_y,
                               start_year,
                               end_year,
                               agg_fun         = c("median", "mean", "full"),
                               loc_col         = "loc",
                               year_col        = "year",
                               group_col       = NULL,
                               loc_selection   = NULL,
                               highlight_locs  = NULL,
                               x_label         = NULL,
                               y_label         = NULL,
                               plot_title      = NULL,
                               min_years       = 3L) {   # NEW
  
  library(data.table)
  library(ggplot2)
  library(ggrepel)
  
  agg_fun <- match.arg(agg_fun)
  dt <- as.data.table(dt)
  
  var_x <- as.character(var_x)
  var_y <- as.character(var_y)
  if (length(var_y) != 1L) stop("var_y must be length 1.")
  if (length(var_x) < 1L) stop("var_x must contain at least one variable name.")
  
  # If you only want min_years enforced for aggregated plots, this is fine as-is.
  min_years <- as.integer(min_years)
  if (is.na(min_years) || min_years < 1L) stop("min_years must be a positive integer.")
  
  make_label <- function(x) {
    x <- gsub("_", " ", x)
    x <- gsub("^log ", "Log ", x)
    x
  }
  
  x_labels_vec <- sapply(var_x, make_label, USE.NAMES = TRUE)
  if (length(var_x) == 1L && !is.null(x_label)) x_labels_vec[var_x] <- x_label
  if (is.null(y_label)) y_label <- make_label(var_y)
  
  cols_needed <- c(loc_col, year_col, var_y, var_x, group_col)
  cols_needed <- cols_needed[!is.na(cols_needed)]
  missing_cols <- setdiff(cols_needed, names(dt))
  if (length(missing_cols) > 0L) stop("Missing: ", paste(missing_cols, collapse = ", "))
  
  if (!is.null(loc_selection) && length(loc_selection) > 0L) {
    dt <- dt[get(loc_col) %in% loc_selection]
    if (nrow(dt) == 0L) stop("No rows left after loc_selection.")
  }
  
  dt_sub <- dt[
    get(year_col) >= start_year & get(year_col) <= end_year & !is.na(get(var_y)),
    ..cols_needed
  ]
  if (nrow(dt_sub) == 0L) stop("No observations with non-NA var_y in year range.")
  
  rename_if_needed <- function(DT, old, new) {
    if (!is.null(old) && old != new) setnames(DT, old, new)
  }
  
  rename_if_needed(dt_sub, loc_col,  "loc")
  rename_if_needed(dt_sub, year_col, "year")
  rename_if_needed(dt_sub, var_y,    "y")
  if (!is.null(group_col)) rename_if_needed(dt_sub, group_col, "group")
  
  id_vars <- c("loc", "year", "y")
  if (!is.null(group_col)) id_vars <- c(id_vars, "group")
  
  dt_long <- melt(
    dt_sub,
    id.vars       = id_vars,
    measure.vars  = var_x,
    variable.name = "x_var",
    value.name    = "x"
  )
  
  # NEW: pairwise-complete filtering (per year, per x_var)
  dt_long <- dt_long[!is.na(x) & !is.na(y)]
  if (nrow(dt_long) == 0L) stop("No pairwise-complete (x,y) observations in year range.")
  
  dt_long[, x_var_label := x_labels_vec[as.character(x_var)] ]
  
  if (agg_fun %in% c("median", "mean")) {
    fun <- if (agg_fun == "median") median else mean
    
    if (!is.null(group_col)) {
      dt_plot <- dt_long[
        ,
        {
          if (.N < min_years) {
            .(x = NA_real_, y = NA_real_, group = unique(group)[1])
          } else {
            .(x = fun(x, na.rm = TRUE), y = fun(y, na.rm = TRUE), group = unique(group)[1])
          }
        },
        by = .(loc, x_var, x_var_label)
      ]
    } else {
      dt_plot <- dt_long[
        ,
        {
          if (.N < min_years) {
            .(x = NA_real_, y = NA_real_)
          } else {
            .(x = fun(x, na.rm = TRUE), y = fun(y, na.rm = TRUE))
          }
        },
        by = .(loc, x_var, x_var_label)
      ]
    }
    
  } else {
    dt_plot <- dt_long
  }
  
  dt_plot <- dt_plot[!is.na(x) & !is.na(y)]
  if (nrow(dt_plot) == 0L) stop("No non-NA (x,y) pairs after filtering/aggregation.")
  
  if (!is.null(highlight_locs)) {
    dt_plot[, highlighted := loc %in% highlight_locs]
  } else {
    dt_plot[, highlighted := FALSE]
  }
  
  dt_plot[, label_alpha := ifelse(highlighted, 1, 0.6)]
  dt_plot[, label_font  := ifelse(highlighted, "bold", "plain")]
  
  group_colors <- c(
    "Europe"                = "#4E79A7",
    "Americas"              = "#F28E2B",
    "Asia-Pacific"          = "#E15759",
    "Middle East & Africa"  = "#76B7B2",
    "Offshore"              = "#59A14F",
    "Other"                 = "grey60"
  )
  
  default_title <- if (length(var_x) == 1L) {
    paste0("Scatterplot of ", y_label, " vs ", x_labels_vec[var_x],
           " (", start_year, "–", end_year, ", ", agg_fun,
           if (agg_fun %in% c("median","mean")) paste0(", min_years=", min_years) else "",
           ")")
  } else {
    paste0("Scatterplots of ", y_label, " vs multiple X variables",
           " (", start_year, "–", end_year, ", ", agg_fun,
           if (agg_fun %in% c("median","mean")) paste0(", min_years=", min_years) else "",
           ")")
  }
  
  plot_title <- if (is.null(plot_title)) default_title else plot_title
  
  p <- ggplot(dt_plot, aes(x = x, y = y)) +
    geom_hline(yintercept = 0, color = "grey20", linewidth = 0.5, alpha = 0.8) +
    {
      if (!is.null(group_col)) geom_point(aes(color = group), size = 2.3, alpha = 0.8)
      else geom_point(size = 2.3, alpha = 0.6, color = "grey60")
    } +
    {
      if (!is.null(group_col)) scale_color_manual(values = group_colors)
    } +
    geom_smooth(method = "lm", se = FALSE, linewidth = 0.6,
                color = "darkred", alpha = 0.7) +
    geom_text_repel(
      aes(label = loc, alpha = label_alpha, fontface = label_font),
      size = 3,
      box.padding = 0.15,
      point.padding = 0.05,
      segment.size = 0.25,
      min.segment.length = 0
    ) +
    scale_alpha_identity() +
    facet_wrap(~ x_var_label, scales = "free_x") +
    theme_classic() +
    theme(
      axis.line        = element_blank(),
      panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
      strip.background = element_rect(color = "black", fill = "grey90"),
      legend.position  = "top"
    ) +
    labs(
      x     = if (length(var_x) == 1L) x_labels_vec[var_x] else NULL,
      y     = y_label,
      title = plot_title,
      color = if (!is.null(group_col)) "Group" else NULL
    )
  
  p
}

plot_panel_scatter <- function(dt,
                               var_x,
                               var_y,
                               start_year,
                               end_year,
                               agg_fun         = c("median", "mean", "full"),
                               loc_col         = "loc",
                               year_col        = "year",
                               group_col       = NULL,
                               loc_selection   = NULL,
                               highlight_locs  = NULL,
                               x_label         = NULL,
                               y_label         = NULL,
                               plot_title      = NULL,
                               min_years       = 3L,
                               
                               # NEW: rho (correlation) annotation toggle + options
                               show_rho        = FALSE,
                               rho_method      = c("pearson", "spearman"),
                               rho_digits      = 2L,
                               rho_position    = c("topright", "topleft"),
                               rho_by_group    = FALSE) {
  
  library(data.table)
  library(ggplot2)
  library(ggrepel)
  
  agg_fun      <- match.arg(agg_fun)
  rho_method   <- match.arg(rho_method)
  rho_position <- match.arg(rho_position)
  
  dt <- as.data.table(dt)
  
  var_x <- as.character(var_x)
  var_y <- as.character(var_y)
  if (length(var_y) != 1L) stop("var_y must be length 1.")
  if (length(var_x) < 1L) stop("var_x must contain at least one variable name.")
  
  min_years <- as.integer(min_years)
  if (is.na(min_years) || min_years < 1L) stop("min_years must be a positive integer.")
  
  rho_digits <- as.integer(rho_digits)
  if (is.na(rho_digits) || rho_digits < 0L) stop("rho_digits must be a non-negative integer.")
  
  make_label <- function(x) {
    x <- gsub("_", " ", x)
    x <- gsub("^log ", "Log ", x)
    x
  }
  
  x_labels_vec <- sapply(var_x, make_label, USE.NAMES = TRUE)
  if (length(var_x) == 1L && !is.null(x_label)) x_labels_vec[var_x] <- x_label
  if (is.null(y_label)) y_label <- make_label(var_y)
  
  cols_needed <- c(loc_col, year_col, var_y, var_x, group_col)
  cols_needed <- cols_needed[!is.na(cols_needed)]
  missing_cols <- setdiff(cols_needed, names(dt))
  if (length(missing_cols) > 0L) stop("Missing: ", paste(missing_cols, collapse = ", "))
  
  if (!is.null(loc_selection) && length(loc_selection) > 0L) {
    dt <- dt[get(loc_col) %in% loc_selection]
    if (nrow(dt) == 0L) stop("No rows left after loc_selection.")
  }
  
  dt_sub <- dt[
    get(year_col) >= start_year & get(year_col) <= end_year & !is.na(get(var_y)),
    ..cols_needed
  ]
  if (nrow(dt_sub) == 0L) stop("No observations with non-NA var_y in year range.")
  
  rename_if_needed <- function(DT, old, new) {
    if (!is.null(old) && old != new) setnames(DT, old, new)
  }
  
  rename_if_needed(dt_sub, loc_col,  "loc")
  rename_if_needed(dt_sub, year_col, "year")
  rename_if_needed(dt_sub, var_y,    "y")
  if (!is.null(group_col)) rename_if_needed(dt_sub, group_col, "group")
  
  id_vars <- c("loc", "year", "y")
  if (!is.null(group_col)) id_vars <- c(id_vars, "group")
  
  dt_long <- melt(
    dt_sub,
    id.vars       = id_vars,
    measure.vars  = var_x,
    variable.name = "x_var",
    value.name    = "x"
  )
  
  # pairwise-complete filtering (per year, per x_var)
  dt_long <- dt_long[!is.na(x) & !is.na(y)]
  if (nrow(dt_long) == 0L) stop("No pairwise-complete (x,y) observations in year range.")
  
  dt_long[, x_var_label := x_labels_vec[as.character(x_var)] ]
  
  if (agg_fun %in% c("median", "mean")) {
    fun <- if (agg_fun == "median") median else mean
    
    if (!is.null(group_col)) {
      dt_plot <- dt_long[
        ,
        {
          if (.N < min_years) {
            .(x = NA_real_, y = NA_real_, group = unique(group)[1])
          } else {
            .(x = fun(x, na.rm = TRUE), y = fun(y, na.rm = TRUE), group = unique(group)[1])
          }
        },
        by = .(loc, x_var, x_var_label)
      ]
    } else {
      dt_plot <- dt_long[
        ,
        {
          if (.N < min_years) {
            .(x = NA_real_, y = NA_real_)
          } else {
            .(x = fun(x, na.rm = TRUE), y = fun(y, na.rm = TRUE))
          }
        },
        by = .(loc, x_var, x_var_label)
      ]
    }
    
  } else {
    dt_plot <- dt_long
  }
  
  dt_plot <- dt_plot[!is.na(x) & !is.na(y)]
  if (nrow(dt_plot) == 0L) stop("No non-NA (x,y) pairs after filtering/aggregation.")
  
  if (!is.null(highlight_locs)) {
    dt_plot[, highlighted := loc %in% highlight_locs]
  } else {
    dt_plot[, highlighted := FALSE]
  }
  
  dt_plot[, label_alpha := ifelse(highlighted, 1, 0.6)]
  dt_plot[, label_font  := ifelse(highlighted, "bold", "plain")]
  
  group_colors <- c(
    "Europe"                = "#4E79A7",
    "Americas"              = "#F28E2B",
    "Asia-Pacific"          = "#E15759",
    "Middle East & Africa"  = "#76B7B2",
    "Offshore"              = "#59A14F",
    "Other"                 = "grey60"
  )
  
  default_title <- if (length(var_x) == 1L) {
    paste0("Scatterplot of ", y_label, " vs ", x_labels_vec[var_x],
           " (", start_year, "–", end_year, ", ", agg_fun,
           if (agg_fun %in% c("median","mean")) paste0(", min_years=", min_years) else "",
           ")")
  } else {
    paste0("Scatterplots of ", y_label, " vs multiple X variables",
           " (", start_year, "–", end_year, ", ", agg_fun,
           if (agg_fun %in% c("median","mean")) paste0(", min_years=", min_years) else "",
           ")")
  }
  
  plot_title <- if (is.null(plot_title)) default_title else plot_title
  
  # NEW: rho per facet (and optionally per group)
  rho_dt <- NULL
  if (isTRUE(show_rho)) {
    if (!is.null(group_col) && isTRUE(rho_by_group)) {
      rho_dt <- dt_plot[
        ,
        .(
          rho = suppressWarnings(cor(x, y, use = "pairwise.complete.obs", method = rho_method))
        ),
        by = .(x_var_label, group)
      ]
    } else {
      rho_dt <- dt_plot[
        ,
        .(
          rho = suppressWarnings(cor(x, y, use = "pairwise.complete.obs", method = rho_method))
        ),
        by = .(x_var_label)
      ]
    }
    
    rho_dt[, rho_label := sprintf("\u03C1 = %.*f", rho_digits, rho)]
  }
  
  # rho placement inside each facet
  rho_x <- if (rho_position == "topright") Inf else -Inf
  rho_hjust <- if (rho_position == "topright") 1.1 else -0.1
  
  p <- ggplot(dt_plot, aes(x = x, y = y)) +
    geom_hline(yintercept = 0, color = "grey20", linewidth = 0.5, alpha = 0.8) +
    {
      if (!is.null(group_col)) geom_point(aes(color = group), size = 2.3, alpha = 0.8)
      else geom_point(size = 2.3, alpha = 0.6, color = "grey60")
    } +
    {
      if (!is.null(group_col)) scale_color_manual(values = group_colors)
    } +
    geom_smooth(method = "lm", se = FALSE, linewidth = 0.6,
                color = "darkred", alpha = 0.7) +
    geom_text_repel(
      aes(label = loc, alpha = label_alpha, fontface = label_font),
      size = 3,
      box.padding = 0.15,
      point.padding = 0.05,
      segment.size = 0.25,
      min.segment.length = 0
    ) +
    scale_alpha_identity() +
    {
      if (isTRUE(show_rho) && !is.null(rho_dt)) {
        geom_text(
          data = rho_dt,
          aes(
            x = rho_x,
            y = Inf,
            label = rho_label,
            color = if (!is.null(group_col) && isTRUE(rho_by_group)) group else NULL
          ),
          inherit.aes = FALSE,
          hjust = rho_hjust,
          vjust = 1.2,
          size  = 3.2,
          fontface = "italic"
        )
      }
    } +
    facet_wrap(~ x_var_label, scales = "free_x") +
    theme_classic() +
    theme(
      axis.line        = element_blank(),
      panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
      strip.background = element_rect(color = "black", fill = "grey90"),
      legend.position  = "top"
    ) +
    labs(
      x     = if (length(var_x) == 1L) x_labels_vec[var_x] else NULL,
      y     = y_label,
      title = plot_title,
      color = if (!is.null(group_col)) "Group" else NULL
    )
  
  p
}

# =============================================================================.
# Data Cleaning ----
# =============================================================================.

## 0) data load ----
df <- read_parquet("../01_data/11_wrds_R_output/final_combined.parquet")
setDT(df)

# remove "_final" name
setnames(df, gsub("_final", "", names(df)))
df[, ym := format(as.IDate(month), "%Y-%m")]

### (a) add iso2 code to make it comparable with BK2023 ----
df[, iso2_country := countrycode(loc, "iso3c", "iso2c")] # iso 2 country codes
df[, country_name := countrycode(loc, "iso3c", "country.name")] # country names
df[, comb_ccpi_score := 0.5*nat_ccpi_score + 0.5*internat_ccpi_score] # combined ccpi score
df[, R_m := ret_m * 100] # returns in percentages

### (b) add OECD CAPMF policy variables ----
oecd_capmf <- read_parquet("../01_data/00_R_outputs/oecd_capmf.parquet")
setDT(oecd_capmf)

setkey(df, loc, year)
setkey(oecd_capmf, iso3_country, year)

cols <- setdiff(names(oecd_capmf), c("iso3_country","year"))

df[oecd_capmf, (cols) := mget(paste0("i.", cols)), 
   on=.(loc=iso3_country, year)]

rm(oecd_capmf)

### (c) add standardized Germanwatch/ccpi scores ----
germanwatch_output_std <- read_parquet("../01_data/00_R_outputs/germanwatch_output_std.parquet")
setDT(germanwatch_output_std)

germanwatch_output_std[, c(
  "country"
) := NULL]
 
setkey(df, loc, year)
setkey(germanwatch_output_std, iso3_country, year)

cols <- setdiff(names(germanwatch_output_std), c("iso3_country","year"))
df[germanwatch_output_std, (cols) := mget(paste0("i.", cols)), 
   on=.(loc=iso3_country, year)]

rm(germanwatch_output_std)

# remove "_final" name
setnames(df, gsub("_ccpi_std", "_ccpi", names(df)))

### (d) add standardized climate concern shocks ----
monthly_merged_unexpected_attention <- read_parquet("../01_data/11_wrds_R_output/monthly_merged_unexpected_attention.parquet")
setDT(monthly_merged_unexpected_attention)
monthly_merged_unexpected_attention <- monthly_merged_unexpected_attention[month >= "2007-01-01"]

umc_arx_ar1_visualization <- read_parquet("../01_data/00_R_outputs/mccc_UMC_AR1_ARX.parquet")
setDT(umc_arx_ar1_visualization)
umc_arx_ar1_visualization <-umc_arx_ar1_visualization[month >= "2007-01-01"]

# join the two sets
final_merged <- merge(
  monthly_merged_unexpected_attention[,.(month,TRI_monthly_roll,TRI,UCAI_global_GDP)],
  umc_arx_ar1_visualization[,.(month,UMC_AR1,UMC_ARX)],
  by = "month",
  all = TRUE
)

stopifnot(final_merged[, .N, by=month][N > 1, .N] == 0)

### (e) add z-transformation to climate concern shocks ----
shock_vars <- c(
  "TRI_monthly_roll",
  "TRI",
  "UCAI_global_GDP",
  "UMC_AR1",
  "UMC_ARX"
)

final_merged[, paste0(shock_vars, "_z") := lapply(.SD, function(x) {
  as.numeric(scale(x))
}), .SDcols = shock_vars]

shock_cols <- c(
  "TRI_monthly_roll_z",
  "TRI_z",
  "UCAI_global_GDP_z",
  "UMC_AR1_z",
  "UMC_ARX_z"
)

setkey(df, month)
setkey(final_merged, month)

df[final_merged,
   (shock_cols) := mget(paste0("i.", shock_cols)),
   on = "month"]

setorder(df, gvkey,month)

rm(monthly_merged_unexpected_attention)
rm(umc_arx_ar1_visualization)

### (f) z-transform country level UCAI ----
cai_country_AR <- read_parquet("../01_data/11_wrds_R_output/cai_country_AR.parquet")
setDT(cai_country_AR)
cai_country_AR[!is.na(UCAI),
               UCAI_z := as.numeric(scale(UCAI))]

cai_country_AR_use <- cai_country_AR[!is.na(UCAI_z)]


stopifnot(cai_country_AR_use[, .N, by=.(iso3_country, month)][N > 1, .N] == 0)

df[cai_country_AR_use,
   UCAI_country_z := i.UCAI_z,
   on = .(loc = iso3_country, month)]

setorder(df, gvkey, month)
rm(cai_country_AR)
rm(cai_country_AR_use)

### (g) z-transform pai and cfa_cva_ratio ----
pai_cfa_cva <- read_parquet("../01_data/11_wrds_R_output/pai_cfa_cva.parquet")
setDT(pai_cfa_cva)

pai_cfa_cva[!is.na(PAI),
               PAI_z := as.numeric(scale(PAI))]

pai_cfa_cva[!is.na(cfa_cva_ratio),
            cfa_cva_ratio_z := as.numeric(scale(cfa_cva_ratio))]

# extend panel by one year (last_year + 1)
first_year <- pai_cfa_cva[, min(year, na.rm = TRUE)]
last_year  <- pai_cfa_cva[, max(year, na.rm = TRUE)]

years_full <- seq(first_year, last_year + 1L)

panel_full <- CJ(
  iso3_country = unique(pai_cfa_cva$iso3_country),
  year         = years_full
)

pai_cfa_cva_full <- pai_cfa_cva[panel_full, on = .(iso3_country, year)]
setorder(pai_cfa_cva_full, iso3_country, year)

# lag standardized variables within country
std_cols <- c("PAI_z", "cfa_cva_ratio_z")

pai_cfa_cva_full[, (paste0(std_cols, "_lag1")) :=
                   lapply(.SD, shift, n = 1L, type = "lag"),
                 by = iso3_country,
                 .SDcols = std_cols]

### (h) merge lagged z-scores into df (country-year merge, df has loc = iso3_country) ----
temp_cols <- c("PAI_z", "cfa_cva_ratio_z", "PAI_z_lag1", "cfa_cva_ratio_z_lag1")

setkey(df, loc, year)
setkey(pai_cfa_cva_full, iso3_country, year)

df[pai_cfa_cva_full,
   (temp_cols) := mget(paste0("i.", temp_cols)),
   on = .(loc = iso3_country, year)]

setorder(df, gvkey, month)

rm(pai_cfa_cva)
rm(pai_cfa_cva_full)
rm(panel_full)

### (i) add log emission levels ----
to_log <- c(
  "ghg_scope1",
  "ghg_scope2_loc",
  "ghg_scope3_up",
  "ghg_scope1_lag6m",
  "ghg_scope2_loc_lag6m",
  "ghg_scope3_up_lag6m",
  "at"
)

df[, (paste0("log_", to_log)) := lapply(.SD, \(x) fifelse(x > 0, log(as.numeric(x)), NA_real_)),
   .SDcols = to_log]

### (j) perform intensity scaling used by BK2021 and Aswani et al. (2024) ----
int_cols <- c(
  "ghg_scope1_intensity",
  "ghg_scope2_loc_intensity",
  "ghg_scope3_up_intensity",
  "ghg_scope1_intensity_lag6m",
  "ghg_scope2_loc_intensity_lag6m",
  "ghg_scope3_up_intensity_lag6m"
)

present <- intersect(int_cols, names(df))
df[, (present) := lapply(.SD, \(x) x/100), .SDcols = present]




## 1) additional filtering ----
### (a) remove observations where no emissions are available (e.g. lagged and non-lagged) ----
emission_cols <- c(
  "ghg_scope1",
  "ghg_scope1_class",
  "ghg_scope2_loc",
  "ghg_scope2_loc_class",
  "ghg_scope3_up",
  "ghg_scope3_up_class",
  "ghg_scope1_intensity",
  "ghg_scope2_loc_intensity",
  "ghg_scope3_up_intensity",
  #"ghg_scope1_yoy",
  #"ghg_scope2_loc_yoy",
  #"ghg_scope3_up_yoy",
  #"ghg_scope1_yoy_class",
  #"ghg_scope2_loc_yoy_class",
  #"ghg_scope3_up_yoy_class",
  #"ghg_scope1_intensity_yoy",
  #"ghg_scope2_loc_intensity_yoy",
  #"ghg_scope3_up_intensity_yoy",
  "ghg_scope1_lag6m",
  "ghg_scope1_class_lag6m",
  "ghg_scope2_loc_lag6m",
  "ghg_scope2_loc_class_lag6m",
  "ghg_scope3_up_lag6m",
  "ghg_scope3_up_class_lag6m",
  "ghg_scope1_intensity_lag6m",
  "ghg_scope2_loc_intensity_lag6m",
  "ghg_scope3_up_intensity_lag6m"
  #"ghg_scope1_yoy_lag6m",
  #"ghg_scope2_loc_yoy_lag6m",
  #"ghg_scope3_up_yoy_lag6m",
  #"ghg_scope1_yoy_class_lag6m",
  #"ghg_scope2_loc_yoy_class_lag6m",
  #"ghg_scope3_up_yoy_class_lag6m",
  #"ghg_scope1_intensity_yoy_lag6m",
  #"ghg_scope2_loc_intensity_yoy_lag6m",
  #"ghg_scope3_up_intensity_yoy_lag6m"
)

emission_cols_present <- intersect(emission_cols, names(df))
stopifnot(length(emission_cols_present) > 0)

before <- nrow(df)
df <- df[rowSums(!is.na(df[, ..emission_cols])) > 0]
after <- nrow(df)
cat("Removed", before - after, "rows with all emission values NA\n")

### (b) remove observations where the number of firms used to calculate market return ----
# is below certain threshold (to avoid beta estimate being biased)

before <- nrow(df)
df <- df[n_stocks_full_comp_loc > 5]
cat("Dropped", before - nrow(df), "rows due to low number of firms for market returns\n")

### (c) remove specific gvkeys due to data errors ----
drop_gvkeys <- c(
  "025447", #SKYX PLATFORMS CORP
  "321951", #PHARNEXT S C A
  "315584", #GENK LOGISTICS NAAMLOZE
  "287205", #ALUMOT BB INVESTMENTS & CONS
  "177405", #HYCROFT MINING HOLDING CORP
  "250878" #EUROPLASMA S.A
)

before <- nrow(df)
df <- df[!gvkey %in% drop_gvkeys]
cat("Dropped", before - nrow(df), "rows due to gvkey exclusions\n")


## 2) Winsorization: following BK 2023 and Aswani et al. (2024) ----
### unlagged ----
cutoffs_pct <- c(
  ret_m = 0.025, # due to some extreme returns
  R_m = 0.025, # due to some extreme returns
  ROA = 1,
  ROS = 1,
  ebit_margin = 1,
  ebitda_margin = 1,
  ghg_scope1_intensity = 2.5,
  ghg_scope2_loc_intensity = 2.5,
  ghg_scope3_up_intensity = 2.5,
  ghg_scope1_yoy = 2.5,
  ghg_scope2_loc_yoy = 2.5,
  ghg_scope3_up_yoy = 2.5,
  #ghg_scope1_intensity_yoy = 2.5, 
  #ghg_scope2_loc_intesity_yoy = 2.5,
  #ghg_scope3_up_intensity_yoy = 2.5,
  #logsize = 2.5, # from Hambel & van der Sanden (2025), BK2023 do not winsorize
  leverage = 2.5,
  bm = 2.5,
  log_bm = 2.5,
  ROE = 2.5,
  eps_growth = 0.5,
  sales_growth = 0.5, #BK2023 do 2.5%
  inv_over_at = 2.5,
  vola = 0.5, #BK2023 do 2.5%
  mom = 0.5,#BK2023 do 2.5%
  beta_24m_lag1_loc = 0.025,
  beta_12m_lag1_loc = 0.025
)

for (nm in names(cutoffs_pct)) {
  if (nm %in% names(df)) {
    df[, (nm) := winsorize_pct(get(nm), pct = cutoffs_pct[[nm]])]
  } else {
    message("Column not found in df: ", nm)
  }
}

### lagged ----
# also apply it to the lagged data
cutoffs_pct_lagged <- c(
  ROA_lag6m = 1,
  ROS_lag6m = 1,
  ebit_margin_lag6m = 1,
  ebitda_margin_lag6m = 1,
  ghg_scope1_intensity_lag6m = 2.5,
  ghg_scope2_loc_intensity_lag6m = 2.5,
  ghg_scope3_up_intensity_lag6m = 2.5,
  ghg_scope1_yoy_lag6m = 2.5,
  ghg_scope2_loc_yoy_lag6m = 2.5,
  ghg_scope3_up_yoy_lag6m = 2.5,
  #ghg_scope1_intensity_yoy = 2.5, 
  #ghg_scope2_loc_intesity_yoy = 2.5,
  #ghg_scope3_up_intensity_yoy = 2.5,
  #logsize = 2.5, # from Hambel & van der Sanden (2025), BK2023 do not winsorize
  leverage_lag6m = 2.5,
  ROE_lag6m = 2.5,
  eps_growth_lag6m = 0.5,
  sales_growth_lag6m = 0.5, #BK2023 do 2.5%
  inv_over_at_lag6m = 2.5
  #vola = 0.5, #BK2023 do 2.5%
  #mom = 0.5 #BK2023 do 2.5%
)

for (nm in names(cutoffs_pct_lagged)) {
  if (nm %in% names(df)) {
    df[, (nm) := winsorize_pct(get(nm), pct = cutoffs_pct_lagged[[nm]])]
  } else {
    message("Column not found in df: ", nm)
  }
}

# =============================================================================.

# Exploratory Data Analysis ----
# =============================================================================.

## 0) Data set preparation ----
df[, ym := format(month, "%Y-%m")] # yearmonth variable

### (a) dummy coding ----
# add indicator variables for events
df[, covid_dummy := fifelse(datadate >= "2020-01-30" &
                              datadate <= "2023-05-05", 
                            1, 0)]
df[, ukraine_dummy := fifelse(datadate >= "2022-02-24", 
                              1, 0)]
df[, gaza_dummy := fifelse(datadate >= "2023-10-07" &
                             datadate <= "2025-10-03", 
                           1, 0)]
df[, paris_dummy := fifelse(datadate >= "2015-12-12", 
                            1, 0)]

df[, paris_USA_dummy := fifelse((
  (datadate >= as.Date("2016-11-04") & datadate <= as.Date("2017-06-01")) |
    (datadate >= as.Date("2021-01-20"))),
                            1, 0)]

df[, tp_dummy := fifelse(datadate >= "2020-12-12", 
                            1, 0)]

### (b) loc groupings ----
europe <- c("AUT","BEL","BGR","CHE","CYP","CZE","DEU","DNK","ESP","EST","FIN","FRA",
            "GBR","GRC","HUN","IRL","ISL","ITA","LTU","LUX","MLT","NLD","NOR","POL",
            "PRT","RUS","SVN","SWE","TUR","UKR")
americas <- c("ARG","BRA","CAN","CHL","COL","MEX","PER","USA","BMU","CYM","VGB")
asia_pacific <- c("AUS","CHN","HKG","IND","IDN","ISR","JPN","KAZ","KOR","MYS","NZL",
                  "PHL","PNG","SGP","THA","TWN","VNM")
middle_east <- c("ARE","BHR","KWT","QAT","SAU","MAR","NGA","ZAF","ZMB")

all_groups <- list(
  Europe        = europe,
  Americas      = americas,
  Asia_Pacific  = asia_pacific,
  Middle_East   = middle_east
)

# detect overlaps
all_codes <- unlist(all_groups)
dup_codes <- unique(all_codes[duplicated(all_codes)])
if (length(dup_codes) > 0) {
  warning("Overlapping ISO codes across regions: ",
          paste(dup_codes, collapse = ", "))
}

# deterministic deduplication
seen <- character()
dedup_groups <- list()

for (nm in names(all_groups)) {
  x_unique <- setdiff(all_groups[[nm]], seen)
  dedup_groups[[nm]] <- x_unique
  seen <- c(seen, x_unique)
}

all_locs <- unique(df$loc)
all_locs_minus_USA <- all_locs[all_locs != "USA"]

g7_locs <- c("CAN","FRA","DEU","ITA","JPN","GBR","USA") 
g20_locs <- c("ARG","AUS","BRA","CAN","CHN","FRA","DEU",
              "IND","IDN","ITA","JPN","MEX","RUS","SAU",
              "ZAF","KOR","TUR","GBR","USA")

brics_locs <- c("EGY","ETH","BRA","CHN","IND","IDN",
                "IRN","RUS","ZAF","ARE")


north_america_locs <- c("USA", "CAN", "MEX")
europe_locs <- c(
  "NLD","GBR","IRL","SWE","DNK","CHE","NOR","FRA","ESP","LUX","ITA","DEU",
  "BEL","FIN","AUT","PRT","IMN","GRC","RUS","GGY","JEY","TUR","POL","CYP",
  "MCO","CZE","HUN","SVN","ISL","MLT","GIB","LTU","EST","BGR","UKR"
)
asia_locs <- c(
  "JPN","CHN","HKG","SGP","KOR","THA","IND","IDN","TWN","ARE","CYM","QAT",
  "KWT","SAU","BHR","MAC","KAZ","PHL","VNM"
)
middle_east_africa_locs <- c("ISR","ARE","QAT","KWT","SAU","BHR","TUR","CYP","ZAF","ZMB","NGA","MAR")
latin_america_locs <- c("BRA","ARG","CHL","COL","PER","MEX")
oceania_locs <- c("AUS","NZL","PNG")


### (c) country-year version of df ----
df_country_year <- df[
  order(loc, year, month),   # or datadate
  .SD[ which.max(rowSums(!is.na(.SD))) ],
  by = .(loc, year)
]

df_country_year <-df_country_year[,.(loc, year,
                                     #UMC_AR1,UMC_ARX,
                                     #TRI,PRI,
                                     #TRI_monthly_roll, PRI_monthly_roll,
                                     #UCAI_country, UCAI_global_GDP, UCAI_global_EQ,
                                     nat_ccpi_score, internat_ccpi_score, comb_ccpi_score,
                                     nat_ccpi, internat_ccpi, combined_ccpi,
                                     PAI, PAI_z,
                                     cfa_cva_ratio, cfa_cva_ratio_z,
                                     capmf_raw, capmf_std, capmf_std_lag1,
                                     capmf_crosssectoral_policies, capmf_crosssectoral_policies_std,
                                     capmf_international_policies, capmf_international_policies_std,
                                     capmf_sectoral_policies, capmf_sectoral_policies_std,
                                     capmf_fossil_fuel_production_policies, capmf_fossil_fuel_production_policies_std,
                                     capmf_ghg_emission_targets, capmf_ghg_emission_targets_std
)]

# join additional controls to it
wb_indicators <- read_parquet("../01_data/11_wrds_R_output/wb_indicators.parquet")
setDT(wb_indicators)

setkey(df_country_year, loc, year)
setkey(wb_indicators, iso3_country, year)

df_country_year[wb_indicators, on = .(loc = iso3_country, year = year),
                names(wb_indicators) := mget(names(wb_indicators))]

setDT(df_country_year)

df_country_year[, country_old := country]

# Replace / create `country` using ISO3 codes in `loc`
df_country_year[, country := countrycode(
  sourcevar   = loc,
  origin      = "iso3c",
  destination = "country.name"
)]

df_country_year[
  ,
  region := fcase(
    loc %in% europe,       "Europe",
    loc %in% americas,     "Americas",
    loc %in% asia_pacific, "Asia-Pacific",
    loc %in% middle_east,  "Middle East & Africa",
    default = "Other"
  )
]

vars_to_lag <- c(
  "PAI","PAI_z",
  "cfa_cva_ratio","cfa_cva_ratio_z",
  "gdp_pc", "gdp_pc_z",
  "total_ff_produced_per_gdp", "total_ff_produced_per_gdp_z",
  "vulnerability", "vulnerability_z",
  "control_corruption", "control_corruption_z"
)

df_country_year[
  order(loc, year),
  paste0(vars_to_lag, "_lag1") := lapply(.SD, shift, 1),
  by = loc,
  .SDcols = vars_to_lag
]

#### (i) join certain world bank variables to df -----
subset_wb <- wb_indicators[,.(year,iso3_country,
                 gdp_pc_z,fossil_rents_z,
                 total_ff_produced_per_gdp_z,
                 vulnerability_z,
                 control_corruption_z)]

setDT(subset_wb)

setkey(df, loc, year)
setkey(subset_wb, iso3_country, year)

df[subset_wb, on = .(loc = iso3_country, year = year),
                names(subset_wb) := mget(names(subset_wb))]

## 1) Summary Statistics monthly ----
### (a) control variables ----
variable_selection <- c(
  #"ret_m",
  "R_m",
  #"ROA", 
  #"ROS",
  #"ebit_margin", 
  #"ebitda_margin", 
  #"ghg_scope1",
  #"ghg_scope2_loc", 
  #"ghg_scope3_up",
  "log_ghg_scope1",
  "log_ghg_scope2_loc", 
  "log_ghg_scope3_up",
  "ghg_scope1_intensity",
  "ghg_scope2_loc_intensity",
  "ghg_scope3_up_intensity",
  "logsize",
  #"log_sales",
  #"log_at",
  "leverage",
  #"bm",
  "log_bm",
  "ROE",
  "eps_growth",
  "sales_growth",
  "log_ppe",
  "inv_over_at",
  "vola",
  "mom",
  #"beta_12m_lag1_loc",
  "beta_24m_lag1_loc")

country_variable_selection <- c(
  #"UMC_AR1_z",
  #"UMC_ARX_z",
  #"TRI_z",
  #"TRI_monthly_roll_z",
  ##"PRI",
  #"UCAI_country_z",
  #"UCAI_global_GDP_z",
  #"UCAI_global_EQ",
  "nat_ccpi",
  "internat_ccpi",
  "combined_ccpi",
  "capmf_std",
  "capmf_international_policies_std",
  "capmf_crosssectoral_policies_std",
  "capmf_sectoral_policies_std",
  "capmf_fossil_fuel_production_policies_std",
  "capmf_ghg_emission_targets_std",
  "PAI",
  "PAI_z",
  "cfa_cva_ratio",
  "cfa_cva_ratio_z",
  "gdp_pc",
  "gdp_pc_z",
  "vulnerability",
  "vulnerability_z",
  "control_corruption",
  "control_corruption_z",
  "total_ff_produced_per_gdp",
  "total_ff_produced_per_gdp_z"
)

#### (i) monthly summary variables ----
length(unique(df[datadate>="2010-01-01" & datadate <= "2025-01-01",]$loc))

summary_variables <- df[datadate>="2010-01-01" & datadate <= "2025-01-01"
  , .(
    Mean    = fmt(sapply(.SD, function(x) mean(x, na.rm = TRUE))),
    SD      = fmt(sapply(.SD, function(x) sd(x, na.rm = TRUE))),
    Min     = fmt(sapply(.SD, function(x) min(x, na.rm = TRUE))),
    Q1      = fmt(sapply(.SD, function(x) quantile(x, 0.25, na.rm = TRUE))),
    Median  = fmt(sapply(.SD, function(x) median(x, na.rm = TRUE))),
    Q3      = fmt(sapply(.SD, function(x) quantile(x, 0.75, na.rm = TRUE))),
    Max     = fmt(sapply(.SD, function(x) max(x, na.rm = TRUE))),
    Missing = sapply(.SD, function(x) sum(is.na(x))),
    N       = sapply(.SD, function(x) sum(!is.na(x)))
  ),
  .SDcols = variable_selection
]

summary_variables <- data.table(
  Variable = variable_selection,
  Winsorization = cutoffs_pct[variable_selection],
  Mean     = summary_variables$Mean,
  SD       = summary_variables$SD,
  Min      = summary_variables$Min,
  Q1       = summary_variables$Q1,
  Median   = summary_variables$Median,
  Q3       = summary_variables$Q3,
  Max      = summary_variables$Max,
  Missing  = summary_variables$Missing,
  N        = summary_variables$N
)

summary_variables[, perc_missing := round((Missing / (Missing+N)) * 100,1)]

summary_variables[, Winsorization := ifelse(is.na(Winsorization), "-", Winsorization)]

# Ensure correct column order
setcolorder(summary_variables,
            c("Variable", "Winsorization", 
              "Mean", "SD", "Min", "Q1", "Median", "Q3", "Max", "N", "Missing","perc_missing"))

summary_variables

## 1) Define groups for each variable
summary_variables[, group := fcase(
  Variable %chin% c("ret_m", "R_m", "ROA", "ROS",
                    "ebit_margin", "ebitda_margin"),
  "Dependent variable",
  
  Variable %chin% c("ghg_scope1",
                    "ghg_scope2_loc",
                    "ghg_scope3_up",
                    "log_ghg_scope1",
                    "log_ghg_scope2_loc",
                    "log_ghg_scope3_up",
                    "ghg_scope1_intensity",
                    "ghg_scope2_loc_intensity",
                    "ghg_scope3_up_intensity"),
  "Emissions variables",
  
  default = "Controls"
)]

# enforce group order
summary_variables[, group := factor(
  group,
  levels = c("Dependent variable", "Emissions variables", "Controls")
)]

# preserve order from variable_selection *within* each group
summary_variables[, var_order := match(Variable, variable_selection)]
setorder(summary_variables, group, var_order)
summary_variables[, var_order := NULL]

## 2) Columns that appear in final output
final_cols <- setdiff(names(summary_variables), "group")

## 3) Split, insert header row before each block
splits <- split(summary_variables[, ..final_cols],
                summary_variables$group)

summary_variables_grouped <- rbindlist(
  lapply(names(splits), function(g) {
    dt  <- splits[[g]]
    hdr <- make_header_row(g, final_cols)
    
    rbind(
      as.data.table(hdr),
      dt,
      use.names = TRUE
    )
  }),
  use.names = TRUE
)

##### * table to word ----
summar_vars_plot <- summary_variables_grouped %>%
  mutate(
    # Ensure numeric first (in case something is labelled/character)
    N = as.numeric(N),
    Missing = as.numeric(Missing),
  )

ft <- flextable(summar_vars_plot)

# --- Header labels ---
ft <- set_header_labels(
  ft,
  Winsorization = "Wins.\n(cutoff %)",
  Missing = "N\nmissing",
  perc_missing = "%\nmissing"
)

# --- Alignment ---
ft <- align(ft, align = "left", part = "header")
ft <- align(ft, j = c("Winsorization","N","Mean","SD","Min","Q1","Median","Q3","Max","Missing","perc_missing"), align = "center", part = "all")
#ft <- align(ft, j = c("Missing","perc_missing"), align = "right", part = "body")

#right_cols <- c(
#  "Frequency", "Percentage", "Pct_est_obs",
#  "S1TOT", "S2TOT", "S3TOT",
#  "TOTS1", "TOTS2", "TOTS3",
#  "S1INT", "S2INT", "S3INT"
#)

# --- Typography / spacing ---
ft <- font(ft, fontname = "Times New Roman", part = "all")
ft <- fontsize(ft, size = 8, part = "all")
ft <- bold(ft, part = "header")
ft <- padding(ft, padding = 2, part = "all")
ft <- line_spacing(ft, space = 1, part = "all")

# --- Borders: same thickness above header, below header, and bottom rule ---
b_main <- fp_border(color = "black", width = 1)
ft <- border_remove(ft)
ft <- hline_top(ft, border = b_main, part = "header")
ft <- hline(ft, border = b_main, part = "header")
ft <- hline_bottom(ft, border = b_main, part = "body")

# --- Layout ---
ft <- autofit(ft)
ft <- set_table_properties(ft, width = 1, layout = "autofit")

#ft <- align(ft, j = right_cols, align = "right", part = "body")
hdr_vals <- c("Dependent variable", "Emissions variables", "Controls")

# indent all other rows in the Variable column
ft <- padding(
  ft,
  i = ~ !(Variable %in% hdr_vals),
  j = "Variable",
  padding.left = 12,
  part = "body"
)

ft <- bold(ft, i = ~ Variable %in% hdr_vals, j = "Variable", bold = TRUE, part = "body")
# optional: add a thin line above each section header except the first one
#b_thin <- fp_border(color = "black", width = 0.5)
#ft <- hline(ft, i = ~ Variable %in% hdr_vals, border = b_thin, part = "body")

doc <- read_docx()
doc <- body_add_flextable(doc, ft)

print(doc, target = "../07_word/outputs/summary_vars_2010_2025.docx")
rm(ft,doc,summar_vars_plot)

#### (ii) country-year summary variables ----
cy_vars <- intersect(country_variable_selection, names(df_country_year))

#cy_dt <- unique(
#  df[year >= 2010, c("loc", "year", cy_vars), with = FALSE],
#  by = c("loc", "year")
#)

cy_dt <- df_country_year


setDT(cy_dt)
cy_dt[, year := as.integer(year)]

# Restrict to the target window first (optional but usually desired)
cy_dt_win <- cy_dt[year >= 2010 & year <= 2025]

# Countries present in your data (within window)
locs <- unique(cy_dt_win$loc)

# Complete skeleton: all loc-year pairs 2010..2025
panel <- CJ(loc = locs, year = 2010:2025, unique = TRUE)
setkey(panel, loc, year)

# Left-join your observed data onto the skeleton -> missing combos become NA
setkey(cy_dt_win, loc, year)
cy_bal <- cy_dt_win[panel]

# Sanity checks
stopifnot(nrow(cy_bal) == length(locs) * length(2010:2025))
stopifnot(cy_bal[, all(.N == length(2010:2025)), by = loc]$V1)

# If you want: enforce sorted order and key
setorder(cy_bal, loc, year)
setkey(cy_bal, loc, year)


# optional sanity check: should be unique by construction
stopifnot(cy_bal[, .N, by = .(loc, year)][N > 1, .N] == 0)

# 2) summary stats computed at country-year level
summary_country_year <- cy_bal[
  , .(
    Mean    = fmt(sapply(.SD, \(x) mean(x, na.rm = TRUE))),
    SD      = fmt(sapply(.SD, \(x) sd(x, na.rm = TRUE))),
    Min     = fmt(sapply(.SD, \(x) min(x, na.rm = TRUE))),
    Q1      = fmt(sapply(.SD, \(x) quantile(x, 0.25, na.rm = TRUE))),
    Median  = fmt(sapply(.SD, \(x) median(x, na.rm = TRUE))),
    Q3      = fmt(sapply(.SD, \(x) quantile(x, 0.75, na.rm = TRUE))),
    Max     = fmt(sapply(.SD, \(x) max(x, na.rm = TRUE))),
    Missing = sapply(.SD, \(x) sum(is.na(x))),
    N       = sapply(.SD, \(x) sum(!is.na(x)))
  ),
  .SDcols = cy_vars
]

# 3) format into the same “long” table structure you used
summary_country_year <- data.table(
  Variable      = cy_vars,
  #Winsorization = cutoffs_pct[cy_vars],
  Mean          = summary_country_year$Mean,
  SD            = summary_country_year$SD,
  Min           = summary_country_year$Min,
  Q1            = summary_country_year$Q1,
  Median        = summary_country_year$Median,
  Q3            = summary_country_year$Q3,
  Max           = summary_country_year$Max,
  N             = summary_country_year$N,
  Missing       = summary_country_year$Missing
)

summary_country_year[, perc_missing := round((Missing / (Missing + N)) * 100, 1)]
#summary_country_year[, Winsorization := fifelse(is.na(Winsorization), "-", Winsorization)]

setcolorder(summary_country_year,
            c("Variable",
              "Mean", "SD", "Min", "Q1", "Median", "Q3", "Max","N", "Missing", "perc_missing"))

summary_country_year

## 1) Define groups for each variable
summary_country_year[, group := fcase(
  Variable %chin% c("nat_ccpi", "internat_ccpi", "combined_ccpi",
                    "nat_ccpi_score","internat_ccpi_score", "comb_ccpi_score"),
  "Germanwatch / CCPI policy scores",
  
  Variable %chin% c("PAI",
                    "cfa_cva_ratio",
                    "PAI_z",
                    "cfa_cva_ratio_z",
                    "gdp_pc",
                    "gdp_pc_z",
                    "vulnerability",
                    "vulnerability_z",
                    "control_corruption",
                    "control_corruption_z",
                    "total_ff_produced_per_gdp",
                    "total_ff_produced_per_gdp_z"
                    ),
  "Fundamental policy variables",
  
  default = "CAPMF OECD variables"
)]

# enforce group order
summary_country_year[, group := factor(
  group,
  levels = c("Germanwatch / CCPI policy scores", 
             "CAPMF OECD variables",
             "Fundamental policy variables"
             )
)]

# preserve order from variable_selection *within* each group
summary_country_year[, var_order := match(Variable, country_variable_selection)]
setorder(summary_country_year, group, var_order)
summary_country_year[, var_order := NULL]

## 2) Columns that appear in final output
final_cols <- setdiff(names(summary_country_year), "group")

## 3) Split, insert header row before each block
splits <- split(summary_country_year[, ..final_cols],
                summary_country_year$group)

summary_country_year_grouped <- rbindlist(
  lapply(names(splits), function(g) {
    dt  <- splits[[g]]
    hdr <- make_header_row(g, final_cols)
    
    rbind(
      as.data.table(hdr),
      dt,
      use.names = TRUE
    )
  }),
  use.names = TRUE
)

##### * table to word ----
num_cols <- c("Mean","SD","Min","Q1","Median","Q3","Max")
to_num <- function(x) suppressWarnings(as.numeric(gsub(",", "", x)))

summar_vars_plot <- summary_country_year_grouped %>%
  mutate(
    N = as.numeric(N),
    Missing = as.numeric(Missing),
    across(all_of(num_cols), ~ suppressWarnings(as.numeric(gsub(",", "", .x))))
  )

summar_vars_plot <- summar_vars_plot %>%
  mutate(across(all_of(num_cols), to_num)) %>%   # ensure numeric
  mutate(across(all_of(num_cols), ~ .x))         # no-op, keeps it numeric

# round ONLY the gdp_pc row
summar_vars_plot <- summar_vars_plot %>%
  mutate(across(
    all_of(num_cols),
    ~ ifelse(Variable == "gdp_pc", round(.x, 0), .x)
  ))

ft <- flextable(summar_vars_plot)

# --- Header labels ---
ft <- set_header_labels(
  ft,
  Missing = "N\nmissing",
  perc_missing = "%\nmissing"
)

# --- Alignment ---
ft <- align(ft, align = "left", part = "header")
ft <- align(ft, j = c("N","Mean","SD","Min","Q1","Median","Q3","Max","Missing","perc_missing"), align = "center", part = "all")
#ft <- align(ft, j = c("Missing","perc_missing"), align = "right", part = "body")

#right_cols <- c(
#  "Frequency", "Percentage", "Pct_est_obs",
#  "S1TOT", "S2TOT", "S3TOT",
#  "TOTS1", "TOTS2", "TOTS3",
#  "S1INT", "S2INT", "S3INT"
#)

# --- Typography / spacing ---
ft <- font(ft, fontname = "Times New Roman", part = "all")
ft <- fontsize(ft, size = 8, part = "all")
ft <- bold(ft, part = "header")
ft <- padding(ft, padding = 2, part = "all")
ft <- line_spacing(ft, space = 1, part = "all")

# --- Borders: same thickness above header, below header, and bottom rule ---
b_main <- fp_border(color = "black", width = 1)
ft <- border_remove(ft)
ft <- hline_top(ft, border = b_main, part = "header")
ft <- hline(ft, border = b_main, part = "header")
ft <- hline_bottom(ft, border = b_main, part = "body")

# --- Layout ---
ft <- autofit(ft)
ft <- set_table_properties(ft, width = 1, layout = "autofit")

#ft <- align(ft, j = right_cols, align = "right", part = "body")
hdr_vals <- c("Germanwatch / CCPI policy scores", 
              "Fundamental policy variables", 
              "CAPMF OECD variables")

# indent all other rows in the Variable column
ft <- padding(
  ft,
  i = ~ !(Variable %in% hdr_vals),
  j = "Variable",
  padding.left = 12,
  part = "body"
)

ft <- bold(ft, i = ~ Variable %in% hdr_vals, j = "Variable", bold = TRUE, part = "body")
# optional: add a thin line above each section header except the first one
#b_thin <- fp_border(color = "black", width = 0.5)
#ft <- hline(ft, i = ~ Variable %in% hdr_vals, border = b_thin, part = "body")
num_cols <- c("Mean","SD","Min","Q1","Median","Q3","Max")
ft <- colformat_num(ft, i = ~ Variable == "gdp_pc", j = num_cols, digits = 0, na_str = "-")

doc <- read_docx()
doc <- body_add_flextable(doc, ft)

print(doc, target = "../07_word/outputs/summary_policy_vars_2010_2025.docx")
rm(ft,doc,summar_vars_plot)
rm(summary_country_year,summary_country_year_grouped,
   summary_variables, summary_variables_grouped,
   splits,
   all_groups, b_main, dedup_groups,
   cy_dt_win,
   panel)

### (b) industry breakdown ----
dt_unique <- unique(df[, .(gvkey, gind, gics_industry)])
industry_summary <- dt_unique[
  ,
  .(`# Co` = uniqueN(gvkey)),
  by = .(gics_industry = gics_industry, gind = gind)
]
setorder(industry_summary, -`# Co`)

### (c) correlation analysis ----
#### large comp ----
correlation_variables <- c(
  "R_m", 
  "ROA", 
  "ROS",
  #"ebit_margin", 
  #"ebitda_margin", 
  #"ghg_scope1",
  #"ghg_scope2_loc", 
  #"ghg_scope3_up",
  "log_ghg_scope1",
  "log_ghg_scope2_loc", 
  "log_ghg_scope3_up",
  "ghg_scope1_intensity",
  "ghg_scope2_loc_intensity",
  "ghg_scope3_up_intensity",
  "logsize",
  "log_sales",
  "log_sales_lag6m",
  "leverage",
  #"bm",
  "log_bm",
  "ROE",
  "eps_growth",
  "sales_growth",
  "log_ppe",
  "inv_over_at",
  "vola",
  "mom",
  "beta_12m_lag1_loc",
  "beta_24m_lag1_loc"
  #"UMC",
  #"TRI",
  #"PRI",
  #"UCAI_country",
  #"UCAI_global_GDP",
  #"UCAI_global_EQ",
  #"nat_ccpi_score",
  #"internat_ccpi_score",
  #"PAI",
  #"cfa_cva_ratio"
)

cor_mat <- cor(df[, ..correlation_variables], use = "pairwise.complete.obs")

# large selection plot
corrplot(
  cor_mat,
  #number.cex = 0.3,
  method = "square",
  #order = "FPC",
  tl.cex = 0.5,
  tl.col = 'black'
)

#### emissions and firm size ----
correlation_variables_ghg <- c(
  "ghg_scope1",
  "ghg_scope2_loc", 
  "ghg_scope3_up",
  "log_ghg_scope1",
  "log_ghg_scope2_loc", 
  "log_ghg_scope3_up",
  "ghg_scope1_intensity",
  "ghg_scope2_loc_intensity",
  "ghg_scope3_up_intensity",
  "logsize",
  "log_sales",
  "log_at"
)

cor_mat_ghg <- cor(df[, ..correlation_variables_ghg], use = "pairwise.complete.obs")

# ghg selection plot
corrplot(
  cor_mat_ghg,
  method = "shade",
  #order = "FPC",
  addCoefasPercent = T,tl.cex = 0.5,
  tl.col = 'black', type = "lower"
)

#### country policy measures ----
correlation_variables_country <- c(
  "UMC_ARX",
  "UMC_AR1",
  "TRI",
  "PRI",
  "UCAI_country",
  "UCAI_global_GDP",
  "UCAI_global_EQ",
  "nat_ccpi_score",
  "internat_ccpi_score",
  "PAI",
  "cfa_cva_ratio",
  "capmf_std",
  "capmf_fossil_fuel_production_policies",
  "ghg_scope1_intensity",
  "ghg_scope2_loc_intensity",
  "ghg_scope3_up_intensity",
  "log_ghg_scope1",
  "log_ghg_scope2_loc", 
  "log_ghg_scope3_up"
)

cor_mat_country <- cor(df[, ..correlation_variables_country], use = "pairwise.complete.obs")

# ghg selection plot
corrplot(
  cor_mat_country,
  method = "square",
  #order = "FPC",
  addCoefasPercent = TRUE,tl.cex = 0.5,
  tl.col = 'black'
)


correlation_variables_country <- c(
  #"nat_ccpi_score",
  #"internat_ccpi_score",
  #"comb_ccpi_score",
  "nat_ccpi",
  "internat_ccpi",
  "combined_ccpi",
  "PAI",
  "cfa_cva_ratio",
  "capmf_std",
  "capmf_fossil_fuel_production_policies",
  "capmf_crosssectoral_policies",
  "capmf_international_policies",
  "capmf_sectoral_policies",
  "capmf_ghg_emission_targets"
)

cor_mat_country <- cor(df[, ..correlation_variables_country], use = "pairwise.complete.obs")

# ghg selection plot
corrplot(type = "lower",
  cor_mat_country,
  method = "square",
  #order = "FPC",
  addCoefasPercent = TRUE,tl.cex = 0.5,
  tl.col = 'black'
)



### (d) total market cap coverage ----
monthly_loc <- df[
  month >= as.Date("2010-01-01") & month <= as.Date("2025-01-01"),
  .(total_mktcap_usd = sum(mktcap_usd, na.rm = TRUE)),
  by = .(loc, month)
]

key_locs <- c("USA", "CHN", "JPN")

monthly_stack <- monthly_loc[
  , .(grp = fifelse(loc %chin% key_locs, loc, "ROW"),
      total_mktcap_usd),
  by = .(month, loc)
][
  , .(total_mktcap_usd = sum(total_mktcap_usd, na.rm = TRUE)),
  by = .(month, grp)
]

all_months <- data.table(month = seq(as.Date("2010-01-01"), as.Date("2025-01-01"), by = "month"))
all_grps   <- data.table(grp = c("USA", "CHN", "JPN", "ROW"))

monthly_stack <- CJ(month = all_months$month, grp = all_grps$grp, unique = TRUE)[
  monthly_stack, on = .(month, grp)
][
  is.na(total_mktcap_usd), total_mktcap_usd := 0
]

monthly_stack[, grp := factor(grp, levels = c("ROW", "JPN", "CHN", "USA"))]
setorder(monthly_stack, month, grp)

fill_colors <- c(
  "USA" = "#1B4F72",
  "CHN" = "#C0392B",
  "JPN" = "#2E8B57",
  "ROW" = "#B0B0B0"
)

plot_mktcap <- ggplot(
  monthly_stack,
  aes(x = month, y = total_mktcap_usd / 1e12, fill = grp, group = grp)
) +
  geom_area(position = position_stack(reverse = FALSE), alpha = 0.75) +
  scale_fill_manual(values = fill_colors) +
  scale_y_continuous(limits = c(0, NA), expand = c(0, 0)) +
  scale_x_date(
    limits = c(as.Date("2010-01-01"), as.Date("2025-01-01")),
    date_breaks = "1 year",
    date_labels = "%Y",
    expand = c(0, 0)
  ) +
  labs(x = "Year", y = "Total market cap (USD, trillions)", fill = NULL) +
  theme_classic(base_size = 10) +
  theme(
    axis.line          = element_line(linewidth = 0.6),
    axis.ticks         = element_line(linewidth = 0.7),
    panel.grid.major.y = element_line(colour = "grey88"),
    plot.margin        = margin(10, 20, 10, 20)
  ) +
  coord_cartesian(clip = "off")

plot_share <- ggplot(
  monthly_stack,
  aes(x = month, y = total_mktcap_usd, fill = grp, group = grp)
) +
  geom_area(position = "fill", alpha = 0.75) +
  scale_fill_manual(values = fill_colors) +
  scale_y_continuous(labels = percent, expand = c(0, 0)) +
  scale_x_date(
    limits = c(as.Date("2010-01-01"), as.Date("2025-01-01")),
    date_breaks = "1 year",
    date_labels = "%Y",
    expand = c(0, 0)
  ) +
  labs(x = "Year", y = "Share of global market cap", fill = NULL) +
  theme_classic(base_size = 10) +
  theme(
    axis.line          = element_line(linewidth = 0.6),
    axis.ticks         = element_line(linewidth = 0.7),
    panel.grid.major.y = element_line(colour = "grey88"),
    plot.margin        = margin(10, 20, 10, 20)
  ) +
  coord_cartesian(clip = "off")

combined_plot <- (plot_mktcap / plot_share) +
  plot_layout(ncol = 2, widths = c(1, 1),
    guides = "collect"
  ) +
  plot_annotation(
    tag_levels = "A",
    tag_prefix = "(",
    tag_suffix = ")"
  ) &
  theme(
    legend.position      = "right",
    legend.direction     = "vertical",
    legend.justification = "center",
    legend.key.height    = unit(0.4, "cm"),
    legend.key.width     = unit(0.9, "cm")
  )

combined_plot


##### * export to image ----
ggsave(
  filename = "../07_word/outputs/chart_market_cap_2010_2025.png",
  plot     = combined_plot,
  #device   = "emf",
  width    = 12.5,
  height   = 3.5,
  units    = "in",
  dpi = 800
)

## 2) Trucost specific summary statistics ----
### (a) Country level ----
## 1) add emission calendar year rule: if financialyear ends before june of year t
# the emissions are assigned to the previous year
df[, emission_calendar_year := fifelse(
  month(periodenddate) <= 5,
  year(periodenddate) - 1L,
  year(periodenddate)
)]

## 2) Firm-year (now firm-emission-year) panel 
df_firm_fy <- df[emission_calendar_year >= 2010
  , .(
    S1_year = first(ghg_scope1),
    S2_year = first(ghg_scope2_loc),
    S3_year = first(ghg_scope3_up),
    I1_year = first(ghg_scope1_intensity),
    I2_year = first(ghg_scope2_loc_intensity),
    I3_year = first(ghg_scope3_up_intensity)
  ),
  by = .(loc, gvkey, emission_calendar_year)
]

## 3) S1TOT / S2TOT / S3TOT: firm-level averages by country
emis_by_country_year <- df_firm_fy[
  , .(
    S1_cty = mean(S1_year, na.rm = TRUE),
    S2_cty = mean(S2_year, na.rm = TRUE),
    S3_cty = mean(S3_year, na.rm = TRUE),
    I1_cty = mean(I1_year, na.rm = TRUE),
    I2_cty = mean(I2_year, na.rm = TRUE),
    I3_cty = mean(I3_year, na.rm = TRUE)
  ),
  by = .(loc, emission_calendar_year)
]

emis_by_country <- emis_by_country_year[
  , .(
    S1TOT = round(mean(S1_cty, na.rm = TRUE)),
    S2TOT = round(mean(S2_cty, na.rm = TRUE)),
    S3TOT = round(mean(S3_cty, na.rm = TRUE)),
    S1INT = round(mean(I1_cty, na.rm = TRUE), 3),
    S2INT = round(mean(I2_cty, na.rm = TRUE), 3),
    S3INT = round(mean(I3_cty, na.rm = TRUE), 3)
  ),
  by = loc
]

## 4) TOTS1 / TOTS2 / TOTS3: country–emission-year totals, averaged across years
emis_sums_by_country <- df_firm_fy[
  , .(
    S1_total_year = sum_or_na(S1_year),
    S2_total_year = sum_or_na(S2_year),
    S3_total_year = sum_or_na(S3_year)
  ),
  by = .(loc, emission_calendar_year)
][
  , .(
    TOTS1 = round(mean(S1_total_year, na.rm = TRUE)),
    TOTS2 = round(mean(S2_total_year, na.rm = TRUE)),
    TOTS3 = round(mean(S3_total_year, na.rm = TRUE))
  ),
  by = loc
]

mktcap_monthly_loc <- df[
  month >= as.Date("2010-01-01") & month <= as.Date("2025-01-01"),
  .(mktcap_monthly = sum(mktcap_usd, na.rm = TRUE)),
  by = .(loc, month)
]

mktcap_avg_monthly_by_country <- mktcap_monthly_loc[
  , .(
    AVG_MTH_MKTCAP_USD     = mean(mktcap_monthly, na.rm = TRUE),
    AVG_MTH_MKTCAP_USD_TRN = mean(mktcap_monthly, na.rm = TRUE) / 1e12
  ),
  by = loc
][
  , `:=`(
    AVG_MTH_MKTCAP_USD     = round(AVG_MTH_MKTCAP_USD, 0),
    AVG_MTH_MKTCAP_USD_TRN = round(AVG_MTH_MKTCAP_USD_TRN, 3)
  )
]

## 5) Country-level summary stats (using emission_calendar_year)
country_stats <- df[emission_calendar_year >= 2010,] %>%
  group_by(loc, iso2_country, country_name) %>%
  summarise(
    Frequency           = n(),
    `# co.`             = n_distinct(gvkey),
    first_emission_year = min(emission_calendar_year, na.rm = TRUE),
    Obs_estimated       = sum(ghg_scope1_class == "estimated", na.rm = TRUE),
    .groups             = "drop"
  ) %>%
  mutate(
    Percentage  = round(Frequency / sum(Frequency) * 100, 3),
    Pct_est_obs = round(100 * Obs_estimated / Frequency, 1)
  ) %>%
  left_join(emis_by_country,      by = "loc") %>%
  left_join(emis_sums_by_country, by = "loc") %>%
  left_join(mktcap_avg_monthly_by_country, by = "loc") %>%
  select(
    iso2_country,
    loc,
    country_name,
    first_emission_year,
    Frequency,
    Percentage,
    Pct_est_obs,
    `# co.`,
    S1TOT, S2TOT, S3TOT,
    S1INT, S2INT, S3INT,
    TOTS1, TOTS2, TOTS3,
    everything(),
    AVG_MTH_MKTCAP_USD_TRN
  ) %>% select(-Obs_estimated) %>%
  arrange(iso2_country)

# Drop helper tables
rm(emis_sums_by_country, emis_by_country, df_firm_fy)

#### * table to word -----
country_stats_plot <- country_stats %>%
  mutate(
    first_emission_year = as.integer(first_emission_year),
    
    # Ensure numeric first (in case something is labelled/character)
    Percentage  = as.numeric(Percentage),
    Pct_est_obs = as.numeric(Pct_est_obs),
    S1INT = as.numeric(S1INT),
    S2INT = as.numeric(S2INT),
    S3INT = as.numeric(S3INT),
    
    # FORCE display with exactly 2 decimals (this is the key)
    Percentage = sprintf("%.2f", Percentage),
    S1INT = sprintf("%.2f", S1INT),
    S2INT = sprintf("%.2f", S2INT),
    S3INT = sprintf("%.2f", S3INT)
  ) %>%
  select(
    iso2_country, loc, country_name,
    first_emission_year, Frequency, Percentage, Pct_est_obs, `# co.`,
    S1TOT, S2TOT, S3TOT,
    TOTS1, TOTS2, TOTS3,
    S1INT, S2INT, S3INT,
    AVG_MTH_MKTCAP_USD_TRN
  )

ft <- flextable(country_stats_plot)

# --- Header labels ---
ft <- set_header_labels(
  ft,
  iso2_country = "ISO2",
  loc = "ISO3",
  country_name = "Country",
  first_emission_year = "1st\nObs.Year",
  Frequency = "Freq.",
  Percentage = "%\ntot. Obs",
  Pct_est_obs = "%\nEst. Obs",
  `# co.` = "# Co.",
  AVG_MTH_MKTCAP_USD_TRN = "MKTCAP\n (USD Trn.)"
)

# --- Alignment ---
ft <- align(ft, align = "center", part = "header")
ft <- align(ft, j = c("iso2_country", "loc", "first_emission_year", "# co."), align = "center", part = "all")
ft <- align(ft, j = "country_name", align = "left", part = "all")

right_cols <- c(
  "Frequency", "Percentage", "Pct_est_obs",
  "S1TOT", "S2TOT", "S3TOT",
  "TOTS1", "TOTS2", "TOTS3",
  "S1INT", "S2INT", "S3INT"
)
ft <- align(ft, j = right_cols, align = "right", part = "body")

# --- Number formatting (skip Percentage + S*INT because they are now character strings) ---
ft <- colformat_num(ft, j = "first_emission_year", digits = 0, big.mark = "")
ft <- colformat_num(ft, j = "Frequency", digits = 0, big.mark = "'")
ft <- colformat_num(ft, j = "Pct_est_obs", digits = 2)  # set to 2; change to 1 if you prefer

ft <- colformat_num(
  ft,
  j = c("S1TOT","S2TOT","S3TOT","TOTS1","TOTS2","TOTS3"),
  digits = 0, big.mark = ","
)

# --- Typography / spacing ---
ft <- font(ft, fontname = "Times New Roman", part = "all")
ft <- fontsize(ft, size = 8, part = "all")
ft <- bold(ft, part = "header")
ft <- padding(ft, padding = 1.5, part = "all")
ft <- line_spacing(ft, space = 0.75, part = "all")

# --- Borders: same thickness above header, below header, and bottom rule ---
b_main <- fp_border(color = "black", width = 1)
ft <- border_remove(ft)
ft <- hline_top(ft, border = b_main, part = "header")
ft <- hline(ft, border = b_main, part = "header")
ft <- hline_bottom(ft, border = b_main, part = "body")

# --- Layout ---
ft <- autofit(ft)
ft <- set_table_properties(ft, width = 1, layout = "autofit")

# --- Write to Word in landscape ---
doc <- read_docx()
doc <- body_set_default_section(
  doc,
  prop_section(page_size = page_size(orient = "landscape"))
)
doc <- body_add_flextable(doc, ft)

print(doc, target = "../07_word/outputs/table_country_emissions_2010_2025.docx")
rm(ft,doc,country_stats_plot)

### (b.1) US share of estimate to reported over time ----
## 6) Share of estimate vs reported over time (by year)

fy_USA <- df[loc == "USA", .(
  cls = fifelse(
    any(ghg_scope1_class == "disclosed", na.rm = TRUE), "disclosed",
    fifelse(any(ghg_scope1_class == "estimated", na.rm = TRUE), "estimated", NA_character_)
  )
), by = .(year, gvkey)]


# 2) Firm counts by year (firm-year unit)
firm_counts_USA <- fy_USA[, .(
  Firms_disclosed = sum(cls == "disclosed", na.rm = TRUE),
  Firms_estimated = sum(cls == "estimated", na.rm = TRUE)
), by = year]

firm_counts_USA[, Firms_full := Firms_disclosed + Firms_estimated]

# 3) Observation counts by year (row unit; typically firm-month)
obs_counts_USA <- df[loc == "USA", .(
  Obs_disclosed = sum(ghg_scope1_class == "disclosed", na.rm = TRUE),
  Obs_estimated = sum(ghg_scope1_class == "estimated", na.rm = TRUE)
), by = year]

obs_counts_USA[, Obs_full := Obs_disclosed + Obs_estimated]
obs_counts_USA[, Pct_est_obs := round(100 * Obs_estimated / Obs_full, 1)]

# 4) Combine into one table
setkey(firm_counts_USA, year)
setkey(obs_counts_USA, year)

tc_estimate_over_time_USA <- obs_counts_USA[firm_counts_USA]  # left join keeps all years in obs_counts
setorder(tc_estimate_over_time_USA, year)

tc_estimate_over_time_USA <- tc_estimate_over_time_USA[year >= 2010]

## 7) Plot: evolution over emission calendar years
# 1) Long format
plot_data <- tc_estimate_over_time_USA %>%
  select(
    year,
    `Full Sample` = Firms_full,
    Estimated     = Firms_estimated,
    Disclosed     = Firms_disclosed
  ) %>%
  pivot_longer(-year, names_to = "Series", values_to = "Firms")

plot_data_filtered <- plot_data %>%
  filter(year >= 2010 & year <= 2025)

last_year <- max(plot_data_filtered$year)

label_data <- plot_data_filtered %>%
  group_by(Series) %>%
  filter(year == last_year)

# plot
plot_1 <- ggplot(plot_data_filtered, aes(x = year, y = Firms, color = Series)) +
  
  geom_line(linewidth = 1.2,  alpha = 0.7) +
  
  geom_vline(
    xintercept = 2015,
    linetype   = "dotted",
    linewidth  = 0.8,
    color      = "black"
  ) +
  
  geom_text_repel(
    data = label_data,
    aes(label = Series, color = Series),
    direction        = "y",
    hjust            = 0,               
    xlim             = c(last_year + 2.5, NA),
    size             = 3.5,
    box.padding      = 0.4,
    segment.size     = 0.5,
    segment.alpha    = 0.7,
    segment.linetype = "dotted",
    segment.curvature = -0.1,
    min.segment.length = 0,
    segment.ncp      = 3,
    segment.angle    = 20,
    show.legend      = FALSE
  ) +
  scale_y_continuous(
    limits = c(0, NA),
    expand = c(0, 0)
  )+
  
  scale_color_manual(
    values = c(
      "Full Sample" = "#1B4F72",
      "Estimated"   = "#C0392B",
      "Disclosed"   = "#2E8B57"
    )
  ) +
  
  scale_x_continuous(
    limits = c(2010, last_year),        
    breaks = seq(2010, last_year, 1),
    expand = c(0, 0)
  ) +
  
  labs(
    #title = "Evolution of Estimated vs Disclosed Firms Over Time (Scope 1)",
    x     = "Year",
    y     = "Number of Firms"
  ) +
  
  theme_classic(base_size = 10) +
  theme(
    legend.position      = "none",
    plot.title           = element_text(hjust = 0.5, face = "bold"),
    axis.line            = element_line(linewidth = 0.6),
    axis.ticks           = element_line(linewidth = 0.7),
    panel.grid.major.y   = element_line(colour = "grey88"),
    plot.margin          = margin(6, 80, 6, 6)
  ) +
  
  coord_cartesian(clip = "off")


#### * export to image----
ggsave(
  filename = "../07_word/outputs/plot_USA_share_estimated_2010_2025.png",
  plot     = plot_1,
  #device   = "emf",
  width    = 12.5,
  height   = 3.5,
  units    = "in",
  dpi = 800
)

### (b.2) share of estimate to reported over time ----
## 6) Share of estimate vs reported over time (by year)

fy <- df[, .(
  cls = fifelse(
    any(ghg_scope1_class == "disclosed", na.rm = TRUE), "disclosed",
    fifelse(any(ghg_scope1_class == "estimated", na.rm = TRUE), "estimated", NA_character_)
  )
), by = .(year, gvkey)]

# 2) Firm counts by year (firm-year unit)
firm_counts <- fy[, .(
  Firms_disclosed = sum(cls == "disclosed", na.rm = TRUE),
  Firms_estimated = sum(cls == "estimated", na.rm = TRUE)
), by = year]

firm_counts[, Firms_full := Firms_disclosed + Firms_estimated]

# 3) Observation counts by year (row unit; typically firm-month)
obs_counts <- df[, .(
  Obs_disclosed = sum(ghg_scope1_class == "disclosed", na.rm = TRUE),
  Obs_estimated = sum(ghg_scope1_class == "estimated", na.rm = TRUE)
), by = year]

obs_counts[, Obs_full := Obs_disclosed + Obs_estimated]
obs_counts[, Pct_est_obs := round(100 * Obs_estimated / Obs_full, 1)]

# 4) Combine into one table
setkey(firm_counts, year)
setkey(obs_counts, year)

tc_estimate_over_time <- obs_counts[firm_counts]  # left join keeps all years in obs_counts
setorder(tc_estimate_over_time, year)

tc_estimate_over_time <- tc_estimate_over_time[year >= 2010]

## 7) Plot: evolution over emission calendar years
# 1) Long format
plot_data <- tc_estimate_over_time %>%
  select(
    year,
    `Full Sample` = Firms_full,
    Estimated     = Firms_estimated,
    Disclosed     = Firms_disclosed
  ) %>%
  pivot_longer(-year, names_to = "Series", values_to = "Firms")

plot_data_filtered <- plot_data %>%
  filter(year >= 2010 & year <= 2025)

last_year <- max(plot_data_filtered$year)

label_data <- plot_data_filtered %>%
  group_by(Series) %>%
  filter(year == last_year)

# plot
plot_1 <- ggplot(plot_data_filtered, aes(x = year, y = Firms, color = Series)) +
  
  geom_line(linewidth = 1.2,  alpha = 0.7) +
  
  geom_vline(
    xintercept = 2015,
    linetype   = "dotted",
    linewidth  = 0.8,
    color      = "black"
  ) +
  
  geom_text_repel(
    data = label_data,
    aes(label = Series, color = Series),
    direction        = "y",
    hjust            = 0,               
    xlim             = c(last_year + 2.5, NA),
    size             = 3.5,
    box.padding      = 0.4,
    segment.size     = 0.5,
    segment.alpha    = 0.7,
    segment.linetype = "dotted",
    segment.curvature = -0.1,
    min.segment.length = 0,
    segment.ncp      = 3,
    segment.angle    = 20,
    show.legend      = FALSE
  ) +
  
  scale_color_manual(
    values = c(
      "Full Sample" = "#1B4F72",
      "Estimated"   = "#C0392B",
      "Disclosed"   = "#2E8B57"
    )
  ) +
  
  scale_x_continuous(
    limits = c(2010, last_year),        
    breaks = seq(2010, last_year, 1),
    expand = c(0, 0)
  ) +
  
  labs(
    #title = "Evolution of Estimated vs Disclosed Firms Over Time (Scope 1)",
    x     = "Year",
    y     = "Number of Firms"
  ) +
  
  theme_classic(base_size = 10) +
  theme(
    legend.position      = "none",
    plot.title           = element_text(hjust = 0.5, face = "bold"),
    axis.line            = element_line(linewidth = 0.6),
    axis.ticks           = element_line(linewidth = 0.7),
    panel.grid.major.y   = element_line(colour = "grey88"),
    plot.margin          = margin(6, 80, 6, 6)
  ) +
  
  coord_cartesian(clip = "off")

#### * export to image----
ggsave(
  filename = "../07_word/outputs/plot_share_estimated_2010_2025.png",
  plot     = plot_1,
  #device   = "emf",
  width    = 10.5,
  height   = 3.5,
  units    = "in",
  dpi = 800
)


### (c.1) Constant Sample carbon emissions over time ----
tc_ghg_over_time <- df[
  datadate >= as.Date("2017-01-01") &
  datadate <= as.Date("2025-01-01") &
  gvkey %in% intersect(
    unique(df$gvkey[df$month == as.Date("2017-01-01")]),
    unique(df$gvkey[df$month == as.Date("2025-01-01")])
  )
] %>%
  group_by(month) %>%
  summarise(
    avg_ghg_scope1_intensity     = mean(ghg_scope1_intensity,     na.rm = TRUE),
    avg_ghg_scope2_loc_intensity = mean(ghg_scope2_loc_intensity, na.rm = TRUE),
    avg_ghg_scope3_up_intensity  = mean(ghg_scope3_up_intensity,  na.rm = TRUE),
    avg_ghg_scope1               = mean(ghg_scope1,               na.rm = TRUE),
    avg_ghg_scope2_loc           = mean(ghg_scope2_loc,           na.rm = TRUE),
    avg_ghg_scope3_up            = mean(ghg_scope3_up,            na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(month)

gvkey_count <- df[
  datadate >= as.Date("2017-01-01") &
  datadate <= as.Date("2025-01-01") &
  gvkey %in% intersect(
    unique(df$gvkey[df$month == as.Date("2017-01-01")]),
    unique(df$gvkey[df$month == as.Date("2025-01-01")])
  )
] %>%
  filter(!is.na(ghg_scope1)) %>%
  group_by(month) %>%
  summarise(
    n_firms = n_distinct(gvkey),
    .groups = "drop"
  )

## 2) Long formats: levels and intensities
# Emissions levels
levels_long <- tc_ghg_over_time %>%
  transmute(
    month,
    `Scope 1`          = avg_ghg_scope1,
    `Scope 2 (loc.)`   = avg_ghg_scope2_loc,
    `Scope 3 upstream` = avg_ghg_scope3_up
  ) %>%
  pivot_longer(
    -month,
    names_to  = "Scope",
    values_to = "Emissions"
  )

# Intensities
intensity_long <- tc_ghg_over_time %>%
  transmute(
    month,
    `Scope 1`          = avg_ghg_scope1_intensity,
    `Scope 2 (loc.)`   = avg_ghg_scope2_loc_intensity,
    `Scope 3 upstream` = avg_ghg_scope3_up_intensity
  ) %>%
  pivot_longer(
    -month,
    names_to  = "Scope",
    values_to = "Intensity"
  )


## 3) color mapping
scope_cols <- c(
  "Scope 1"          = "#1B3A8A",  # blue
  "Scope 2 (loc.)"   = "#E5533D",  # orange
  "Scope 3 upstream" = "#1F9E89"   # green
)


## 4) Top panel: emissions levels
p_levels <- ggplot(levels_long,
                   aes(x = month, y = Emissions, colour = Scope)) +
  geom_line(linewidth = 1) +
  scale_colour_manual(values = scope_cols) +
  scale_x_date(
    date_breaks = "2 years",
    date_labels = "%Y",
    expand      = c(0, 0)
  ) +
  scale_y_continuous(
    labels = label_number(scale = 1e-6, suffix = ""),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = NULL,
    y = "Average emissions\n(MtCO\u2082e)"
  ) +
  theme_classic(base_size = 10) +
  theme(
    panel.grid.major.y = element_line(colour = "grey88"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    legend.position    = "none",
    axis.line        = element_line(linewidth = 0.6),
    axis.text.x        = element_blank(),
    axis.ticks.x       = element_blank(),
    plot.margin        = margin(5.5, 5.5, 4, 5.5)
  )

## 5) middle panel: intensities
p_intensity <- ggplot(intensity_long,
                      aes(x = month, y = Intensity, colour = Scope)) +
  geom_line(linewidth = 1) +
  scale_colour_manual(values = scope_cols) +
  scale_x_date(
    date_breaks = "1 year",
    date_labels = "%Y",
    expand      = c(0, 0)
  ) +
  scale_y_continuous(
    labels = label_number(accuracy = 0.1),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = NULL,
    y = "Average\nCO\u2082 intensity"
  ) +
  theme_classic(base_size = 10) +
  theme(
    panel.grid.major.y = element_line(colour = "grey88"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    legend.position    = "none",
    axis.line          = element_line(linewidth = 0.6),
    axis.text.x        = element_blank(),
    axis.ticks.x       = element_blank(),
    plot.margin        = margin(4, 5.5, 4, 5.5)
  )

## 6) bottom panel: 
share_long <- df[
  datadate >= as.Date("2017-01-01") &
    datadate <= as.Date("2025-01-01") &
    gvkey %in% intersect(
      unique(df$gvkey[df$month == as.Date("2017-01-01")]),
      unique(df$gvkey[df$month == as.Date("2025-01-01")])
    )
] %>%
  group_by(month) %>%
  summarise(
    `Scope 1`          = mean(ghg_scope1_class     == "estimated", na.rm = TRUE),
    `Scope 2 (loc.)`   = mean(ghg_scope2_loc_class == "estimated", na.rm = TRUE),
    `Scope 3 upstream` = mean(ghg_scope3_up_class  == "estimated", na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    -month,
    names_to  = "Scope",
    values_to = "Share_estimated"
  )

p_share <- ggplot(share_long,
                  aes(x = month, y = Share_estimated, colour = Scope)) +
  geom_line(linewidth = 1) +
  scale_colour_manual(values = scope_cols) +
  scale_x_date(
    date_breaks = "1 year",
    date_labels = "%Y",
    expand      = c(0, 0)
  ) +
  scale_y_continuous(
    labels = scales::label_percent(accuracy = 1),
    limits = c(0, 1),
    breaks = c(0, 0.5, 1),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = NULL,
    y = "Share\nestimated (obs.)"
  ) +
  theme_classic(base_size = 10) +
  theme(
    panel.grid.major.y = element_line(colour = "grey88"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    legend.position    = "none",
    axis.line          = element_line(linewidth = 0.6),
    axis.text.x        = element_blank(),
    axis.ticks.x       = element_blank(),
    
    plot.margin        = margin(4, 5.5, 5.5, 5.5)
  ) +
  geom_hline(
    yintercept = c(0, 0.5, 1),
    linetype   = "dotted",
    linewidth  = 0.3,
    colour     = "grey85"
  )


p_gvkeys <- ggplot(gvkey_count, aes(x = month, y = n_firms)) +
  geom_col(
    fill  = "grey70",
    alpha = 0.7,
    width = 20   # good width for monthly bars on a date axis
  ) +
  scale_x_date(
    date_breaks = "1 year",
    date_labels = "%Y",
    expand      = c(0, 0)
  ) +
  scale_y_continuous(
    labels = scales::label_comma(),
    limits = c(0, 15000),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = NULL,
    y = "Companies w.\nscope 1 data"
  ) +
  theme_classic(base_size = 10) +
  theme(
    panel.grid.major.y = element_line(colour = "grey88"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    legend.position    = "none",
    axis.line          = element_line(linewidth = 0.6),
    axis.text.x        = element_text(vjust = 0.5),
    plot.margin        = margin(4, 5.5, 5.5, 5.5)
  )

## 1) Event date and label
event_dt <- tibble::tibble(
  ym_date = as.Date(c(
    "2016-12-01", 
    "2019-01-01",
    "2021-12-31",
    "2023-01-01"
  )),
  label = c(
    "Trucost sample\n increase",
    "End of BK2023 &\nARR2024 coverage",
    "End of\nZ2025\ncoverage",
    "End of HvdS2025\n coverage"
  )
)


## 3) Apply panels
#p_levels    <- add_event_line(p_levels,    show_label = TRUE)   # line + label
#p_intensity <- add_event_line(p_intensity, show_label = FALSE)   # line only
#p_share     <- add_event_line(p_share,     show_label = FALSE)   # line only
#p_gvkeys    <- add_event_line(p_gvkeys, show_label = FALSE) # barchart only

## 6) Stack with patchwork: shared x alignment, shared legend at top
combined_plot_4 <- (
  p_levels / plot_spacer() /
    p_intensity / plot_spacer() /
    p_share / plot_spacer() /
    p_gvkeys
) +
  plot_layout(
    heights = c(1, 0.05, 1, 0.05, 1, 0.05, 0.9),
    guides  = "collect"
  ) &
  theme(
    legend.position   = "top",
    legend.direction  = "horizontal",
    legend.key.width  = unit(2, "lines"),
    legend.margin     = margin(b = 3),
    plot.margin       = margin(5, 10, 5, 5)
  )

combined_plot_4 <- combined_plot_4 +
  plot_annotation(
    title    = NULL
  ) &
  theme(
    plot.title    = element_text(hjust = 0.5, face = "bold", size = 13),
    plot.caption  = element_text(hjust = 0, size = 9),
    plot.margin = margin(5, 10, 5, 14),
    axis.text.y  = element_text(size = 7),
    axis.title.y = element_text(size = 8)
  )

combined_plot_4

#### * export to image ----
ggsave(
  filename = "../07_word/outputs/plot_CONST_average_emissions_int_cov_2010_2025.png",
  plot     = combined_plot_4,
  #device   = "emf",
  width    = 10.5,
  height   = 5.5,
  units    = "in",
  dpi = 600
)


### (c.2) carbon emissions over time ----
tc_ghg_over_time <- df[datadate >= "2010-01-01" & datadate <= "2025-01-01"] %>%
  group_by(month) %>%
  summarise(
    avg_ghg_scope1_intensity     = mean(ghg_scope1_intensity,     na.rm = TRUE),
    avg_ghg_scope2_loc_intensity = mean(ghg_scope2_loc_intensity, na.rm = TRUE),
    avg_ghg_scope3_up_intensity  = mean(ghg_scope3_up_intensity,  na.rm = TRUE),
    avg_ghg_scope1               = mean(ghg_scope1,               na.rm = TRUE),
    avg_ghg_scope2_loc           = mean(ghg_scope2_loc,           na.rm = TRUE),
    avg_ghg_scope3_up            = mean(ghg_scope3_up,            na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(month)

gvkey_count <- df[datadate >= "2010-01-01" & datadate <= "2025-01-01"] %>%
  filter(!is.na(ghg_scope1)) %>%
  group_by(month) %>%
  summarise(
    n_firms = n_distinct(gvkey),
    .groups = "drop"
  )

## 2) Long formats: levels and intensities
# Emissions levels
levels_long <- tc_ghg_over_time %>%
  transmute(
    month,
    `Scope 1`          = avg_ghg_scope1,
    `Scope 2 (loc.)`   = avg_ghg_scope2_loc,
    `Scope 3 upstream` = avg_ghg_scope3_up
  ) %>%
  pivot_longer(
    -month,
    names_to  = "Scope",
    values_to = "Emissions"
  )

# Intensities
intensity_long <- tc_ghg_over_time %>%
  transmute(
    month,
    `Scope 1`          = avg_ghg_scope1_intensity,
    `Scope 2 (loc.)`   = avg_ghg_scope2_loc_intensity,
    `Scope 3 upstream` = avg_ghg_scope3_up_intensity
  ) %>%
  pivot_longer(
    -month,
    names_to  = "Scope",
    values_to = "Intensity"
  )


## 3) color mapping
scope_cols <- c(
  "Scope 1"          = "#1B3A8A",  # blue
  "Scope 2 (loc.)"   = "#E5533D",  # orange
  "Scope 3 upstream" = "#1F9E89"   # green
)


## 4) Top panel: emissions levels
p_levels <- ggplot(levels_long,
                   aes(x = month, y = Emissions, colour = Scope)) +
  geom_line(linewidth = 1) +
  scale_colour_manual(values = scope_cols) +
  scale_x_date(
    date_breaks = "2 years",
    date_labels = "%Y",
    expand      = c(0, 0)
  ) +
  scale_y_continuous(
    labels = label_number(scale = 1e-6, suffix = ""),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = NULL,
    y = "Average emissions\n(MtCO\u2082e)"
  ) +
  theme_classic(base_size = 10) +
  theme(
    panel.grid.major.y = element_line(colour = "grey88"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    legend.position    = "none",
    axis.line        = element_line(linewidth = 0.6),
    axis.text.x        = element_blank(),
    axis.ticks.x       = element_blank(),
    plot.margin        = margin(5.5, 5.5, 4, 5.5)
  )

## 5) middle panel: intensities
p_intensity <- ggplot(intensity_long,
                      aes(x = month, y = Intensity, colour = Scope)) +
  geom_line(linewidth = 1) +
  scale_colour_manual(values = scope_cols) +
  scale_x_date(
    date_breaks = "1 year",
    date_labels = "%Y",
    expand      = c(0, 0)
  ) +
  scale_y_continuous(
    labels = label_number(accuracy = 0.1),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = NULL,
    y = "Average\nCO\u2082 intensity"
  ) +
  theme_classic(base_size = 10) +
  theme(
    panel.grid.major.y = element_line(colour = "grey88"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    legend.position    = "none",
    axis.line          = element_line(linewidth = 0.6),
    axis.text.x        = element_blank(),
    axis.ticks.x       = element_blank(),
    plot.margin        = margin(4, 5.5, 4, 5.5)
  )

## 6) bottom panel: 
share_long <- df[datadate >= ("2010-01-01") & datadate <= ("2025-01-01")] %>%
  group_by(month) %>%
  summarise(
    `Scope 1`          = mean(ghg_scope1_class     == "estimated", na.rm = TRUE),
    `Scope 2 (loc.)`   = mean(ghg_scope2_loc_class == "estimated", na.rm = TRUE),
    `Scope 3 upstream` = mean(ghg_scope3_up_class  == "estimated", na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    -month,
    names_to  = "Scope",
    values_to = "Share_estimated"
  )

p_share <- ggplot(share_long,
                  aes(x = month, y = Share_estimated, colour = Scope)) +
  geom_line(linewidth = 1) +
  scale_colour_manual(values = scope_cols) +
  scale_x_date(
    date_breaks = "1 year",
    date_labels = "%Y",
    expand      = c(0, 0)
  ) +
  scale_y_continuous(
    labels = scales::label_percent(accuracy = 1),
    limits = c(0, 1),
    breaks = c(0, 0.5, 1),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = NULL,
    y = "Share\nestimated (obs.)"
  ) +
  theme_classic(base_size = 10) +
  theme(
    panel.grid.major.y = element_line(colour = "grey88"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    legend.position    = "none",
    axis.line          = element_line(linewidth = 0.6),
    axis.text.x        = element_blank(),
    axis.ticks.x       = element_blank(),
    
    plot.margin        = margin(4, 5.5, 5.5, 5.5)
  ) +
  geom_hline(
    yintercept = c(0, 0.5, 1),
    linetype   = "dotted",
    linewidth  = 0.3,
    colour     = "grey85"
  )


p_gvkeys <- ggplot(gvkey_count, aes(x = month, y = n_firms)) +
  geom_col(
    fill  = "grey70",
    alpha = 0.7,
    width = 20   # good width for monthly bars on a date axis
  ) +
  scale_x_date(
    date_breaks = "1 year",
    date_labels = "%Y",
    expand      = c(0, 0)
  ) +
  scale_y_continuous(
    labels = scales::label_comma(),
    limits = c(0, 20000),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = NULL,
    y = "Companies w.\nscope 1 data"
  ) +
  theme_classic(base_size = 10) +
  theme(
    panel.grid.major.y = element_line(colour = "grey88"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    legend.position    = "none",
    axis.line          = element_line(linewidth = 0.6),
    axis.text.x        = element_text(vjust = 0.5),
    plot.margin        = margin(4, 5.5, 5.5, 5.5)
  )

## 1) Event date and label
event_dt <- tibble::tibble(
  ym_date = as.Date(c(
    "2016-12-01", 
    "2019-01-01",
    "2021-12-31",
    "2023-01-01"
  )),
  label = c(
    "Trucost sample\n increase",
    "End of BK2023 &\nARR2024 coverage",
    "End of\nZ2025\ncoverage",
    "End of HvdS2025\n coverage"
  )
)


## 3) Apply panels
p_levels    <- add_event_line(p_levels,    show_label = TRUE)   # line + label
p_intensity <- add_event_line(p_intensity, show_label = FALSE)   # line only
p_share     <- add_event_line(p_share,     show_label = FALSE)   # line only
p_gvkeys    <- add_event_line(p_gvkeys, show_label = FALSE) # barchart only

## 6) Stack with patchwork: shared x alignment, shared legend at top
combined_plot_4 <- (
  p_levels / plot_spacer() /
    p_intensity / plot_spacer() /
    p_share / plot_spacer() /
    p_gvkeys
) +
  plot_layout(
    heights = c(1, 0.05, 1, 0.05, 1, 0.05, 0.9),
    guides  = "collect"
  ) &
  theme(
    legend.position   = "top",
    legend.direction  = "horizontal",
    legend.key.width  = unit(2, "lines"),
    legend.margin     = margin(b = 3),
    plot.margin       = margin(5, 10, 5, 5)
  )

combined_plot_4 <- combined_plot_4 +
  plot_annotation(
    title    = NULL
  ) &
  theme(
    plot.title    = element_text(hjust = 0.5, face = "bold", size = 13),
    plot.caption  = element_text(hjust = 0, size = 9),
    plot.margin = margin(5, 10, 5, 14),
    axis.text.y  = element_text(size = 7),
    axis.title.y = element_text(size = 8)
  )

combined_plot_4

#### * export to image ----
ggsave(
  filename = "../07_word/outputs/plot_average_emissions_int_cov_2010_2025.png",
  plot     = combined_plot_4,
  #device   = "emf",
  width    = 10.5,
  height   = 5.5,
  units    = "in",
  dpi = 600
)



### (d) Scatterplot log sales to log emissions----
plot_ghg_scatter_faceted(
  data        = df,
  year_select = 2024,
  ghg_var     = log_ghg_scope1,
  size_var    = log_sales,
  gvkeys      = c("101304", "355385", "144496", "232646", "220833")
)

plot_ghg_scatter_faceted(
  data        = df,
  year_select = 2024,
  ghg_var     = ghg_scope1_intensity,
  size_var    = log_sales,
  gvkeys      = c("101304", "355385", "144496", "232646", "220833")
)

p1 <- plot_ghg_scatter_faceted(
  data        = df,
  year_select = 2023,
  ghg_var     = log_ghg_scope1,
  size_var    = log_sales,
  gvkeys      = c("101304", "355385", "144496")
) +
  labs(title = NULL) +
  theme(plot.title = element_blank(),
        axis.title.x = element_blank())+
  theme(
    panel.border = element_rect(
      colour   = "black",
      fill     = NA,
      linewidth = 0.7
    )
  )

p2 <- plot_ghg_scatter_faceted(
  data        = df,
  year_select = 2023,
  ghg_var     = ghg_scope1_intensity,
  size_var    = log_sales,
  gvkeys      = c("101304", "355385", "144496")
) +
  labs(title = NULL) +
  theme(plot.title = element_blank(),
        strip.text = element_blank(),
        strip.background = element_blank())+
  theme(
    panel.border = element_rect(
      colour   = "black",
      fill     = NA,
      linewidth = 0.7
    )
  )

combined <- (p1 / p2) 
combined

#### * export to image ----
ggsave(
  filename = "../07_word/outputs/estimated_vs_disclosed_log_sales_2023.png",
  plot     = combined,
  #device   = "emf",
  width    = 12.5,
  height   = 6.5,
  units    = "in",
  dpi = 800
)

## 3) Climate attention and concern measures ----
### (a) comparison with events (indexes) ----
monthly_merged_attention <- read_parquet("../01_data/11_wrds_R_output/monthly_merged_attention.parquet")
setDT(monthly_merged_attention)
monthly_merged_attention <-monthly_merged_attention[month >= "2007-01-01"]


## 1) Long format for stacked line charts
att_long <- melt(
  monthly_merged_attention,
  id.vars = "month",
  measure.vars = c("mccc_monthly",
                   "Transition_concern_monthly",
                   #"Physical_concern_monthly",
                   "cai_global_gdp_monthly"
                   #"cai_global_eq_monthly"
  ),
  variable.name = "series",
  value.name   = "value"
)

att_long[, series := factor(
  series,
  levels = c("mccc_monthly",
             "Transition_concern_monthly",
             #"Physical_concern_monthly",
             "cai_global_gdp_monthly"
             #"cai_global_eq_monthly"
  ),
  labels = c("MCCC",
             "TRI",
             #"Physical concern",
             "CAI global GDP"
             #"CAI global equity"
  )
)]

# 1) All events in a single named vector
event_all <- c(
  "2007-12-01" = "2007 COP13 ",
  "2009-12-01" = "2009 COP15 ",
  "2012-12-01" = "2012 COP18 ",
  #"2014-07-01" = "Oil Price Drop Start ",
  #"2015-01-31" = "Oil Price Drop Lowest Month ",
  "2015-12-01" = "2021 COP21\n Paris Agr. ",
  "2016-11-01" = "Trump I elected ",
  #"2016-12-07" = "Scott Pruitt EPA ",
  "2017-06-01" = "Trump I quits \nParis Agr. ",
  "2019-09-01" = "Strikes f. Future ",
  "2020-01-01" = "COVID outbreak ",
  "2020-11-03" = "Biden elected ",
  "2021-01-20" = "Biden rejoins \nParis Agr. ",
  "2021-10-01" = "2021 COP26 ",
  "2022-02-24" = "UKR war ",
  "2023-10-07" = "Gaza war ",
  "2023-12-01" = "2023 COP28 ",
  "2024-11-01" = "Trump II ",
  "2025-01-25" = "Trump II quits \nParis Agr. "
)

# 2) Data for vertical lines
event_lines_dt <- data.table(
  ym_date = as.Date(names(event_all)),
  label   = unname(event_all)
)

# 3) Data for labels (force ALL labels into the TOP facet)
top_series <- levels(att_long$series)[1]

event_labels_dt <- data.table(
  ym_date = as.Date(names(event_all)),
  label   = unname(event_all),
  series  = factor(top_series, levels = levels(att_long$series))
)

# 4) Plot
p_1 <- ggplot(att_long, aes(x = month, y = value)) +
  geom_line(na.rm = TRUE, linewidth = 0.9, alpha = 0.7, colour = "#1B3A8A") +
  facet_grid(series ~ ., scales = "free_y", switch = "y") +
  
  # vlines through ALL panels
  geom_vline(
    data     = event_lines_dt,
    aes(xintercept = ym_date),
    linetype  = "dashed",
    linewidth = 0.7,
    colour    = "grey20",
    alpha     = 0.6
  ) +
  
  # labels ONLY in the TOP panel (NO series in aes!)
  geom_text(
    data        = event_labels_dt,
    inherit.aes = FALSE,
    aes(x = ym_date, y = Inf, label = label),
    angle = 90,
    vjust = 1.1,
    hjust = 1,
    size  = 2.8
  ) +
  
  scale_x_date(limits = as.Date(c("2010-01-01", NA)),date_breaks = "1 years", date_labels = "%y",expand      = c(0, 0)) +
  scale_y_continuous(expand = expansion(mult = c(0.0, 0.15))) +
  labs(x = "Year", y = NULL) +
  theme_bw() +
  theme(
    strip.placement  = "outside",
    strip.background = element_blank(),
    plot.margin      = margin(t = 1, r = 10, b = 1, l = 10)
  ) +
  coord_cartesian(clip = "off")

p_1

### (b) AR comparison with events (indexes) ----
## 1) Long format for stacked line charts
att_long <- melt(
  final_merged,
  id.vars = "month",
  measure.vars = c("UMC_ARX_z",
                   "TRI_monthly_roll_z",
                   #"Physical_concern_monthly",
                   "UCAI_global_GDP_z"
                   #"cai_global_eq_monthly"
  ),
  variable.name = "series",
  value.name   = "value"
)

att_long[, series := factor(
  series,
  levels = c("UMC_ARX_z",
             "TRI_monthly_roll_z",
             #"Physical_concern_monthly",
             "UCAI_global_GDP_z"
             #"cai_global_eq_monthly"
  ),
  labels = c("UMC ARX",
             "TRI\nmonthly roll",
             #"Physical concern",
             "UCAI GDP"
             #"CAI global equity"
  )
)]

# 1) All events in a single named vector
event_all <- c(
  "2007-12-01" = "2007 COP13 ",
  "2009-12-01" = "2009 COP15 ",
  "2012-12-01" = "2012 COP18 ",
  #"2014-07-01" = "Oil Price Drop Start ",
  #"2015-01-31" = "Oil Price Drop Lowest Month ",
  "2015-12-01" = "2021 COP21\n Paris Agr. ",
  "2016-11-01" = "Trump I elected ",
  #"2016-12-07" = "Scott Pruitt EPA ",
  "2017-06-01" = "Trump I quits \nParis Agr. ",
  "2019-09-01" = "Strikes f. Future ",
  "2020-01-01" = "COVID outbreak ",
  "2020-11-03" = "Biden elected ",
  "2021-01-20" = "Biden rejoins \nParis Agr. ",
  "2021-10-01" = "2021 COP26 ",
  "2022-02-24" = "UKR war ",
  "2023-10-07" = "Gaza war ",
  "2023-12-01" = "2023 COP28 ",
  "2024-11-01" = "Trump II ",
  "2025-01-25" = "Trump II quits \nParis Agr. "
)

# 2) Data for vertical lines
event_lines_dt <- data.table(
  ym_date = as.Date(names(event_all))#,
  #label   = unname(event_all)
)

# 3) Data for labels (force ALL labels into the TOP facet)
#top_series <- levels(att_long$series)[1]

event_labels_dt <- data.table(
  ym_date = as.Date(names(event_all))#,
  #label   = unname(event_all),
  #series  = factor(top_series, levels = levels(att_long$series))
)

# 4) Plot
p_2 <- ggplot(att_long, aes(x = month, y = value)) +
  geom_hline(yintercept = 0, linewidth = 0.7, color = "grey20", alpha = 0.7
  )+
  geom_line(na.rm = TRUE, linewidth = 0.9, alpha = 0.7, colour = "#C0392B") +
  facet_grid(series ~ ., scales = "free_y", switch = "y") +
  
  # vlines through ALL panels
  geom_vline(
    data     = event_lines_dt,
    aes(xintercept = ym_date),
    linetype  = "dashed",
    linewidth = 0.7,
    colour    = "grey20",
    alpha     = 0.6
  ) +
  
  ## labels ONLY in the TOP panel (NO series in aes!)
  #geom_text(
  #  data        = event_labels_dt,
  #  inherit.aes = FALSE,
  #  aes(x = ym_date, y = Inf
  #      #, label = label
  #      ),
  #  angle = 90,
  #  vjust = 1.1,
  #  hjust = 1,
  #  size  = 2.8
  #) +
  
  scale_x_date(limits = as.Date(c("2010-01-01", NA)),date_breaks = "1 years", date_labels = "%y",expand      = c(0, 0)) +
  scale_y_continuous(expand = expansion(mult = c(0.0, 0.15))) +
  labs(x = "Year", y = NULL) +
  theme_bw() +
  theme(
    strip.placement  = "outside",
    strip.background = element_blank(),
    plot.margin      = margin(t = 1, r = 10, b = 1, l = 10)
  ) +
  coord_cartesian(clip = "off")

p_2


### (c) combined ----

p_1_top <- p_1 +
  theme(
    axis.title.x = element_blank(),
    axis.text.x  = element_blank(),
    axis.ticks.x = element_blank()
  )


x_lims <- as.Date(c("2010-01-01", NA))

p_2 <- p_2 + scale_x_date(limits = x_lims, date_breaks = "1 years",
                          date_labels = "%y", expand = c(0, 0))

p_1_top <- p_1_top + scale_x_date(limits = x_lims, date_breaks = "1 years",
                                  date_labels = "%y", expand = c(0, 0))

# Stack: p1 on top, p2 bottom
p_stacked <- p_1_top / p_2 +
  plot_layout(heights = c(2, 2))+
  plot_annotation(
    tag_levels = "A",
    tag_prefix = "(",
    tag_suffix = ")"
  )

p_stacked

#### * export to image----
ggsave(
  filename = "../07_word/outputs/climate_attention_series_2010-2025.png",
  plot     = p_stacked,
  #device   = "emf",
  width    = 12.5,
  height   = 6.5,
  units    = "in",
  dpi = 1000
)



## 4) Country specific policy measures ----
### (a) plot scores over time ----
#### (a.1) full sample ----
plot_country_timeseries("cfa_cva_ratio_z")
plot_country_timeseries("PAI_z")

#### (a.2) country highlights ----
p1 <- plot_country_timeseries_highlight(
  var_name = c("capmf_std","combined_ccpi"),
  highlight_countries = c("CHN","IND","RUS","USA","JPN","DEU","GBR"),
  first_year = 2010
) +
  plot_annotation(
    tag_levels = "A",
    tag_prefix = "(",
    tag_suffix = ")"
  ) &
  theme(plot.title = element_blank())

plot_country_timeseries_highlight(
  var_name = c("capmf_international_policies","capmf_international_policies_std"),
  highlight_countries = c("CHN","IND","RUS","USA","JPN","DEU","GBR"),
  first_year = 2010
)

plot_country_timeseries_highlight(
  var_name = c("PAI","PAI_z"),
  highlight_countries = c("CHN","IND","RUS","USA","JPN","DEU","GBR"),
  first_year = 2010
)

plot_country_timeseries_highlight(
  var_name = c("cfa_cva_ratio","cfa_cva_ratio_z"),
  highlight_countries = c("CHN","IND","RUS","USA","JPN","DEU","GBR"),
  first_year = 2010
)

##### * export to image ----
ggsave(
  filename = "../07_word/outputs/policy_scores_country_highlight_2010_2025.png",
  plot     = p1,
  #device   = "emf",
  width    = 12.5,
  height   = 5.5,
  units    = "in",
  dpi = 800
)


### (b) Full Country ----
#### (b.1) boxplots ----
plot_country_boxplots(
  df_country_year,
  value_var  = "capmf_std",
  start_year = 2010,
  end_year   = 2024,
  order_by   = "median",   # or "end"
  show_points = F
)

plot_country_boxplots(
  df_country_year,
  value_var  = "combined_ccpi",
  start_year = 2010,
  end_year   = 2024,
  order_by   = "median",   # or "end"
  show_points = F
)

#### (b.2) scatterplots of median value ----
plot_1 <- plot_country_scatter(group_facet_toggle = F,
  dt            = df_country_year,
  var_y         = "capmf_std",
  var_x        = "combined_ccpi",
  start_year    = 2010,
  end_year      = 2025,
  agg_fun       = "median",
  loc_col       = "loc",
  group_col     = "region",
  highlight_iso3 = c("NOR", "CHN", "TUR","JPN","ARG")
) + ggplot2::labs(title = NULL)

plot_1


plot_2 <- plot_country_scatter(
  group_facet_toggle = TRUE,
  dt         = df_country_year,
  var_y      = "capmf_std",
  var_x      = "PAI_z",
  start_year = 2010,
  end_year   = 2025,
  agg_fun    = "median",
  loc_col    = "loc",
  group_col  = "region"
) +
  labs(title = NULL) +
  facet_wrap(~group, nrow = 1, ncol = 4, drop = FALSE)   # <-- force 1x4

plot_3 <- plot_country_scatter(
  group_facet_toggle = TRUE,
  dt         = df_country_year,
  var_y      = "combined_ccpi",
  var_x      = "PAI_z",
  start_year = 2010,
  end_year   = 2025,
  agg_fun    = "median",
  loc_col    = "loc",
  group_col  = "region"
) +
  labs(title = NULL) +
  facet_wrap(~group, nrow = 1, ncol = 4, drop = FALSE)   # <-- force 1x4


## 2) Force identical x-scale in BOTH plots (required for a “shared” PAI axis)
x_rng <- range(df_country_year$PAI_z, na.rm = TRUE)

border_theme <- theme(
  panel.border = element_rect(
    colour   = "black",
    fill     = NA,
    linewidth = 0.6
  )
)


plot_2 <- plot_2 +
  border_theme +
  scale_x_continuous(limits = x_rng) +
  labs(x=NULL)+
  theme(
    axis.text.x  = element_blank(),
    axis.ticks.x = element_blank()
  )

plot_3 <- plot_3 +
  border_theme +
  scale_x_continuous(limits = x_rng)+
  theme(
    strip.background = element_blank(),
    strip.text       = element_blank()
  )

## 3) Combine vertically: 4 facets on top + 4 facets on bottom, aligned
combined_plot <- (plot_2 / plot_3) +
  plot_layout(ncol = 1) +
  plot_annotation(
    tag_levels = "A",
    tag_prefix = "(",
    tag_suffix = ")"
  ) &
  theme(
    panel.border = element_rect(
      colour   = "black",
      fill     = NA,
      linewidth = 0.6
    ),
    strip.placement = "outside"  # helps keep strips consistent
  )

combined_plot


##### * export to image ----
ggsave(
  filename = "../07_word/outputs/scatterplot_policy_2010_2025.png",
  plot     = plot_1,
  #device   = "emf",
  width    = 12.5,
  height   = 4.5,
  units    = "in",
  dpi = 800
)

ggsave(
  filename = "../07_word/outputs/scatterplot_policy_REGIONS_PAI_2010_2025.png",
  plot     = combined_plot,
  #device   = "emf",
  width    = 9.5,
  height   = 3.5,
  units    = "in",
  dpi = 800
)


# =============================================================================.

# Regression Analysis ----
# =============================================================================.

# This section performs the main pooled OLS analysis of the thesis 

## Main variable selections ----
# define control variables
all_controls_lag6m <- c(
  "logsize",
  "leverage_lag6m",
  "log_bm",
  "ROE_lag6m",
  "eps_growth_lag6m",
  "sales_growth_lag6m",
  "log_ppe_lag6m",
  "inv_over_at_lag6m",
  "vola",
  "mom",
  "beta_24m_lag1_loc"
)

all_controls <- c(
  "logsize",
  "leverage",
  "log_bm",
  "ROE",
  "eps_growth",
  "sales_growth",
  "log_ppe",
  "inv_over_at",
  "vola",
  "mom",
  "beta_24m_lag1_loc"
)

CY_CONTROL_VARS <- c(
  "nat_ccpi",
  "internat_ccpi",
  "combined_ccpi",
  "PAI_z",
  "cfa_cva_ratio_z",
  "capmf_std",
  "capmf_crosssectoral_policies_std",
  "capmf_sectoral_policies_std",
  "capmf_international_policies_std",
  "capmf_ghg_emission_targets_std",
  "capmf_fossil_fuel_production_policies_std",
  "gdp_pc_z",
  "total_ff_produced_per_gdp_z",
  "vulnerability_z",
  "control_corruption_z"
)

## 1) control variable regressions ----
### (a) regress emissions levels & intensities on firm characteristics ----
#df[, est_scope1 := as.integer(ghg_scope1_class == "estimated")]
#df[, est_scope2 := as.integer(ghg_scope2_loc_class == "estimated")]
#df[, est_scope3 := as.integer(ghg_scope3_up_class == "estimated")]
#
#m1 <- feols(log_ghg_scope1 ~ 
#              log_sales+
#              sales_growth
#            | gind + ym + loc,
#            cluster = ~ gvkey + ym,
#            data = df[ghg_scope1_class == "estimated"])
#
#m2 <- feols(log_ghg_scope1 ~ 
#              log_sales+
#              sales_growth +
#              beta_24m_lag1_loc +
#              logsize +
#              log_bm +
#              ROA +
#              mom + 
#              vola +
#              leverage +
#              log_ppe +
#              eps_growth
#            | gind + ym + loc,
#            cluster = ~ gvkey + ym,
#            data = df[ghg_scope1_class == "estimated"])
#
#m3 <- feols(log_ghg_scope1 ~ 
#              log_sales+
#              sales_growth
#            | gind + ym + loc,
#            cluster = ~ gvkey + ym,
#            data = df[ghg_scope1_class == "disclosed"])
#
#m4 <- feols(log_ghg_scope1 ~ 
#              log_sales+
#              sales_growth +
#              beta_24m_lag1_loc +
#              logsize +
#              log_bm +
#              ROA +
#              mom + 
#              vola +
#              leverage +
#              log_ppe +
#              eps_growth
#            | gind + ym + loc,
#            cluster = ~ gvkey + ym,
#            data = df[ghg_scope1_class == "disclosed"])
#
#View(etable(m1,m2,m3,m4))
#
#
#m_level_scope1_pooled <- feols(
#  log_ghg_scope1 ~
#    est_scope1 + sales_growth + beta_24m_lag1_loc +
#    logsize + log_bm + ROA + mom + vola +
#    leverage + log_ppe + eps_growth |
#    gind + ym + loc,
#  cluster = ~ gvkey + ym,
#  data = df
#)
#
#m_level_scope2_pooled <- feols(
#  log_ghg_scope2_loc ~
#    est_scope2 + sales_growth + beta_24m_lag1_loc +
#    logsize + log_bm + ROA + mom + vola +
#    leverage + log_ppe + eps_growth |
#    gind + ym + loc,
#  cluster = ~ gvkey + ym,
#  data = df
#)
#
#m_level_scope3_pooled <- feols(
#  log_ghg_scope3_up ~
#    est_scope3 + sales_growth + beta_24m_lag1_loc +
#    logsize + log_bm + ROA + mom + vola +
#    leverage + log_ppe + eps_growth |
#    gind + ym + loc,
#  cluster = ~ gvkey + ym,
#  data = df
#)
#
#m_intensity_scope1_pooled <- feols(
#  ghg_scope1_intensity ~
#    est_scope1 + sales_growth + beta_24m_lag1_loc +
#    logsize + log_bm + ROA + mom + vola +
#    leverage + log_ppe + eps_growth |
#    gind + ym + loc,
#  cluster = ~ gvkey + ym,
#  data = df
#)
#
#m_intensity_scope2_pooled <- feols(
#  ghg_scope2_loc_intensity ~
#    est_scope2 + sales_growth + beta_24m_lag1_loc +
#    logsize + log_bm + ROA + mom + vola +
#    leverage + log_ppe + eps_growth |
#    gind + ym + loc,
#  cluster = ~ gvkey + ym,
#  data = df
#)
#
#m_intensity_scope3_pooled <- feols(
#  ghg_scope3_up_intensity ~
#    est_scope3 + sales_growth + beta_24m_lag1_loc +
#    logsize + log_bm + ROA + mom + vola +
#    leverage + log_ppe + eps_growth |
#    gind + ym + loc,
#  cluster = ~ gvkey + ym,
#  data = df
#)
#
#View(etable(
#  m_level_scope1_pooled,
#  m_level_scope2_pooled,
#  m_level_scope3_pooled,
#  m_intensity_scope1_pooled,
#  m_intensity_scope2_pooled,
#  m_intensity_scope3_pooled,
#  order = c("est_scope1", "est_scope2", "est_scope3")
#))


### (b) regress cfa/cva ratios on climate change policy measures ----
# lagged underlying data
# Combined CCPI outcome
ccpi_dep <- "combined_ccpi"

capmf_std_dep <- "capmf_std"

# CAPMF outcomes
capmf_dep <- c(
  "capmf_std", # aggregate of 56 measures
  "capmf_international_policies", # overarching score
  "capmf_sectoral_policies", # overarching score
  "capmf_crosssectoral_policies", # overarching score
  "capmf_fossil_fuel_production_policies", # subscore of cross sectoral
  "capmf_ghg_emission_targets" # subscore of cross sectoral
)

# 2. TABLE 1.a: Combined CCPI, year FE only vs country+year FE
m_pai_ccpi_noloc      <- run_pai_cfa_decomp_model(ccpi_dep,
                                                  type_model = "pai",
                                                  fixed_effect = "year",
                                                  z_rhs = T, 
                                                  lag = T)
m_cfa_ccpi_noloc      <- run_pai_cfa_decomp_model(ccpi_dep,
                                                  type_model = "cfa",
                                                  fixed_effect = "year",
                                                  z_rhs = T, 
                                                  lag = T)
m_decomp_s_ccpi_noloc <- run_pai_cfa_decomp_model(ccpi_dep,
                                                  type_model = "decomp_structural",
                                                  fixed_effect = "year",
                                                  z_rhs = T, 
                                                  lag = T)
m_decomp_f_ccpi_noloc <- run_pai_cfa_decomp_model(ccpi_dep,
                                                  type_model = "decomp_full",
                                                  fixed_effect = "year",
                                                  z_rhs = T, 
                                                  lag = T)

m_pai_ccpi_loc      <- run_pai_cfa_decomp_model(ccpi_dep,
                                                  type_model = "pai",
                                                  fixed_effect = "loc_year",
                                                  z_rhs = T, 
                                                  lag = T)
m_cfa_ccpi_loc      <- run_pai_cfa_decomp_model(ccpi_dep,
                                                  type_model = "cfa",
                                                  fixed_effect = "loc_year",
                                                  z_rhs = T, 
                                                  lag = T)
m_decomp_s_ccpi_loc <- run_pai_cfa_decomp_model(ccpi_dep,
                                                  type_model = "decomp_structural",
                                                  fixed_effect = "loc_year",
                                                  z_rhs = T, 
                                                  lag = T)
m_decomp_f_ccpi_loc <- run_pai_cfa_decomp_model(ccpi_dep,
                                                  type_model = "decomp_full",
                                                  fixed_effect = "loc_year",
                                                  z_rhs = T, 
                                                  lag = T)

tab_ccpi_both_fe <- etable(
  m_pai_ccpi_noloc,
  m_cfa_ccpi_noloc,
  m_decomp_s_ccpi_noloc,
  m_decomp_f_ccpi_noloc,
  m_pai_ccpi_loc,
  m_cfa_ccpi_loc,
  m_decomp_s_ccpi_loc,
  m_decomp_f_ccpi_loc,
  #headers = c(
  #  "PAI (Year FE)",
  #  "CFA/CVA (Year FE)",
  #  "FF+VULN (Year FE)",
  #  "FF+VULN+GDPpc+Corruption (Year FE)",
  #  "PAI (Loc+Year FE)",
  #  "CFA/CVA (Loc+Year FE)",
  #  "FF+VULN (Loc+Year FE)",
  #  "FF+VULN+GDPpc+Corruption (Loc+Year FE)"
  #),
  cluster = "loc",
  se.below = TRUE,
  digits = 3,
  signif.code = c("*" = 0.1, "**" = 0.05, "***" = 0.01)
)

View(tab_ccpi_both_fe)

# 2. TABLE 1.b: Combined CAPMF, year FE only vs country+year FE

m_pai_capmf_std_noloc      <- run_pai_cfa_decomp_model(capmf_std_dep,
                                                       type_model = "pai",
                                                       fixed_effect = "year",
                                                       z_rhs = T, 
                                                       lag = T)
m_cfa_capmf_std_noloc      <- run_pai_cfa_decomp_model(capmf_std_dep,
                                                       type_model = "cfa",
                                                       fixed_effect = "year",
                                                       z_rhs = T, 
                                                       lag = T)
m_decomp_s_capmf_std_noloc <- run_pai_cfa_decomp_model(capmf_std_dep,
                                                       type_model = "decomp_structural",
                                                       fixed_effect = "year",
                                                       z_rhs = T, 
                                                       lag = T)
m_decomp_f_capmf_std_noloc <- run_pai_cfa_decomp_model(capmf_std_dep,
                                                       type_model = "decomp_full",
                                                       fixed_effect = "year",
                                                       z_rhs = T, 
                                                       lag = T)

m_pai_capmf_std_loc      <- run_pai_cfa_decomp_model(capmf_std_dep,
                                                     type_model = "pai",
                                                     fixed_effect = "loc_year",
                                                     z_rhs = T, 
                                                     lag = T)
m_cfa_capmf_std_loc      <- run_pai_cfa_decomp_model(capmf_std_dep,
                                                     type_model = "cfa",
                                                     fixed_effect = "loc_year",
                                                     z_rhs = T, 
                                                     lag = T)
m_decomp_s_capmf_std_loc <- run_pai_cfa_decomp_model(capmf_std_dep,
                                                     type_model = "decomp_structural",
                                                     fixed_effect = "loc_year",
                                                     z_rhs = T, 
                                                     lag = T)
m_decomp_f_capmf_std_loc <- run_pai_cfa_decomp_model(capmf_std_dep,
                                                     type_model = "decomp_full",
                                                     fixed_effect = "loc_year",
                                                     z_rhs = T, 
                                                     lag = T)

tab_capmf_std_both_fe <- etable(
  m_pai_capmf_std_noloc,
  m_cfa_capmf_std_noloc,
  m_decomp_s_capmf_std_noloc,
  m_decomp_f_capmf_std_noloc,
  m_pai_capmf_std_loc,
  m_cfa_capmf_std_loc,
  m_decomp_s_capmf_std_loc,
  m_decomp_f_capmf_std_loc,
  #headers = c(
  #  "PAI (Year FE)",
  #  "CFA/CVA (Year FE)",
  #  "FF+VULN (Year FE)",
  #  "FF+VULN+GDPpc+Corruption (Year FE)",
  #  "PAI (Loc+Year FE)",
  #  "CFA/CVA (Loc+Year FE)",
  #  "FF+VULN (Loc+Year FE)",
  #  "FF+VULN+GDPpc+Corruption (Loc+Year FE)"
  #),
  cluster = "loc",
  se.below = TRUE,
  digits = 3,
  signif.code = c("*" = 0.1, "**" = 0.05, "***" = 0.01)
)

View(tab_capmf_std_both_fe)

#### * 2.b output ----
models <- list(
  m_pai_ccpi_loc,
  m_cfa_ccpi_loc,
  m_decomp_s_ccpi_loc,
  m_decomp_f_ccpi_loc,
  m_pai_capmf_std_loc,
  m_cfa_capmf_std_loc,
  m_decomp_s_capmf_std_loc,
  m_decomp_f_capmf_std_loc
)

get_dv <- function(m){
  f <- tryCatch(stats::formula(m), error = function(e) NULL)
  if (!is.null(f)) return(as.character(f)[2])
  tryCatch(as.character(m$fml)[2], error = function(e) NA_character_)
}

dv <- vapply(models, get_dv, character(1))

gm2 <- data.frame(
  raw   = c("nobs",
            "vcov.type",
            "FE: year",
            "FE: loc",
            "r.squared", 
            "adj.r.squared",
            "r2.within"),
  clean = c("Observations",
            "S.E.: Clustered",
            "Year-fixed effects",
            "Country-fixed effects",
            "R-squared", 
            "Adj. R-squared",
            "Within R-squared"),
  fmt   = c(0, 0, 0,0,3,3,3)
)

ft_reg <- modelsummary(
  models,
  output    = "flextable",
  vcov      = ~ loc,
  fmt       = 2,
  stars     = c("*"=.1, "**"=.05, "***"=.01),
  statistic = "({std.error})",
  gof_map   = gm2
)

ft_reg <- font(ft_reg, fontname = "Times New Roman", part = "all")
ft_reg <- fontsize(ft_reg, size = 8, part = "all")
ft_reg <- bold(ft_reg, part = "header")
ft_reg <- padding(ft_reg, padding = 1.5, part = "all")
ft_reg <- line_spacing(ft_reg, space = 0.75, part = "all")

ft_reg <- italic(ft_reg, j = 1, part = "body")

b_main <- fp_border(color = "black", width = 1)
ft_reg <- border_remove(ft_reg)
ft_reg <- hline_top(ft_reg, border = b_main, part = "header")
ft_reg <- hline(ft_reg, border = b_main, part = "header")
ft_reg <- hline_bottom(ft_reg, border = b_main, part = "body")

dv1 <- dv[1]  # DV for models 1-4
dv2 <- dv[5]  # DV for models 5-8

ft_reg <- add_header_row(
  ft_reg,
  values    = c("", dv1, dv2),
  colwidths = c(1, 4, 4)
)

# Put "Dependent variable:" in the top-left header cell (new row 1)
ft_reg <- compose(
  ft_reg,
  i = 1, j = 1,
  part  = "header",
  value = as_paragraph("Dependent variable:")
)

# Optional: center the group headers (dv1, dv2) and vertically align nicely
ft_reg <- align(ft_reg, i = 1, j = 2:9, align = "center", part = "header")
ft_reg <- valign(ft_reg, i = 1, j = 1:9, valign = "center", part = "header")

ft_reg <- autofit(ft_reg)
ft_reg <- set_table_properties(ft_reg, width = 1, layout = "autofit")

b_thin <- fp_border(color = "black", width = .25)

# Find where GOF starts in the body (column 1 contains row labels)
gof_start <- match("Observations", ft_reg$body$dataset[[1]])

# Draw a line between the last coefficient row and first GOF row
if (!is.na(gof_start) && gof_start > 1) {
  ft_reg <- hline(ft_reg, i = gof_start - 1, border = b_thin, part = "body")
}

gof_start <- match("Observations", ft_reg$body$dataset[[1]])

# Remove italics from GOF rows in the first column
if (!is.na(gof_start)) {
  ft_reg <- italic(
    ft_reg,
    i = gof_start:nrow(ft_reg$body$dataset),
    j = 1,
    italic = FALSE,
    part = "body"
  )
}

doc <- read_docx()
doc <- body_add_flextable(doc, ft_reg)

print(doc, target = "../07_word/outputs/reg_policy.docx")



# 3. TABLE 2: CAPMF policies fully deconstructed (with loc + year FE)

m_capmf_decomp_full <- lapply(
  capmf_dep,
  function(y)
    run_pai_cfa_decomp_model(
      y,
      type_model   = "decomp_full",
      fixed_effect = "loc_year",
      lag          = TRUE,
      z_rhs        = TRUE
    )
)
names(m_capmf_decomp_full) <- capmf_dep

tab_capmf_decomp <- etable(
  m_capmf_decomp_full,
  headers = c(
    "Full",
    "Crosssectoral",
    "International",
    "Sectoral",
    "FossilProd",
    "Targets"
  ),
  cluster = "loc",
  se.below = TRUE,
  digits = 3,
  signif.code = c("*" = 0.1, "**" = 0.05, "***" = 0.01)
)

View(tab_capmf_decomp)

# 4. TABLE 3: CAPMF policies with only PAI and CFA/CVA (grouped by dependent variable)
m_capmf_pai <- lapply(
  capmf_dep,
  function(y)
    run_pai_cfa_decomp_model(
      y,
      type_model   = "pai",
      fixed_effect = "loc_year",
      lag          = TRUE,
      z_rhs        = TRUE
    )
)

m_capmf_cfa <- lapply(
  capmf_dep,
  function(y)
    run_pai_cfa_decomp_model(
      y,
      type_model   = "cfa",
      fixed_effect = "loc_year",
      lag          = TRUE,
      z_rhs        = TRUE
    )
)

models_capmf_pai_cfa <- c(m_capmf_pai, m_capmf_cfa)

headers_capmf_pai_cfa <- c(
  paste0(
    gsub("_policies", "", gsub("capmf_", "", capmf_dep)),
    ":PAI"
  ),
  paste0(
    gsub("_policies", "", gsub("capmf_", "", capmf_dep)),
    ":CFA"
  )
)

tab_capmf_pai_cfa <- etable(
  models_capmf_pai_cfa,
  headers = headers_capmf_pai_cfa,
  cluster = "loc",
  se.below = TRUE,
  digits = 3,
  signif.code = c("*" = 0.1, "**" = 0.05, "***" = 0.01)
)

View(tab_capmf_pai_cfa)




## 2) pooled OLS ----
### (a) Pooled OLS with country-, year/month-, and industry-fixed effects ----
#### full sample ----
# run regressions
res <- run_ghg_specs(
  df,
  ret_col = "R_m",
  ghg_measure = "intensity",
  ghg_class = "full",
  lag6m = T, # applies to ghg cols only
  loc_filter_toggle = F,
  loc_selection = c(""),
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = T,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c("logsize")
)

# create output table
tab_full <- do.call(
  etable,
  c(
    res$models,
    list(
      se.below = TRUE,
      digits   = 3,
      drop     = res$drop_controls,
      signif.code =  c("*" = 0.1, "**" = 0.05, "***" = 0.01)
    )
  )
)

rm(res)
View(tab_full)


#### firm-disclosed ----
res <- run_ghg_specs(
  df,
  ret_col = "R_m",
  ghg_measure = "intensity",
  ghg_class = "disclosed",
  lag6m = TRUE,
  loc_filter_toggle = FALSE,
  loc_selection = c(""),
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = TRUE,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c("logsize")
)

# create output table
tab_disclosed <- do.call(
  etable,
  c(
    res$models,
    list(
      se.below = TRUE,
      digits   = 3,
      drop     = res$drop_controls,
      signif.code =  c("*" = 0.1, "**" = 0.05, "***" = 0.01)
    )
  )
)

rm(res)
View(tab_disclosed)


#### estimated ----
res <- run_ghg_specs(
  df,
  ret_col = "R_m",
  ghg_measure = "intensity",
  ghg_class = "estimated",
  lag6m = TRUE,
  loc_filter_toggle = FALSE,
  loc_selection = c(""),
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = TRUE,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c("logsize")
)

# create output table
tab_estimated <- do.call(
  etable,
  c(
    res$models,
    list(
      se.below = TRUE,
      digits   = 3,
      drop     = res$drop_controls,
      signif.code =  c("*" = 0.1, "**" = 0.05, "***" = 0.01)
    )
  )
)

rm(res)
View(tab_estimated)


#### country specific tests ----
res <- run_ghg_specs(
  df,
  ret_col = "R_m",
  ghg_measure = "intensity",
  ghg_class = "disclosed",
  lag6m = T,
  loc_filter_toggle = TRUE,
  loc_selection = c("USA"),
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = T,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c("logsize")
)

# create output table
tab_country <- do.call(
  etable,
  c(
    res$models,
    list(
      se.below = TRUE,
      digits   = 3,
      drop     = res$drop_controls,
      signif.code =  c("*" = 0.1, "**" = 0.05, "***" = 0.01)
    )
  )
)

rm(res)
View(tab_country)


#### profitability measures ----
res <- run_ghg_specs(
  df,
  ret_col = "ebit_margin",
  ghg_measure = "intensity",
  ghg_class = "full",
  lag6m = T,
  loc_filter_toggle = F,
  loc_selection = c("USA"),
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = T,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c("logsize")
)

# create output table
tab_prof <- do.call(
  etable,
  c(
    res$models,
    list(
      se.below = TRUE,
      digits   = 3,
      drop     = res$drop_controls,
      signif.code =  c("*" = 0.1, "**" = 0.05, "***" = 0.01)
    )
  )
)

rm(res)
View(tab_prof)


##### * for table ----
res_glob <- run_ghg_specs(
  df,
  ret_col = "R_m",
  ghg_measure = "intensity",
  ghg_class = "disclosed",
  lag6m = TRUE,
  loc_filter_toggle = FALSE,
  loc_selection = c(""),
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = TRUE,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c("logsize")
)

disc_glob <- res_glob$models[c("m10", "m11", "m12")]
rm(res_glob)

res_US <- run_ghg_specs(
  df,
  ret_col = "R_m",
  ghg_measure = "intensity",
  ghg_class = "disclosed",
  lag6m = TRUE,
  loc_filter_toggle = T,
  loc_selection = c("USA"),
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = F,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c("logsize")
)

disc_US <- res_US$models[c("m10", "m11", "m12")]
rm(res_US)

res_ROW <- run_ghg_specs(
  df,
  ret_col = "R_m",
  ghg_measure = "intensity",
  ghg_class = "disclosed",
  lag6m = TRUE,
  loc_filter_toggle = T,
  loc_selection = unique(df$loc)[unique(df$loc) != "USA"],
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = TRUE,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c("logsize")
)

disc_ROW <- res_ROW$models[c("m10", "m11", "m12")]
rm(res_ROW)

names(disc_glob) <- paste0("Glob_", names(disc_glob))
names(disc_US)   <- paste0("US_",   names(disc_US))
names(disc_ROW)  <- paste0("ROW_",  names(disc_ROW))

models <- c(disc_glob, disc_US,disc_ROW)

gm2 <- data.frame(
  raw   = c("nobs","vcov.type","FE: ym","FE: year","FE: loc","FE: gind","r.squared","adj.r.squared","r2.within"),
  clean = c("Observations","S.E.: Clustered","Year-month-fixed effects","Year-fixed effects","Country-fixed effects","Industry-fixed effects",
            "R-squared","Adj. R-squared","Within R-squared"),
  fmt   = c(rep(0, 6), rep(3, 3)),
  stringsAsFactors = FALSE
)

# --- Configure group titles ---
group_titles <- c(
  Glob = "Global (discl.)",
  US     = "United States (discl.)",
  ROW    = "Rest of World (discl.)"
)

intensity_vars <- c(
  "ghg_scope1_intensity_lag6m",
  "ghg_scope2_loc_intensity_lag6m",
  "ghg_scope3_up_intensity_lag6m"
)
all_coefs <- unique(unlist(lapply(models, function(m) names(coef(m)))))

## remaining coefficients after intensity variables
other_coefs <- setdiff(all_coefs, intensity_vars)

coef_order_list <- c(intensity_vars, other_coefs)

# --- Build regression table ---
ft_reg <- modelsummary(
  models,
  output    = "flextable",
  vcov      = ~ gvkey + ym,
  fmt       = 2,
  stars     = c("*"=.1, "**"=.05, "***"=.01),
  statistic = "({std.error})",
  gof_map   = gm2, 
  coef_map = coef_order_list
  #coef_omit = paste(all_controls_lag6m, collapse = "|")
)

# --- Dynamic column labels: "(k)\nRet" ---
lab <- mk_col_labels(names(models), dv = "Ret")
ft_reg <- do.call(flextable::set_header_labels, c(list(x = ft_reg), as.list(lab)))

# --- Dynamic group header row (spanners) ---
gh <- build_group_header(names(models), group_titles)
ft_reg <- flextable::add_header_row(
  ft_reg,
  values    = gh$values,
  colwidths = gh$colwidths,
  top       = TRUE
)

# --- "Dependent variable:" in top-left (second header row, first column) ---
ft_reg <- flextable::compose(
  ft_reg,
  i = 2, j = 1,
  part  = "header",
  value = flextable::as_paragraph("Dependent variable:")
)

# --- Styling ---
b_main <- fp_border(color = "black", width = 1)
b_thin <- fp_border(color = "black", width = .25)

ft_reg <- ft_reg |>
  font(fontname = "Times New Roman", part = "all") |>
  fontsize(size = 8, part = "all") |>
  bold(part = "header") |>
  padding(padding = 1.5, part = "all") |>
  line_spacing(space = 0.75, part = "all") |>
  italic(j = 1, part = "body") |>
  border_remove() |>
  hline_top(border = b_main, part = "header") |>
  hline(border = b_main, part = "header") |>
  hline_bottom(border = b_main, part = "body") |>
  align(i = 1, j = 2:(1 + length(models)), align = "center", part = "header") |>
  valign(i = 1, j = 1:(1 + length(models)), valign = "center", part = "header") |>
  autofit() |>
  set_table_properties(width = 1, layout = "autofit")

# --- Post-hoc: format Observations row with thousands separators + GOF tweaks ---
gof_start <- match("Observations", ft_reg$body$dataset[[1]])

if (!is.na(gof_start)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # format Observations
  vals_fmt <- ft_reg$body$dataset[gof_start, model_cols] |>
    unlist(use.names = FALSE) |>
    gsub(",", "", x = _) |>
    suppressWarnings() |>
    as.numeric() |>
    formatC(format = "f", digits = 0, big.mark = ",")
  
  ft_reg$body$dataset[gof_start, model_cols] <- as.list(vals_fmt)
  
  ft_reg <- ft_reg |>
    compose(i = gof_start, j = model_cols,
            value = as_paragraph(as_chunk(vals_fmt))) |>
    hline(i = gof_start - 1, border = b_thin, part = "body") |>
    italic(i = gof_start:nrow(ft_reg$body$dataset), j = 1, italic = FALSE, part = "body")
}

se_row <- match("S.E.: Clustered", ft_reg$body$dataset[[1]])

if (!is.na(se_row)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # Current cell strings for that row (one per model column)
  vals <- unlist(ft_reg$body$dataset[se_row, model_cols], use.names = FALSE)
  
  # Remove only "by:" (keep gvkey & ym)
  vals2 <- gsub("\\s*by:\\s*", " ", vals)   # "Clustered by: gvkey & ym" -> "Clustered gvkey & ym"
  vals2 <- gsub("\\s{2,}", " ", vals2)      # clean double spaces
  vals2 <- trimws(vals2)
  
  # Write back + recompose for rendering
  ft_reg$body$dataset[se_row, model_cols] <- as.list(vals2)
  ft_reg <- flextable::compose(
    ft_reg,
    i = se_row, j = model_cols,
    value = flextable::as_paragraph(flextable::as_chunk(vals2))
  )
}

#ctrl_label <- "Controls"
#ctrl_mark  <- "lag6m"   # or "Yes"
#
## number of columns in the body (including first label column)
#ncols <- ncol(ft_reg$body$dataset)
#
## add the row at the end of the body
#ft_reg <- flextable::add_body_row(
#  ft_reg,
#  values = c(ctrl_label, rep(ctrl_mark, ncols - 1)),
#  top    = F
#)


# ft_reg is your final flextable object
ft_reg

doc <- read_docx()
doc <- body_add_flextable(doc, ft_reg)

#print(doc, target = "../07_word/outputs/glob_us_row_discl_int.docx")
print(doc, target = "../07_word/outputs/glob_us_row_discl_int_ALL_Controls.docx")
#print(doc, target = "../07_word/outputs/glob_us_row_full_int.docx")


### (b) Investor Preference Dimension ----
# run regressions
res <- run_ghg_specs_inv_pref_only_inter(
  df,
  ret_col = "R_m",
  include_scope2 = TRUE,
  include_scope3 = F,
  ghg_measure = "intensity",
  ghg_class = "disclosed",
  lag6m = T, # applies to ghg cols only
  loc_filter_toggle = T,
  loc_selection = europe_locs,#unique(df[!is.na(df$UCAI_country_z)]$loc),
  time_frame_toggle = T,
  datadate_min = "2015-11-01", # careful here
  datadate_max = "2022-12-01", # careful here
  industry_var = "gind",
  country_fe_toggle = T,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c(""),
  inv_pref_var = c("UCAI_global_GDP_z"))


# create output table
tab_test <- do.call(
  etable,
  c(
    res$models,
    list(
      se.below = TRUE,
      digits   = 3,
      drop     = res$drop_controls,
      signif.code =  c("*" = 0.1, "**" = 0.05, "***" = 0.01)
    )
  )
)

cat(res$spec_text)
rm(res)
View(tab_test)


##### * for table ----
inv_pref_vec <- c("UMC_AR1_z","TRI_monthly_roll_z","UCAI_global_GDP_z")  # extend: c("UMC_AR1_z", "XYZ_z", ...)

## define your location sets once
loc_US  <- "USA"
loc_ROW <- unique(df$loc)[unique(df$loc) != "USA"]

## scenario grid (what differs across your runs)
scenarios <- list(
  Glob = list(
    loc_filter_toggle   = FALSE,
    loc_selection       = NULL,
    country_fe_toggle   = TRUE
  ),
  US = list(
    loc_filter_toggle   = TRUE,
    loc_selection       = loc_US,
    country_fe_toggle   = FALSE
  )
  #,
  #ROW = list(
  #  loc_filter_toggle   = TRUE,
  #  loc_selection       = loc_ROW,
  #  country_fe_toggle   = TRUE
  #)
)

## helper: run one scenario + one inv_pref var and return named models
run_one <- function(inv_pref, sc_name, sc) {
  res <- run_ghg_specs_inv_pref_only_inter(
    df,
    ret_col             = "R_m",
    include_scope2      = T,
    include_scope3      = FALSE,
    ghg_measure         = "intensity",
    ghg_class           = "disclosed",
    lag6m               = TRUE,  # applies to ghg cols only
    loc_filter_toggle   = sc$loc_filter_toggle,
    loc_selection       = sc$loc_selection,
    time_frame_toggle   = TRUE,
    datadate_min        = "2015-11-01",
    datadate_max        = "2022-12-01",
    industry_var        = "gind",
    country_fe_toggle   = sc$country_fe_toggle,
    country_fe_variable = "loc",
    controls            = all_controls_lag6m,
    display_controls    = c(""),
    inv_pref_var        = inv_pref
  )
  
  models <- res$models
  rm(res)
  
  ## name scheme: <Scenario>_<InvPref>_<original model name>
  prefix <- paste0(sc_name, "_", inv_pref, "_")
  names(models) <- paste0(prefix, names(models))
  models
}

## run everything: returns ONE combined named list of models
models <- unlist(
  lapply(inv_pref_vec, function(v) {
    unlist(
      lapply(names(scenarios), function(sc_name) {
        run_one(inv_pref = v, sc_name = sc_name, sc = scenarios[[sc_name]])
      }),
      recursive = FALSE
    )
  }),
  recursive = FALSE
)

gm2 <- data.frame(
  raw   = c("nobs","vcov.type","FE: ym","FE: year","FE: loc","FE: gind","r.squared","adj.r.squared","r2.within"),
  clean = c("Observations","S.E.: Clustered","Year-month-fixed effects","Year-fixed effects","Country-fixed effects","Industry-fixed effects",
            "R-squared","Adj. R-squared","Within R-squared"),
  fmt   = c(rep(0, 6), rep(3, 3)),
  stringsAsFactors = FALSE
)

group_titles <- c(
  Glob = "Global (discl.)",
  US     = "United States (discl.)",
  ROW    = "Rest of World (discl.)"
)

# set order of models
## choose which scope to tabulate
scope_choice <- "scope2"

## 1) FILTER FIRST (based only on the suffix)
fe_order <- if (scope_choice == "scope1") c("m1","m3") else c("m2","m4")

if (scope_choice == "scope1") {
  models <- models[grepl("_(m1|m3)$", names(models))]
} else if (scope_choice == "scope2") {
  models <- models[grepl("_(m2|m4)$", names(models))]
} else {
  stop("scope_choice must be 'scope1' or 'scope2'")
}
## 2) ORDER (Glob, then US, then ROW; within each, m1 then m2 OR m3 then m4; within that, interactions)
country_order <- c("Glob", "US", "ROW")

nm <- names(models)

country <- sub("_.*", "", nm)                 # Glob / US / ROW
fe      <- sub(".*_(m[1-4])$", "\\1", nm)     # m1..m4 from suffix

## interaction = everything between <country>_ and _mX (no assumptions about underscores inside)
interaction <- sub(paste0("^", country, "_"), "", nm)
interaction <- sub("_(m[1-4])$", "", interaction)

## order interactions by your inv_pref_vec if possible; otherwise fall back to appearance order
interaction_order <- inv_pref_vec
if (!all(interaction %in% interaction_order)) {
  interaction_order <- unique(interaction)
}

ord <- order(
  match(country, country_order),
  match(fe, fe_order),
  match(interaction, interaction_order)
)

models <- models[ord]

# --- Build regression table ---
ft_reg <- modelsummary(
  models,
  output    = "flextable",
  vcov      = ~ gvkey + ym,
  fmt       = 3,
  stars     = c("*"=.1, "**"=.05, "***"=.01),
  statistic = "({std.error})",
  gof_map   = gm2,
  coef_omit = paste(all_controls_lag6m, collapse = "|")
)

# --- Dynamic column labels: "(k)\nRet" ---
lab <- mk_col_labels(names(models), dv = "Ret")
ft_reg <- do.call(flextable::set_header_labels, c(list(x = ft_reg), as.list(lab)))

# --- Dynamic group header row (spanners) ---
gh <- build_group_header(names(models), group_titles)
ft_reg <- flextable::add_header_row(
  ft_reg,
  values    = gh$values,
  colwidths = gh$colwidths,
  top       = TRUE
)

# --- "Dependent variable:" in top-left (second header row, first column) ---
ft_reg <- flextable::compose(
  ft_reg,
  i = 2, j = 1,
  part  = "header",
  value = flextable::as_paragraph("Dependent variable:")
)

# --- Styling ---
b_main <- fp_border(color = "black", width = 1)
b_thin <- fp_border(color = "black", width = .25)

ft_reg <- ft_reg |>
  font(fontname = "Times New Roman", part = "all") |>
  fontsize(size = 8, part = "all") |>
  bold(part = "header") |>
  padding(padding = 1.5, part = "all") |>
  line_spacing(space = 0.75, part = "all") |>
  italic(j = 1, part = "body") |>
  border_remove() |>
  hline_top(border = b_main, part = "header") |>
  hline(border = b_main, part = "header") |>
  hline_bottom(border = b_main, part = "body") |>
  align(i = 1, j = 2:(1 + length(models)), align = "center", part = "header") |>
  valign(i = 1, j = 1:(1 + length(models)), valign = "center", part = "header") |>
  autofit() |>
  set_table_properties(width = 1, layout = "autofit")

# --- Post-hoc: format Observations row with thousands separators + GOF tweaks ---
gof_start <- match("Observations", ft_reg$body$dataset[[1]])

if (!is.na(gof_start)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # format Observations
  vals_fmt <- ft_reg$body$dataset[gof_start, model_cols] |>
    unlist(use.names = FALSE) |>
    gsub(",", "", x = _) |>
    suppressWarnings() |>
    as.numeric() |>
    formatC(format = "f", digits = 0, big.mark = ",")
  
  ft_reg$body$dataset[gof_start, model_cols] <- as.list(vals_fmt)
  
  ft_reg <- ft_reg |>
    compose(i = gof_start, j = model_cols,
            value = as_paragraph(as_chunk(vals_fmt))) |>
    hline(i = gof_start - 1, border = b_thin, part = "body") |>
    italic(i = gof_start:nrow(ft_reg$body$dataset), j = 1, italic = FALSE, part = "body")
}

se_row <- match("S.E.: Clustered", ft_reg$body$dataset[[1]])

if (!is.na(se_row)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # Current cell strings for that row (one per model column)
  vals <- unlist(ft_reg$body$dataset[se_row, model_cols], use.names = FALSE)
  
  # Remove only "by:" (keep gvkey & ym)
  vals2 <- gsub("\\s*by:\\s*", " ", vals)   # "Clustered by: gvkey & ym" -> "Clustered gvkey & ym"
  vals2 <- gsub("\\s{2,}", " ", vals2)      # clean double spaces
  vals2 <- trimws(vals2)
  
  # Write back + recompose for rendering
  ft_reg$body$dataset[se_row, model_cols] <- as.list(vals2)
  ft_reg <- flextable::compose(
    ft_reg,
    i = se_row, j = model_cols,
    value = flextable::as_paragraph(flextable::as_chunk(vals2))
  )
}

ft_reg

doc <- read_docx()
doc <- body_add_flextable(doc, ft_reg)

#print(doc, target = "../07_word/outputs/Clim_attention_glob_us_discl_int_scope2_2010-2025.docx")
#print(doc, target = "../07_word/outputs/Clim_attention_glob_us_discl_int_scope2_2015-2022.docx")
#print(doc, target = "../07_word/outputs/Clim_attention_glob_us_discl_int_scope1_2015-2022.docx")
#print(doc, target = "../07_word/outputs/Clim_attention_glob_us_discl_int_scope1_2010-2025.docx")


### (c) Climate Policy Dimension ----
res <- run_ghg_specs_inv_pref(
  df,
  ret_col = "R_m",
  include_scope2 = TRUE,
  include_scope3 = F,
  ghg_measure = "intensity",
  ghg_class = "disclosed",
  lag6m = TRUE, # applies to ghg cols only
  loc_filter_toggle = F,
  loc_selection = unique(df$loc)[!unique(df$loc)%in%c("SWE")], #unique(df[!is.na(df$UCAI_country_z)]$loc)
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = T,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c(""),
  inv_pref_var = "PAI_z")


# create output table
tab_test <- do.call(
  etable,
  c(
    res$models,
    list(
      se.below = TRUE,
      digits   = 3,
      drop     = res$drop_controls,
      signif.code =  c("*" = 0.1, "**" = 0.05, "***" = 0.01)
    )
  )
)

rm(res)
View(tab_test)

##### * for table CCPI ----
inv_pref_vec <- c("combined_ccpi","nat_ccpi","internat_ccpi")  # extend: c("UMC_AR1_z", "XYZ_z", ...)

## define your location sets once
loc_US  <- asia_pacific#"USA"
loc_ROW <- europe_locs#unique(df$loc)[unique(df$loc) != "USA"]


## scenario grid (what differs across your runs)
scenarios <- list(
  Glob = list(
    loc_filter_toggle   = FALSE,
    loc_selection       = NULL,
    country_fe_toggle   = TRUE
  )
  ,
  US = list(
    loc_filter_toggle   = TRUE,
    loc_selection       = loc_US,
    country_fe_toggle   = TRUE
  )
  ,
  ROW = list(
    loc_filter_toggle   = TRUE,
    loc_selection       = loc_ROW,
    country_fe_toggle   = TRUE
  )
)

## helper: run one scenario + one inv_pref var and return named models
run_one <- function(inv_pref, sc_name, sc) {
  res <- run_ghg_specs_inv_pref(
    df,
    ret_col             = "R_m",
    include_scope2      = T,
    include_scope3      = FALSE,
    ghg_measure         = "intensity",
    ghg_class           = "disclosed",
    lag6m               = TRUE,  # applies to ghg cols only
    loc_filter_toggle   = sc$loc_filter_toggle,
    loc_selection       = sc$loc_selection,
    time_frame_toggle   = TRUE,
    datadate_min        = "2010-01-01",
    datadate_max        = "2024-01-01",
    industry_var        = "gind",
    country_fe_toggle   = sc$country_fe_toggle,
    country_fe_variable = "loc",
    controls            = all_controls_lag6m,
    display_controls    = c(""),
    inv_pref_var        = inv_pref
  )
  
  models <- res$models
  rm(res)
  
  ## name scheme: <Scenario>_<InvPref>_<original model name>
  prefix <- paste0(sc_name, "_", inv_pref, "_")
  names(models) <- paste0(prefix, names(models))
  models
}

## run everything: returns ONE combined named list of models
models <- unlist(
  lapply(inv_pref_vec, function(v) {
    unlist(
      lapply(names(scenarios), function(sc_name) {
        run_one(inv_pref = v, sc_name = sc_name, sc = scenarios[[sc_name]])
      }),
      recursive = FALSE
    )
  }),
  recursive = FALSE
)

gm2 <- data.frame(
  raw   = c("nobs","vcov.type","FE: ym","FE: year","FE: loc","FE: gind","r.squared","adj.r.squared","r2.within"),
  clean = c("Observations","S.E.: Clustered","Year-month-fixed effects","Year-fixed effects","Country-fixed effects","Industry-fixed effects",
            "R-squared","Adj. R-squared","Within R-squared"),
  fmt   = c(rep(0, 6), rep(3, 3)),
  stringsAsFactors = FALSE
)

group_titles <- c(
  Glob = "Global (discl.)",
  US     = "APAC (discl.)",
  ROW    = "Europe (discl.)"
)

scope_choice <- "scope1"

## 1) FILTER FIRST (based only on the suffix)
fe_order <- if (scope_choice == "scope1") c("m1","m3") else c("m2","m4")

if (scope_choice == "scope1") {
  models <- models[grepl("_(m3)$", names(models))]
} else if (scope_choice == "scope2") {
  models <- models[grepl("_(m4)$", names(models))]
} else {
  stop("scope_choice must be 'scope1' or 'scope2'")
}
## 2) ORDER (Glob, then US, then ROW; within each, m1 then m2 OR m3 then m4; within that, interactions)
country_order <- c("Glob", "US", "ROW")

nm <- names(models)

country <- sub("_.*", "", nm)                 # Glob / US / ROW
fe      <- sub(".*_(m[1-4])$", "\\1", nm)     # m1..m4 from suffix

## interaction = everything between <country>_ and _mX (no assumptions about underscores inside)
interaction <- sub(paste0("^", country, "_"), "", nm)
interaction <- sub("_(m[1-4])$", "", interaction)

## order interactions by your inv_pref_vec if possible; otherwise fall back to appearance order
interaction_order <- inv_pref_vec
if (!all(interaction %in% interaction_order)) {
  interaction_order <- unique(interaction)
}

ord <- order(
  match(country, country_order),
  match(fe, fe_order),
  match(interaction, interaction_order)
)

models <- models[ord]

# --- Build regression table ---
ft_reg <- modelsummary(
  models,
  output    = "flextable",
  vcov      = ~ gvkey + ym,
  fmt       = 3,
  stars     = c("*"=.1, "**"=.05, "***"=.01),
  statistic = "({std.error})",
  gof_map   = gm2,
  coef_omit = paste(all_controls_lag6m, collapse = "|")
)

# --- Dynamic column labels: "(k)\nRet" ---
lab <- mk_col_labels(names(models), dv = "Ret")
ft_reg <- do.call(flextable::set_header_labels, c(list(x = ft_reg), as.list(lab)))

# --- Dynamic group header row (spanners) ---
gh <- build_group_header(names(models), group_titles)
ft_reg <- flextable::add_header_row(
  ft_reg,
  values    = gh$values,
  colwidths = gh$colwidths,
  top       = TRUE
)

# --- "Dependent variable:" in top-left (second header row, first column) ---
ft_reg <- flextable::compose(
  ft_reg,
  i = 2, j = 1,
  part  = "header",
  value = flextable::as_paragraph("Dependent variable:")
)

# --- Styling ---
b_main <- fp_border(color = "black", width = 1)
b_thin <- fp_border(color = "black", width = .25)

ft_reg <- ft_reg |>
  font(fontname = "Times New Roman", part = "all") |>
  fontsize(size = 8, part = "all") |>
  bold(part = "header") |>
  padding(padding = 1.5, part = "all") |>
  line_spacing(space = 0.75, part = "all") |>
  italic(j = 1, part = "body") |>
  border_remove() |>
  hline_top(border = b_main, part = "header") |>
  hline(border = b_main, part = "header") |>
  hline_bottom(border = b_main, part = "body") |>
  align(i = 1, j = 2:(1 + length(models)), align = "center", part = "header") |>
  valign(i = 1, j = 1:(1 + length(models)), valign = "center", part = "header") |>
  autofit() |>
  set_table_properties(width = 1, layout = "autofit")

# --- Post-hoc: format Observations row with thousands separators + GOF tweaks ---
gof_start <- match("Observations", ft_reg$body$dataset[[1]])

if (!is.na(gof_start)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # format Observations
  vals_fmt <- ft_reg$body$dataset[gof_start, model_cols] |>
    unlist(use.names = FALSE) |>
    gsub(",", "", x = _) |>
    suppressWarnings() |>
    as.numeric() |>
    formatC(format = "f", digits = 0, big.mark = ",")
  
  ft_reg$body$dataset[gof_start, model_cols] <- as.list(vals_fmt)
  
  ft_reg <- ft_reg |>
    compose(i = gof_start, j = model_cols,
            value = as_paragraph(as_chunk(vals_fmt))) |>
    hline(i = gof_start - 1, border = b_thin, part = "body") |>
    italic(i = gof_start:nrow(ft_reg$body$dataset), j = 1, italic = FALSE, part = "body")
}

se_row <- match("S.E.: Clustered", ft_reg$body$dataset[[1]])

if (!is.na(se_row)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # Current cell strings for that row (one per model column)
  vals <- unlist(ft_reg$body$dataset[se_row, model_cols], use.names = FALSE)
  
  # Remove only "by:" (keep gvkey & ym)
  vals2 <- gsub("\\s*by:\\s*", " ", vals)   # "Clustered by: gvkey & ym" -> "Clustered gvkey & ym"
  vals2 <- gsub("\\s{2,}", " ", vals2)      # clean double spaces
  vals2 <- trimws(vals2)
  
  # Write back + recompose for rendering
  ft_reg$body$dataset[se_row, model_cols] <- as.list(vals2)
  ft_reg <- flextable::compose(
    ft_reg,
    i = se_row, j = model_cols,
    value = flextable::as_paragraph(flextable::as_chunk(vals2))
  )
}

ft_reg

doc <- read_docx()
doc <- body_add_flextable(doc, ft_reg)

print(doc, target = "../07_word/outputs/ccpi_glob_apac_eur_discl_int_scope1_2010-2025.docx")
print(doc, target = "../07_word/outputs/ccpi_glob_apac_eur_discl_int_scope1_2010-2024.docx")

##### * for table CAPMF ----
inv_pref_vec <- c("capmf_std",
                  "capmf_crosssectoral_policies_std",
                  "capmf_international_policies_std")

## define your location sets once
loc_US  <- asia_pacific#"USA"
loc_ROW <- europe_locs#unique(df$loc)[unique(df$loc) != "USA"]

## scenario grid (what differs across your runs)
scenarios <- list(
  Glob = list(
    loc_filter_toggle   = FALSE,
    loc_selection       = NULL,
    country_fe_toggle   = TRUE
  )
  ,
  US = list(
    loc_filter_toggle   = TRUE,
    loc_selection       = loc_US,
    country_fe_toggle   = TRUE
  )
  ,
  ROW = list(
    loc_filter_toggle   = TRUE,
    loc_selection       = loc_ROW,
    country_fe_toggle   = TRUE
  )
)

## helper: run one scenario + one inv_pref var and return named models
run_one <- function(inv_pref, sc_name, sc) {
  res <- run_ghg_specs_inv_pref(
    df,
    ret_col             = "R_m",
    include_scope2      = T,
    include_scope3      = FALSE,
    ghg_measure         = "intensity",
    ghg_class           = "disclosed",
    lag6m               = TRUE,  # applies to ghg cols only
    loc_filter_toggle   = sc$loc_filter_toggle,
    loc_selection       = sc$loc_selection,
    time_frame_toggle   = TRUE,
    datadate_min        = "2010-01-01",
    datadate_max        = "2024-01-01",
    industry_var        = "gind",
    country_fe_toggle   = sc$country_fe_toggle,
    country_fe_variable = "loc",
    controls            = all_controls_lag6m,
    display_controls    = c(""),
    inv_pref_var        = inv_pref
  )
  
  models <- res$models
  rm(res)
  
  ## name scheme: <Scenario>_<InvPref>_<original model name>
  prefix <- paste0(sc_name, "_", inv_pref, "_")
  names(models) <- paste0(prefix, names(models))
  models
}

## run everything: returns ONE combined named list of models
models <- unlist(
  lapply(inv_pref_vec, function(v) {
    unlist(
      lapply(names(scenarios), function(sc_name) {
        run_one(inv_pref = v, sc_name = sc_name, sc = scenarios[[sc_name]])
      }),
      recursive = FALSE
    )
  }),
  recursive = FALSE
)

gm2 <- data.frame(
  raw   = c("nobs","vcov.type","FE: ym","FE: year","FE: loc","FE: gind","r.squared","adj.r.squared","r2.within"),
  clean = c("Observations","S.E.: Clustered","Year-month-fixed effects","Year-fixed effects","Country-fixed effects","Industry-fixed effects",
            "R-squared","Adj. R-squared","Within R-squared"),
  fmt   = c(rep(0, 6), rep(3, 3)),
  stringsAsFactors = FALSE
)

group_titles <- c(
  Glob = "Global (no U.S.) (discl.)",
  US     = "APAC (discl.)",
  ROW    = "Europe (discl.)"
)

scope_choice <- "scope1"

## 1) FILTER FIRST (based only on the suffix)
fe_order <- if (scope_choice == "scope1") c("m1","m3") else c("m2","m4")

if (scope_choice == "scope1") {
  models <- models[grepl("_(m3)$", names(models))]
} else if (scope_choice == "scope2") {
  models <- models[grepl("_(m4)$", names(models))]
} else {
  stop("scope_choice must be 'scope1' or 'scope2'")
}
## 2) ORDER (Glob, then US, then ROW; within each, m1 then m2 OR m3 then m4; within that, interactions)
country_order <- c("Glob", "US", "ROW")

nm <- names(models)

country <- sub("_.*", "", nm)                 # Glob / US / ROW
fe      <- sub(".*_(m[1-4])$", "\\1", nm)     # m1..m4 from suffix

## interaction = everything between <country>_ and _mX (no assumptions about underscores inside)
interaction <- sub(paste0("^", country, "_"), "", nm)
interaction <- sub("_(m[1-4])$", "", interaction)

## order interactions by your inv_pref_vec if possible; otherwise fall back to appearance order
interaction_order <- inv_pref_vec
if (!all(interaction %in% interaction_order)) {
  interaction_order <- unique(interaction)
}

ord <- order(
  match(country, country_order),
  match(fe, fe_order),
  match(interaction, interaction_order)
)

models <- models[ord]

# --- Build regression table ---
ft_reg <- modelsummary(
  models,
  output    = "flextable",
  vcov      = ~ gvkey + ym,
  fmt       = 3,
  stars     = c("*"=.1, "**"=.05, "***"=.01),
  statistic = "({std.error})",
  gof_map   = gm2,
  coef_omit = paste(all_controls_lag6m, collapse = "|")
)

# --- Dynamic column labels: "(k)\nRet" ---
lab <- mk_col_labels(names(models), dv = "Ret")
ft_reg <- do.call(flextable::set_header_labels, c(list(x = ft_reg), as.list(lab)))

# --- Dynamic group header row (spanners) ---
gh <- build_group_header(names(models), group_titles)
ft_reg <- flextable::add_header_row(
  ft_reg,
  values    = gh$values,
  colwidths = gh$colwidths,
  top       = TRUE
)

# --- "Dependent variable:" in top-left (second header row, first column) ---
ft_reg <- flextable::compose(
  ft_reg,
  i = 2, j = 1,
  part  = "header",
  value = flextable::as_paragraph("Dependent variable:")
)

# --- Styling ---
b_main <- fp_border(color = "black", width = 1)
b_thin <- fp_border(color = "black", width = .25)

ft_reg <- ft_reg |>
  font(fontname = "Times New Roman", part = "all") |>
  fontsize(size = 8, part = "all") |>
  bold(part = "header") |>
  padding(padding = 1.5, part = "all") |>
  line_spacing(space = 0.75, part = "all") |>
  italic(j = 1, part = "body") |>
  border_remove() |>
  hline_top(border = b_main, part = "header") |>
  hline(border = b_main, part = "header") |>
  hline_bottom(border = b_main, part = "body") |>
  align(i = 1, j = 2:(1 + length(models)), align = "center", part = "header") |>
  valign(i = 1, j = 1:(1 + length(models)), valign = "center", part = "header") |>
  autofit() |>
  set_table_properties(width = 1, layout = "autofit")

# --- Post-hoc: format Observations row with thousands separators + GOF tweaks ---
gof_start <- match("Observations", ft_reg$body$dataset[[1]])

if (!is.na(gof_start)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # format Observations
  vals_fmt <- ft_reg$body$dataset[gof_start, model_cols] |>
    unlist(use.names = FALSE) |>
    gsub(",", "", x = _) |>
    suppressWarnings() |>
    as.numeric() |>
    formatC(format = "f", digits = 0, big.mark = ",")
  
  ft_reg$body$dataset[gof_start, model_cols] <- as.list(vals_fmt)
  
  ft_reg <- ft_reg |>
    compose(i = gof_start, j = model_cols,
            value = as_paragraph(as_chunk(vals_fmt))) |>
    hline(i = gof_start - 1, border = b_thin, part = "body") |>
    italic(i = gof_start:nrow(ft_reg$body$dataset), j = 1, italic = FALSE, part = "body")
}

se_row <- match("S.E.: Clustered", ft_reg$body$dataset[[1]])

if (!is.na(se_row)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # Current cell strings for that row (one per model column)
  vals <- unlist(ft_reg$body$dataset[se_row, model_cols], use.names = FALSE)
  
  # Remove only "by:" (keep gvkey & ym)
  vals2 <- gsub("\\s*by:\\s*", " ", vals)   # "Clustered by: gvkey & ym" -> "Clustered gvkey & ym"
  vals2 <- gsub("\\s{2,}", " ", vals2)      # clean double spaces
  vals2 <- trimws(vals2)
  
  # Write back + recompose for rendering
  ft_reg$body$dataset[se_row, model_cols] <- as.list(vals2)
  ft_reg <- flextable::compose(
    ft_reg,
    i = se_row, j = model_cols,
    value = flextable::as_paragraph(flextable::as_chunk(vals2))
  )
}

ft_reg

doc <- read_docx()
doc <- body_add_flextable(doc, ft_reg)

print(doc, target = "../07_word/outputs/capmf_glob_apac_eur_discl_int_scope1_2010-2024.docx")



##### * for table PAI, CFA/CVA ----
inv_pref_vec <- c("PAI_z",
                  "cfa_cva_ratio_z")

## define your location sets once
loc_US  <- asia_pacific#"USA"
loc_ROW <- europe_locs#unique(df$loc)[unique(df$loc) != "USA"]

## scenario grid (what differs across your runs)
scenarios <- list(
  Glob = list(
    loc_filter_toggle   = FALSE,
    loc_selection       = NULL,
    country_fe_toggle   = TRUE
  )
  ,
  US = list(
    loc_filter_toggle   = TRUE,
    loc_selection       = loc_US,
    country_fe_toggle   = TRUE
  )
  ,
  ROW = list(
    loc_filter_toggle   = TRUE,
    loc_selection       = loc_ROW,
    country_fe_toggle   = TRUE
  )
)

## helper: run one scenario + one inv_pref var and return named models
run_one <- function(inv_pref, sc_name, sc) {
  res <- run_ghg_specs_inv_pref(
    df,
    ret_col             = "R_m",
    include_scope2      = T,
    include_scope3      = FALSE,
    ghg_measure         = "intensity",
    ghg_class           = "disclosed",
    lag6m               = TRUE,  # applies to ghg cols only
    loc_filter_toggle   = sc$loc_filter_toggle,
    loc_selection       = sc$loc_selection,
    time_frame_toggle   = TRUE,
    datadate_min        = "2010-01-01",
    datadate_max        = "2024-01-01",
    industry_var        = "gind",
    country_fe_toggle   = sc$country_fe_toggle,
    country_fe_variable = "loc",
    controls            = all_controls_lag6m,
    display_controls    = c(""),
    inv_pref_var        = inv_pref
  )
  
  models <- res$models
  rm(res)
  
  ## name scheme: <Scenario>_<InvPref>_<original model name>
  prefix <- paste0(sc_name, "_", inv_pref, "_")
  names(models) <- paste0(prefix, names(models))
  models
}

## run everything: returns ONE combined named list of models
models <- unlist(
  lapply(inv_pref_vec, function(v) {
    unlist(
      lapply(names(scenarios), function(sc_name) {
        run_one(inv_pref = v, sc_name = sc_name, sc = scenarios[[sc_name]])
      }),
      recursive = FALSE
    )
  }),
  recursive = FALSE
)

gm2 <- data.frame(
  raw   = c("nobs","vcov.type","FE: ym","FE: year","FE: loc","FE: gind","r.squared","adj.r.squared","r2.within"),
  clean = c("Observations","S.E.: Clustered","Year-month-fixed effects","Year-fixed effects","Country-fixed effects","Industry-fixed effects",
            "R-squared","Adj. R-squared","Within R-squared"),
  fmt   = c(rep(0, 6), rep(3, 3)),
  stringsAsFactors = FALSE
)

group_titles <- c(
  Glob = "Global (discl.)",
  US     = "APAC (discl.)",
  ROW    = "Europe (discl.)"
)

scope_choice <- "scope1"

## 1) FILTER FIRST (based only on the suffix)
fe_order <- if (scope_choice == "scope1") c("m1","m3") else c("m2","m4")

if (scope_choice == "scope1") {
  models <- models[grepl("_(m3)$", names(models))]
} else if (scope_choice == "scope2") {
  models <- models[grepl("_(m4)$", names(models))]
} else {
  stop("scope_choice must be 'scope1' or 'scope2'")
}
## 2) ORDER (Glob, then US, then ROW; within each, m1 then m2 OR m3 then m4; within that, interactions)
country_order <- c("Glob", "US", "ROW")

nm <- names(models)

country <- sub("_.*", "", nm)                 # Glob / US / ROW
fe      <- sub(".*_(m[1-4])$", "\\1", nm)     # m1..m4 from suffix

## interaction = everything between <country>_ and _mX (no assumptions about underscores inside)
interaction <- sub(paste0("^", country, "_"), "", nm)
interaction <- sub("_(m[1-4])$", "", interaction)

## order interactions by your inv_pref_vec if possible; otherwise fall back to appearance order
interaction_order <- inv_pref_vec
if (!all(interaction %in% interaction_order)) {
  interaction_order <- unique(interaction)
}

ord <- order(
  match(country, country_order),
  match(fe, fe_order),
  match(interaction, interaction_order)
)

models <- models[ord]

# --- Build regression table ---
ft_reg <- modelsummary(
  models,
  output    = "flextable",
  vcov      = ~ gvkey + ym,
  fmt       = 3,
  stars     = c("*"=.1, "**"=.05, "***"=.01),
  statistic = "({std.error})",
  gof_map   = gm2,
  coef_omit = paste(all_controls_lag6m, collapse = "|")
)

# --- Dynamic column labels: "(k)\nRet" ---
lab <- mk_col_labels(names(models), dv = "Ret")
ft_reg <- do.call(flextable::set_header_labels, c(list(x = ft_reg), as.list(lab)))

# --- Dynamic group header row (spanners) ---
gh <- build_group_header(names(models), group_titles)
ft_reg <- flextable::add_header_row(
  ft_reg,
  values    = gh$values,
  colwidths = gh$colwidths,
  top       = TRUE
)

# --- "Dependent variable:" in top-left (second header row, first column) ---
ft_reg <- flextable::compose(
  ft_reg,
  i = 2, j = 1,
  part  = "header",
  value = flextable::as_paragraph("Dependent variable:")
)

# --- Styling ---
b_main <- fp_border(color = "black", width = 1)
b_thin <- fp_border(color = "black", width = .25)

ft_reg <- ft_reg |>
  font(fontname = "Times New Roman", part = "all") |>
  fontsize(size = 8, part = "all") |>
  bold(part = "header") |>
  padding(padding = 1.5, part = "all") |>
  line_spacing(space = 0.75, part = "all") |>
  italic(j = 1, part = "body") |>
  border_remove() |>
  hline_top(border = b_main, part = "header") |>
  hline(border = b_main, part = "header") |>
  hline_bottom(border = b_main, part = "body") |>
  align(i = 1, j = 2:(1 + length(models)), align = "center", part = "header") |>
  valign(i = 1, j = 1:(1 + length(models)), valign = "center", part = "header") |>
  autofit() |>
  set_table_properties(width = 1, layout = "autofit")

# --- Post-hoc: format Observations row with thousands separators + GOF tweaks ---
gof_start <- match("Observations", ft_reg$body$dataset[[1]])

if (!is.na(gof_start)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # format Observations
  vals_fmt <- ft_reg$body$dataset[gof_start, model_cols] |>
    unlist(use.names = FALSE) |>
    gsub(",", "", x = _) |>
    suppressWarnings() |>
    as.numeric() |>
    formatC(format = "f", digits = 0, big.mark = ",")
  
  ft_reg$body$dataset[gof_start, model_cols] <- as.list(vals_fmt)
  
  ft_reg <- ft_reg |>
    compose(i = gof_start, j = model_cols,
            value = as_paragraph(as_chunk(vals_fmt))) |>
    hline(i = gof_start - 1, border = b_thin, part = "body") |>
    italic(i = gof_start:nrow(ft_reg$body$dataset), j = 1, italic = FALSE, part = "body")
}

se_row <- match("S.E.: Clustered", ft_reg$body$dataset[[1]])

if (!is.na(se_row)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # Current cell strings for that row (one per model column)
  vals <- unlist(ft_reg$body$dataset[se_row, model_cols], use.names = FALSE)
  
  # Remove only "by:" (keep gvkey & ym)
  vals2 <- gsub("\\s*by:\\s*", " ", vals)   # "Clustered by: gvkey & ym" -> "Clustered gvkey & ym"
  vals2 <- gsub("\\s{2,}", " ", vals2)      # clean double spaces
  vals2 <- trimws(vals2)
  
  # Write back + recompose for rendering
  ft_reg$body$dataset[se_row, model_cols] <- as.list(vals2)
  ft_reg <- flextable::compose(
    ft_reg,
    i = se_row, j = model_cols,
    value = flextable::as_paragraph(flextable::as_chunk(vals2))
  )
}

ft_reg

doc <- read_docx()
doc <- body_add_flextable(doc, ft_reg)

print(doc, target = "../07_word/outputs/PAI_CFA_glob_apac_eur_discl_int_scope1_2010-2024.docx")



##### * for table Policy fundamentals ----
inv_pref_vec <- c("total_ff_produced_per_gdp_z",
                  "gdp_pc_z",
                  "vulnerability_z",
                  "control_corruption_z")

## define your location sets once
loc_US  <- asia_pacific#"USA"
loc_ROW <- europe_locs#unique(df$loc)[unique(df$loc) != "USA"]

## scenario grid (what differs across your runs)
scenarios <- list(
  Glob = list(
    loc_filter_toggle   = FALSE,
    loc_selection       = NULL,
    country_fe_toggle   = TRUE
  )
  ,
  US = list(
    loc_filter_toggle   = TRUE,
    loc_selection       = loc_US,
    country_fe_toggle   = TRUE
  )
  ,
  ROW = list(
    loc_filter_toggle   = TRUE,
    loc_selection       = loc_ROW,
    country_fe_toggle   = TRUE
  )
)

## helper: run one scenario + one inv_pref var and return named models
run_one <- function(inv_pref, sc_name, sc) {
  res <- run_ghg_specs_inv_pref(
    df,
    ret_col             = "R_m",
    include_scope2      = T,
    include_scope3      = FALSE,
    ghg_measure         = "intensity",
    ghg_class           = "disclosed",
    lag6m               = TRUE,  # applies to ghg cols only
    loc_filter_toggle   = sc$loc_filter_toggle,
    loc_selection       = sc$loc_selection,
    time_frame_toggle   = TRUE,
    datadate_min        = "2010-01-01",
    datadate_max        = "2024-01-01",
    industry_var        = "gind",
    country_fe_toggle   = sc$country_fe_toggle,
    country_fe_variable = "loc",
    controls            = all_controls_lag6m,
    display_controls    = c(""),
    inv_pref_var        = inv_pref
  )
  
  models <- res$models
  rm(res)
  
  ## name scheme: <Scenario>_<InvPref>_<original model name>
  prefix <- paste0(sc_name, "_", inv_pref, "_")
  names(models) <- paste0(prefix, names(models))
  models
}

## run everything: returns ONE combined named list of models
models <- unlist(
  lapply(inv_pref_vec, function(v) {
    unlist(
      lapply(names(scenarios), function(sc_name) {
        run_one(inv_pref = v, sc_name = sc_name, sc = scenarios[[sc_name]])
      }),
      recursive = FALSE
    )
  }),
  recursive = FALSE
)

gm2 <- data.frame(
  raw   = c("nobs","vcov.type","FE: ym","FE: year","FE: loc","FE: gind","r.squared","adj.r.squared","r2.within"),
  clean = c("Observations","S.E.: Clustered","Year-month-fixed effects","Year-fixed effects","Country-fixed effects","Industry-fixed effects",
            "R-squared","Adj. R-squared","Within R-squared"),
  fmt   = c(rep(0, 6), rep(3, 3)),
  stringsAsFactors = FALSE
)

group_titles <- c(
  Glob = "Global (discl.)",
  US     = "APAC (discl.)",
  ROW    = "Europe (discl.)"
)

scope_choice <- "scope1"

## 1) FILTER FIRST (based only on the suffix)
fe_order <- if (scope_choice == "scope1") c("m1","m3") else c("m2","m4")

if (scope_choice == "scope1") {
  models <- models[grepl("_(m3)$", names(models))]
} else if (scope_choice == "scope2") {
  models <- models[grepl("_(m4)$", names(models))]
} else {
  stop("scope_choice must be 'scope1' or 'scope2'")
}
## 2) ORDER (Glob, then US, then ROW; within each, m1 then m2 OR m3 then m4; within that, interactions)
country_order <- c("Glob", "US", "ROW")

nm <- names(models)

country <- sub("_.*", "", nm)                 # Glob / US / ROW
fe      <- sub(".*_(m[1-4])$", "\\1", nm)     # m1..m4 from suffix

## interaction = everything between <country>_ and _mX (no assumptions about underscores inside)
interaction <- sub(paste0("^", country, "_"), "", nm)
interaction <- sub("_(m[1-4])$", "", interaction)

## order interactions by your inv_pref_vec if possible; otherwise fall back to appearance order
interaction_order <- inv_pref_vec
if (!all(interaction %in% interaction_order)) {
  interaction_order <- unique(interaction)
}

ord <- order(
  match(country, country_order),
  match(fe, fe_order),
  match(interaction, interaction_order)
)

models <- models[ord]

# --- Build regression table ---
ft_reg <- modelsummary(
  models,
  output    = "flextable",
  vcov      = ~ gvkey + ym,
  fmt       = 3,
  stars     = c("*"=.1, "**"=.05, "***"=.01),
  statistic = "({std.error})",
  gof_map   = gm2,
  coef_omit = paste(all_controls_lag6m, collapse = "|")
)

# --- Dynamic column labels: "(k)\nRet" ---
lab <- mk_col_labels(names(models), dv = "Ret")
ft_reg <- do.call(flextable::set_header_labels, c(list(x = ft_reg), as.list(lab)))

# --- Dynamic group header row (spanners) ---
gh <- build_group_header(names(models), group_titles)
ft_reg <- flextable::add_header_row(
  ft_reg,
  values    = gh$values,
  colwidths = gh$colwidths,
  top       = TRUE
)

# --- "Dependent variable:" in top-left (second header row, first column) ---
ft_reg <- flextable::compose(
  ft_reg,
  i = 2, j = 1,
  part  = "header",
  value = flextable::as_paragraph("Dependent variable:")
)

# --- Styling ---
b_main <- fp_border(color = "black", width = 1)
b_thin <- fp_border(color = "black", width = .25)

ft_reg <- ft_reg |>
  font(fontname = "Times New Roman", part = "all") |>
  fontsize(size = 8, part = "all") |>
  bold(part = "header") |>
  padding(padding = 1.5, part = "all") |>
  line_spacing(space = 0.75, part = "all") |>
  italic(j = 1, part = "body") |>
  border_remove() |>
  hline_top(border = b_main, part = "header") |>
  hline(border = b_main, part = "header") |>
  hline_bottom(border = b_main, part = "body") |>
  align(i = 1, j = 2:(1 + length(models)), align = "center", part = "header") |>
  valign(i = 1, j = 1:(1 + length(models)), valign = "center", part = "header") |>
  autofit() |>
  set_table_properties(width = 1, layout = "autofit")

# --- Post-hoc: format Observations row with thousands separators + GOF tweaks ---
gof_start <- match("Observations", ft_reg$body$dataset[[1]])

if (!is.na(gof_start)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # format Observations
  vals_fmt <- ft_reg$body$dataset[gof_start, model_cols] |>
    unlist(use.names = FALSE) |>
    gsub(",", "", x = _) |>
    suppressWarnings() |>
    as.numeric() |>
    formatC(format = "f", digits = 0, big.mark = ",")
  
  ft_reg$body$dataset[gof_start, model_cols] <- as.list(vals_fmt)
  
  ft_reg <- ft_reg |>
    compose(i = gof_start, j = model_cols,
            value = as_paragraph(as_chunk(vals_fmt))) |>
    hline(i = gof_start - 1, border = b_thin, part = "body") |>
    italic(i = gof_start:nrow(ft_reg$body$dataset), j = 1, italic = FALSE, part = "body")
}

se_row <- match("S.E.: Clustered", ft_reg$body$dataset[[1]])

if (!is.na(se_row)) {
  model_cols <- 2:ncol(ft_reg$body$dataset)
  
  # Current cell strings for that row (one per model column)
  vals <- unlist(ft_reg$body$dataset[se_row, model_cols], use.names = FALSE)
  
  # Remove only "by:" (keep gvkey & ym)
  vals2 <- gsub("\\s*by:\\s*", " ", vals)   # "Clustered by: gvkey & ym" -> "Clustered gvkey & ym"
  vals2 <- gsub("\\s{2,}", " ", vals2)      # clean double spaces
  vals2 <- trimws(vals2)
  
  # Write back + recompose for rendering
  ft_reg$body$dataset[se_row, model_cols] <- as.list(vals2)
  ft_reg <- flextable::compose(
    ft_reg,
    i = se_row, j = model_cols,
    value = flextable::as_paragraph(flextable::as_chunk(vals2))
  )
}

ft_reg

doc <- read_docx()
doc <- body_add_flextable(doc, ft_reg)

print(doc, target = "../07_word/outputs/policy_funda_glob_apac_eur_discl_int_scope1_2010-2024.docx")



### (d) Combined preference and climate policy ----
res <- run_ghg_specs_inv_pref(
  df,
  ret_col = "R_m",
  include_scope2 = TRUE,
  include_scope3 = F,
  ghg_measure = "intensity",
  ghg_class = "disclosed",
  lag6m = TRUE, # applies to ghg cols only
  loc_filter_toggle = T,
  loc_selection = g20_locs,
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  industry_var = "gind",
  country_fe_toggle = T,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = c(""),
  inv_pref_var = c("capmf_std","TRI_monthly_roll_z"))

# create output table
tab_test <- do.call(
  etable,
  c(
    res$models,
    list(
      se.below = TRUE,
      digits   = 3,
      drop     = res$drop_controls,
      signif.code =  c("*" = 0.1, "**" = 0.05, "***" = 0.01)
    )
  )
)

rm(res)
View(tab_test)


### (e) Event dummies ----
# run regressions
res <- run_ghg_specs_dummy_full_spec(
  df,
  
  ret_col = "R_m",
  
  ghg_measure = "intensity",
  ghg_class = "disclosed",
  lag6m = TRUE, # applies to ghg cols only
  
  toggle_S2 = T, 
  toggle_S3 = T,
  
  loc_filter_toggle = F,
  loc_selection = c("USA"),
  
  time_frame_toggle = T,
  datadate_min = "2010-01-01",
  datadate_max = "2025-01-01",
  
  industry_var = "gind",
  country_fe_toggle = T,
  country_fe_variable = "loc",
  controls = all_controls_lag6m,
  display_controls = NULL,
  
  dummy_toggle = T,
  dummy_cols = c("paris_dummy","covid_dummy"),
  
  interact_toggle = F,
  interact_with_vars = c("internat_ccpi")
)

# create output table
tab_dummy <- do.call(
  etable,
  c(
    res$models,
    list(
      se.below = TRUE,
      digits   = 3,
      drop     = res$drop_controls,
      signif.code =  c("*" = 0.1, "**" = 0.05, "***" = 0.01)
    )
  )
)

cat(res$spec_text)
rm(res)
View(tab_dummy)


## 3) other dependent variables ----

## 4) time-series averages of carbon beta ----
### (a.1) timeseries ym beta for full sample ----
a_time_series_ym_beta <- run_ghg_monthly_betas(
  df = df,                         
  ret_col     = "R_m",             
  ghg_measure = "intensity",             
  scope       = c("S1","S2"),  
  lag6m       = TRUE,              
  ghg_class   = "disclosed",       
  loc_filter_toggle = F,        
  loc_selection     = c("USA","CAN","CHN","JPN"),
  group_by_loc      = F,
  time_frame_toggle = T,        
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,  
  min_firms   = 50L,
  fixed_effect = c("gind","loc")    
)

a_time_series_ym_beta <- run_ghg_monthly_window_betas(
  df = df,                         
  ret_col     = "R_m",             
  ghg_measure = "intensity",             
  scope       = c("S1","S2"),  
  lag6m       = TRUE,              
  ghg_class   = "disclosed",       
  loc_filter_toggle = F,        
  loc_selection     = c("USA","CAN","CHN","JPN"),
  group_by_loc      = F,
  time_frame_toggle = T,        
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,  
  min_firms   = 50L,
  fixed_effect = c("gind","loc"),
  window = 1
)

plot_ghg_betas(
  ts_dt           = a_time_series_ym_beta,
  scope           = "S1",
  plot_all_locs   = F,
  loc_selection   = c("USA"),
  ci_mult         = 1.96,
  facet_ncol      = 1,
  facet_scales    = "fixed",
  plot_date_min   = as.Date("2010-01-01"),
  plot_date_max   = as.Date("2025-01-01"),
  zero_linewidth  = 0.2,
  ma_window = 12
)

### (a.2) timeseries yearly beta for full sample ----
a_time_series_y_beta <- run_ghg_yearly_betas(
  df = df,                         
  ret_col     = "R_m",             
  ghg_measure = "intensity",             
  scope       = c("S1","S2"),  
  lag6m       = TRUE,              
  ghg_class   = "disclosed",       
  loc_filter_toggle = F,        
  loc_selection     = c("USA","CAN","CHN","JPN"),
  group_by_loc      = F,
  time_frame_toggle = T,        
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,  
  min_firms   = 50L,
  fixed_effect = c("gind"),
  cluster_vars = c("gvkey")     
)

plot_ghg_yearly_betas(
  ts_dt           = a_time_series_y_beta,
  scope           = "S1",
  plot_all_locs   = F,
  loc_selection   = c("USA"),
  ci_mult         = 1.96,
  facet_ncol      = 1,
  facet_scales    = "fixed",
  plot_date_min   = as.Date("2010-01-01"),
  plot_date_max   = as.Date("2025-01-01"),
  zero_linewidth  = 0.1,
  ma_window = 0
)


### (b.1) timeseries ym beta for each country ----
b_time_series_beta_ym_country <- run_ghg_monthly_betas(
  df = df,                         
  ret_col     = "R_m",             
  ghg_measure = "intensity",             
  scope       = c("S1"),  
  lag6m       = TRUE,              
  ghg_class   = "disclosed",       
  loc_filter_toggle = F,        
  loc_selection     = c("USA","CAN","CHN","JPN"),
  group_by_loc      = T,
  time_frame_toggle = T,        
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,  
  min_firms   = 50L,
  fixed_effect = c("gind")
)

setDT(b_time_series_beta_ym_country)

b_ts_with_controls_ym_country <- add_country_year_controls(
  control_vars = CY_CONTROL_VARS, 
  dt = b_time_series_beta_ym_country,
  df_country_year = df_country_year
)

b_ts_with_controls_ym_country <- add_region(b_ts_with_controls_ym_country)

#### (i) country-wise plots of ym beta timeseries ----
plot_ghg_betas(
  ts_dt           = b_ts_with_controls_ym_country,
  scope           = "S1",
  plot_all_locs   = T,
  loc_selection   = c("USA","CHN","DEU"),
  ci_mult         = 1.96,
  facet_ncol      = 1,
  facet_scales    = "free_y",
  plot_date_min   = as.Date("2017-01-01"),
  plot_date_max   = as.Date("2025-01-01"),
  zero_linewidth  = 0.1,
  ma_window = 12
)

#### (ii) scatter variable wise plots of median ym beta timeseries ----
plot_panel_scatter(
  dt            = b_ts_with_controls_ym_country,
  var_x         = c(
                    #"combined_ccpi",
                    #"nat_ccpi",
                    #"internat_ccpi"
    
                    #"capmf_std",
                    #"capmf_international_policies_std",
                    #"capmf_crosssectoral_policies_std",
                    #"capmf_sectoral_policies_std",
                    #"capmf_ghg_emission_targets_std",
                    #"capmf_fossil_fuel_production_policies_std"
    
                    "PAI_z",
                    "cfa_cva_ratio_z",
                    "total_ff_produced_per_gdp_z",
                    "vulnerability_z",
                    "control_corruption_z",
                    "gdp_pc_z"
                    ),
  var_y         = "beta_carbon_S1",
  start_year    = 2010,
  end_year      = 2024,
  agg_fun       = "median",
  loc_col       = "loc",
  year_col      = "year",
  group_col     = "region",                 # if you have a region column
  loc_selection = unique(df$loc)[!unique(df$loc)%in%c("SWE")],
  highlight_locs = c("USA", "DEU", "CHN"),
  min_years = 5
  #x_label       = "Combined CCPI (std.)",
  #y_label       = "Carbon beta S1",
  #plot_title    = "Carbon beta S1 vs CCPI, 2015–2020 (means)"
)

dt_summary <- b_ts_with_controls_ym_country[
  !is.na(beta_carbon_S1),
  .(
    n_obs       = .N,
    year_min    = min(year, na.rm = TRUE),
    year_max    = max(year, na.rm = TRUE)
  ),
  by = loc
]

dt_summary

#### (iii) country-wise plots of yearly average ym beta timeseries ----
plot_beta_vs_country_vars(facet_by_country = T,
  beta_dt        = b_ts_with_controls_ym_country,
  data_df        = df,
  scope          = "S1",
  loc_selection  = g20_locs,
  country_vars   = c("PAI_z"),
  min_months_per_year = 2,
  agg_fun_beta = median,
  free_y_axis = F
)

### (b.2) timeseries yearly beta for each country ----
b_time_series_y_beta_country <- run_ghg_yearly_betas(
  df = df,                         
  ret_col     = "R_m",             
  ghg_measure = "intensity",             
  scope       = c("S1"),  
  lag6m       = TRUE,              
  ghg_class   = "disclosed",
  loc_filter_toggle = F,        
  loc_selection     = c("USA","CAN","CHN","JPN"),
  group_by_loc      = T,
  time_frame_toggle = T,        
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,  
  min_firms   = 500L,
  fixed_effect = c("gind"),
  cluster_vars = c("gvkey")
)

setDT(b_time_series_y_beta_country)

b_time_series_y_beta_country <- add_country_year_controls(
  control_vars = CY_CONTROL_VARS, 
  dt = b_time_series_y_beta_country,
  df_country_year = df_country_year
)

b_time_series_y_beta_country <- add_region(b_time_series_y_beta_country)

#### (i) scatter variable wise plots of median y beta timeseries ----
plot_panel_scatter(
  dt            = b_time_series_y_beta_country,
  var_x         = c(
    #"combined_ccpi",
    #"nat_ccpi",
    #"internat_ccpi"
    
    "capmf_std",
    "capmf_international_policies_std",
    "capmf_crosssectoral_policies_std",
    "capmf_sectoral_policies_std",
    "capmf_ghg_emission_targets_std",
    "capmf_fossil_fuel_production_policies_std"
    
    #"PAI_z",
    #"cfa_cva_ratio_z",
    #"total_ff_produced_per_gdp_z",
    #"vulnerability_z",
    #"control_corruption_z",
    #"gdp_pc_z"
  ),
  var_y         = "beta_carbon_S1",
  start_year    = 2010,
  end_year      = 2024,
  agg_fun       = "median",
  loc_col       = "loc",
  year_col      = "year",
  group_col     = "region",
  loc_selection = NULL,
  min_years = 3,
  show_rho = T,rho_position = "topleft"
  #highlight_locs = c("USA", "DEU", "CHN"),
  #x_label       = "Combined CCPI (std.)",
  #y_label       = "Carbon beta S1",
  #plot_title    = "Carbon beta S1 vs CCPI, 2015–2020 (means)"
)

dt_summary <- b_time_series_y_beta_country[
  !is.na(beta_carbon_S1),
  .(
    n_obs       = .N,
    year_min    = min(year, na.rm = TRUE),
    year_max    = max(year, na.rm = TRUE)
  ),
  by = loc
]

dt_summary

##### * export to image ----
plot_country_scatter(
  group_facet_toggle = TRUE,
  dt         = b_time_series_y_beta_country,
  var_x      = "cfa_cva_ratio_z",
  var_y      = "beta_carbon_S1",
  start_year = 2010,
  end_year   = 2024,
  agg_fun    = "median",
  loc_col    = "loc",
  group_col  = "region"
) +
  labs(title = NULL) +
  facet_wrap(~group, nrow = 1, ncol = 4, drop = FALSE)

DT <- copy(b_time_series_y_beta_country)

beta_col <- "beta_carbon_S1"

vars <- c(
  "combined_ccpi",
  "nat_ccpi",
  "internat_ccpi",
  "capmf_std",
  "capmf_fossil_fuel_production_policies_std",
  #"capmf_ghg_emission_targets_std",
  "PAI_z",
  "cfa_cva_ratio_z"
  #"gdp_pc_z",
  #"vulnerability_z",
  #"total_ff_produced_per_gdp_z"
)

start_year <- 2010L
end_year   <- 2024L

DT <- DT[year >= start_year & year <= end_year]
DT <- DT[!is.na(region) & nzchar(region)]
DT <- DT[!is.na(loc) & nzchar(loc)]
DT <- DT[region != "Middle East & Africa"]

L <- melt(
  DT,
  id.vars = c("loc", "region", "year", beta_col),
  measure.vars = vars,
  variable.name = "x_var",
  value.name = "x"
)

L <- L[!is.na(get(beta_col)) & !is.na(x)]

P <- L[, .(
  x_med    = median(x, na.rm = TRUE),
  beta_med = median(get(beta_col), na.rm = TRUE),
  n_pair   = .N
), by = .(region, loc, x_var)]

P <- P[n_pair >= 2]

P_global <- copy(P)
P_global[, facet_region := "Global"]
P[, facet_region := as.character(region)]

P <- rbind(P, P_global, use.names = TRUE)

P[, x_var := factor(x_var, levels = vars)]

facet_levels <- c("Global", "Europe", "Asia-Pacific", "Americas")
P[, facet_region := factor(facet_region, levels = facet_levels)]

g <- ggplot(P, aes(x = x_med, y = beta_med)) +
  geom_hline(yintercept = 0, linewidth = 0.3, colour = "grey30") +
  geom_point(size = 2.3, alpha = 0.8, colour = "grey40") +
  geom_smooth(method = "lm", se = F, linewidth = 0.6, colour = "darkred", alpha = 0.15, fill = "darkred") +
  facet_grid(
    rows = vars(facet_region),
    cols = vars(x_var),
    scales = "free_x"
  ) +
  labs(
    x = NULL,
    y = expression(~"Median yearly"~beta~"(Scope 1 Intensity)")
  ) +
  scale_y_continuous(labels = label_number(accuracy = 0.01)) +
  theme_classic() +
  theme(
    axis.line        = element_blank(),
    plot.title       = element_blank(),
    plot.subtitle    = element_blank(),
    panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
    strip.text.x     = element_text(size = 10, face = "bold"),
    strip.text.y     = element_text(size = 10, face = "bold"),
    strip.background = element_rect(fill = "grey90", colour = "black", linewidth = 0.6),
    legend.position  = "none",
    panel.spacing.y  = unit(0.6, "lines"),
    panel.spacing.x  = unit(0.8, "lines")
  )

g

region_cols <- c(
  "Americas"     = "#F28E2B",  # orange
  "Europe"       = "#4E79A7",  # blue
  "Asia-Pacific" = "#E15759"   # red
)

g <- g +
  geom_point(aes(colour = region), size = 2.1, alpha = 0.85) +
  geom_text_repel(
    data = subset(P, facet_region != "Global"),
    aes(label = loc, colour = region),
    size = 2.2,
    seed = 1,
    max.overlaps = Inf,
    box.padding = 0.15,
    point.padding = 0.10,
    min.segment.length = 0,
    segment.size = 0.25,
    segment.alpha = 0.4,
    show.legend = FALSE
  ) +
  scale_colour_manual(values = region_cols, drop = FALSE) +
  theme(legend.position = "none")

g <- g +
  theme(
    strip.text.x = element_text(size = 6, face = "bold"),
    strip.text.y = element_text(size = 6, face = "bold")
  )


ggsave(
  filename = "../07_word/outputs/scatterplot_policy_median_beta_2010_2024.png",
  plot     = g,
  #device   = "emf",
  width    = 14.5,
  height   = 7.5,
  units    = "in",
  dpi = 1000
)



#### (ii) country-wise plots of yearly beta to variable timeseries ----
plots <- plot_beta_vs_country_vars_panel(facet_by_country = T,
  dt = b_time_series_y_beta_country,
  var_x = c("PAI_z"),
  scope = "S1",
  start_year = 2010,
  end_year = 2024,
  loc_selection = g7_locs,
  #highlight_locs = c("USA", "DEU", "CHN"),
  label_years = F,
  weight_by_n = F
)

# test 2  ----
DT <- copy(b_time_series_y_beta_country)

beta_col <- "beta_carbon_S1"

vars <- c(
  "combined_ccpi",
  "nat_ccpi",
  "capmf_std",
  "PAI_z",
  "cfa_cva_ratio_z"
)

start_year <- 2010L
end_year   <- 2024L

# --- choose locs here ---
sel_locs <- c("USA","CHN")   # <- replace with your loc identifiers

DT <- DT[year >= start_year & year <= end_year]
DT <- DT[!is.na(region) & nzchar(region)]
DT <- DT[!is.na(loc) & nzchar(loc)]

# keep only selected locs
DT <- DT[loc %chin% sel_locs]

# long format: one row per (loc, year, x_var)
L <- melt(
  DT,
  id.vars = c("loc", "region", "year", beta_col),
  measure.vars = vars,
  variable.name = "x_var",
  value.name = "x"
)

# keep complete pairs
L <- L[!is.na(get(beta_col)) & !is.na(x)]

# optional: require at least 2 years per (loc, x_var)
L <- L[, n_pair := .N, by = .(loc, x_var)]
L <- L[n_pair >= 2L]
L[, n_pair := NULL]

# stable beta name for ggplot
L[, beta := get(beta_col)]
L[, x_var := factor(x_var, levels = vars)]
L[, loc   := factor(loc, levels = sel_locs)]

g <- ggplot(L, aes(x = x, y = beta)) +
  geom_hline(yintercept = 0, linewidth = 0.3, colour = "grey30") +
  geom_point(size = 1.8, alpha = 0.7, colour = "grey40") +
  geom_smooth(
    method = "lm", se = FALSE, linewidth = 0.6,
    colour = "darkred", alpha = 0.15, fill = "darkred"
  ) +
  facet_grid(
    rows = vars(loc),
    cols = vars(x_var),
    scales = "free_x"
  ) +
  labs(
    x = NULL,
    y = expression(hat(beta)~"(carbon intensity)")
  ) +
  scale_y_continuous(labels = scales::label_number(accuracy = 0.01)) +
  theme_classic() +
  theme(
    axis.line        = element_blank(),
    plot.title       = element_blank(),
    plot.subtitle    = element_blank(),
    panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
    strip.text.x     = element_text(size = 10, face = "bold"),
    strip.text.y     = element_text(size = 10, face = "bold"),
    strip.background = element_rect(fill = "grey90", colour = "black", linewidth = 0.6),
    legend.position  = "none",
    panel.spacing.y  = unit(0.6, "lines"),
    panel.spacing.x  = unit(0.8, "lines")
  )

g


# test 2 end ----


#### (iii)
plot_country_scatter(dt = b_time_series_y_beta_country,
                     var_x = "capmf_std",
                     var_y = "beta_carbon_S1",
                     start_year = 2010,
                     end_year = 2024,
                     agg_fun = "median",
                     loc_col = "loc",
                     group_col = "region",
                     group_facet_toggle = T)

### (c) casestudies ----
#### (i) USA casestudy ----
c_time_series_beta_USA <- run_ghg_monthly_betas(
  df = df,                         
  ret_col     = "R_m",             
  ghg_measure = "intensity",             
  scope       = c("S1","S2"),  
  lag6m       = TRUE,              
  ghg_class   = "disclosed",       
  loc_filter_toggle = T,        
  loc_selection     = c("USA"),
  group_by_loc      = F,
  time_frame_toggle = T,        
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,  
  min_firms   = 50L,
  inv_pref_var = c(),
  fixed_effect = c("gind")
)

c_time_series_beta_USA <- run_ghg_monthly_window_betas(
  df = df,                         
  ret_col     = "R_m",             
  ghg_measure = "intensity",             
  scope       = c("S1","S2"),  
  lag6m       = TRUE,              
  ghg_class   = "disclosed",       
  loc_filter_toggle = T,        
  loc_selection     = c("USA"),
  group_by_loc      = F,
  time_frame_toggle = T,        
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,  
  min_firms   = 50L,
  inv_pref_var = c(),
  fixed_effect = c("gind"),
  window = 3
)

events <- c(
  "2007-12-01" = "2007 COP13",
  "2009-12-01" = "2009 COP15",
  "2012-12-01" = "2012 COP18",
  #"2014-07-01" = "Oil Price Drop Start",
  #"2015-01-31" = "Oil Price Drop Lowest Month",
  "2015-12-01" = "2015 COP21 Paris Agr.",
  "2016-11-01" = "Trump I elected",
  #"2016-12-07" = "Scott Pruitt EPA",
  "2017-06-01" = "Trump I quits Paris Agr.",
  "2019-09-01" = "Strikes f. Future",
  "2020-01-01" = "COVID outbreak",
  "2020-11-03" = "Biden elected",
  "2021-01-20" = "Biden rejoins Paris Agr.",
  "2021-10-01" = "2021 COP26",
  "2022-02-24" = "UKR war",
  "2023-10-07" = "Gaza war",
  "2023-12-01" = "2023 COP28",
  "2024-11-01" = "Trump II elected",
  "2025-01-25" = "Trump II quits Paris Agr."
)

election_periods_USA <- data.frame(
  start = as.Date(c(
    "2017-01-20",  # Trump I
    "2021-01-20",  # Biden
    "2025-01-20"   # Trump II (if shading forward)
  )),
  end = as.Date(c(
    "2021-01-20",  # Trump -> Biden
    "2025-01-20",  # Biden -> Trump II
    "2025-06-01"   # Trump II end (projected)
  )),
  label = c(
    "Trump I",
    "Biden",
    "Trump II"
  )
)

p1 <- plot_ghg_betas(events = events,
               ts_dt           = c_time_series_beta_USA,
               scope           = "S1",
               plot_all_locs   = F,
               loc_selection   = c("USA"),
               ci_mult         = 1.96,
               facet_ncol      = 1,
               facet_scales    = "free_y",
               plot_date_min   = as.Date("2010-01-01"),
               plot_date_max   = as.Date("2025-06-01"),
               zero_linewidth  = 0.9,
               ma_window = 12,
               election_periods = election_periods_USA
)
p1 <-  p1 + labs(title = NULL)

##### export to image * ----
ggsave(
  filename = "../07_word/outputs/USA_beta_timeseries_2010_2025.png",
  plot     = p1,
  #device   = "emf",
  width    = 12.5,
  height   = 6.5,
  units    = "in",
  dpi = 1000
)




#### (ii) Europe casestudy ----
c_time_series_beta_EU <- run_ghg_monthly_betas(
  df = df, 
  ret_col     = "R_m",
  ghg_measure = "intensity",
  scope       = c("S1"),
  lag6m       = TRUE,
  ghg_class   = "disclosed",
  loc_filter_toggle = T,
  loc_selection     = europe,
  group_by_loc      = F,
  time_frame_toggle = T,
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,
  min_firms   = 50L,
  inv_pref_var = c(),
  fixed_effect = c("gind","loc")
)

c_time_series_beta_EU <- run_ghg_monthly_window_betas(
  df = df, 
  ret_col     = "R_m",
  ghg_measure = "intensity",
  scope       = c("S1"),
  lag6m       = TRUE,
  ghg_class   = "disclosed",
  loc_filter_toggle = T,
  loc_selection     = europe,
  group_by_loc      = F,
  time_frame_toggle = T,
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,
  min_firms   = 50L,
  inv_pref_var = c(),
  fixed_effect = c("gind","loc"),
  window = 3
)

events <- c(
  "2007-12-01" = "2007 COP13",
  "2009-12-01" = "2009 COP15",
  "2012-12-01" = "2012 COP18",
  "2014-07-01" = "Oil Price Drop Start",
  "2015-01-31" = "Oil Price Drop Lowest Month",
  "2015-12-01" = "2015 COP21 Paris Agr.",
  "2016-11-01" = "Trump I elected",
  #"2016-12-07" = "Scott Pruitt EPA",
  "2017-06-01" = "Trump I quits Paris Agr.",
  "2019-09-01" = "Strikes f. Future",
  "2020-01-01" = "COVID outbreak",
  "2020-11-03" = "Biden elected",
  "2021-01-20" = "Biden rejoins Paris Agr.",
  "2021-10-01" = "2021 COP26",
  "2022-02-24" = "UKR war",
  "2023-10-07" = "Gaza war",
  "2023-12-01" = "2023 COP28",
  "2024-11-01" = "Trump II elected",
  "2025-01-25" = "Trump II quits Paris Agr."
)

election_periods_USA <- data.frame(
  start = as.Date(c(
    "2017-01-20",  # Trump I
    "2021-01-20",  # Biden
    "2025-01-20"   # Trump II (if shading forward)
  )),
  end = as.Date(c(
    "2021-01-20",  # Trump -> Biden
    "2025-01-20",  # Biden -> Trump II
    "2025-06-01"   # Trump II end (projected)
  )),
  label = c(
    "Trump I",
    "Biden",
    "Trump II"
  )
)

plot_ghg_betas(events = events,
               ts_dt           = c_time_series_beta_EU,
               scope           = "S1",
               plot_all_locs   = F,
               loc_selection   = c("USA"),
               ci_mult         = 1.96,
               facet_ncol      = 1,
               facet_scales    = "free_y",
               plot_date_min   = as.Date("2010-01-01"),
               plot_date_max   = as.Date("2025-06-01"),
               zero_linewidth  = 0.9,
               ma_window = 12,
               election_periods = election_periods_USA
)


#### (iii) ROW casestudy ----
c_time_series_beta_row <- run_ghg_monthly_window_betas(
  df = df, 
  ret_col     = "R_m",
  ghg_measure = "intensity",
  scope       = c("S1","S2"),
  lag6m       = TRUE,
  ghg_class   = "disclosed",
  loc_filter_toggle = T,
  loc_selection     = all_locs_minus_USA,
  group_by_loc      = F,
  time_frame_toggle = T,
  datadate_min      = as.Date("2010-01-01"),
  datadate_max      = as.Date("2025-01-01"),
  controls    = all_controls_lag6m,
  min_firms   = 50L,
  inv_pref_var = c(),
  fixed_effect = c("gind","loc"),
  window = 3
)

events <- c(
  "2007-12-01" = "2007 COP13",
  "2009-12-01" = "2009 COP15",
  "2012-12-01" = "2012 COP18",
  "2014-07-01" = "Oil Price Drop Start",
  "2015-01-31" = "Oil Price Drop Lowest Month",
  "2015-12-01" = "2015 COP21 Paris Agr.",
  "2016-11-01" = "Trump I elected",
  #"2016-12-07" = "Scott Pruitt EPA",
  "2017-06-01" = "Trump I quits Paris Agr.",
  "2019-09-01" = "Strikes f. Future",
  "2020-01-01" = "COVID outbreak",
  "2020-11-03" = "Biden elected",
  "2021-01-20" = "Biden rejoins Paris Agr.",
  "2021-10-01" = "2021 COP26",
  "2022-02-24" = "UKR war",
  "2023-10-07" = "Gaza war",
  "2023-12-01" = "2023 COP28",
  "2024-11-01" = "Trump II elected",
  "2025-01-25" = "Trump II quits Paris Agr."
)

election_periods_USA <- data.frame(
  start = as.Date(c(
    "2017-01-20",  # Trump I
    "2021-01-20",  # Biden
    "2025-01-20"   # Trump II (if shading forward)
  )),
  end = as.Date(c(
    "2021-01-20",  # Trump -> Biden
    "2025-01-20",  # Biden -> Trump II
    "2025-06-01"   # Trump II end (projected)
  )),
  label = c(
    "Trump I",
    "Biden",
    "Trump II"
  )
)

plot_ghg_betas(events = events,
               ts_dt           = c_time_series_beta_row,
               scope           = "S1",
               plot_all_locs   = F,
               loc_selection   = c("USA"),
               ci_mult         = 1.96,
               facet_ncol      = 1,
               facet_scales    = "free_y",
               plot_date_min   = as.Date("2010-01-01"),
               plot_date_max   = as.Date("2025-06-01"),
               zero_linewidth  = 0.9,
               ma_window = 12,
               election_periods = election_periods_USA
)

#### (iv) USA vs. ROW casestudy ----
plot_ghg_betas_stacked(events = events,text_align = "left",
                       title = "Monthly estimated carbon betas w. 12-month MA",
                       ts_dt_top       = c_time_series_beta_USA,
                       text_top = "Panel A: United States (w. industry-FEs)",
                       ts_dt_bottom    = c_time_series_beta_EU,
                       text_bottom = "Panel B: Europe (w. country- & industry-FEs)",
                       scope           = "S1",
                       plot_all_locs   = F,
                       loc_selection   = c("USA"),
                       ci_mult         = 1.96,
                       facet_ncol      = 1,
                       facet_scales    = "free_y",
                       plot_date_min   = as.Date("2010-01-01"),
                       plot_date_max   = as.Date("2025-06-01"),
                       zero_linewidth  = 0.9,
                       ma_window = 12,
                       election_periods = election_periods_USA,
                       show_bottom_event_labels = F,
                       x_label = "Year")


# Appendix ----
## old functions ----
run_ghg_yearly_betas <- function(
    df,
    ghg_measure        = c("log", "intensity"),
    scope              = c("S1", "S2", "S3"),
    lag6m              = FALSE,
    ghg_class          = c("full", "estimated", "disclosed"),
    loc_filter_toggle  = FALSE,
    loc_selection      = character(0),
    time_frame_toggle  = FALSE,
    datadate_min       = NULL,
    datadate_max       = NULL,
    ret_col            = "R_m",
    controls           = character(0),
    min_firms          = 30L,
    group_by_loc       = FALSE,
    inv_pref_var       = NULL,   # length 0, 1, or 2
    fixed_effect       = NULL
) {
  library(data.table)
  library(fixest)
  
  ghg_measure <- match.arg(ghg_measure)
  ghg_class   <- match.arg(ghg_class)
  scope       <- match.arg(scope, several.ok = TRUE)
  
  # --- normalize interaction variable ---
  if (is.null(inv_pref_var) || length(inv_pref_var) == 0) {
    inv_pref_var <- NULL
  } else {
    inv_pref_var <- inv_pref_var[nzchar(inv_pref_var)]
    if (length(inv_pref_var) == 0) {
      inv_pref_var <- NULL
    } else if (length(inv_pref_var) > 2) {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  # --- normalize fixed_effect argument ---
  if (is.null(fixed_effect) || length(fixed_effect) == 0) {
    fixed_effect <- NULL
  } else {
    fixed_effect <- fixed_effect[nzchar(fixed_effect)]
    if (length(fixed_effect) == 0) {
      fixed_effect <- NULL
    }
  }
  
  suf <- if (lag6m) "_lag6m" else ""
  
  df <- as.data.table(df)
  
  # 0) Ensure a yearly index exists
  if (!"year" %in% names(df)) {
    if ("datadate" %in% names(df)) {
      df[, year := as.integer(format(as.IDate(datadate), "%Y"))]
    } else if ("month" %in% names(df)) {
      df[, year := as.integer(format(as.IDate(month), "%Y"))]
    } else if ("ym" %in% names(df)) {
      df[, year := as.integer(substr(ym, 1, 4))]
    } else {
      stop("run_ghg_yearly_betas: need 'year' or one of ('datadate','month','ym') to construct year.")
    }
  }
  
  # 1) Location filter
  if (loc_filter_toggle) {
    df <- df[loc %in% loc_selection]
  }
  
  # 2) Date filter
  if (time_frame_toggle) {
    if (!is.null(datadate_min)) df <- df[datadate >= datadate_min]
    if (!is.null(datadate_max)) df <- df[datadate <= datadate_max]
  }
  
  # 3) Class columns
  class_cols <- list(
    S1 = paste0("ghg_scope1_class",     suf),
    S2 = paste0("ghg_scope2_loc_class", suf),
    S3 = paste0("ghg_scope3_up_class",  suf)
  )
  
  # 4) Scope variables
  if (ghg_measure == "log") {
    scope_vars <- list(
      S1 = paste0("log_ghg_scope1",     suf),
      S2 = paste0("log_ghg_scope2_loc", suf),
      S3 = paste0("log_ghg_scope3_up",  suf)
    )
  } else {
    scope_vars <- list(
      S1 = paste0("ghg_scope1_intensity",     suf),
      S2 = paste0("ghg_scope2_loc_intensity", suf),
      S3 = paste0("ghg_scope3_up_intensity",  suf)
    )
  }
  
  # --- helper to build interaction term (carbon * inv_pref_var) ---
  build_scope_term <- function(scope_var, inv_pref_var) {
    if (is.null(inv_pref_var)) {
      scope_var
    } else if (length(inv_pref_var) == 1) {
      paste0(scope_var, " * ", inv_pref_var[1])
    } else if (length(inv_pref_var) == 2) {
      paste0(scope_var, " * ", inv_pref_var[1], " * ", inv_pref_var[2])
    } else {
      stop("inv_pref_var must have length 0, 1, or 2.")
    }
  }
  
  # 5) Helper to run CS regressions for one scope
  run_scope <- function(scope_label) {
    scope_var <- scope_vars[[scope_label]]
    class_col <- class_cols[[scope_label]]
    
    if (!scope_var %in% names(df)) {
      warning("Scope variable not found in df: ", scope_var, ". Skipping scope ", scope_label, ".")
      return(NULL)
    }
    
    df_scope <- copy(df)
    
    # Filter by ghg_class if requested
    if (ghg_class != "full" && class_col %in% names(df_scope)) {
      df_scope <- df_scope[get(class_col) == ghg_class]
    }
    
    # Keep only rows where key columns exist (carbon + return)
    df_scope <- df_scope[!is.na(get(scope_var)) & !is.na(get(ret_col))]
    
    if (nrow(df_scope) == 0L) {
      warning("No observations left for scope ", scope_label, " after filtering.")
      return(NULL)
    }
    
    # --- build RHS: (scope_var [+ interactions]) + controls ---
    scope_term <- build_scope_term(scope_var, inv_pref_var)
    rhs_terms  <- c(scope_term, controls)
    rhs_str    <- paste(rhs_terms, collapse = " + ")
    
    # --- build formula with or without fixed effects ---
    if (is.null(fixed_effect)) {
      fml <- as.formula(paste(ret_col, "~", rhs_str))
    } else {
      fe_str <- paste(fixed_effect, collapse = " + ")
      fml <- as.formula(paste0(ret_col, " ~ ", rhs_str, " | ", fe_str))
    }
    
    # Grouping: year or (loc, year)
    by_cols <- if (group_by_loc) c("loc", "year") else "year"
    
    # Run cross-sectional regressions by group
    res <- df_scope[
      ,
      {
        # Cheap early exit: not enough rows even before fixest drops NAs
        if (.N < min_firms) {
          list(beta_carbon = NA_real_, se_carbon = NA_real_, n_firms = .N)
        } else {
          out <- tryCatch(
            {
              mod <- feols(fml, data = .SD)
              
              # FIX: enforce min_firms on the *estimation sample* size
              if (nobs(mod) < min_firms) {
                return(list(beta_carbon = NA_real_, se_carbon = NA_real_, n_firms = nobs(mod)))
              }
              
              cf <- summary(mod)$coeftable
              
              if (!scope_var %in% rownames(cf)) {
                list(beta_carbon = NA_real_, se_carbon = NA_real_, n_firms = nobs(mod))
              } else {
                list(
                  beta_carbon = cf[scope_var, "Estimate"],
                  se_carbon   = cf[scope_var, "Std. Error"],
                  n_firms     = nobs(mod)
                )
              }
            },
            error = function(e) {
              list(beta_carbon = NA_real_, se_carbon = NA_real_, n_firms = .N)
            }
          )
          out
        }
      },
      by = by_cols
    ]
    
    res[, scope := scope_label]
    res[]
  }
  
  # 6) Run for selected scopes
  res_list <- lapply(scope, run_scope)
  names(res_list) <- scope
  res_list <- res_list[!vapply(res_list, is.null, logical(1L))]
  
  if (length(res_list) == 0L) {
    stop("run_ghg_yearly_betas: no valid scope results produced.")
  }
  
  series_long <- rbindlist(res_list, use.names = TRUE, fill = TRUE)
  
  if (group_by_loc) {
    setorder(series_long, loc, scope, year)
  } else {
    setorder(series_long, scope, year)
  }
  
  # 7) Wide output via dcast (including n_firms -> n_*)
  if (group_by_loc) {
    series_wide <- dcast(
      series_long,
      loc + year ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms")
    )
    setorder(series_wide, loc, year)
  } else {
    series_wide <- dcast(
      series_long,
      year ~ scope,
      value.var = c("beta_carbon", "se_carbon", "n_firms")
    )
    setorder(series_wide, year)
  }
  
  # Rename n_firms_* columns to n_* for clarity
  n_cols <- grep("^n_firms_", names(series_wide), value = TRUE)
  if (length(n_cols) > 0L) {
    setnames(series_wide, n_cols, sub("^n_firms_", "n_", n_cols))
  }
  
  series_wide[]
}


plot_country_scatter <- function(dt,
                                 var_x,
                                 var_y,
                                 start_year,
                                 end_year,
                                 agg_fun    = c("median", "mean", "full"),
                                 loc_col    = "loc",
                                 group_col  = NULL,
                                 highlight_iso3 = NULL,
                                 group_facet_toggle = FALSE) {
  
  library(data.table)
  library(ggplot2)
  library(ggrepel)
  
  agg_fun <- match.arg(agg_fun)
  dt <- as.data.table(dt)
  
  make_label <- function(x) {
    x <- gsub("_", " ", x)
    x <- gsub("^log ", "Log ", x)
    x
  }
  x_lab <- make_label(var_x)
  y_lab <- make_label(var_y)
  
  if (!isFALSE(group_facet_toggle) && is.null(group_col)) {
    warning("group_facet_toggle is TRUE but group_col is NULL; faceting skipped.")
    group_facet_toggle <- FALSE
  }
  
  cols_needed <- c("year", loc_col, var_x, var_y, group_col)
  cols_needed <- cols_needed[!is.na(cols_needed)]
  missing_cols <- setdiff(cols_needed, names(dt))
  if (length(missing_cols) > 0L) {
    stop("Missing columns: ", paste(missing_cols, collapse = ", "))
  }
  
  dt_sub <- dt[
    year >= start_year & year <= end_year,
    ..cols_needed
  ]
  
  setnames(dt_sub, old = loc_col, new = "loc")
  setnames(dt_sub, old = var_x,   new = "x")
  setnames(dt_sub, old = var_y,   new = "y")
  if (!is.null(group_col)) setnames(dt_sub, old = group_col, new = "group")
  
  if (agg_fun %in% c("median", "mean")) {
    fun <- if (agg_fun == "median") median else mean
    
    if (!is.null(group_col)) {
      dt_plot <- dt_sub[
        ,
        .(
          x = fun(x, na.rm = TRUE),
          y = fun(y, na.rm = TRUE)
        ),
        by = .(loc, group)
      ]
    } else {
      dt_plot <- dt_sub[
        ,
        .(
          x = fun(x, na.rm = TRUE),
          y = fun(y, na.rm = TRUE)
        ),
        by = loc
      ]
    }
  } else {
    dt_plot <- dt_sub
  }
  
  dt_plot <- dt_plot[!is.na(x) & !is.na(y)]
  
  if (!is.null(highlight_iso3)) {
    dt_plot[, highlighted := loc %in% highlight_iso3]
  } else {
    dt_plot[, highlighted := FALSE]
  }
  
  dt_plot[, label_alpha := ifelse(highlighted, 1, 0.6)]
  dt_plot[, label_font  := ifelse(highlighted, "bold", "plain")]
  
  group_colors <- c(
    "Europe"                = "#4E79A7",
    "Americas"              = "#F28E2B",
    "Asia-Pacific"          = "#E15759",
    "Middle East & Africa"  = "#76B7B2",
    "Offshore"              = "#59A14F",
    "Other"                 = "grey60"
  )
  
  p <- ggplot(dt_plot, aes(x = x, y = y)) +
    {
      if (!is.null(group_col)) {
        geom_point(aes(color = group), size = 2.3, alpha = 0.8)
      } else {
        geom_point(size = 2.3, alpha = 0.6, color = "grey60")
      }
    } +
    {
      if (!is.null(group_col)) {
        scale_color_manual(values = group_colors)
      }
    } +
    geom_smooth(method = "lm", se = FALSE, linewidth = 0.6, color = "darkred", alpha = 0.7) +
    ggrepel::geom_text_repel(
      aes(label = loc, alpha = label_alpha, fontface = label_font),
      size               = 3,
      box.padding        = 0.15,
      point.padding      = 0.05,
      segment.size       = 0.25,
      min.segment.length = 0,
      show.legend        = FALSE
    ) +
    scale_alpha_identity() +
    theme_classic() +
    theme(
      axis.line        = element_blank(),
      panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6),
      strip.background = element_rect(color = "black", fill = "grey90"),
      legend.position  = "top"
    ) +
    labs(
      x = x_lab,
      y = y_lab,
      title = paste0(
        "Scatterplot of ", y_lab, " vs ", x_lab,
        " (", start_year, "–", end_year, ", ", agg_fun, ")"
      ),
      color = if (!is.null(group_col)) "Group" else NULL
    )
  
  if (isTRUE(group_facet_toggle)) {
    p <- p +
      facet_wrap(~group) +
      guides(color = "none")
  }
  
  p
}
