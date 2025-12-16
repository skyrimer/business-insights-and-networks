
library(dplyr)
library(readxl)
library(config)

# We use nstandr for name standardization (Magerman method)
if (!requireNamespace("nstandr", quietly = TRUE)) {
  if (!requireNamespace("devtools", quietly = TRUE)) install.packages("devtools")
  devtools::install_github("stasvlasov/nstandr")
}
library(nstandr)

# ----------------------------------------
# Helper Functions
# ----------------------------------------

# Normalize messy Excel headers
normalize_orbis_name <- function(x) {
  x <- gsub("\\s+", " ", x)
  x <- trimws(x)
  x <- tolower(x)
  x <- gsub("[^a-z0-9]", "_", x)
  gsub("_+", "_", x)
}

# Ensure no empty/NA column names
ensure_orbis_headers <- function(df) {
  nm <- names(df)
  empty <- nm == "" | is.na(nm)
  if (any(empty)) {
    nm[empty] <- paste0("unnamed_col_", seq_len(sum(empty)))
    nm <- make.unique(nm, sep = "_")
    names(df) <- nm
  }
  df
}

# Apply column name mapping
apply_orbis_col_map <- function(df, mapping) {
  normalized_names <- normalize_orbis_name(names(df))
  for (target in names(mapping)) {
    source <- mapping[[target]]
    idx <- match(source, normalized_names)
    if (!is.na(idx)) {
      names(df)[idx] <- target
    }
  }
  df
}

# ----------------------------------------
# Configuration
# ----------------------------------------
orbis_col_map <- c(
  company_name = "company_name_latin_alphabet",
  country_iso = "country_iso_code",
  city = "city_latin_alphabet",
  nace_code = "nace_rev_2_core_code_4_digits",
  consolidation_code = "consolidation_code",
  last_avail_year = "last_avail_year",
  country = "country",
  entity_type = "entity_type",
  employees_last_value = "number_of_employees_last_avail_value",
  current_assets_usd = "current_assets_th_usd_last_avail_yr",
  n_companies_group = "no_of_companies_in_corporate_group",
  n_shareholders = "no_of_shareholders",
  n_subsidiaries = "no_of_subsidiaries",
  roa_net_income = "roa_using_net_income_last_avail_yr",
  total_assets_eur = "total_assets_th_eur_last_avail_yr",
  rnd_over_operating = "r_d_expenses_operating_revenue_last_avail_yr",
  export_over_operating = "export_revenue_operating_revenue_last_avail_yr",
  long_term_debt_eur = "long_term_debt_th_eur_last_avail_yr"
)

orbis_numeric_cols <- c(
  "employees_last_value",
  "current_assets_usd",
  "n_companies_group",
  "n_shareholders",
  "n_subsidiaries",
  "roa_net_income",
  "total_assets_eur",
  "rnd_over_operating",
  "export_over_operating",
  "long_term_debt_eur"
)

# ----------------------------------------
# Load Configuration
# ----------------------------------------
cfg <- config::get()

# ----------------------------------------
# Load and Clean Orbis Data
# ----------------------------------------
message("Loading Orbis data...")

orbis_data <- read_excel(
  cfg$orbis_raw_file,
  sheet = "Results",
  .name_repair = "minimal"
) %>%
  ensure_orbis_headers() %>%
  rename_with(normalize_orbis_name) %>%
  select(-matches("^unnamed"), -matches("_[0-9]+$")) %>%
  { apply_orbis_col_map(., orbis_col_map) } %>%
  # Convert "n.a." to NA
  mutate(across(where(is.character), ~ na_if(.x, "n.a."))) %>%
  # Convert numeric columns
  mutate(across(any_of(orbis_numeric_cols), ~ as.numeric(.x))) %>%
  # Create entity type features
  mutate(
    entity_type = factor(entity_type),
    is_guo = as.integer(entity_type == "GUO")
  )

message("Orbis companies loaded (before imputation): ", nrow(orbis_data))

# ----------------------------------------
# Report Missing Values Before Imputation
# ----------------------------------------
message("\n--- Missing Values Before Imputation ---")

missing_report <- data.frame(
  variable = orbis_numeric_cols,
  n_missing = sapply(orbis_numeric_cols, function(col) sum(is.na(orbis_data[[col]]))),
  pct_missing = sapply(orbis_numeric_cols, function(col) {
    round(sum(is.na(orbis_data[[col]])) / nrow(orbis_data) * 100, 1)
  })
)

print(missing_report)

# ----------------------------------------
# Median Imputation
# ----------------------------------------
message("\n--- Imputing Missing Values with Median ---")

# Store original values for tracking
orbis_data <- orbis_data %>%
  mutate(
    employees_imputed = is.na(employees_last_value),
    assets_imputed = is.na(total_assets_eur)
  )

# Compute medians (before imputation)
medians <- sapply(orbis_numeric_cols, function(col) {
  median(orbis_data[[col]], na.rm = TRUE)
})

message("\nMedian values used for imputation:")
for (i in seq_along(orbis_numeric_cols)) {
  message("  ", orbis_numeric_cols[i], ": ", round(medians[i], 2))
}

# Apply median imputation to all numeric columns
for (col in orbis_numeric_cols) {
  med_val <- median(orbis_data[[col]], na.rm = TRUE)
  n_imputed <- sum(is.na(orbis_data[[col]]))

  if (n_imputed > 0 && !is.na(med_val)) {
    orbis_data[[col]] <- ifelse(is.na(orbis_data[[col]]), med_val, orbis_data[[col]])
    message("  Imputed ", n_imputed, " values in ", col)
  }
}

# ----------------------------------------
# Report After Imputation
# ----------------------------------------
message("\n--- Missing Values After Imputation ---")

missing_after <- data.frame(
  variable = orbis_numeric_cols,
  n_missing = sapply(orbis_numeric_cols, function(col) sum(is.na(orbis_data[[col]])))
)

print(missing_after)

# Summary of imputation
message("\n--- Imputation Summary ---")
message("Rows with imputed employees: ", sum(orbis_data$employees_imputed))
message("Rows with imputed assets: ", sum(orbis_data$assets_imputed))
message("Total rows retained: ", nrow(orbis_data))

# ----------------------------------------
# Standardize Company Names
# ----------------------------------------
orbis_data$company_name_std <- orbis_data$company_name %>%
  trimws() %>%
  standardize_magerman()

# ----------------------------------------
# Save Output
# ----------------------------------------
write.csv(orbis_data, cfg$orbis_cleaned_file, row.names = FALSE)
message("\nSaved to ", cfg$orbis_cleaned_file)
message("Done.")