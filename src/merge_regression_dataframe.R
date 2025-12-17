library(dplyr)
library(tidyr)
library(arrow)
library(stringdist)
library(stringr)
library(config)

if (!requireNamespace("nstandr", quietly = TRUE)) {
  if (!requireNamespace("devtools", quietly = TRUE)) install.packages("devtools")
  devtools::install_github("stasvlasov/nstandr")
}
library(nstandr)

# ----------------------------------------
# Hybrid Matching Functions
# ----------------------------------------

compute_match_score <- function(name1, name2) {
  if (nchar(name1) == 0 || nchar(name2) == 0) return(0)

  jw_score <- 1 - stringdist(name1, name2, method = "jw", p = 0.1)

  lv_dist <- stringdist(name1, name2, method = "lv")
  max_len <- max(nchar(name1), nchar(name2))
  lv_score <- 1 - (lv_dist / max_len)

  tokens1 <- unlist(str_split(tolower(name1), "\\s+"))
  tokens2 <- unlist(str_split(tolower(name2), "\\s+"))
  tokens1 <- tokens1[tokens1 != ""]
  tokens2 <- tokens2[tokens2 != ""]

  if (length(tokens1) == 0 || length(tokens2) == 0) {
    jaccard_score <- 0
  } else {
    jaccard_score <- length(intersect(tokens1, tokens2)) /
                     length(union(tokens1, tokens2))
  }

  final_score <- 0.5 * jw_score + 0.3 * lv_score + 0.2 * jaccard_score
  return(final_score)
}

find_best_match_hybrid <- function(name, candidates_df, name_col, threshold = 0.85) {
  prefix <- substr(tolower(name), 1, 3)

  blocked_candidates <- candidates_df %>%
    filter(prefix_3 == prefix) %>%
    pull(!!sym(name_col))

  if (length(blocked_candidates) == 0) {
    prefix2 <- substr(tolower(name), 1, 2)
    blocked_candidates <- candidates_df %>%
      filter(prefix_2 == prefix2) %>%
      pull(!!sym(name_col))
  }

  if (length(blocked_candidates) == 0) {
    return(list(match = NA_character_, score = NA_real_))
  }

  scores <- sapply(blocked_candidates, function(c) compute_match_score(name, c))
  best_idx <- which.max(scores)

  if (scores[best_idx] >= threshold) {
    return(list(match = blocked_candidates[best_idx], score = scores[best_idx]))
  }

  return(list(match = NA_character_, score = NA_real_))
}

# ----------------------------------------
# Load Configuration
# ----------------------------------------
cfg <- config::get()

# ----------------------------------------
# 1. Load Data Sources
# ----------------------------------------
message("Loading datasets...")

patent_data <- read_parquet(cfg$patents_processed_file)
sdc_data <- read.csv(cfg$sdc_filtered_file, stringsAsFactors = FALSE)
orbis_data <- read.csv(cfg$orbis_cleaned_file, stringsAsFactors = FALSE)

message("Patent data rows: ", nrow(patent_data))
message("SDC data rows: ", nrow(sdc_data))
message("Orbis data rows: ", nrow(orbis_data))

# ============================================
# SDC + ORBIS MERGE (Direct join on company names)
# ============================================
# Note: ORBIS company_name has been pre-renamed to match SDC format in orbis_data.R
message("\n========================================")
message("    SDC + ORBIS MERGE (DIRECT JOIN)")
message("========================================")

# Store original names before any standardization
sdc_data <- sdc_data %>%
  mutate(participants_original = participants)

# Select ORBIS columns to merge
orbis_merge_cols <- c(
  "company_name", "country_iso", "city", "nace_code", "entity_type",
  "employees_last_value", "total_assets_eur", "total_assets_usd",
  "n_companies_group", "n_shareholders", "n_subsidiaries",
  "roa", "rd_ratio", "export_ratio", "export_revenue",
  "long_term_debt", "is_guo", "is_controlled_sub", "size_classification"
)

orbis_for_merge <- orbis_data %>%
  select(any_of(orbis_merge_cols))

# Direct join: SDC participants -> ORBIS company_name
merged_data <- sdc_data %>%
  left_join(
    orbis_for_merge,
    by = c("participants_original" = "company_name")
  ) %>%
  mutate(orbis_match_type = ifelse(!is.na(country_iso) | !is.na(employees_last_value) | !is.na(total_assets_eur),
                                     "matched", "unmatched"))

orbis_matched <- sum(merged_data$orbis_match_type == "matched")
message("Successfully matched with ORBIS: ", orbis_matched, " / ", nrow(merged_data),
        " (", round(orbis_matched / nrow(merged_data) * 100, 1), "%)")

# ----------------------------------------
# 2. Standardize Company Names (AFTER MERGE)
# ----------------------------------------
message("\nStandardizing company names...")

patent_data <- patent_data %>%
  mutate(
    assignee_original = assignee,
    assignee_std = assignee %>% trimws() %>% standardize_magerman(),
    prefix_3 = substr(tolower(assignee_std), 1, 3),
    prefix_2 = substr(tolower(assignee_std), 1, 2)
  )

merged_data <- merged_data %>%
  mutate(
    participants_std = participants_original %>% trimws() %>% standardize_magerman()
  )


# ============================================
# PATENT MATCHING
# ============================================
message("\n========================================")
message("         PATENT MATCHING")
message("========================================")

# ----------------------------------------
# Phase 1: Exact Patent Matching
# ----------------------------------------
message("\n--- Phase 1: Exact Matching ---")

merged_data <- merged_data %>%
  left_join(
    patent_data %>% select(-assignee_original, -prefix_3, -prefix_2),
    by = c("participants_std" = "assignee_std")
  ) %>%
  mutate(patent_match_type = ifelse(!is.na(assignee), "exact", NA_character_))

exact_patent_matches <- sum(!is.na(merged_data$assignee))
message("Exact matches: ", exact_patent_matches, " / ", nrow(merged_data),
        " (", round(exact_patent_matches / nrow(merged_data) * 100, 1), "%)")

# ----------------------------------------
# Phase 2: Hybrid Fuzzy Patent Matching
# ----------------------------------------
message("\n--- Phase 2: Hybrid Fuzzy Matching ---")

# Get unique unmatched company names to avoid redundant matching
unmatched_companies <- merged_data %>%
  filter(is.na(assignee)) %>%
  distinct(participants_std) %>%
  pull(participants_std)

message("Unique companies without exact match: ", length(unmatched_companies))

if (length(unmatched_companies) > 0) {
  message("Running hybrid matching (Jaro-Winkler + Levenshtein + Jaccard)...")

  total_unmatched <- length(unmatched_companies)
  pb_interval <- max(1, floor(total_unmatched / 10))

  fuzzy_results <- vector("list", total_unmatched)

  for (i in seq_along(unmatched_companies)) {
    name <- unmatched_companies[i]
    fuzzy_results[[i]] <- find_best_match_hybrid(name, patent_data, "assignee_std", threshold = 0.85)

    if (i %% pb_interval == 0) {
      message("  Progress: ", round(i / total_unmatched * 100), "%")
    }
  }

  # Create lookup table for fuzzy matches
  fuzzy_lookup <- data.frame(
    participants_std = unmatched_companies,
    fuzzy_match_std = sapply(fuzzy_results, function(x) x$match),
    stringsAsFactors = FALSE
  )

  fuzzy_patent_success <- sum(!is.na(fuzzy_lookup$fuzzy_match_std))
  message("\nHybrid matches found: ", fuzzy_patent_success, " / ", length(unmatched_companies))

  if (fuzzy_patent_success > 0) {
    valid_scores <- sapply(fuzzy_results, function(x) x$score)
    valid_scores <- valid_scores[!is.na(valid_scores)]
    message("  Score range: ", round(min(valid_scores), 3), " - ", round(max(valid_scores), 3))
    message("  Mean score: ", round(mean(valid_scores), 3))

    # Join fuzzy matches back to merged_data
    fuzzy_lookup <- fuzzy_lookup %>%
      filter(!is.na(fuzzy_match_std)) %>%
      left_join(
        patent_data %>% select(-assignee_original, -prefix_3, -prefix_2),
        by = c("fuzzy_match_std" = "assignee_std")
      )

    # Update merged_data with fuzzy matches
    for (i in seq_len(nrow(fuzzy_lookup))) {
      match_rows <- which(merged_data$participants_std == fuzzy_lookup$participants_std[i] &
                          is.na(merged_data$assignee))
      if (length(match_rows) > 0) {
        merged_data$assignee[match_rows] <- fuzzy_lookup$assignee[i]
        merged_data$total_applications[match_rows] <- fuzzy_lookup$total_applications[i]
        merged_data$granted_patents[match_rows] <- fuzzy_lookup$granted_patents[i]
        merged_data$avg_ipc_scope[match_rows] <- fuzzy_lookup$avg_ipc_scope[i]
        merged_data$avg_team_size[match_rows] <- fuzzy_lookup$avg_team_size[i]
        merged_data$grant_success_rate[match_rows] <- fuzzy_lookup$grant_success_rate[i]
        merged_data$total_ipc_mentions[match_rows] <- fuzzy_lookup$total_ipc_mentions[i]
        merged_data$tech_specialist_hhi[match_rows] <- fuzzy_lookup$tech_specialist_hhi[i]
        merged_data$patent_match_type[match_rows] <- "fuzzy"
      }
    }
  }
} else {
  fuzzy_patent_success <- 0
}

# Fill patent NAs with zeros
merged_data <- merged_data %>%
  mutate(
    patent_match_type = replace_na(patent_match_type, "unmatched"),
    total_applications = replace_na(total_applications, 0),
    granted_patents = replace_na(granted_patents, 0),
    avg_ipc_scope = replace_na(avg_ipc_scope, 0),
    avg_team_size = replace_na(avg_team_size, 0),
    grant_success_rate = replace_na(grant_success_rate, 0),
    total_ipc_mentions = replace_na(total_ipc_mentions, 0),
    tech_specialist_hhi = replace_na(tech_specialist_hhi, 0)
  )

# ============================================
# FINAL SUMMARY
# ============================================
total_patent_matched <- sum(merged_data$total_applications > 0)
total_orbis_matched <- sum(merged_data$orbis_match_type == "mapped")
unique_companies <- n_distinct(merged_data$participants_std)
unique_deals <- n_distinct(merged_data$deal_number)

message("\n========================================")
message("         FINAL MERGE SUMMARY")
message("========================================")
message("Total rows:             ", nrow(merged_data))
message("Unique companies:       ", unique_companies)
message("Unique deals:           ", unique_deals)
message("----------------------------------------")
message("SDC + ORBIS (via mapping):")
message("  Matched rows:         ", total_orbis_matched,
        " (", round(total_orbis_matched / nrow(merged_data) * 100, 1), "%)")
message("  Unmatched rows:       ", nrow(merged_data) - total_orbis_matched,
        " (", round((nrow(merged_data) - total_orbis_matched) / nrow(merged_data) * 100, 1), "%)")
message("----------------------------------------")
message("PATENT MATCHING:")
message("  Exact matches:        ", exact_patent_matches,
        " (", round(exact_patent_matches / nrow(merged_data) * 100, 1), "%)")
message("  Fuzzy matches:        ", sum(merged_data$patent_match_type == "fuzzy"),
        " (", round(sum(merged_data$patent_match_type == "fuzzy") / nrow(merged_data) * 100, 1), "%)")
message("  Total matched:        ", total_patent_matched,
        " (", round(total_patent_matched / nrow(merged_data) * 100, 1), "%)")
message("========================================")

message("\nOrbis match type distribution:")
print(table(merged_data$orbis_match_type))

message("\nPatent match type distribution:")
print(table(merged_data$patent_match_type))

# ----------------------------------------
# Save Output
# ----------------------------------------
write_parquet(merged_data, cfg$merged_data_parquet, compression = "gzip")
write.csv(merged_data, cfg$merged_data_csv, row.names = FALSE)
write_parquet(merged_data, cfg$merged_data_parquet, compression = "gzip")

message("\nSaved CSV to: ", cfg$merged_data_csv)
message("Saved Parquet to: ", cfg$merged_data_parquet)
message("Done.")