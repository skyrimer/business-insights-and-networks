
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

# ----------------------------------------
# 2. Standardize Company Names
# ----------------------------------------
message("\nStandardizing company names...")

patent_data <- patent_data %>%
  mutate(
    assignee_original = assignee,
    assignee_std = assignee %>% trimws() %>% standardize_magerman(),
    prefix_3 = substr(tolower(assignee_std), 1, 3),
    prefix_2 = substr(tolower(assignee_std), 1, 2)
  )

sdc_data <- sdc_data %>%
  mutate(
    participants_original = participants,
    participants_std = participants %>% trimws() %>% standardize_magerman()
  )

orbis_data <- orbis_data %>%
  mutate(
    company_name_std = company_name_std %>% trimws(),
    prefix_3 = substr(tolower(company_name_std), 1, 3),
    prefix_2 = substr(tolower(company_name_std), 1, 2)
  )

# ----------------------------------------
# 3. Create Company-Level SDC Summary
# ----------------------------------------
sdc_companies <- sdc_data %>%
  group_by(participants_std) %>%
  summarise(
    company_name = first(participants_original),
    n_deals = n_distinct(deal_number),
    countries = paste(unique(participant_nation), collapse = "; "),
    primary_sic = first(sic_primary),
    first_deal_date = min(date_announced, na.rm = TRUE),
    last_deal_date = max(date_announced, na.rm = TRUE),
    .groups = "drop"
  )

message("Unique companies in SDC: ", nrow(sdc_companies))

# ============================================
# PATENT MATCHING
# ============================================
message("\n========================================")
message("         PATENT MATCHING")
message("========================================")

# ----------------------------------------
# 4. Phase 1: Exact Patent Matching
# ----------------------------------------
message("\n--- Phase 1: Exact Matching ---")

merged_data <- sdc_companies %>%
  left_join(
    patent_data %>% select(-assignee_original, -prefix_3, -prefix_2),
    by = c("participants_std" = "assignee_std")
  ) %>%
  mutate(patent_match_type = ifelse(!is.na(assignee), "exact", NA_character_))

exact_patent_matches <- sum(!is.na(merged_data$assignee))
message("Exact matches: ", exact_patent_matches, " / ", nrow(merged_data),
        " (", round(exact_patent_matches / nrow(merged_data) * 100, 1), "%)")

# ----------------------------------------
# 5. Phase 2: Hybrid Fuzzy Patent Matching
# ----------------------------------------
message("\n--- Phase 2: Hybrid Fuzzy Matching ---")

unmatched_idx <- which(is.na(merged_data$assignee))
message("Companies without exact match: ", length(unmatched_idx))

if (length(unmatched_idx) > 0) {
  message("Running hybrid matching (Jaro-Winkler + Levenshtein + Jaccard)...")

  total_unmatched <- length(unmatched_idx)
  pb_interval <- max(1, floor(total_unmatched / 10))

  fuzzy_results <- vector("list", total_unmatched)

  for (i in seq_along(unmatched_idx)) {
    name <- merged_data$participants_std[unmatched_idx[i]]
    fuzzy_results[[i]] <- find_best_match_hybrid(name, patent_data, "assignee_std", threshold = 0.85)

    if (i %% pb_interval == 0) {
      message("  Progress: ", round(i / total_unmatched * 100), "%")
    }
  }

  fuzzy_matches_std <- sapply(fuzzy_results, function(x) x$match)
  fuzzy_scores <- sapply(fuzzy_results, function(x) x$score)

  fuzzy_patent_success <- sum(!is.na(fuzzy_matches_std))
  message("\nHybrid matches found: ", fuzzy_patent_success, " / ", length(unmatched_idx))

  if (fuzzy_patent_success > 0) {
    valid_scores <- fuzzy_scores[!is.na(fuzzy_scores)]
    message("  Score range: ", round(min(valid_scores), 3), " - ", round(max(valid_scores), 3))
    message("  Mean score: ", round(mean(valid_scores), 3))

    for (i in seq_along(unmatched_idx)) {
      if (!is.na(fuzzy_matches_std[i])) {
        row_idx <- unmatched_idx[i]
        patent_row <- patent_data %>%
          filter(assignee_std == fuzzy_matches_std[i]) %>%
          slice(1)

        if (nrow(patent_row) > 0) {
          merged_data$assignee[row_idx] <- patent_row$assignee
          merged_data$total_applications[row_idx] <- patent_row$total_applications
          merged_data$granted_patents[row_idx] <- patent_row$granted_patents
          merged_data$avg_ipc_scope[row_idx] <- patent_row$avg_ipc_scope
          merged_data$avg_team_size[row_idx] <- patent_row$avg_team_size
          merged_data$grant_success_rate[row_idx] <- patent_row$grant_success_rate
          merged_data$total_ipc_mentions[row_idx] <- patent_row$total_ipc_mentions
          merged_data$tech_specialist_hhi[row_idx] <- patent_row$tech_specialist_hhi
          merged_data$patent_match_type[row_idx] <- "fuzzy"
        }
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
# ORBIS MATCHING
# ============================================
message("\n========================================")
message("         ORBIS MATCHING")
message("========================================")

# Orbis columns to merge
orbis_merge_cols <- c(
  "company_name_std", "country_iso", "city", "nace_code", "entity_type",
  "employees_last_value", "current_assets_usd", "n_companies_group",
  "n_shareholders", "n_subsidiaries", "roa_net_income", "total_assets_eur",
  "rnd_over_operating", "export_over_operating", "long_term_debt_eur", "is_guo"
)

orbis_for_merge <- orbis_data %>%
  select(any_of(c(orbis_merge_cols, "prefix_3", "prefix_2")))

# ----------------------------------------
# 6. Phase 1: Exact Orbis Matching
# ----------------------------------------
message("\n--- Phase 1: Exact Matching ---")

merged_data <- merged_data %>%
  left_join(
    orbis_for_merge %>% select(-prefix_3, -prefix_2),
    by = c("participants_std" = "company_name_std")
  ) %>%
  mutate(orbis_match_type = ifelse(!is.na(employees_last_value), "exact", NA_character_))

exact_orbis_matches <- sum(!is.na(merged_data$employees_last_value))
message("Exact matches: ", exact_orbis_matches, " / ", nrow(merged_data),
        " (", round(exact_orbis_matches / nrow(merged_data) * 100, 1), "%)")

# ----------------------------------------
# 7. Phase 2: Hybrid Fuzzy Orbis Matching
# ----------------------------------------
message("\n--- Phase 2: Hybrid Fuzzy Matching ---")

unmatched_orbis_idx <- which(is.na(merged_data$employees_last_value))
message("Companies without exact Orbis match: ", length(unmatched_orbis_idx))

if (length(unmatched_orbis_idx) > 0) {
  message("Running hybrid matching for Orbis...")

  total_unmatched_orbis <- length(unmatched_orbis_idx)
  pb_interval_orbis <- max(1, floor(total_unmatched_orbis / 10))

  fuzzy_orbis_results <- vector("list", total_unmatched_orbis)

  for (i in seq_along(unmatched_orbis_idx)) {
    name <- merged_data$participants_std[unmatched_orbis_idx[i]]
    fuzzy_orbis_results[[i]] <- find_best_match_hybrid(name, orbis_for_merge, "company_name_std", threshold = 0.85)

    if (i %% pb_interval_orbis == 0) {
      message("  Progress: ", round(i / total_unmatched_orbis * 100), "%")
    }
  }

  fuzzy_orbis_matches_std <- sapply(fuzzy_orbis_results, function(x) x$match)
  fuzzy_orbis_scores <- sapply(fuzzy_orbis_results, function(x) x$score)

  fuzzy_orbis_success <- sum(!is.na(fuzzy_orbis_matches_std))
  message("\nHybrid matches found: ", fuzzy_orbis_success, " / ", length(unmatched_orbis_idx))

  if (fuzzy_orbis_success > 0) {
    valid_orbis_scores <- fuzzy_orbis_scores[!is.na(fuzzy_orbis_scores)]
    message("  Score range: ", round(min(valid_orbis_scores), 3), " - ", round(max(valid_orbis_scores), 3))
    message("  Mean score: ", round(mean(valid_orbis_scores), 3))

    for (i in seq_along(unmatched_orbis_idx)) {
      if (!is.na(fuzzy_orbis_matches_std[i])) {
        row_idx <- unmatched_orbis_idx[i]
        orbis_row <- orbis_data %>%
          filter(company_name_std == fuzzy_orbis_matches_std[i]) %>%
          slice(1)

        if (nrow(orbis_row) > 0) {
          merged_data$country_iso[row_idx] <- orbis_row$country_iso
          merged_data$city[row_idx] <- orbis_row$city
          merged_data$nace_code[row_idx] <- orbis_row$nace_code
          merged_data$entity_type[row_idx] <- orbis_row$entity_type
          merged_data$employees_last_value[row_idx] <- orbis_row$employees_last_value
          merged_data$current_assets_usd[row_idx] <- orbis_row$current_assets_usd
          merged_data$n_companies_group[row_idx] <- orbis_row$n_companies_group
          merged_data$n_shareholders[row_idx] <- orbis_row$n_shareholders
          merged_data$n_subsidiaries[row_idx] <- orbis_row$n_subsidiaries
          merged_data$roa_net_income[row_idx] <- orbis_row$roa_net_income
          merged_data$total_assets_eur[row_idx] <- orbis_row$total_assets_eur
          merged_data$rnd_over_operating[row_idx] <- orbis_row$rnd_over_operating
          merged_data$export_over_operating[row_idx] <- orbis_row$export_over_operating
          merged_data$long_term_debt_eur[row_idx] <- orbis_row$long_term_debt_eur
          merged_data$is_guo[row_idx] <- orbis_row$is_guo
          merged_data$orbis_match_type[row_idx] <- "fuzzy"
        }
      }
    }
  }
} else {
  fuzzy_orbis_success <- 0
}

merged_data <- merged_data %>%
  mutate(orbis_match_type = replace_na(orbis_match_type, "unmatched"))

# ============================================
# FINAL SUMMARY
# ============================================
total_patent_matched <- sum(merged_data$total_applications > 0)
total_orbis_matched <- sum(!is.na(merged_data$employees_last_value))

message("\n========================================")
message("         FINAL MERGE SUMMARY")
message("========================================")
message("Total companies:        ", nrow(merged_data))
message("----------------------------------------")
message("PATENT MATCHING:")
message("  Exact matches:        ", exact_patent_matches,
        " (", round(exact_patent_matches / nrow(merged_data) * 100, 1), "%)")
message("  Fuzzy matches:        ", fuzzy_patent_success,
        " (", round(fuzzy_patent_success / nrow(merged_data) * 100, 1), "%)")
message("  Total matched:        ", total_patent_matched,
        " (", round(total_patent_matched / nrow(merged_data) * 100, 1), "%)")
message("----------------------------------------")
message("ORBIS MATCHING:")
message("  Exact matches:        ", exact_orbis_matches,
        " (", round(exact_orbis_matches / nrow(merged_data) * 100, 1), "%)")
message("  Fuzzy matches:        ", fuzzy_orbis_success,
        " (", round(fuzzy_orbis_success / nrow(merged_data) * 100, 1), "%)")
message("  Total matched:        ", total_orbis_matched,
        " (", round(total_orbis_matched / nrow(merged_data) * 100, 1), "%)")
message("========================================")

message("\nPatent match type distribution:")
print(table(merged_data$patent_match_type))

message("\nOrbis match type distribution:")
print(table(merged_data$orbis_match_type))

# ----------------------------------------
# 8. Save Output
# ----------------------------------------
write_parquet(merged_data, cfg$merged_data_parquet, compression = "gzip")
write.csv(merged_data, cfg$merged_data_csv, row.names = FALSE)

message("\nSaved to ", cfg$merged_data_parquet)
message("Done.")