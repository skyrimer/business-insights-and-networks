library(dplyr)
library(tidyr)
library(stringdist)
library(stringr)
library(config)

source("src/reporting_utils.R")

if (!requireNamespace("nstandr", quietly = TRUE)) {
  if (!requireNamespace("devtools", quietly = TRUE)) install.packages("devtools")
  devtools::install_github("stasvlasov/nstandr")
}
library(nstandr)

cfg <- config::get()

compute_match_score <- function(name1, name2, jw_weight = cfg$jw_weight,
                                lv_weight = cfg$lv_weight, jaccard_weight = cfg$jaccard_weight) {
  if (nchar(name1) == 0 || nchar(name2) == 0) {
    return(0)
  }

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

  final_score <- jw_weight * jw_score + lv_weight * lv_score + jaccard_weight * jaccard_score
  return(final_score)
}

find_best_match_hybrid <- function(name, candidates_df, name_col, threshold = cfg$fuzzy_match_threshold) {
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

print_header("DATA MERGING PIPELINE")
print_section("Loading datasets...")

patent_data <- read.csv(cfg$patents_processed_file, stringsAsFactors = FALSE)
sdc_data <- read.csv(cfg$sdc_filtered_file, stringsAsFactors = FALSE)
orbis_data <- read.csv(cfg$orbis_cleaned_file, stringsAsFactors = FALSE)

print_metrics(list(
  "Patent data rows" = nrow(patent_data),
  "SDC data rows" = nrow(sdc_data),
  "Orbis data rows" = nrow(orbis_data)
))

print_header("SDC + ORBIS MERGE (DIRECT JOIN)")

sdc_data <- sdc_data %>%
  mutate(participants_original = participants)

orbis_merge_cols <- c(
  "company_name", "country_iso", "city", "nace_code", "entity_type",
  "employees_last_value", "total_assets_eur", "total_assets_usd",
  "n_companies_group", "n_shareholders", "n_subsidiaries",
  "roa", "rd_ratio", "export_ratio", "export_revenue",
  "long_term_debt", "is_guo", "is_controlled_sub", "size_classification"
)

orbis_for_merge <- orbis_data %>%
  select(any_of(orbis_merge_cols))

merged_data <- sdc_data %>%
  left_join(
    orbis_for_merge,
    by = c("participants_original" = "company_name")
  ) %>%
  mutate(orbis_match_type = ifelse(!is.na(country_iso) | !is.na(employees_last_value) | !is.na(total_assets_eur),
    "matched", "unmatched"
  ))

orbis_matched <- sum(merged_data$orbis_match_type == "matched")
print_match_summary(orbis_matched, nrow(merged_data), "ORBIS Direct Join Results")

print_section("Standardizing company names...")

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

print_header("PATENT MATCHING")
print_subheader("Phase 1: Exact Matching")

merged_data <- merged_data %>%
  left_join(
    patent_data %>% select(-assignee_original, -prefix_3, -prefix_2),
    by = c("participants_std" = "assignee_std")
  ) %>%
  mutate(patent_match_type = ifelse(!is.na(assignee), "exact", NA_character_))

exact_patent_matches <- sum(!is.na(merged_data$assignee))
print_match_summary(exact_patent_matches, nrow(merged_data), "Exact Patent Matches")

print_subheader("Phase 2: Hybrid Fuzzy Matching")

unmatched_companies <- merged_data %>%
  filter(is.na(assignee)) %>%
  distinct(participants_std) %>%
  pull(participants_std)

print_info(sprintf("Unique companies without exact match: %d", length(unmatched_companies)))

if (length(unmatched_companies) > 0) {
  print_info("Running hybrid matching (Jaro-Winkler + Levenshtein + Jaccard)...")

  total_unmatched <- length(unmatched_companies)
  fuzzy_results <- vector("list", total_unmatched)

  pb <- create_progress_bar(total_unmatched, "Fuzzy Matching")

  for (i in seq_along(unmatched_companies)) {
    fuzzy_results[[i]] <- find_best_match_hybrid(unmatched_companies[i], patent_data, "assignee_std", threshold = cfg$fuzzy_match_threshold)
    pb <- update_progress(pb)
  }

  fuzzy_lookup <- data.frame(
    participants_std = unmatched_companies,
    fuzzy_match_std = sapply(fuzzy_results, function(x) x$match),
    stringsAsFactors = FALSE
  )

  fuzzy_patent_success <- sum(!is.na(fuzzy_lookup$fuzzy_match_std))

  cat("\n")
  print_match_summary(fuzzy_patent_success, length(unmatched_companies), "Fuzzy Patent Matches")

  if (fuzzy_patent_success > 0) {
    valid_scores <- sapply(fuzzy_results, function(x) x$score)
    valid_scores <- valid_scores[!is.na(valid_scores)]
    print_metrics(list(
      "Score range" = sprintf("%.3f - %.3f", min(valid_scores), max(valid_scores)),
      "Mean score" = mean(valid_scores)
    ))

    fuzzy_lookup <- fuzzy_lookup %>%
      filter(!is.na(fuzzy_match_std)) %>%
      left_join(
        patent_data %>% select(-assignee_original, -prefix_3, -prefix_2),
        by = c("fuzzy_match_std" = "assignee_std")
      )

    merged_data <- merged_data %>%
      left_join(
        fuzzy_lookup %>%
          select(participants_std,
            fuzzy_assignee = assignee, fuzzy_total_applications = total_applications,
            fuzzy_granted_patents = granted_patents, fuzzy_avg_ipc_scope = avg_ipc_scope,
            fuzzy_avg_team_size = avg_team_size, fuzzy_grant_success_rate = grant_success_rate,
            fuzzy_total_ipc_mentions = total_ipc_mentions, fuzzy_tech_specialist_hhi = tech_specialist_hhi
          ),
        by = "participants_std"
      ) %>%
      mutate(
        assignee = coalesce(assignee, fuzzy_assignee),
        total_applications = coalesce(total_applications, fuzzy_total_applications),
        granted_patents = coalesce(granted_patents, fuzzy_granted_patents),
        avg_ipc_scope = coalesce(avg_ipc_scope, fuzzy_avg_ipc_scope),
        avg_team_size = coalesce(avg_team_size, fuzzy_avg_team_size),
        grant_success_rate = coalesce(grant_success_rate, fuzzy_grant_success_rate),
        total_ipc_mentions = coalesce(total_ipc_mentions, fuzzy_total_ipc_mentions),
        tech_specialist_hhi = coalesce(tech_specialist_hhi, fuzzy_tech_specialist_hhi),
        patent_match_type = if_else(!is.na(fuzzy_assignee) & is.na(patent_match_type), "fuzzy", patent_match_type)
      ) %>%
      select(-starts_with("fuzzy_"))
  }
} else {
  fuzzy_patent_success <- 0
}

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

total_patent_matched <- sum(merged_data$total_applications > 0)
total_orbis_matched <- sum(merged_data$orbis_match_type == "matched")
unique_companies <- n_distinct(merged_data$participants_std)
unique_deals <- n_distinct(merged_data$deal_number)

print_header("FINAL MERGE SUMMARY")

print_metrics(list(
  "Total rows" = nrow(merged_data),
  "Unique companies" = unique_companies,
  "Unique deals" = unique_deals
))

print_subheader("SDC + ORBIS Matching")
print_match_summary(total_orbis_matched, nrow(merged_data), "ORBIS Matches")

print_subheader("Patent Matching")
print_match_summary(exact_patent_matches, nrow(merged_data), "Exact Matches", show_bar = FALSE)
print_match_summary(sum(merged_data$patent_match_type == "fuzzy"), nrow(merged_data), "Fuzzy Matches", show_bar = FALSE)
print_match_summary(total_patent_matched, nrow(merged_data), "Total Patent Matches")

orbis_dist <- as.data.frame(table(merged_data$orbis_match_type))
names(orbis_dist) <- c("Match Type", "Count")
orbis_dist$Percentage <- sprintf("%.1f%%", orbis_dist$Count / nrow(merged_data) * 100)
print_summary_table(orbis_dist, "ORBIS Match Type Distribution")

patent_dist <- as.data.frame(table(merged_data$patent_match_type))
names(patent_dist) <- c("Match Type", "Count")
patent_dist$Percentage <- sprintf("%.1f%%", patent_dist$Count / nrow(merged_data) * 100)
print_summary_table(patent_dist, "Patent Match Type Distribution")

print_section("Saving output files...")

write.csv(merged_data, cfg$merged_data_file, row.names = FALSE)

print_success(sprintf("Merged data saved to: %s", cfg$merged_data_file))
print_success("Data merging pipeline completed!")
