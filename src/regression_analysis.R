library(dplyr)
library(tidyr)
library(arrow)
library(igraph)
library(purrr)
library(countrycode)
library(config)

# ----------------------------------------
# Load Configuration
# ----------------------------------------
cfg <- config::get()

# ----------------------------------------
# 1. Load Merged Data
# ----------------------------------------
message("Loading merged dataset...")

merged_data <- read_parquet(cfg$merged_data_parquet)

message("Merged data rows: ", nrow(merged_data))
message("Unique companies: ", n_distinct(merged_data$participants_std))
message("Unique deals: ", n_distinct(merged_data$deal_number))

# Report available columns from ORBIS
orbis_cols <- c("employees_last_value", "total_assets_eur", "n_shareholders",
                "n_subsidiaries", "n_companies_group", "country_iso", "is_guo")
available_orbis <- intersect(orbis_cols, names(merged_data))
message("\nAvailable ORBIS columns: ", paste(available_orbis, collapse = ", "))

# ----------------------------------------
# 2. Build Alliance Network
# ----------------------------------------
message("\nBuilding alliance network...")

# Standardize participant names if not already done
if (!requireNamespace("nstandr", quietly = TRUE)) {
  devtools::install_github("stasvlasov/nstandr")
}
library(nstandr)

# Create firm-firm edge list from deals
edges <- merged_data %>%
  group_by(deal_number) %>%
  filter(n_distinct(participants_std) > 1) %>%
  summarise(
    pairs = list(combn(unique(participants_std), 2, simplify = FALSE)),
    .groups = "drop"
  ) %>%
  unnest(pairs) %>%
  transmute(
    from = map_chr(pairs, 1),
    to = map_chr(pairs, 2)
  )

# Build graph
firm_graph <- graph_from_data_frame(edges, directed = FALSE)
firm_graph <- simplify(firm_graph)

message("Network nodes: ", vcount(firm_graph))
message("Network edges: ", ecount(firm_graph))

# ----------------------------------------
# 3. Compute Network Position Metrics
# ----------------------------------------
message("\nComputing network metrics...")

network_metrics <- data.frame(
  participants_std = V(firm_graph)$name,
  degree = degree(firm_graph),
  betweenness = betweenness(firm_graph, normalized = TRUE),
  closeness = closeness(firm_graph, normalized = TRUE),
  eigenvector = eigen_centrality(firm_graph)$vector,
  stringsAsFactors = FALSE
)

# ----------------------------------------
# 4. Compute Geographic Reach (Moderator 1: H2)
# ----------------------------------------
message("\nComputing geographic reach (Moderator for H2)...")

# Map participant nations to continents
iso_custom <- c("Utd Arab Em" = "ARE", "Hong Kong" = "HKG", "Taiwan" = "TWN")

merged_with_continent <- merged_data %>%
  mutate(
    participant_iso = countrycode(participant_nation,
                                  origin = "country.name",
                                  destination = "iso3c",
                                  custom_match = iso_custom),
    continent = countrycode(participant_iso,
                            origin = "iso3c",
                            destination = "continent")
  ) %>%
  mutate(continent = replace_na(continent, "Other"))

# For each deal, check if it spans multiple continents
deal_continents <- merged_with_continent %>%
  group_by(deal_number) %>%
  summarise(
    n_continents_in_deal = n_distinct(continent),
    is_cross_continental = n_distinct(continent) > 1,
    .groups = "drop"
  )

# For each firm, compute geographic reach metrics
geo_reach <- merged_with_continent %>%
  left_join(deal_continents, by = "deal_number") %>%
  group_by(participants_std) %>%
  summarise(
    n_total_deals = n_distinct(deal_number),
    n_cross_continental_deals = n_distinct(deal_number[is_cross_continental]),
    cross_continental_share = n_cross_continental_deals / n_total_deals,
    n_continents_reached = n_distinct(continent),
    .groups = "drop"
  )

# ----------------------------------------
# 5. Create Firm-Level Dataset for Regression
# ----------------------------------------
message("\nAggregating to firm-level for regression analysis...")

# Helper function to safely get first value
safe_first <- function(x) {
  if (length(x) == 0 || all(is.na(x))) return(NA)
  first(x)
}

# Aggregate deal-level data to firm-level
# Only include columns that exist in the data
firm_level_data <- merged_data %>%
  group_by(participants_std) %>%
  summarise(
    # Company identifiers
    company_name = safe_first(participants_original),

    # Innovation Output (DV): Patent applications
    total_applications = safe_first(total_applications),
    granted_patents = safe_first(granted_patents),

    # Firm characteristics from ORBIS (updated column names)
    employees_last_value = if ("employees_last_value" %in% names(merged_data)) safe_first(employees_last_value) else NA_real_,
    total_assets_eur = if ("total_assets_eur" %in% names(merged_data)) safe_first(total_assets_eur) else NA_real_,
    total_assets_usd = if ("total_assets_usd" %in% names(merged_data)) safe_first(total_assets_usd) else NA_real_,
    n_shareholders = if ("n_shareholders" %in% names(merged_data)) safe_first(n_shareholders) else NA_real_,
    n_subsidiaries = if ("n_subsidiaries" %in% names(merged_data)) safe_first(n_subsidiaries) else NA_real_,
    n_companies_group = if ("n_companies_group" %in% names(merged_data)) safe_first(n_companies_group) else NA_real_,
    country_iso = if ("country_iso" %in% names(merged_data)) safe_first(country_iso) else NA_character_,
    is_guo = if ("is_guo" %in% names(merged_data)) safe_first(is_guo) else NA_integer_,
    is_controlled_sub = if ("is_controlled_sub" %in% names(merged_data)) safe_first(is_controlled_sub) else NA_integer_,

    # Financial metrics from ORBIS (new columns)
    roa = if ("roa" %in% names(merged_data)) safe_first(roa) else NA_real_,
    rd_ratio = if ("rd_ratio" %in% names(merged_data)) safe_first(rd_ratio) else NA_real_,
    long_term_debt = if ("long_term_debt" %in% names(merged_data)) safe_first(long_term_debt) else NA_real_,
    size_classification = if ("size_classification" %in% names(merged_data)) safe_first(size_classification) else NA_character_,

    # Patent-derived metrics
    tech_specialist_hhi = safe_first(tech_specialist_hhi),
    avg_ipc_scope = safe_first(avg_ipc_scope),
    grant_success_rate = safe_first(grant_success_rate),

    # Alliance activity
    n_deals = n_distinct(deal_number),

    # Match quality indicators
    orbis_match_type = safe_first(orbis_match_type),
    patent_match_type = safe_first(patent_match_type),

    .groups = "drop"
  )

message("Firm-level observations: ", nrow(firm_level_data))


# ----------------------------------------
# 6. Compute Partner Size Diversity (Main IV)
# ----------------------------------------
message("\nComputing partner size diversity (Main IV for H1)...")

# Create employee lookup from firm-level data
emp_lookup <- firm_level_data %>%
  filter(!is.na(employees_last_value)) %>%
  select(participants_std, employees_last_value) %>%
  distinct()

message("Firms with employee data: ", nrow(emp_lookup))

# Function to compute Coefficient of Variation of partner sizes
compute_partner_size_diversity <- function(focal_name, graph, emp_df) {
  if (!focal_name %in% V(graph)$name) return(NA_real_)

  neighbors_names <- neighbors(graph, focal_name, mode = "all")$name
  if (length(neighbors_names) == 0) return(NA_real_)

  partner_sizes <- emp_df$employees_last_value[emp_df$participants_std %in% neighbors_names]
  partner_sizes <- partner_sizes[!is.na(partner_sizes)]

  if (length(partner_sizes) == 0) return(NA_real_)
  if (length(partner_sizes) == 1) return(0)

  cv <- sd(partner_sizes) / mean(partner_sizes)
  return(cv)
}

# Compute for all firms
firm_level_data$partner_size_diversity <- sapply(
  firm_level_data$participants_std,
  function(x) compute_partner_size_diversity(x, firm_graph, emp_lookup)
)

message("Firms with partner diversity computed: ", sum(!is.na(firm_level_data$partner_size_diversity)))

# ----------------------------------------
# 7. Additional Partner Portfolio Metrics
# ----------------------------------------
message("\nComputing additional partner metrics...")

compute_partner_metrics <- function(focal_name, graph, emp_df) {
  if (!focal_name %in% V(graph)$name) {
    return(list(
      n_partners = 0,
      avg_partner_size = NA_real_,
      max_partner_size = NA_real_,
      min_partner_size = NA_real_
    ))
  }

  neighbors_names <- neighbors(graph, focal_name, mode = "all")$name
  partner_sizes <- emp_df$employees_last_value[emp_df$participants_std %in% neighbors_names]
  partner_sizes <- partner_sizes[!is.na(partner_sizes)]

  list(
    n_partners = length(neighbors_names),
    avg_partner_size = if (length(partner_sizes) > 0) mean(partner_sizes) else NA_real_,
    max_partner_size = if (length(partner_sizes) > 0) max(partner_sizes) else NA_real_,
    min_partner_size = if (length(partner_sizes) > 0) min(partner_sizes) else NA_real_
  )
}

partner_metrics <- lapply(firm_level_data$participants_std, function(x) {
  compute_partner_metrics(x, firm_graph, emp_lookup)
})

firm_level_data$n_partners <- sapply(partner_metrics, function(x) x$n_partners)
firm_level_data$avg_partner_size <- sapply(partner_metrics, function(x) x$avg_partner_size)
firm_level_data$max_partner_size <- sapply(partner_metrics, function(x) x$max_partner_size)
firm_level_data$min_partner_size <- sapply(partner_metrics, function(x) x$min_partner_size)

# ----------------------------------------
# 8. Merge All Metrics
# ----------------------------------------
message("\nMerging all metrics...")

# Merge network metrics
firm_level_data <- firm_level_data %>%
  left_join(network_metrics, by = "participants_std") %>%
  mutate(
    degree = replace_na(degree, 0),
    betweenness = replace_na(betweenness, 0),
    closeness = replace_na(closeness, 0),
    eigenvector = replace_na(eigenvector, 0)
  )

# Merge geographic reach
firm_level_data <- firm_level_data %>%
  left_join(geo_reach, by = "participants_std") %>%
  mutate(
    cross_continental_share = replace_na(cross_continental_share, 0),
    n_continents_reached = replace_na(n_continents_reached, 1),
    n_total_deals = coalesce(n_total_deals, n_deals)
  )

# ----------------------------------------
# 9. Prepare Regression Variables
# ----------------------------------------
message("\nPreparing regression variables...")

firm_level_data <- firm_level_data %>%
  mutate(
    # Quadratic term for inverted-U test (H1)
    partner_size_diversity_sq = partner_size_diversity^2,

    # Log transformations for skewed variables (controls)
    ln_employees = log(replace_na(employees_last_value, 0) + 1),
    ln_total_assets = log(replace_na(total_assets_eur, 0) + 1),
    ln_avg_partner_size = log(replace_na(avg_partner_size, 0) + 1),
    ln_long_term_debt = log(replace_na(long_term_debt, 0) + 1),

    # Alliance experience (number of deals)
    alliance_experience = n_deals,

    # Tech specialization (from patent data)
    tech_specialization = replace_na(tech_specialist_hhi, 0),

    # Corporate structure indicators (from ORBIS)
    ln_subsidiaries = log(replace_na(n_subsidiaries, 0) + 1),
    ln_shareholders = log(replace_na(n_shareholders, 0) + 1),
    is_guo = replace_na(is_guo, 0),
    is_controlled_sub = replace_na(is_controlled_sub, 0),

    # Financial metrics (from ORBIS)
    roa_clean = replace_na(roa, 0),
    rd_ratio_clean = replace_na(rd_ratio, 0)
  )

# ----------------------------------------
# 10. Create Analysis Sample
# ----------------------------------------
message("\nCreating analysis sample...")

# Filter to firms with required data for regression
analysis_sample <- firm_level_data %>%
  filter(
    !is.na(partner_size_diversity),
    !is.na(employees_last_value),
    n_partners > 0
  )

message("Analysis sample size: ", nrow(analysis_sample))
message("Firms with patents (total_applications > 0): ", sum(analysis_sample$total_applications > 0))

# Check if we have enough observations
if (nrow(analysis_sample) < 10) {
  warning("Analysis sample is very small (", nrow(analysis_sample), " observations).")
  warning("Results may not be reliable. Consider relaxing filter criteria.")
}

message("\n========================================")
message("      DESCRIPTIVE STATISTICS")
message("========================================\n")

desc_vars <- c("total_applications", "partner_size_diversity",
               "cross_continental_share", "betweenness", "degree",
               "employees_last_value", "n_partners", "n_deals",
               "roa", "rd_ratio", "total_assets_eur")

# Only include variables that exist
desc_vars <- intersect(desc_vars, names(analysis_sample))

desc_stats <- analysis_sample %>%
  select(all_of(desc_vars)) %>%
  summarise(across(everything(), list(
    n = ~sum(!is.na(.)),
    mean = ~mean(., na.rm = TRUE),
    sd = ~sd(., na.rm = TRUE),
    min = ~min(., na.rm = TRUE),
    max = ~max(., na.rm = TRUE)
  ))) %>%
  pivot_longer(everything(), names_to = c("variable", "stat"), names_sep = "_(?=[^_]+$)") %>%
  pivot_wider(names_from = stat, values_from = value)

print(desc_stats)

# ----------------------------------------
# Correlation Matrix
# ----------------------------------------
message("\n--- Correlation Matrix (Key Variables) ---\n")

cor_vars <- c("total_applications", "partner_size_diversity",
              "cross_continental_share", "betweenness", "ln_employees", "degree",
              "roa_clean", "rd_ratio_clean")
cor_vars <- intersect(cor_vars, names(analysis_sample))

cor_matrix <- analysis_sample %>%
  select(all_of(cor_vars)) %>%
  cor(use = "pairwise.complete.obs")

print(round(cor_matrix, 3))

# ============================================
# REGRESSION ANALYSIS
# ============================================
message("\n========================================")
message("      REGRESSION ANALYSIS")
message("========================================")

# ----------------------------------------
# Model 1: Base Model (Controls Only)
# ----------------------------------------
message("\n--- Model 1: Base Controls ---\n")

model_1 <- lm(total_applications ~
                ln_employees +
                ln_total_assets +
                degree +
                alliance_experience +
                tech_specialization +
                roa_clean +
                rd_ratio_clean,
              data = analysis_sample)

print(summary(model_1))

# ----------------------------------------
# Model 2: Linear Effect of Partner Size Diversity
# ----------------------------------------
message("\n--- Model 2: Linear Partner Size Diversity ---\n")

model_2 <- lm(total_applications ~
                partner_size_diversity +
                ln_employees +
                ln_total_assets +
                degree +
                alliance_experience +
                tech_specialization +
                roa_clean +
                rd_ratio_clean,
              data = analysis_sample)

print(summary(model_2))

# ----------------------------------------
# Model 3: H1 - Inverted U-Shaped Relationship
# ----------------------------------------
message("\n--- Model 3: H1 - Inverted U-Shape (Quadratic Term) ---")
message("H1: There is an inverted U-shaped relationship between partner size")
message("    diversity and innovation output.\n")

model_3 <- lm(total_applications ~
                partner_size_diversity +
                partner_size_diversity_sq +
                ln_employees +
                ln_total_assets +
                degree +
                alliance_experience +
                tech_specialization +
                roa_clean +
                rd_ratio_clean,
              data = analysis_sample)

print(summary(model_3))

# Check if quadratic term is significant and negative (inverted U)
coef_psd <- coef(model_3)["partner_size_diversity"]
coef_psd_sq <- coef(model_3)["partner_size_diversity_sq"]
message("\nH1 Test Results:")
message("  Linear coefficient (partner_size_diversity): ", round(coef_psd, 4))
message("  Quadratic coefficient (partner_size_diversity_sq): ", round(coef_psd_sq, 4))
message("  Expected: Positive linear, negative quadratic for inverted U-shape")

# Calculate optimal level of partner size diversity (turning point)
if (!is.na(coef_psd_sq) && coef_psd_sq < 0 && coef_psd > 0) {
  optimal_psd <- -coef_psd / (2 * coef_psd_sq)
  message("  Optimal partner size diversity (turning point): ", round(optimal_psd, 3))
  message("  Interpretation: Innovation peaks at CV = ", round(optimal_psd, 3))
}

# ----------------------------------------
# Model 4: H2 - Geographic Reach Moderation
# ----------------------------------------
message("\n--- Model 4: H2 - Geographic Reach Moderation ---")
message("H2: The inverted U-shaped relationship is STRONGER for firms with")
message("    higher share of cross-continental alliances.\n")

model_4 <- lm(total_applications ~
                partner_size_diversity +
                partner_size_diversity_sq +
                cross_continental_share +
                partner_size_diversity:cross_continental_share +
                partner_size_diversity_sq:cross_continental_share +
                ln_employees +
                ln_total_assets +
                degree +
                alliance_experience +
                tech_specialization +
                roa_clean +
                rd_ratio_clean,
              data = analysis_sample)

print(summary(model_4))

message("\nH2 Test Results:")
message("  Expected: Negative interaction (PSD_sq × cross_continental_share)")
message("  Meaning: Geographic distance amplifies coordination costs at extreme diversity")

# ----------------------------------------
# Model 5: H3 - Betweenness Centrality Moderation
# ----------------------------------------
message("\n--- Model 5: H3 - Betweenness Centrality Moderation ---")
message("H3: Firms with higher betweenness centrality experience a WEAKER")
message("    negative effect of extreme partner size diversity.\n")

model_5 <- lm(total_applications ~
                partner_size_diversity +
                partner_size_diversity_sq +
                betweenness +
                partner_size_diversity:betweenness +
                partner_size_diversity_sq:betweenness +
                ln_employees +
                ln_total_assets +
                degree +
                alliance_experience +
                tech_specialization +
                roa_clean +
                rd_ratio_clean,
              data = analysis_sample)

print(summary(model_5))

message("\nH3 Test Results:")
message("  Expected: Positive interaction (PSD_sq × betweenness)")
message("  Meaning: Brokerage position buffers against coordination costs")

# ----------------------------------------
# Model 6: Full Model (All Hypotheses)
# ----------------------------------------
message("\n--- Model 6: Full Model (All Hypotheses) ---\n")

model_6 <- lm(total_applications ~
                partner_size_diversity +
                partner_size_diversity_sq +
                cross_continental_share +
                betweenness +
                partner_size_diversity:cross_continental_share +
                partner_size_diversity_sq:cross_continental_share +
                partner_size_diversity:betweenness +
                partner_size_diversity_sq:betweenness +
                ln_employees +
                ln_total_assets +
                degree +
                alliance_experience +
                tech_specialization +
                roa_clean +
                rd_ratio_clean,
              data = analysis_sample)

print(summary(model_6))

# ============================================
# MODEL COMPARISON
# ============================================
message("\n========================================")
message("      MODEL COMPARISON")
message("========================================\n")

model_comparison <- data.frame(
  Model = c("M1: Controls", "M2: Linear PSD", "M3: H1 (Inverted-U)",
            "M4: H2 (Geographic)", "M5: H3 (Network)", "M6: Full"),
  R_squared = c(
    summary(model_1)$r.squared,
    summary(model_2)$r.squared,
    summary(model_3)$r.squared,
    summary(model_4)$r.squared,
    summary(model_5)$r.squared,
    summary(model_6)$r.squared
  ),
  Adj_R_squared = c(
    summary(model_1)$adj.r.squared,
    summary(model_2)$adj.r.squared,
    summary(model_3)$adj.r.squared,
    summary(model_4)$adj.r.squared,
    summary(model_5)$adj.r.squared,
    summary(model_6)$adj.r.squared
  ),
  AIC = c(AIC(model_1), AIC(model_2), AIC(model_3),
          AIC(model_4), AIC(model_5), AIC(model_6)),
  BIC = c(BIC(model_1), BIC(model_2), BIC(model_3),
          BIC(model_4), BIC(model_5), BIC(model_6)),
  N = c(nobs(model_1), nobs(model_2), nobs(model_3),
        nobs(model_4), nobs(model_5), nobs(model_6))
)

print(model_comparison)

# ============================================
# HYPOTHESIS SUMMARY
# ============================================
message("\n========================================")
message("      HYPOTHESIS SUMMARY")
message("========================================\n")

# H1: Inverted U-shaped relationship
message("--- H1: Inverted U-shaped relationship between partner size diversity")
message("        and innovation output ---")
h1_linear <- coef(summary(model_3))["partner_size_diversity", ]
h1_quad <- coef(summary(model_3))["partner_size_diversity_sq", ]

message("  Linear term: coef = ", round(h1_linear["Estimate"], 4),
        ", SE = ", round(h1_linear["Std. Error"], 4),
        ", p = ", round(h1_linear["Pr(>|t|)"], 4))
message("  Quadratic term: coef = ", round(h1_quad["Estimate"], 4),
        ", SE = ", round(h1_quad["Std. Error"], 4),
        ", p = ", round(h1_quad["Pr(>|t|)"], 4))

h1_supported <- h1_linear["Estimate"] > 0 & h1_quad["Estimate"] < 0 & h1_quad["Pr(>|t|)"] < 0.1
message("  H1 Support: ", ifelse(h1_supported, "YES (positive linear, negative quadratic)",
                                  "Insufficient evidence"))

# H2: Geographic reach moderation
message("\n--- H2: Cross-continental alliances strengthen the inverted-U ---")
if ("partner_size_diversity_sq:cross_continental_share" %in% rownames(coef(summary(model_4)))) {
  h2_int <- coef(summary(model_4))["partner_size_diversity_sq:cross_continental_share", ]
  message("  Interaction (PSD_sq × geo): coef = ", round(h2_int["Estimate"], 4),
          ", SE = ", round(h2_int["Std. Error"], 4),
          ", p = ", round(h2_int["Pr(>|t|)"], 4))
  h2_supported <- h2_int["Estimate"] < 0 & h2_int["Pr(>|t|)"] < 0.1
  message("  H2 Support: ", ifelse(h2_supported, "YES (negative interaction)",
                                    "Insufficient evidence"))
}

# H3: Betweenness centrality moderation
message("\n--- H3: Betweenness centrality weakens the negative effect of extreme diversity ---")
if ("partner_size_diversity_sq:betweenness" %in% rownames(coef(summary(model_5)))) {
  h3_int <- coef(summary(model_5))["partner_size_diversity_sq:betweenness", ]
  message("  Interaction (PSD_sq × betweenness): coef = ", round(h3_int["Estimate"], 4),
          ", SE = ", round(h3_int["Std. Error"], 4),
          ", p = ", round(h3_int["Pr(>|t|)"], 4))
  h3_supported <- h3_int["Estimate"] > 0 & h3_int["Pr(>|t|)"] < 0.1
  message("  H3 Support: ", ifelse(h3_supported, "YES (positive interaction)",
                                    "Insufficient evidence"))
}

# ============================================
# SAVE RESULTS
# ============================================
message("\n========================================")
message("      SAVING RESULTS")
message("========================================\n")

# Save analysis sample
write_parquet(analysis_sample, cfg$analysis_sample_parquet, compression = "gzip")
write.csv(analysis_sample, cfg$analysis_sample_csv, row.names = FALSE)

# Save model comparison
write.csv(model_comparison, cfg$model_comparison_file, row.names = FALSE)

# Save regression coefficients for all models
all_models <- list(
  model_1 = model_1,
  model_2 = model_2,
  model_3_H1 = model_3,
  model_4_H2 = model_4,
  model_5_H3 = model_5,
  model_6_Full = model_6
)
saveRDS(all_models, file.path(dirname(cfg$analysis_sample_parquet), "regression_models.rds"))

message("Analysis sample saved to: ", cfg$analysis_sample_parquet)
message("Model comparison saved to: ", cfg$model_comparison_file)
message("Regression models saved to: regression_models.rds")
message("\nDone.")