# ============================================================================
# David vs. Goliath: Partner Size Diversity and Innovation
# Feature Engineering & Regression Analysis
# ============================================================================

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
sdc_data <- read.csv(cfg$sdc_filtered_file, stringsAsFactors = FALSE)

message("Merged data rows: ", nrow(merged_data))

# ----------------------------------------
# 2. Build Alliance Network
# ----------------------------------------
message("\nBuilding alliance network...")

# Standardize SDC participant names to match merged data
if (!requireNamespace("nstandr", quietly = TRUE)) {
  devtools::install_github("stasvlasov/nstandr")
}
library(nstandr)

sdc_data <- sdc_data %>%
  mutate(participants_std = participants %>% trimws() %>% standardize_magerman())

# Create firm-firm edge list from deals
edges <- sdc_data %>%
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
  eigenvector = eigen_centrality(firm_graph)$vector
)

# Merge network metrics
merged_data <- merged_data %>%
  left_join(network_metrics, by = "participants_std") %>%
  mutate(
    degree = replace_na(degree, 0),
    betweenness = replace_na(betweenness, 0),
    closeness = replace_na(closeness, 0),
    eigenvector = replace_na(eigenvector, 0)
  )

# ----------------------------------------
# 4. Compute Geographic Reach (Moderator 1)
# ----------------------------------------
message("\nComputing geographic reach...")

# Map participant nations to continents
iso_custom <- c("Utd Arab Em" = "ARE", "Hong Kong" = "HKG", "Taiwan" = "TWN")

sdc_with_continent <- sdc_data %>%
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
deal_continents <- sdc_with_continent %>%
  group_by(deal_number) %>%
  summarise(
    n_continents_in_deal = n_distinct(continent),
    is_cross_continental = n_distinct(continent) > 1,
    .groups = "drop"
  )

# For each firm, compute geographic reach metrics
geo_reach <- sdc_with_continent %>%
  left_join(deal_continents, by = "deal_number") %>%
  group_by(participants_std) %>%
  summarise(
    n_total_deals = n_distinct(deal_number),
    n_cross_continental_deals = sum(is_cross_continental),
    cross_continental_share = n_cross_continental_deals / n_total_deals,
    n_continents_reached = n_distinct(continent),
    .groups = "drop"
  )

merged_data <- merged_data %>%
  left_join(geo_reach, by = "participants_std") %>%
  mutate(
    cross_continental_share = replace_na(cross_continental_share, 0),
    n_continents_reached = replace_na(n_continents_reached, 1)
  )

# ----------------------------------------
# 5. Compute Partner Size Diversity (Main IV)
# ----------------------------------------
message("\nComputing partner size diversity...")

# Create employee lookup from merged data
emp_lookup <- merged_data %>%
  select(participants_std, employees_last_value) %>%
  filter(!is.na(employees_last_value))

message("Firms with employee data: ", nrow(emp_lookup))

# Function to compute CV of partner sizes
compute_partner_size_diversity <- function(focal_name, graph, emp_df) {
  # Get neighbors in the network
  if (!focal_name %in% V(graph)$name) return(NA_real_)

  neighbors_names <- neighbors(graph, focal_name, mode = "all")$name
  if (length(neighbors_names) == 0) return(NA_real_)

  # Get employee counts for partners
  partner_sizes <- emp_df$employees_last_value[emp_df$participants_std %in% neighbors_names]
  partner_sizes <- partner_sizes[!is.na(partner_sizes)]

  if (length(partner_sizes) == 0) return(NA_real_)
  if (length(partner_sizes) == 1) return(0)  # Single partner = no variance

  # Coefficient of Variation = SD / Mean
  cv <- sd(partner_sizes) / mean(partner_sizes)
  return(cv)
}

# Compute for all firms
merged_data$partner_size_diversity <- sapply(
  merged_data$participants_std,
  function(x) compute_partner_size_diversity(x, firm_graph, emp_lookup)
)

message("Firms with partner diversity computed: ", sum(!is.na(merged_data$partner_size_diversity)))

# ----------------------------------------
# 6. Additional Partner Portfolio Metrics
# ----------------------------------------
message("\nComputing additional partner metrics...")

compute_partner_metrics <- function(focal_name, graph, emp_df) {
  if (!focal_name %in% V(graph)$name) {
    return(list(
      n_partners = 0,
      avg_partner_size = NA_real_,
      max_partner_size = NA_real_,
      min_partner_size = NA_real_,
      size_range = NA_real_
    ))
  }

  neighbors_names <- neighbors(graph, focal_name, mode = "all")$name
  partner_sizes <- emp_df$employees_last_value[emp_df$participants_std %in% neighbors_names]
  partner_sizes <- partner_sizes[!is.na(partner_sizes)]

  list(
    n_partners = length(neighbors_names),
    avg_partner_size = if (length(partner_sizes) > 0) mean(partner_sizes) else NA_real_,
    max_partner_size = if (length(partner_sizes) > 0) max(partner_sizes) else NA_real_,
    min_partner_size = if (length(partner_sizes) > 0) min(partner_sizes) else NA_real_,
    size_range = if (length(partner_sizes) > 1) max(partner_sizes) - min(partner_sizes) else NA_real_
  )
}

partner_metrics <- lapply(merged_data$participants_std, function(x) {
  compute_partner_metrics(x, firm_graph, emp_lookup)
})

merged_data$n_partners <- sapply(partner_metrics, function(x) x$n_partners)
merged_data$avg_partner_size <- sapply(partner_metrics, function(x) x$avg_partner_size)
merged_data$max_partner_size <- sapply(partner_metrics, function(x) x$max_partner_size)
merged_data$min_partner_size <- sapply(partner_metrics, function(x) x$min_partner_size)
merged_data$partner_size_range <- sapply(partner_metrics, function(x) x$size_range)

# ----------------------------------------
# 7. Create Regression Variables
# ----------------------------------------
message("\nPreparing regression variables...")

# Dependent Variable: Innovation Output (patent applications)
# Already in data as: total_applications

# Main IV: Partner Size Diversity (already computed)
# Quadratic term for inverted-U test
merged_data$partner_size_diversity_sq <- merged_data$partner_size_diversity^2

# Moderator 1: Geographic Reach (cross_continental_share)
# Moderator 2: Network Position (betweenness)

# Control Variables
merged_data <- merged_data %>%
  mutate(
    # Log transformations for skewed variables
    ln_employees = log(employees_last_value + 1),
    ln_total_assets = log(total_assets_eur + 1),
    ln_avg_partner_size = log(avg_partner_size + 1),

    # Firm age proxy (years since first deal)
    firm_age = as.numeric(difftime(Sys.Date(), as.Date(first_deal_date), units = "days")) / 365,

    # R&D intensity (already in data: rnd_over_operating)
    rnd_intensity = replace_na(rnd_over_operating, 0),

    # Alliance experience
    alliance_experience = n_deals,

    # Tech specialization (from patent data)
    tech_specialization = tech_specialist_hhi
  )

# ----------------------------------------
# 8. Create Analysis Sample
# ----------------------------------------
message("\nCreating analysis sample...")

# Filter to firms with required data for regression
analysis_sample <- merged_data %>%
  filter(
    !is.na(partner_size_diversity),
    !is.na(employees_last_value),
    n_partners > 0
  )

message("Analysis sample size: ", nrow(analysis_sample))
message("Firms with patents: ", sum(analysis_sample$total_applications > 0))

# ----------------------------------------
# 9. Descriptive Statistics
# ----------------------------------------
message("\n========================================")
message("      DESCRIPTIVE STATISTICS")
message("========================================\n")

desc_vars <- c("total_applications", "partner_size_diversity",
               "cross_continental_share", "betweenness", "degree",
               "employees_last_value", "n_partners", "n_deals")

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
# 10. Correlation Matrix
# ----------------------------------------
message("\n--- Correlation Matrix (Key Variables) ---\n")

cor_vars <- c("total_applications", "partner_size_diversity",
              "cross_continental_share", "betweenness", "ln_employees", "degree")

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
                tech_specialization,
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
                tech_specialization,
              data = analysis_sample)

print(summary(model_2))

# ----------------------------------------
# Model 3: H1 - Inverted U-Shaped Relationship
# ----------------------------------------
message("\n--- Model 3: H1 - Inverted U-Shape (Quadratic Term) ---\n")

model_3 <- lm(total_applications ~
                partner_size_diversity +
                partner_size_diversity_sq +
                ln_employees +
                ln_total_assets +
                degree +
                alliance_experience +
                tech_specialization,
              data = analysis_sample)

print(summary(model_3))

# Check if quadratic term is significant and negative (inverted U)
coef_psd_sq <- coef(model_3)["partner_size_diversity_sq"]
message("\nH1 Test: Quadratic coefficient = ", round(coef_psd_sq, 4))
message("Expected: Negative coefficient for inverted U-shape")

# Calculate optimal level of partner size diversity
if (!is.na(coef_psd_sq) && coef_psd_sq < 0) {
  optimal_psd <- -coef(model_3)["partner_size_diversity"] / (2 * coef_psd_sq)
  message("Optimal partner size diversity (turning point): ", round(optimal_psd, 3))
}

# ----------------------------------------
# Model 4: H2 - Geographic Reach Moderation
# ----------------------------------------
message("\n--- Model 4: H2 - Geographic Reach Moderation ---\n")

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
                tech_specialization,
              data = analysis_sample)

print(summary(model_4))

message("\nH2 Test: Interaction with geographic reach")
message("Expected: Stronger inverted-U for firms with higher cross-continental share")

# ----------------------------------------
# Model 5: H3 - Betweenness Centrality Moderation
# ----------------------------------------
message("\n--- Model 5: H3 - Betweenness Centrality Moderation ---\n")

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
                tech_specialization,
              data = analysis_sample)

print(summary(model_5))

message("\nH3 Test: Interaction with betweenness centrality")
message("Expected: Weaker negative effect of extreme diversity for high-betweenness firms")

# ----------------------------------------
# Model 6: Full Model (All Hypotheses)
# ----------------------------------------
message("\n--- Model 6: Full Model ---\n")

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
                tech_specialization,
              data = analysis_sample)

print(summary(model_6))

# ----------------------------------------
# Model Comparison
# ----------------------------------------
message("\n========================================")
message("      MODEL COMPARISON")
message("========================================\n")

model_comparison <- data.frame(
  Model = c("M1: Controls", "M2: Linear PSD", "M3: H1 (Inverted-U)",
            "M4: H2 (Geo)", "M5: H3 (Network)", "M6: Full"),
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

# ----------------------------------------
# Hypothesis Summary
# ----------------------------------------
message("\n========================================")
message("      HYPOTHESIS SUMMARY")
message("========================================\n")

# H1: Inverted U
h1_linear <- coef(summary(model_3))["partner_size_diversity", ]
h1_quad <- coef(summary(model_3))["partner_size_diversity_sq", ]

message("H1: Inverted U-shaped relationship")
message("  Linear term: coef = ", round(h1_linear["Estimate"], 4),
        ", p = ", round(h1_linear["Pr(>|t|)"], 4))
message("  Quadratic term: coef = ", round(h1_quad["Estimate"], 4),
        ", p = ", round(h1_quad["Pr(>|t|)"], 4))
message("  Support: ", ifelse(h1_quad["Estimate"] < 0 & h1_quad["Pr(>|t|)"] < 0.1,
                               "YES (negative quadratic)", "Insufficient evidence"))

# H2: Geographic moderation
if ("partner_size_diversity_sq:cross_continental_share" %in% rownames(coef(summary(model_4)))) {
  h2_int <- coef(summary(model_4))["partner_size_diversity_sq:cross_continental_share", ]
  message("\nH2: Geographic reach amplifies inverted-U")
  message("  Interaction: coef = ", round(h2_int["Estimate"], 4),
          ", p = ", round(h2_int["Pr(>|t|)"], 4))
  message("  Support: ", ifelse(h2_int["Estimate"] < 0 & h2_int["Pr(>|t|)"] < 0.1,
                                 "YES", "Insufficient evidence"))
}

# H3: Betweenness moderation
if ("partner_size_diversity_sq:betweenness" %in% rownames(coef(summary(model_5)))) {
  h3_int <- coef(summary(model_5))["partner_size_diversity_sq:betweenness", ]
  message("\nH3: Betweenness weakens negative effect")
  message("  Interaction: coef = ", round(h3_int["Estimate"], 4),
          ", p = ", round(h3_int["Pr(>|t|)"], 4))
  message("  Support: ", ifelse(h3_int["Estimate"] > 0 & h3_int["Pr(>|t|)"] < 0.1,
                                 "YES", "Insufficient evidence"))
}

# ----------------------------------------
# Save Results
# ----------------------------------------
message("\n========================================")
message("      SAVING RESULTS")
message("========================================\n")

# Save analysis sample
write_parquet(analysis_sample, cfg$analysis_sample_parquet, compression = "gzip")
write.csv(analysis_sample, cfg$analysis_sample_csv, row.names = FALSE)

# Save model comparison
write.csv(model_comparison, cfg$model_comparison_file, row.names = FALSE)

message("Analysis sample saved to: ", cfg$analysis_sample_parquet)
message("Model comparison saved to: ", cfg$model_comparison_file)
message("\nDone.")