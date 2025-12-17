library(dplyr)
library(tidyr)
library(igraph)
library(purrr)
library(countrycode)
library(config)

source("rds/reporting_utils.R")

cfg <- config::get()

print_header("REGRESSION ANALYSIS PIPELINE")
print_section("Loading merged dataset...")

merged_data <- read.csv(cfg$merged_data_file, stringsAsFactors = FALSE)

print_metrics(list(
  "Merged data rows" = nrow(merged_data),
  "Unique companies" = n_distinct(merged_data$participants_std),
  "Unique deals" = n_distinct(merged_data$deal_number)
))

orbis_cols <- c(
  "employees_last_value", "total_assets_eur", "n_shareholders",
  "n_subsidiaries", "n_companies_group", "country_iso", "is_guo"
)
available_orbis <- intersect(orbis_cols, names(merged_data))
print_info(sprintf("Available ORBIS columns: %s", paste(available_orbis, collapse = ", ")))

print_section("Building alliance network...")

if (!requireNamespace("nstandr", quietly = TRUE)) {
  devtools::install_github("stasvlasov/nstandr")
}
library(nstandr)

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

firm_graph <- graph_from_data_frame(edges, directed = FALSE)
firm_graph <- simplify(firm_graph)

message("Network nodes: ", vcount(firm_graph))
message("Network edges: ", ecount(firm_graph))

message("\nComputing network metrics...")

network_metrics <- data.frame(
  participants_std = V(firm_graph)$name,
  degree = degree(firm_graph),
  betweenness = betweenness(firm_graph, normalized = TRUE),
  closeness = closeness(firm_graph, normalized = TRUE),
  eigenvector = eigen_centrality(firm_graph)$vector,
  stringsAsFactors = FALSE
)

message("\nComputing geographic reach (Moderator for H2)...")

iso_custom <- c("Utd Arab Em" = "ARE", "Hong Kong" = "HKG", "Taiwan" = "TWN")

merged_with_continent <- merged_data %>%
  mutate(
    participant_iso = countrycode(participant_nation,
      origin = "country.name",
      destination = "iso3c",
      custom_match = iso_custom
    ),
    continent = countrycode(participant_iso,
      origin = "iso3c",
      destination = "continent"
    )
  ) %>%
  mutate(continent = replace_na(continent, "Other"))

deal_continents <- merged_with_continent %>%
  group_by(deal_number) %>%
  summarise(
    n_continents_in_deal = n_distinct(continent),
    is_cross_continental = n_distinct(continent) > 1,
    .groups = "drop"
  )

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

message("\nAggregating to firm-level for regression analysis...")

safe_first <- function(x) {
  if (length(x) == 0 || all(is.na(x))) {
    return(NA)
  }
  first(x)
}

firm_level_data <- merged_data %>%
  group_by(participants_std) %>%
  summarise(
    company_name = safe_first(participants_original),
    total_applications = safe_first(total_applications),
    granted_patents = safe_first(granted_patents),
    employees_last_value = if ("employees_last_value" %in% names(merged_data)) safe_first(employees_last_value) else NA_real_,
    total_assets_eur = if ("total_assets_eur" %in% names(merged_data)) safe_first(total_assets_eur) else NA_real_,
    total_assets_usd = if ("total_assets_usd" %in% names(merged_data)) safe_first(total_assets_usd) else NA_real_,
    n_shareholders = if ("n_shareholders" %in% names(merged_data)) safe_first(n_shareholders) else NA_real_,
    n_subsidiaries = if ("n_subsidiaries" %in% names(merged_data)) safe_first(n_subsidiaries) else NA_real_,
    n_companies_group = if ("n_companies_group" %in% names(merged_data)) safe_first(n_companies_group) else NA_real_,
    country_iso = if ("country_iso" %in% names(merged_data)) safe_first(country_iso) else NA_character_,
    is_guo = if ("is_guo" %in% names(merged_data)) safe_first(is_guo) else NA_integer_,
    is_controlled_sub = if ("is_controlled_sub" %in% names(merged_data)) safe_first(is_controlled_sub) else NA_integer_,
    roa = if ("roa" %in% names(merged_data)) safe_first(roa) else NA_real_,
    rd_ratio = if ("rd_ratio" %in% names(merged_data)) safe_first(rd_ratio) else NA_real_,
    long_term_debt = if ("long_term_debt" %in% names(merged_data)) safe_first(long_term_debt) else NA_real_,
    size_classification = if ("size_classification" %in% names(merged_data)) safe_first(size_classification) else NA_character_,
    tech_specialist_hhi = safe_first(tech_specialist_hhi),
    avg_ipc_scope = safe_first(avg_ipc_scope),
    grant_success_rate = safe_first(grant_success_rate),
    n_deals = n_distinct(deal_number),
    orbis_match_type = safe_first(orbis_match_type),
    patent_match_type = safe_first(patent_match_type),
    .groups = "drop"
  )

message("Firm-level observations: ", nrow(firm_level_data))


message("\nComputing partner size diversity (Main IV for H1)...")

emp_lookup <- firm_level_data %>%
  filter(!is.na(employees_last_value)) %>%
  select(participants_std, employees_last_value) %>%
  distinct()

message("Firms with employee data: ", nrow(emp_lookup))

compute_partner_size_diversity <- function(focal_name, graph, emp_df) {
  if (!focal_name %in% V(graph)$name) {
    return(NA_real_)
  }

  neighbors_names <- neighbors(graph, focal_name, mode = "all")$name
  if (length(neighbors_names) == 0) {
    return(NA_real_)
  }

  partner_sizes <- emp_df$employees_last_value[emp_df$participants_std %in% neighbors_names]
  partner_sizes <- partner_sizes[!is.na(partner_sizes)]

  if (length(partner_sizes) == 0) {
    return(NA_real_)
  }
  if (length(partner_sizes) == 1) {
    return(0)
  }

  cv <- sd(partner_sizes) / mean(partner_sizes)
  return(cv)
}

firm_level_data$partner_size_diversity <- vapply(
  firm_level_data$participants_std,
  function(x) compute_partner_size_diversity(x, firm_graph, emp_lookup),
  FUN.VALUE = numeric(1)
)

message("Firms with partner diversity computed: ", sum(!is.na(firm_level_data$partner_size_diversity)))

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

partner_metrics_df <- do.call(rbind, lapply(firm_level_data$participants_std, function(x) {
  as.data.frame(compute_partner_metrics(x, firm_graph, emp_lookup))
}))

firm_level_data <- cbind(firm_level_data, partner_metrics_df)

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

message("\nPreparing regression variables...")

firm_level_data <- firm_level_data %>%
  mutate(
    partner_size_diversity_sq = partner_size_diversity^2,
    ln_employees = log(replace_na(employees_last_value, 0) + 1),
    ln_total_assets = log(replace_na(total_assets_eur, 0) + 1),
    ln_avg_partner_size = log(replace_na(avg_partner_size, 0) + 1),
    ln_long_term_debt = log(replace_na(long_term_debt, 0) + 1),
    alliance_experience = n_deals,
    tech_specialization = replace_na(tech_specialist_hhi, 0),
    ln_subsidiaries = log(replace_na(n_subsidiaries, 0) + 1),
    ln_shareholders = log(replace_na(n_shareholders, 0) + 1),
    is_guo = replace_na(is_guo, 0),
    is_controlled_sub = replace_na(is_controlled_sub, 0),
    roa_clean = replace_na(roa, 0),
    rd_ratio_clean = replace_na(rd_ratio, 0)
  )

message("\nCreating analysis sample...")

analysis_sample <- firm_level_data %>%
  filter(
    !is.na(partner_size_diversity),
    !is.na(employees_last_value),
    n_partners > 0
  )

message("Analysis sample size: ", nrow(analysis_sample))
message("Firms with patents (total_applications > 0): ", sum(analysis_sample$total_applications > 0))

if (nrow(analysis_sample) < 10) {
  warning("Analysis sample is very small (", nrow(analysis_sample), " observations).")
  warning("Results may not be reliable. Consider relaxing filter criteria.")
}

print_header("DESCRIPTIVE STATISTICS")

desc_vars <- c(
  "total_applications", "partner_size_diversity",
  "cross_continental_share", "betweenness", "degree",
  "employees_last_value", "n_partners", "n_deals",
  "roa", "rd_ratio", "total_assets_eur"
)

desc_vars <- intersect(desc_vars, names(analysis_sample))

desc_stats <- analysis_sample %>%
  select(all_of(desc_vars)) %>%
  summarise(across(everything(), list(
    n = ~ sum(!is.na(.)),
    mean = ~ mean(., na.rm = TRUE),
    sd = ~ sd(., na.rm = TRUE),
    min = ~ min(., na.rm = TRUE),
    max = ~ max(., na.rm = TRUE)
  ))) %>%
  pivot_longer(everything(), names_to = c("variable", "stat"), names_sep = "_(?=[^_]+$)") %>%
  pivot_wider(names_from = stat, values_from = value)

print_summary_table(desc_stats, "Descriptive Statistics for Key Variables")

print_subheader("Correlation Matrix (Key Variables)")

cor_vars <- c(
  "total_applications", "partner_size_diversity",
  "cross_continental_share", "betweenness", "ln_employees", "degree",
  "roa_clean", "rd_ratio_clean"
)
cor_vars <- intersect(cor_vars, names(analysis_sample))

cor_matrix <- analysis_sample %>%
  select(all_of(cor_vars)) %>%
  cor(use = "pairwise.complete.obs")

print_summary_table(as.data.frame(round(cor_matrix, 3)), "Correlation Matrix")

print_header("REGRESSION ANALYSIS")

message("\n--- Model 1: Base Controls ---\n")

model_1 <- lm(
  total_applications ~
    ln_employees +
    ln_total_assets +
    degree +
    alliance_experience +
    tech_specialization +
    roa_clean +
    rd_ratio_clean,
  data = analysis_sample
)

print(summary(model_1))

message("\n--- Model 2: Linear Partner Size Diversity ---\n")

model_2 <- lm(
  total_applications ~
    partner_size_diversity +
    ln_employees +
    ln_total_assets +
    degree +
    alliance_experience +
    tech_specialization +
    roa_clean +
    rd_ratio_clean,
  data = analysis_sample
)

print(summary(model_2))

message("\n--- Model 3: H1 - Inverted U-Shape (Quadratic Term) ---")
message("H1: There is an inverted U-shaped relationship between partner size")
message("    diversity and innovation output.\n")

model_3 <- lm(
  total_applications ~
    partner_size_diversity +
    partner_size_diversity_sq +
    ln_employees +
    ln_total_assets +
    degree +
    alliance_experience +
    tech_specialization +
    roa_clean +
    rd_ratio_clean,
  data = analysis_sample
)

print(summary(model_3))

coef_psd <- coef(model_3)["partner_size_diversity"]
coef_psd_sq <- coef(model_3)["partner_size_diversity_sq"]
message("\nH1 Test Results:")
message("  Linear coefficient (partner_size_diversity): ", round(coef_psd, 4))
message("  Quadratic coefficient (partner_size_diversity_sq): ", round(coef_psd_sq, 4))
message("  Expected: Positive linear, negative quadratic for inverted U-shape")

if (!is.na(coef_psd_sq) && coef_psd_sq < 0 && coef_psd > 0) {
  optimal_psd <- -coef_psd / (2 * coef_psd_sq)
  message("  Optimal partner size diversity (turning point): ", round(optimal_psd, 3))
  message("  Interpretation: Innovation peaks at CV = ", round(optimal_psd, 3))
}

message("\n--- Model 4: H2 - Geographic Reach Moderation ---")
message("H2: The inverted U-shaped relationship is STRONGER for firms with")
message("    higher share of cross-continental alliances.\n")

model_4 <- lm(
  total_applications ~
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
  data = analysis_sample
)

print(summary(model_4))

message("\nH2 Test Results:")
message("  Expected: Negative interaction (PSD_sq × cross_continental_share)")
message("  Meaning: Geographic distance amplifies coordination costs at extreme diversity")


message("\n--- Model 5: H3 - Betweenness Centrality Moderation ---")
message("H3: Firms with higher betweenness centrality experience a WEAKER")
message("    negative effect of extreme partner size diversity.\n")

model_5 <- lm(
  total_applications ~
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
  data = analysis_sample
)

print(summary(model_5))

message("\nH3 Test Results:")
message("  Expected: Positive interaction (PSD_sq × betweenness)")
message("  Meaning: Brokerage position buffers against coordination costs")
message("\n--- Model 6: Full Model (All Hypotheses) ---\n")

model_6 <- lm(
  total_applications ~
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
  data = analysis_sample
)

print(summary(model_6))

message("\n========================================")
message("      MODEL COMPARISON")
message("========================================\n")

model_comparison <- data.frame(
  Model = c(
    "M1: Controls", "M2: Linear PSD", "M3: H1 (Inverted-U)",
    "M4: H2 (Geographic)", "M5: H3 (Network)", "M6: Full"
  ),
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
  AIC = c(
    AIC(model_1), AIC(model_2), AIC(model_3),
    AIC(model_4), AIC(model_5), AIC(model_6)
  ),
  BIC = c(
    BIC(model_1), BIC(model_2), BIC(model_3),
    BIC(model_4), BIC(model_5), BIC(model_6)
  ),
  N = c(
    nobs(model_1), nobs(model_2), nobs(model_3),
    nobs(model_4), nobs(model_5), nobs(model_6)
  )
)

print_model_comparison(model_comparison)

print_header("HYPOTHESIS SUMMARY")

h1_linear <- coef(summary(model_3))["partner_size_diversity", ]
h1_quad <- coef(summary(model_3))["partner_size_diversity_sq", ]

print_hypothesis_test(
  "H1: Inverted U-Shape Relationship",
  "Partner size diversity has an inverted U-shaped relationship with innovation output",
  h1_quad["Estimate"],
  h1_quad["Std. Error"],
  h1_quad["Pr(>|t|)"],
  expected_sign = "negative"
)

if ("partner_size_diversity_sq:cross_continental_share" %in% rownames(coef(summary(model_4)))) {
  h2_int <- coef(summary(model_4))["partner_size_diversity_sq:cross_continental_share", ]
  print_hypothesis_test(
    "H2: Geographic Reach Moderation",
    "Cross-continental alliances strengthen the inverted-U relationship",
    h2_int["Estimate"],
    h2_int["Std. Error"],
    h2_int["Pr(>|t|)"],
    expected_sign = "negative"
  )
}

if ("partner_size_diversity_sq:betweenness" %in% rownames(coef(summary(model_5)))) {
  h3_int <- coef(summary(model_5))["partner_size_diversity_sq:betweenness", ]
  print_hypothesis_test(
    "H3: Network Position Moderation",
    "Betweenness centrality weakens the negative effect of extreme diversity",
    h3_int["Estimate"],
    h3_int["Std. Error"],
    h3_int["Pr(>|t|)"],
    expected_sign = "positive"
  )
}

print_header("SAVING RESULTS")

write.csv(analysis_sample, cfg$analysis_sample_file, row.names = FALSE)
write.csv(model_comparison, cfg$model_comparison_file, row.names = FALSE)

all_models <- list(
  model_1 = model_1,
  model_2 = model_2,
  model_3_H1 = model_3,
  model_4_H2 = model_4,
  model_5_H3 = model_5,
  model_6_Full = model_6
)
saveRDS(all_models, cfg$regression_models_file)

print_success(sprintf("Analysis sample saved to: %s", cfg$analysis_sample_file))
print_success(sprintf("Model comparison saved to: %s", cfg$model_comparison_file))
print_success(sprintf("Regression models saved to: %s", cfg$regression_models_file))
print_success("Regression analysis pipeline completed!")
