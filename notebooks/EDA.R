library(tidyverse)
library(janitor)
library(igraph)
library(ggraph)
library(tidygraph)

setwd("C:/Users/Junior/OneDrive - TU Eindhoven/year 3/bus/GA")

sdc <- readRDS("data/SDC_data_2021.rds") %>%
  clean_names() %>%
  mutate(
    participants = participants %>%
      trimws() %>% na_if("") %>% na_if("NA") %>% na_if("<NA>")
  )

sdc_filtered <- sdc %>%
  filter(
    status == "Completed/Signed",
    type == "Strategic Alliance"
  )

continent_map <- list(
  Europe = c("United Kingdom","Germany","France","Italy","Spain","Netherlands",
             "Belgium","Sweden","Norway","Finland","Denmark","Austria",
             "Switzerland","Ireland","Portugal"),
  Asia = c("Japan","China","South Korea","India","Singapore","Hong Kong",
           "Taiwan","Malaysia","Philippines","Indonesia","Thailand"),
  North_America = c("United States","Canada","Mexico"),
  South_America = c("Brazil","Argentina","Chile","Colombia","Peru"),
  Oceania = c("Australia","New Zealand"),
  Africa = c("South Africa","Egypt","Nigeria","Kenya")
)

continent_lookup <- enframe(continent_map, name = "continent", value = "country") %>%
  unnest(country)

sdc_filtered_geo <- sdc_filtered %>%
  left_join(continent_lookup, by = c("participant_nation" = "country")) %>%
  filter(!is.na(participants)) %>%
  rename(firm = participants)

# Firm metadata
firm_info <- sdc_filtered_geo %>%
  select(firm, sic_primary, continent) %>%
  distinct()

edge_list <- sdc_filtered_geo %>%
  group_by(deal_number) %>%
  summarise(firms = list(unique(firm)), .groups = "drop") %>%
  mutate(n_firms = map_int(firms, length)) %>%
  filter(n_firms >= 2) %>%
  mutate(pairs = map(firms, ~ as.data.frame(t(combn(.x, 2))))) %>%
  unnest(pairs) %>%
  rename(from = V1, to = V2) %>%
  distinct()

clean_firm <- function(x) {
  x %>% iconv(to = "ASCII//TRANSLIT", sub = "") %>% trimws() %>% na_if("")
}

edges_final <- edge_list %>%
  mutate(from = clean_firm(from), to = clean_firm(to)) %>%
  drop_na() %>%
  distinct()

edges_with_info <- edges_final %>%
  left_join(firm_info, by = c("from" = "firm")) %>%
  rename(from_sic = sic_primary, from_region = continent) %>%
  left_join(firm_info, by = c("to" = "firm")) %>%
  rename(to_sic = sic_primary, to_region = continent)



make_region_table <- function(sic_code) {
  
  # Firms in this SIC
  firms_sic <- sdc_filtered_geo %>% 
    filter(sic_primary == sic_code)
  
  # --- Number of firms per region ---
  firms_per_region <- firms_sic %>%
    group_by(continent) %>%
    summarise(Number_of_Firms = n_distinct(firm), .groups = "drop")
  
  # --- Number of deals per region ---
  deals_per_region <- firms_sic %>%
    group_by(continent) %>%
    summarise(Number_of_Deals = n_distinct(deal_number), .groups = "drop")
  
  # Merge into one table
  region_table <- firms_per_region %>%
    left_join(deals_per_region, by = "continent") %>%
    arrange(desc(Number_of_Firms))
  
  region_table
}
table_3663 <- make_region_table(3663)
table_7379 <- make_region_table(7379)
table_3679 <- make_region_table(3679)
table_6211 <- make_region_table(6211)
table_3663
table_7379
table_3679
table_6211



plot_sic_bipartite <- function(data, sic_code, region = NULL, title_region = NULL) {
  
  # 1. Filter by SIC
  df <- data %>% filter(SIC_primary == sic_code)
  
  # 2. Filter by region if requested
  if (!is.null(region)) {
    df <- df %>% filter(participant_nation %in% region)
  }
  
  if (nrow(df) == 0) {
    message("No data for this SIC × region.")
    return(NULL)
  }
  
  # 3. Build bipartite incidence matrix (same method as your code)
  bip <- df %>%
    select(participants, deal_number) %>%
    as.matrix()
  
  g <- graph_from_data_frame(bip, directed = FALSE)
  
  # Assign bipartite types
  V(g)$type <- V(g)$name %in% bip[,2]
  
  # 4. Project onto participants
  g_proj <- bipartite_projection(g)$proj1
  
  # 5. Remove loops & duplicates
  g_proj <- simplify(g_proj, remove.multiple = TRUE, remove.loops = TRUE)
  
  # 6. Plot (same visual style as original)
  set.seed(2001525)
  plot(
    g_proj,
    vertex.color = "coral2",
    vertex.label = NA,
    vertex.size = 5,
    edge.width = 2,
    edge.color = adjustcolor("black", alpha.f = 0.3),
    main = paste0(
      "Network — SIC ", sic_code, 
      if (!is.null(title_region)) paste0(" (", title_region, ")"),
      "\nNodes: ", vcount(g_proj),
      " | Edges: ", ecount(g_proj)
    )
  )
  
  return(g_proj)
}

sdc_clean <- sdc_data %>%
  filter(
    status == "Completed/Signed",
    type == "Strategic Alliance"
  ) %>%
  mutate(participants = participants %>% trimws() %>% na_if(""))

g_7379_eu <- plot_sic_bipartite(
  data = sdc_clean,
  sic_code = 7379,
  region = continent_map$Europe,
  title_region = "Europe"
)
g_3663_asia <- plot_sic_bipartite(
  data = sdc_clean,
  sic_code = 3663,
  region = continent_map$Asia,
  title_region = "Asia"
)
g_3679_na <- plot_sic_bipartite(
  data = sdc_clean,
  sic_code = 3679,
  region = continent_map$Asia,
  title_region = "Asia"
)


g_6211_asia <- plot_sic_bipartite(
  data         = sdc_clean,
  sic_code     = 6211,
  region       = continent_map$Asia,
  title_region = "Asia"
)
