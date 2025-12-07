library(tidyverse)
library(janitor)
library(igraph)
library(purrr)
library(ggplot2)
library(readr)
library(ggraph)
library(tidygraph)

sdc <- readRDS("data/SDC_data_2021.rds") %>%
  clean_names() %>%
  mutate(
    participants = participants %>% trimws() %>% na_if("") %>% na_if("NA") %>% na_if("<NA>")
  )

sdc_expanded <- sdc %>%
  filter(!is.na(participants)) %>%
  rename(firm = participants) %>%
  select(deal_number, firm, participant_nation, sic_primary, everything())

edge_list <- sdc_expanded %>%
  group_by(deal_number) %>%
  summarise(firms = list(unique(firm)), .groups = "drop") %>%
  mutate(n_firms = map_int(firms, length)) %>%
  filter(n_firms >= 2) %>%
  mutate(pairs = map(firms, ~ as.data.frame(t(combn(.x, 2))))) %>%
  unnest(pairs) %>%
  rename(from = V1, to = V2) %>%
  distinct()

clean_firm <- function(x) {
  x %>%
    iconv(to = "ASCII//TRANSLIT", sub = "") %>%
    trimws() %>%
    na_if("")
}

edges_final <- edge_list %>%
  mutate(
    from = clean_firm(from),
    to   = clean_firm(to)
  ) %>%
  drop_na() %>%
  distinct()

g <- graph_from_data_frame(edges_final, directed = FALSE) %>%
  simplify(remove.multiple = TRUE, remove.loops = TRUE)

centrality_df <- tibble(
  firm        = names(V(g)),
  degree      = degree(g),
  closeness   = closeness(g, normalized = TRUE),
  betweenness = betweenness(g, normalized = TRUE)
)

cl <- cluster_louvain(g)

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

sdc_geo <- sdc_expanded %>%
  left_join(continent_lookup, by = c("participant_nation" = "country"))

firm_info <- sdc_geo %>%
  select(firm, continent, sic_primary) %>%
  distinct()

edges_with_info <- edges_final %>%
  left_join(firm_info, by = c("from" = "firm")) %>%
  rename(from_region = continent, from_sic = sic_primary) %>%
  left_join(firm_info, by = c("to" = "firm")) %>%
  rename(to_region = continent, to_sic = sic_primary)

plot_sic_network <- function(sic_code, region = NULL, sdc_data, edges) {
  firms_sic <- sdc_data %>% filter(sic_primary == sic_code)
  if (!is.null(region)) {
    firms_sic <- firms_sic %>% filter(continent %in% region)
  }
  focal_firms <- firms_sic$firm
  edges_sic <- edges %>% filter(from %in% focal_firms | to %in% focal_firms)
  if (nrow(edges_sic) == 0) return(NULL)
  g_sic <- graph_from_data_frame(edges_sic, directed = FALSE) %>% simplify()
  cl_sic <- cluster_louvain(g_sic)
  tg <- as_tbl_graph(g_sic) %>% mutate(community = factor(cl_sic$membership))
  set.seed(123)
  ggraph(tg, layout = "fr") +
    geom_edge_link(alpha = 0.08, colour = "grey40") +
    geom_node_point(aes(color = community), size = 2) +
    scale_color_viridis_d(option = "turbo") +
    theme_void() +
    theme(legend.position = "none") +
    ggtitle(paste0("Organizational Alliance Network — SIC ", sic_code,
                   if (!is.null(region)) paste0(" (", region, ")")))
}

plot_sic_network(7379, region = "Asia", sdc_data = sdc_geo, edges = edges_with_info)
plot_sic_network(3669, region = "Asia", sdc_data = sdc_geo, edges = edges_with_info)
plot_sic_network(3699, sdc_data = sdc_geo, edges = edges_with_info)
