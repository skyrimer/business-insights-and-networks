# David vs. Goliath: Partner Size Diversity and Innovation

## Project Overview

This project investigates the relationship between partner size diversity in strategic alliances and firm innovation outcomes. The analysis examines whether there's an inverted U-shaped relationship between the diversity of partner sizes and innovation output, and how this relationship is moderated by geographic reach and network position.

The pipeline integrates three major data sources:
1. **SDC Platinum** - Strategic alliance data
2. **Orbis** - Firm-level financial and organizational data
3. **PatentsView** - Patent application and grant data

---

## Execution Order

**⚠️ IMPORTANT**: Files must be executed in this exact order due to data dependencies.

```
1. sdc_data.R
2. orbis_data.R
3. patents_data.R
4. merge_regression_dataframe.R
5. regression_analysis.R
```

---

## Requirements

### R Version
- R >= 4.0.0

### Required R Packages

```r
# Core data manipulation
install.packages("dplyr")
install.packages("tidyr")
install.packages("purrr")

# Data I/O
install.packages("readxl")
install.packages("arrow")
install.packages("fs")

# String operations and matching
install.packages("stringdist")
install.packages("stringr")
install.packages("janitor")

# Network analysis
install.packages("igraph")

# Geographic data
install.packages("countrycode")

# Configuration management
install.packages("config")

# Special package: Name standardization (from GitHub)
if (!requireNamespace("devtools", quietly = TRUE)) {
  install.packages("devtools")
}
devtools::install_github("stasvlasov/nstandr")
```

### Required Data Files

Place the following files in the specified directories:

```
raw_downloads/
├── SDC_data_2021.rds                      # Strategic alliance data
├── Orbis_155_comp_extra_fields.xlsx       # Firm financial data
└── patents_view/                          # Directory containing patent CSV files
    ├── *.csv                              # Multiple CSV files from PatentsView
```

### Configuration File

The pipeline uses `config.yml` in the project root for centralized path and parameter management.

---

## Configuration (config.yml)

The configuration file controls key parameters and file paths:

### Analysis Parameters
- `focal_industry_sic_code`: Primary SIC code for focal industry (default: 3571 - Electronic Computers)
- `continent`: Target continent for focal firms (default: "Asia")
- `min_date`: Minimum date for alliance filtering (default: "1999-12-31")

### File Paths
All input and output paths are managed through config.yml. To change data locations or output directories, simply edit the config file rather than modifying individual R scripts.

---

## File Descriptions

### 1. sdc_data.R

**Purpose**: Filter and process strategic alliance data to identify focal firms and their alliance network.

**Inputs**:
- `raw_downloads/SDC_data_2021.rds` - Raw SDC Platinum data

**Outputs**:
- `data/filtered_sdc.csv` - Filtered alliance data
- Network visualization plot

**Methodology**:

1. **Data Loading & Cleaning**
   - Load RDS file and clean column names using `janitor::clean_names()`
   - Remove rows with missing participant names

2. **Filtering Criteria**
   - Status: "Completed/Signed"
   - Type: "Strategic Alliance"
   - Date announced: > min_date (from config)
   - Exclude terminated alliances

3. **Geographic Mapping**
   - Map country names to continents using `countrycode` package
   - Handle special cases (Hong Kong, Taiwan, etc.)

4. **Focal Firm Identification**
   - Identify firms matching: `target_sic` AND `target_continent`
   - Extract all deals involving these focal firms
   - Expand to include ALL participants in those deals (may include non-focal firms)

5. **Network Construction**
   - Create firm-firm edge list from multi-participant deals
   - Generate pairwise combinations using `combn()`
   - Visualize network with focal firms highlighted in red

**Key Variables**:
- `focal_firms`: Companies matching SIC and continent criteria
- `relevant_deals`: Deal IDs involving focal firms
- `participants_std`: Standardized company names

---

### 2. orbis_data.R

**Purpose**: Clean and standardize Orbis firm-level financial data with missing value imputation.

**Inputs**:
- `raw_downloads/Orbis_155_comp_extra_fields.xlsx` - Raw Orbis export

**Outputs**:
- `data/orbis_cleaned.csv` - Cleaned firm data with standardized names

**Methodology**:

1. **Header Normalization**
   - Handle messy Excel column headers
   - Map verbose Orbis column names to concise standard names
   - Remove empty/unnamed columns

2. **Column Mapping**
   - 18 key variables mapped (employees, assets, R&D, shareholders, etc.)
   - Convert NACE codes, ISO country codes, entity types

3. **Data Type Conversion**
   - Convert "n.a." strings to proper NA values
   - Transform 10 numeric columns from character to numeric

4. **Missing Value Imputation**
   - **Method**: Median imputation for all numeric variables
   - **Rationale**: Robust to outliers, preserves distribution shape
   - **Tracking**: Create `employees_imputed` and `assets_imputed` flags

5. **Name Standardization**
   - Apply Magerman method via `nstandr::standardize_magerman()`
   - Remove legal suffixes (Ltd, Inc, Corp, etc.)
   - Normalize punctuation and spacing

6. **Feature Engineering**
   - Create `is_guo` indicator (Global Ultimate Owner)
   - Factor encoding for entity types

**Key Variables Created**:
- `company_name_std`: Standardized company name
- `employees_last_value`: Employee count (imputed if missing)
- `total_assets_eur`: Total assets in EUR thousands
- `rnd_over_operating`: R&D intensity ratio
- `is_guo`: Binary indicator for global ultimate owner status

---

### 3. patents_data.R

**Purpose**: Aggregate patent-level data to create firm-level innovation metrics.

**Inputs**:
- `raw_downloads/patents_view/*.csv` - Multiple CSV files from PatentsView

**Outputs**:
- `data/firm_patent_variables.parquet` - Firm-level patent metrics

**Methodology**:

1. **Batch Loading**
   - Read all CSV files from patents_view directory
   - Use `map_dfr()` for row-binding with progress indicator
   - Initial loading as character type for safety

2. **Data Cleaning**
   - Select 10 core variables
   - Filter out unknown assignees
   - Remove extreme values (n_ipc >= 50, n_cpc >= 50)
   - Convert application/grant years to integers

3. **Geographic Standardization**
   - Map ISO2 country codes to full country names
   - Handle missing countries as "Unknown country"

4. **Technology Classification (IPC One-Hot Encoding)**
   - Parse IPC sections (A-H representing different tech domains)
   - Create binary indicators for 8 IPC sections:
     - A: Human Necessities
     - B: Operations & Transport
     - C: Chemistry & Metallurgy
     - D: Textiles & Paper
     - E: Fixed Constructions
     - F: Mechanical Engineering
     - G: Physics (key for computing)
     - H: Electricity

5. **Firm-Level Aggregation**
   - Group by assignee (firm name)
   - Compute 8 base metrics per firm

6. **Innovation Metrics**
   - `total_applications`: Count of unique patent applications
   - `granted_patents`: Count of granted patents
   - `grant_success_rate`: Ratio of granted to total applications
   - `avg_ipc_scope`: Mean number of IPC codes per patent (breadth)
   - `avg_team_size`: Mean inventor team size

7. **Technology Specialization (HHI)**
   - Calculate share of each IPC section (A-H)
   - Compute Herfindahl-Hirschman Index (HHI):
     ```
     HHI = Σ(share_i²) for i in {A,B,C,D,E,F,G,H}
     ```
   - Higher HHI = more specialized
   - Lower HHI = more diversified technology portfolio

**Key Variables Created**:
- `total_applications`: Total patent applications
- `granted_patents`: Number of patents granted
- `grant_success_rate`: Success rate of applications
- `avg_ipc_scope`: Average technological breadth
- `tech_specialist_hhi`: Technology specialization index (0-1)
- `total_ipc_mentions`: Total IPC code mentions across all patents

---

### 4. merge_regression_dataframe.R

**Purpose**: Merge SDC, Orbis, and patent data using sophisticated fuzzy matching algorithms.

**Inputs**:
- `data/firm_patent_variables.parquet` - Patent data
- `data/filtered_sdc.csv` - Alliance data
- `data/orbis_cleaned.csv` - Firm financial data

**Outputs**:
- `data/sdc_patents_orbis_merged.parquet` - Merged dataset (compressed)
- `data/sdc_patents_orbis_merged.csv` - Merged dataset (CSV)

**Methodology**:

1. **Name Standardization**
   - Apply Magerman standardization to all three datasets
   - Create 2-character and 3-character prefix variables for blocking

2. **Company-Level SDC Summary**
   - Aggregate deal-level data to company level
   - Count deals per company
   - Extract first/last deal dates
   - Concatenate countries

3. **Two-Phase Matching Strategy**

   **Phase 1: Exact Matching**
   - Direct join on standardized company names
   - Fast, high precision

   **Phase 2: Hybrid Fuzzy Matching**
   - Applied to unmatched companies from Phase 1

   **Blocking Strategy**:
   - First try: 3-character prefix match
   - Fallback: 2-character prefix match
   - Rationale: Reduces O(n²) comparisons dramatically

   **Similarity Scoring**:
   ```
   Final Score = 0.5 × JW + 0.3 × LV + 0.2 × Jaccard
   ```

   Where:
   - **JW (Jaro-Winkler)**: Character-level similarity (50% weight)
     - Emphasizes prefix similarity
     - Good for typos and abbreviations

   - **LV (Levenshtein)**: Edit distance normalized (30% weight)
     - Counts insertions, deletions, substitutions
     - Normalized by max string length

   - **Jaccard**: Token set similarity (20% weight)
     - Splits names into words
     - Intersection over union of word sets
     - Robust to word order changes

   **Matching Threshold**: 0.85 (adjustable)

4. **Patent Matching**
   - Exact matches: Direct join on standardized names
   - Fuzzy matches: Hybrid algorithm with blocking
   - Fill unmatched with zeros (valid for patent counts)

5. **Orbis Matching**
   - Same two-phase approach
   - 16 firm characteristic variables merged
   - Preserve NA for truly missing financial data

6. **Match Quality Tracking**
   - `patent_match_type`: "exact" | "fuzzy" | "unmatched"
   - `orbis_match_type`: "exact" | "fuzzy" | "unmatched"
   - Report match rates and score distributions

**Key Output Variables**:
- All SDC variables (deals, dates, SIC codes)
- All patent variables (applications, grants, tech specialization)
- All Orbis variables (financials, structure, ownership)
- Match quality indicators

**Performance Notes**:
- Progress indicators for fuzzy matching loops
- Typical runtime: 5-15 minutes depending on dataset size

---

### 5. regression_analysis.R

**Purpose**: Construct alliance networks, engineer features, and test research hypotheses through regression models.

**Inputs**:
- `data/sdc_patents_orbis_merged.parquet` - Merged firm data
- `data/filtered_sdc.csv` - Alliance data (for network construction)

**Outputs**:
- `data/analysis_sample.parquet` - Regression-ready dataset
- `data/analysis_sample.csv` - Same, in CSV format
- `data/model_comparison.csv` - Model fit statistics

**Methodology**:

### Part 1: Network Construction & Metrics

1. **Alliance Network Graph**
   - Nodes: Firms (standardized names)
   - Edges: Co-participation in strategic alliances
   - Create firm-firm edge list from multi-participant deals
   - Build undirected graph using `igraph`
   - Simplify to remove multiple edges

2. **Network Position Metrics**
   - **Degree**: Number of direct alliance partners
   - **Betweenness**: Normalized bridging centrality (0-1)
     - Measures how often a firm lies on shortest paths between others
     - High betweenness = structural broker
   - **Closeness**: Normalized proximity to all other firms (0-1)
     - Inverse of average distance to all nodes
   - **Eigenvector**: Influence based on partner importance
     - Recursive measure: connected to well-connected firms

### Part 2: Geographic Reach Calculation

3. **Cross-Continental Alliance Activity**
   - Map participant countries to continents
   - For each deal: Count distinct continents involved
   - Flag deals as cross-continental (> 1 continent)
   - For each firm compute:
     - `n_cross_continental_deals`: Count of multi-continent deals
     - `cross_continental_share`: Proportion of deals spanning continents
     - `n_continents_reached`: Number of unique continents accessed

### Part 3: Partner Size Diversity (Main Independent Variable)

4. **Coefficient of Variation (CV) Method**
   ```
   CV = SD(partner_sizes) / Mean(partner_sizes)
   ```

   - For each focal firm:
     - Identify all alliance partners (graph neighbors)
     - Extract employee counts for each partner
     - Compute CV as standardized dispersion measure

   - **Rationale for CV**:
     - Scale-independent (unlike standard deviation)
     - Captures relative diversity
     - 0 = all partners same size
     - Higher values = more size heterogeneity

5. **Additional Partner Metrics**
   - `n_partners`: Count of alliance partners
   - `avg_partner_size`: Mean partner employee count
   - `max_partner_size`: Largest partner
   - `min_partner_size`: Smallest partner
   - `partner_size_range`: Absolute difference (max - min)

### Part 4: Control Variables

6. **Firm Characteristics**
   - `ln_employees`: Log(employees + 1) - firm size
   - `ln_total_assets`: Log(assets + 1) - financial capacity
   - `firm_age`: Years since first alliance
   - `rnd_intensity`: R&D / Operating revenue
   - `alliance_experience`: Total number of alliances

7. **Technology Profile**
   - `tech_specialization`: HHI from patent data
   - Controls for focused vs. diversified innovation

8. **Quadratic Term**
   - `partner_size_diversity_sq`: Squared CV
   - Tests inverted U-shaped relationship

### Part 5: Regression Models

**Dependent Variable**: `total_applications` (patent applications as innovation output)

**Model 1: Base Controls**
```
total_applications ~ ln_employees + ln_total_assets + degree +
                     alliance_experience + tech_specialization
```
- Establishes baseline explanatory power

**Model 2: Linear Effect**
```
Model 1 + partner_size_diversity
```
- Tests simple linear relationship

**Model 3: H1 - Inverted U-Shape**
```
Model 2 + partner_size_diversity_sq
```
- **Hypothesis 1**: Partner size diversity has inverted U-shaped effect
- **Expected**: Positive linear term, negative quadratic term
- **Interpretation**:
  - Too little diversity = limited resource access
  - Optimal diversity = maximum learning and innovation
  - Too much diversity = coordination costs dominate

**Model 4: H2 - Geographic Reach Moderation**
```
Model 3 + cross_continental_share +
          partner_size_diversity:cross_continental_share +
          partner_size_diversity_sq:cross_continental_share
```
- **Hypothesis 2**: Geographic reach amplifies the inverted-U
- **Expected**: Negative interaction with quadratic term
- **Interpretation**: Firms with broader geographic reach face steeper costs from extreme diversity

**Model 5: H3 - Betweenness Centrality Moderation**
```
Model 3 + betweenness +
          partner_size_diversity:betweenness +
          partner_size_diversity_sq:betweenness
```
- **Hypothesis 3**: Structural brokerage weakens negative effect of extreme diversity
- **Expected**: Positive interaction with quadratic term
- **Interpretation**: High-betweenness firms can better manage diverse portfolios due to information flow advantages

**Model 6: Full Model**
```
All variables + all interactions from Models 4 & 5
```
- Tests all hypotheses simultaneously
- Controls for both moderators

### Part 6: Model Diagnostics

9. **Descriptive Statistics**
   - Summary stats for all key variables
   - Sample size reporting
   - Distribution checks

10. **Correlation Matrix**
    - Examine multicollinearity among predictors
    - Key relationships between constructs

11. **Model Comparison**
    - R² and Adjusted R²
    - AIC and BIC (information criteria)
    - Sample size per model
    - Incremental explanatory power

12. **Hypothesis Testing**
    - Extract coefficients and p-values
    - Test directional predictions
    - Report significance at p < 0.10 threshold
    - Calculate optimal diversity turning point (if inverted-U confirmed)

**Analysis Sample Filtering**:
- Requires non-missing partner size diversity
- Requires non-missing employee data (for size controls)
- Requires at least one alliance partner (n_partners > 0)

---

## Research Hypotheses

### H1: Inverted U-Shaped Relationship
Partner size diversity has an inverted U-shaped relationship with innovation output. Moderate diversity optimizes learning and resource access, while extreme diversity creates coordination costs.

**Test**: Significant negative coefficient on `partner_size_diversity_sq` in Model 3

### H2: Geographic Reach Moderation
Firms with greater cross-continental alliance activity experience a stronger inverted-U relationship, as geographic complexity amplifies coordination challenges.

**Test**: Significant negative interaction between `partner_size_diversity_sq` and `cross_continental_share` in Model 4

### H3: Network Position Moderation
Firms with high betweenness centrality (structural brokers) face weaker negative effects from extreme diversity due to superior information processing capabilities.

**Test**: Significant positive interaction between `partner_size_diversity_sq` and `betweenness` in Model 5

---

## Output Files

| File | Format | Description |
|------|--------|-------------|
| `filtered_sdc.csv` | CSV | Filtered strategic alliance data for focal industry |
| `orbis_cleaned.csv` | CSV | Cleaned firm financial data with imputed values |
| `firm_patent_variables.parquet` | Parquet | Aggregated patent metrics by firm |
| `sdc_patents_orbis_merged.parquet` | Parquet | Merged dataset (all sources, compressed) |
| `sdc_patents_orbis_merged.csv` | CSV | Merged dataset (human-readable) |
| `analysis_sample.parquet` | Parquet | Regression-ready data with network metrics |
| `analysis_sample.csv` | CSV | Analysis sample (human-readable) |
| `model_comparison.csv` | CSV | Model fit statistics and comparisons |

---

## Running the Pipeline

### Full Pipeline
```r
# Execute in order
source("src/sdc_data.R")
source("src/orbis_data.R")
source("src/patents_data.R")
source("src/merge_regression_dataframe.R")
source("src/regression_analysis.R")
```

### Individual Scripts
Each script can be run independently if the required input files exist:
```r
source("src/sdc_data.R")
```

### Modifying Parameters
Edit `config.yml` to change:
- Focal industry (SIC code)
- Geographic focus (continent)
- Date range (min_date)
- Input/output file paths

---

## Key Methodological Decisions

### Name Matching Strategy
- **Magerman Standardization**: Industry-standard method for company name normalization
- **Hybrid Scoring**: Combines three complementary string similarity metrics
- **Blocking**: Reduces computational complexity from O(n²) to O(n×k) where k << n
- **Threshold**: 0.85 balances precision and recall based on validation

### Missing Data Handling
- **Patents**: Fill with zeros (absence = no patents is meaningful)
- **Orbis Financials**: Median imputation (preserves distribution, robust to outliers)
- **Network Metrics**: Fill with zeros (not in network = isolate)

### Network Metrics Choice
- **Betweenness**: Captures brokerage and information flow
- **Degree**: Simple connectivity measure
- **Eigenvector**: Accounts for partner importance
- All normalized for cross-firm comparability

### Innovation Measure
- **Patent applications** chosen over grants to capture innovation effort
- Grants reflect quality but involve lag and examiner discretion
- Applications better reflect firm's current innovative activity

---

## Troubleshooting

### Common Issues

**Issue**: `nstandr` installation fails
```r
# Solution: Install build tools first
install.packages("devtools")
devtools::install_github("stasvlasov/nstandr", force = TRUE)
```

**Issue**: Memory errors during merge
```r
# Solution: Increase memory limit (Windows)
memory.limit(size = 16000)  # 16GB

# Or process in batches by modifying merge script
```

**Issue**: Low match rates in fuzzy matching
```r
# Solution: Lower threshold in merge_regression_dataframe.R
# Change line 174 and 282: threshold = 0.85 to threshold = 0.80
# Note: This increases recall but may reduce precision
```

**Issue**: Missing raw data files
```
# Ensure directory structure:
raw_downloads/
├── SDC_data_2021.rds
├── Orbis_155_comp_extra_fields.xlsx
└── patents_view/
    └── *.csv
```

---


**Last Updated**: 2025-12-16
