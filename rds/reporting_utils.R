library(knitr)
library(kableExtra)
library(crayon)

print_header <- function(text, width = 80, char = "=") {
  cat("\n")
  cat(rep(char, width), sep = "")
  cat("\n")
  cat(center_text(text, width))
  cat("\n")
  cat(rep(char, width), sep = "")
  cat("\n\n")
}

print_subheader <- function(text, width = 80, char = "-") {
  cat("\n")
  cat(rep(char, width), sep = "")
  cat("\n")
  cat(center_text(text, width))
  cat("\n")
  cat(rep(char, width), sep = "")
  cat("\n\n")
}

print_section <- function(text, width = 80) {
  cat("\n")
  cat(crayon::bold(crayon::blue("▶ ", text)))
  cat("\n")
}

print_success <- function(text) {
  cat(crayon::green("✓"), text, "\n")
}

print_info <- function(text) {
  cat(crayon::blue("ℹ"), text, "\n")
}

print_warning <- function(text) {
  cat(crayon::yellow("⚠"), text, "\n")
}

print_error <- function(text) {
  cat(crayon::red("✗"), text, "\n")
}

center_text <- function(text, width = 80) {
  padding <- (width - nchar(text)) / 2
  paste0(strrep(" ", floor(padding)), text, strrep(" ", ceiling(padding)))
}

print_summary_table <- function(df, title = NULL, max_rows = 20) {
  if (!is.null(title)) {
    cat(crayon::bold(crayon::cyan(title)), "\n")
  }

  if (nrow(df) > max_rows) {
    cat(crayon::yellow(sprintf("(Showing first %d of %d rows)\n\n", max_rows, nrow(df))))
    df <- head(df, max_rows)
  }

  print(kable(df, format = "simple", align = "l", digits = 3))
  cat("\n")
}
create_progress_bar <- function(total, title = "Progress") {
  list(
    total = total,
    current = 0,
    title = title,
    start_time = Sys.time()
  )
}

update_progress <- function(pb, increment = 1) {
  pb$current <- pb$current + increment
  pct <- round(pb$current / pb$total * 100)
  elapsed <- as.numeric(difftime(Sys.time(), pb$start_time, units = "secs"))

  if (pb$current == pb$total || pb$current %% max(1, floor(pb$total / 20)) == 0) {
    bar_width <- 40
    filled <- floor(pct / 100 * bar_width)
    bar <- paste0(
      "[",
      strrep("█", filled),
      strrep("░", bar_width - filled),
      "]"
    )

    eta <- if (pb$current > 0) {
      (elapsed / pb$current) * (pb$total - pb$current)
    } else {
      0
    }

    cat(
      "\r", crayon::blue(pb$title), ": ",
      crayon::green(bar), " ",
      sprintf("%3d%%", pct), " ",
      sprintf("(%d/%d)", pb$current, pb$total), " ",
      sprintf("ETA: %.0fs", eta)
    )

    if (pb$current == pb$total) {
      cat(crayon::green(" ✓ Done!"), sprintf(" (%.1fs)\n", elapsed))
    }
  }

  pb
}

print_metrics <- function(metrics_list, title = NULL) {
  if (!is.null(title)) {
    print_section(title)
  }

  max_label_width <- max(nchar(names(metrics_list)))

  for (name in names(metrics_list)) {
    value <- metrics_list[[name]]
    label <- sprintf("%-*s", max_label_width + 2, paste0(name, ":"))

    if (is.numeric(value)) {
      if (value >= 1000) {
        formatted <- format(value, big.mark = ",", scientific = FALSE)
      } else if (value < 1 && value > 0) {
        formatted <- sprintf("%.3f", value)
      } else {
        formatted <- sprintf("%.2f", value)
      }
    } else {
      formatted <- as.character(value)
    }

    cat("  ", crayon::silver(label), crayon::bold(formatted), "\n")
  }
  cat("\n")
}

# ----------------------------------------
# Matching Summary with Visual Bar
# ----------------------------------------

print_match_summary <- function(matched, total, label = "Matches", show_bar = TRUE) {
  pct <- round(matched / total * 100, 1)
  unmatched <- total - matched

  cat("  ", crayon::bold(label), "\n")
  cat("    Matched:   ", crayon::green(sprintf("%6d / %d (%.1f%%)", matched, total, pct)), "\n")
  cat("    Unmatched: ", crayon::silver(sprintf("%6d / %d (%.1f%%)", unmatched, total, 100 - pct)), "\n")

  if (show_bar) {
    bar_width <- 50
    filled <- floor(pct / 100 * bar_width)
    bar <- paste0(
      crayon::green(strrep("█", filled)),
      crayon::silver(strrep("░", bar_width - filled))
    )
    cat("    ", bar, "\n")
  }
  cat("\n")
}

print_model_comparison <- function(models_df, highlight_best = TRUE) {
  print_section("Model Comparison")

  if (highlight_best) {
    # Highlight best R-squared (highest)
    best_r2_idx <- which.max(models_df$R_squared)
    # Highlight best AIC (lowest)
    best_aic_idx <- which.min(models_df$AIC)
  }

  # Format numeric columns
  formatted_df <- models_df
  formatted_df$R_squared <- sprintf("%.4f", formatted_df$R_squared)
  formatted_df$Adj_R_squared <- sprintf("%.4f", formatted_df$Adj_R_squared)
  formatted_df$AIC <- sprintf("%.2f", formatted_df$AIC)
  formatted_df$BIC <- sprintf("%.2f", formatted_df$BIC)

  print(kable(formatted_df, format = "simple", align = "lrrrrr"))
  cat("\n")

  if (highlight_best) {
    print_success(sprintf("Best R²: %s", models_df$Model[best_r2_idx]))
    print_success(sprintf("Best AIC: %s", models_df$Model[best_aic_idx]))
    cat("\n")
  }
}

print_hypothesis_test <- function(hypothesis_name, description,
                                  coefficient, std_error, p_value,
                                  expected_sign = NULL, significance_level = 0.05) {
  cat(crayon::bold(crayon::cyan(hypothesis_name)), "\n")
  cat(crayon::silver(description), "\n\n")

  cat("  Coefficient: ", sprintf("%.4f", coefficient), "\n")
  cat("  Std. Error:  ", sprintf("%.4f", std_error), "\n")
  cat("  P-value:     ", sprintf("%.4f", p_value), "\n")

  # Determine support
  is_significant <- p_value < significance_level
  sign_correct <- if (!is.null(expected_sign)) {
    (expected_sign == "positive" && coefficient > 0) ||
      (expected_sign == "negative" && coefficient < 0)
  } else {
    TRUE
  }

  supported <- is_significant && sign_correct

  cat("  Support:     ")
  if (supported) {
    cat(crayon::green(crayon::bold("✓ YES")))
    if (p_value < 0.001) {
      cat(" (p < 0.001)")
    } else if (p_value < 0.01) {
      cat(" (p < 0.01)")
    } else if (p_value < 0.05) {
      cat(" (p < 0.05)")
    } else {
      cat(" (p < 0.10)")
    }
  } else {
    cat(crayon::red("✗ NO"))
    if (!is_significant) {
      cat(" (not significant)")
    } else if (!sign_correct) {
      cat(" (wrong sign)")
    }
  }
  cat("\n\n")
}

save_text_report <- function(content, filepath) {
  # Strip ANSI color codes for file output
  clean_content <- gsub("\033\\[[0-9;]+m", "", content)
  writeLines(clean_content, filepath)
  print_success(sprintf("Report saved to: %s", filepath))
}
