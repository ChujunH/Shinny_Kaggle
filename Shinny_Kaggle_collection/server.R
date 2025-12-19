library(shiny)
library(readr)
library(dplyr)
library(stringr)
library(DT)
library(ggplot2)
library(tools)

# ====== Kaggle CLI path (yours) ======
KAGGLE_BIN <- "/opt/anaconda3/bin/kaggle"

# Helper: default fallback for NULL/NA/""
`%||%` <- function(a, b) {
  if (is.null(a) || length(a) == 0 || is.na(a) || a == "") b else a
}

# Columns in your metadata CSV (you provided names(A))
COL_TABULAR <- "tabular_files"
COL_URL     <- "url"
COL_ID      <- "dataset_id"   # usually owner/dataset
COL_TITLE   <- "title"
COL_CREATOR <- "creator"
COL_UPDATED <- "last_updated"
COL_DESC    <- "description"
COL_CITE    <- "citation"
COL_NOTE    <- "note"

extract_kaggle_ref <- function(dataset_id, url) {
  # Prefer dataset_id if it already looks like owner/dsname
  if (!is.null(dataset_id) && !is.na(dataset_id) && nzchar(dataset_id) && str_detect(dataset_id, "/")) {
    return(dataset_id)
  }
  # Else parse from URL
  m <- str_match(url, "kaggle\\.com/datasets/([^/?#]+/[^/?#]+)")[, 2]
  ifelse(is.na(m), NA_character_, m)
}

kaggle_list_files <- function(ref) {
  if (!file.exists(KAGGLE_BIN)) {
    stop(paste0("Kaggle CLI not found at: ", KAGGLE_BIN,
                "\nPlease update KAGGLE_BIN in server.R to your `which kaggle` path."))
  }
  
  out <- tryCatch(
    system2(KAGGLE_BIN, c("datasets", "files", "-d", ref), stdout = TRUE, stderr = TRUE),
    error = function(e) {
      stop(paste0("Failed to run Kaggle CLI from R.\n", conditionMessage(e)))
    }
  )
  
  # Find header line
  lines <- out[str_detect(out, "\\S")]
  hdr_i <- which(str_detect(lines, "^name\\s+size\\s+creationDate"))
  if (length(hdr_i) == 0) {
    stop(paste("Failed to parse file list.\nOutput:\n", paste(out, collapse = "\n")))
  }
  
  # Parse rows after header + separator
  data_lines <- lines[(hdr_i + 2):length(lines)]
  data_lines <- data_lines[!str_detect(data_lines, "^[-]+$")]
  data_lines <- data_lines[str_detect(data_lines, "\\S")]
  
  parsed <- lapply(data_lines, function(x) {
    parts <- str_split(x, "\\s{2,}", simplify = TRUE)
    parts <- parts[parts != ""]
    if (length(parts) < 3) return(NULL)
    data.frame(
      name = parts[1],
      size = parts[2],
      creationDate = parts[3],
      stringsAsFactors = FALSE
    )
  })
  
  df <- bind_rows(parsed)
  
  # Only tabular files
  df %>%
    filter(str_detect(tolower(name), "\\.(csv|tsv)$")) %>%
    arrange(name)
}

kaggle_download_to_temp <- function(ref, file_in_dataset, cache_dir) {
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  
  safe_ref <- gsub("[^A-Za-z0-9._-]+", "_", ref)
  out_dir <- file.path(cache_dir, paste0("kaggle_cache_", safe_ref))
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  
  target_path <- file.path(out_dir, basename(file_in_dataset))
  if (file.exists(target_path)) return(target_path)
  
  args <- c("datasets", "download", "-d", ref, "-f", file_in_dataset, "--path", out_dir, "--unzip")
  status <- tryCatch(
    system2(KAGGLE_BIN, args, stdout = TRUE, stderr = TRUE),
    error = function(e) {
      stop(paste0("Kaggle download failed.\n", conditionMessage(e)))
    }
  )
  
  if (!file.exists(target_path)) {
    stop(paste0(
      "Kaggle download failed.\nCommand: ", KAGGLE_BIN, " ", paste(args, collapse = " "),
      "\n\nOutput:\n", paste(status, collapse = "\n")
    ))
  }
  
  target_path
}

shinyServer(function(input, output, session) {
  
  cache_dir <- reactiveVal(file.path(tempdir(), "kaggle_shiny_cache"))
  
  observeEvent(input$clear_cache, {
    p <- cache_dir()
    if (dir.exists(p)) unlink(p, recursive = TRUE, force = TRUE)
    dir.create(p, showWarnings = FALSE, recursive = TRUE)
  }, ignoreInit = TRUE)
  
  meta <- eventReactive(input$load_meta, {
    validate(need(file.exists(input$meta_path), "Metadata CSV not found."))
    readr::read_csv(input$meta_path, show_col_types = FALSE)
  }, ignoreInit = FALSE)
  
  meta_filtered <- reactive({
    df <- meta()
    validate(
      need(COL_TABULAR %in% names(df), paste0("Missing column: ", COL_TABULAR)),
      need(COL_URL %in% names(df), paste0("Missing column: ", COL_URL)),
      need(COL_ID %in% names(df), paste0("Missing column: ", COL_ID))
    )
    
    df %>%
      mutate(kaggle_ref = mapply(extract_kaggle_ref, .data[[COL_ID]], .data[[COL_URL]])) %>%
      filter(.data[[COL_TABULAR]] == 1, !is.na(kaggle_ref))
  })
  
  output$meta_tbl <- renderDT({
    DT::datatable(meta_filtered(), options = list(pageLength = 10), rownames = FALSE)
  })
  
  output$dataset_ui <- renderUI({
    df <- meta_filtered()
    validate(need(nrow(df) > 0, "No rows after filtering tabular_files == 1."))
    
    title_vec <- df[[COL_TITLE]]
    idx <- is.na(title_vec) | title_vec == ""
    title_vec[idx] <- df$kaggle_ref[idx]
    
    labels <- paste0(title_vec, "  (", df$kaggle_ref, ")")
    choices <- setNames(df$kaggle_ref, labels)
    
    selectInput("ref_pick", "Select Kaggle dataset", choices = choices)
  })
  
  
  # List files for selected dataset (CSV/TSV)
  files_df <- reactive({
    req(input$ref_pick)
    kaggle_list_files(input$ref_pick)
  })
  
  output$file_ui <- renderUI({
    fd <- files_df()
    validate(need(nrow(fd) > 0, "No CSV/TSV files found in this dataset (or listing failed)."))
    selectInput("file_pick", "Select file inside dataset (-f)", choices = fd$name)
  })
  
  selected_meta <- reactive({
    df <- meta_filtered()
    req(input$ref_pick)
    df %>% filter(kaggle_ref == input$ref_pick) %>% slice(1)
  })
  
  # Local path of selected file (download to temp cache)
  local_path <- reactive({
    req(input$ref_pick, input$file_pick)
    kaggle_download_to_temp(
      ref = input$ref_pick,
      file_in_dataset = input$file_pick,
      cache_dir = cache_dir()
    )
  })
  
  # Read CSV
  dat <- reactive({
    p <- local_path()
    readr::read_csv(
      p,
      guess_max = 10000,
      show_col_types = FALSE,
      na = c("", "NA", "NaN", "null")
    )
  })
  
  # Small dataset stats card for Description tab
  data_card <- reactive({
    df <- dat()
    
    miss <- sapply(df, function(x) mean(is.na(x)))
    if (length(miss) == 0) {
      top_col <- NA_character_
      top_rate <- NA_real_
    } else {
      top_col <- names(which.max(miss))
      top_rate <- as.numeric(max(miss))
    }
    
    list(
      n_rows = nrow(df),
      n_cols = ncol(df),
      top_missing_col = top_col,
      top_missing_rate = top_rate
    )
  })
  
  # Description tab output (from metadata CSV + card from dat())
  output$desc_out <- renderUI({
    row <- selected_meta()
    req(nrow(row) == 1)
    
    desc <- row$description[[1]]
    cite <- row$citation[[1]]
    
    tagList(
      tags$h4("Description"),
      tags$pre(style = "white-space: pre-wrap;", ifelse(is.na(desc) || desc == "", "(empty)", desc)),
      tags$hr(),
      tags$h4("Citation"),
      tags$pre(style = "white-space: pre-wrap;", ifelse(is.na(cite) || cite == "", "(empty)", cite))
    )
  })
  
  
  
  
  # Download selected CSV
  output$download_selected <- downloadHandler(
    filename = function() {
      req(input$ref_pick, input$file_pick)
      paste0(gsub("/", "__", input$ref_pick), "__", basename(input$file_pick))
    },
    content = function(file) {
      file.copy(local_path(), file, overwrite = TRUE)
    },
    contentType = "text/csv"
  )
  
  # Types & Missing tab
  output$type_out <- renderPrint({
    df <- dat()
    
    cat("Rows:", nrow(df), "\n")
    cat("Columns:", ncol(df), "\n\n")
    
    types <- sapply(df, function(x) paste(class(x), collapse = "/"))
    miss  <- sapply(df, function(x) mean(is.na(x)))
    
    cat("Column type counts:\n")
    print(sort(table(types), decreasing = TRUE))
    
    cat("\nTop missing-rate columns:\n")
    print(head(sort(miss, decreasing = TRUE), 20))
  })
  
  
  output$summary_out <- renderPrint({
    summary(dat())
  })
  
  output$head_ui <- renderUI({
    if (isTRUE(input$use_dt)) DTOutput("head_dt") else tableOutput("head_tbl")
  })
  
  output$head_dt <- renderDT({
    req(isTRUE(input$use_dt))
    DT::datatable(
      head(dat(), input$head_n),
      options = list(pageLength = min(10, input$head_n)),
      rownames = FALSE
    )
  })
  
  output$head_tbl <- renderTable({
    req(!isTRUE(input$use_dt))
    head(dat(), input$head_n)
  })
  
  output$var_ui <- renderUI({
    df <- dat()
    selectInput("plot_var", "Variable", choices = names(df))
  })
  
  output$plot_out <- renderPlot({
    df <- dat()
    req(input$plot_var)
    
    x <- df[[input$plot_var]]
    if (isTRUE(input$drop_na_plot)) x <- x[!is.na(x)]
    
    # numeric: histogram
    if (is.numeric(x)) {
      dd <- data.frame(x = x)
      ggplot(dd, aes(x = x)) +
        geom_histogram(bins = 30) +
        labs(x = input$plot_var, y = "Count")
    } else {
      # categorical: bar / pie
      dd <- data.frame(x = as.factor(x))
      
      if (input$chart_type == "bar") {
        ggplot(dd, aes(x = x)) +
          geom_bar() +
          coord_flip() +
          labs(x = input$plot_var, y = "Count")
      } else {
        pdat <- as.data.frame(table(dd$x))
        colnames(pdat) <- c("x", "n")
        
        ggplot(pdat, aes(x = "", y = n, fill = x)) +
          geom_col(width = 1) +
          coord_polar(theta = "y") +
          theme_void() +
          labs(fill = input$plot_var)
      }
    }
  })
  
  
  output$debug_out <- renderPrint({
    list(
      kaggle_bin = KAGGLE_BIN,
      meta_path = input$meta_path,
      cache_dir = cache_dir(),
      picked_ref = input$ref_pick,
      picked_file = input$file_pick,
      local_path = tryCatch(local_path(), error = function(e) paste("local_path() error:", conditionMessage(e)))
    )
  })
  
})
