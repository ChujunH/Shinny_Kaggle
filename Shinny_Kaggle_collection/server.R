library(shiny)
library(readr)
library(dplyr)
library(stringr)
library(DT)
library(ggplot2)
library(tools)
library(httr)
library(jsonlite)

# ====== Kaggle API credentials ======
get_kaggle_auth <- function() {
  username <- Sys.getenv("KAGGLE_USERNAME")
  key <- Sys.getenv("KAGGLE_KEY")
  
  if (username == "" || key == "") {
    stop("KAGGLE_USERNAME and KAGGLE_KEY must be set as environment variables")
  }
  
  list(username = username, key = key)
}

# Helper
`%||%` <- function(a, b) {
  if (is.null(a) || length(a) == 0 || is.na(a) || a == "") b else a
}

# Column names
COL_TABULAR <- "tabular_files"
COL_URL     <- "url"
COL_ID      <- "dataset_id"
COL_TITLE   <- "title"
COL_CREATOR <- "creator"
COL_UPDATED <- "last_updated"
COL_DESC    <- "description"
COL_CITE    <- "citation"
COL_NOTE    <- "note"

extract_kaggle_ref <- function(dataset_id, url) {
  if (!is.null(dataset_id) && !is.na(dataset_id) && nzchar(dataset_id) && str_detect(dataset_id, "/")) {
    return(dataset_id)
  }
  m <- str_match(url, "kaggle\\.com/datasets/([^/?#]+/[^/?#]+)")[, 2]
  ifelse(is.na(m), NA_character_, m)
}

# ====== Kaggle API functions ======
kaggle_list_files <- function(ref) {
  auth <- get_kaggle_auth()
  
  url <- paste0("https://www.kaggle.com/api/v1/datasets/list/", ref, "/files")
  
  response <- tryCatch({
    GET(
      url,
      authenticate(auth$username, auth$key, type = "basic"),
      timeout(30)
    )
  }, error = function(e) {
    stop(paste0("Failed to connect to Kaggle API: ", conditionMessage(e)))
  })
  
  if (status_code(response) != 200) {
    stop(paste0(
      "Kaggle API error (", status_code(response), "): ",
      content(response, "text", encoding = "UTF-8")
    ))
  }
  
  result <- content(response, "parsed")
  
  if (length(result$datasetFiles) == 0) {
    return(data.frame(
      name = character(0),
      size = character(0),
      creationDate = character(0),
      stringsAsFactors = FALSE
    ))
  }
  
  files_list <- lapply(result$datasetFiles, function(f) {
    data.frame(
      name = f$name %||% NA_character_,
      size = format(structure(f$totalBytes %||% 0, class = "object_size"), units = "auto"),
      creationDate = f$creationDate %||% NA_character_,
      stringsAsFactors = FALSE
    )
  })
  
  df <- bind_rows(files_list)
  
  df %>%
    filter(str_detect(tolower(name), "\\.(csv|tsv)$")) %>%
    arrange(name)
}

kaggle_download_to_temp <- function(ref, file_in_dataset, cache_dir) {
  auth <- get_kaggle_auth()
  
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  
  safe_ref <- gsub("[^A-Za-z0-9._-]+", "_", ref)
  out_dir <- file.path(cache_dir, paste0("kaggle_cache_", safe_ref))
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  
  target_path <- file.path(out_dir, basename(file_in_dataset))
  
  if (file.exists(target_path)) return(target_path)
  
  ref_parts <- str_split(ref, "/", simplify = TRUE)
  if (ncol(ref_parts) != 2) {
    stop(paste0("Invalid dataset reference: ", ref))
  }
  
  owner <- ref_parts[1]
  dataset <- ref_parts[2]
  
  url <- paste0(
    "https://www.kaggle.com/api/v1/datasets/download/",
    owner, "/", dataset, "/", file_in_dataset
  )
  
  response <- tryCatch({
    GET(
      url,
      authenticate(auth$username, auth$key, type = "basic"),
      write_disk(target_path, overwrite = TRUE),
      progress(),
      timeout(300)
    )
  }, error = function(e) {
    stop(paste0("Download failed: ", conditionMessage(e)))
  })
  
  if (status_code(response) != 200) {
    if (file.exists(target_path)) file.remove(target_path)
    stop(paste0(
      "Kaggle download failed (", status_code(response), "): ",
      content(response, "text", encoding = "UTF-8")
    ))
  }
  
  if (!file.exists(target_path)) {
    stop("Download completed but file not found")
  }
  
  target_path
}

# ====== Shiny Server ======
shinyServer(function(input, output, session) {
  
  cache_dir <- reactiveVal(file.path(tempdir(), "kaggle_shiny_cache"))
  
  observeEvent(input$clear_cache, {
    p <- cache_dir()
    if (dir.exists(p)) unlink(p, recursive = TRUE, force = TRUE)
    dir.create(p, showWarnings = FALSE, recursive = TRUE)
    showNotification("Cache cleared", type = "message")
  }, ignoreInit = TRUE)
  
  # Load metadata
  meta <- eventReactive(input$load_meta, {
    req(input$meta_path)
    
    if (!file.exists(input$meta_path)) {
      showNotification(
        paste0("File not found: ", input$meta_path),
        type = "error",
        duration = 10
      )
      return(data.frame())
    }
    
    tryCatch({
      readr::read_csv(input$meta_path, show_col_types = FALSE)
    }, error = function(e) {
      showNotification(
        paste0("Error reading CSV: ", conditionMessage(e)),
        type = "error",
        duration = 10
      )
      return(data.frame())
    })
  })
  
  # Filter metadata
  meta_filtered <- reactive({
    df <- meta()
    
    if (is.null(df) || nrow(df) == 0) {
      return(data.frame())
    }
    
    # Check required columns
    missing_cols <- c()
    if (!(COL_TABULAR %in% names(df))) missing_cols <- c(missing_cols, COL_TABULAR)
    if (!(COL_URL %in% names(df))) missing_cols <- c(missing_cols, COL_URL)
    if (!(COL_ID %in% names(df))) missing_cols <- c(missing_cols, COL_ID)
    
    if (length(missing_cols) > 0) {
      showNotification(
        paste0("Missing columns: ", paste(missing_cols, collapse = ", ")),
        type = "error",
        duration = 10
      )
      return(data.frame())
    }
    
    df %>%
      mutate(kaggle_ref = mapply(extract_kaggle_ref, .data[[COL_ID]], .data[[COL_URL]])) %>%
      filter(.data[[COL_TABULAR]] == 1, !is.na(kaggle_ref))
  })
  
  # Display metadata table
  output$meta_tbl <- renderDT({
    df <- meta_filtered()
    req(nrow(df) > 0)
    DT::datatable(df, options = list(pageLength = 10), rownames = FALSE)
  })
  
  # Dataset selector
  output$dataset_ui <- renderUI({
    df <- meta_filtered()
    
    if (is.null(df) || nrow(df) == 0) {
      return(helpText("No datasets available. Click 'Load metadata' first."))
    }
    
    title_vec <- df[[COL_TITLE]]
    idx <- is.na(title_vec) | title_vec == ""
    title_vec[idx] <- df$kaggle_ref[idx]
    
    labels <- paste0(title_vec, "  (", df$kaggle_ref, ")")
    choices <- setNames(df$kaggle_ref, labels)
    
    selectInput("ref_pick", "Select Kaggle dataset", choices = choices)
  })
  
  # List files in dataset
  files_df <- reactive({
    req(input$ref_pick)
    
    tryCatch({
      kaggle_list_files(input$ref_pick)
    }, error = function(e) {
      showNotification(
        paste0("Error listing files: ", conditionMessage(e)),
        type = "error",
        duration = 10
      )
      return(data.frame())
    })
  })
  
  # File selector
  output$file_ui <- renderUI({
    fd <- files_df()
    
    if (is.null(fd) || nrow(fd) == 0) {
      return(helpText("No CSV/TSV files found in this dataset"))
    }
    
    selectInput("file_pick", "Select file inside dataset", choices = fd$name)
  })
  
  # Selected metadata row
  selected_meta <- reactive({
    df <- meta_filtered()
    req(input$ref_pick, nrow(df) > 0)
    df %>% filter(kaggle_ref == input$ref_pick) %>% slice(1)
  })
  
  # Download and get local path
  local_path <- reactive({
    req(input$ref_pick, input$file_pick)
    
    tryCatch({
      kaggle_download_to_temp(
        ref = input$ref_pick,
        file_in_dataset = input$file_pick,
        cache_dir = cache_dir()
      )
    }, error = function(e) {
      showNotification(
        paste0("Download error: ", conditionMessage(e)),
        type = "error",
        duration = 10
      )
      return(NULL)
    })
  })
  
  # Read data
  dat <- reactive({
    p <- local_path()
    req(!is.null(p), file.exists(p))
    
    readr::read_csv(
      p,
      guess_max = 10000,
      show_col_types = FALSE,
      na = c("", "NA", "NaN", "null")
    )
  })
  
  # Description output
  output$desc_out <- renderUI({
    row <- selected_meta()
    req(nrow(row) == 1)
    
    desc <- if (COL_DESC %in% names(row)) row[[COL_DESC]][1] else NA
    cite <- if (COL_CITE %in% names(row)) row[[COL_CITE]][1] else NA
    
    tagList(
      tags$h4("Description"),
      tags$pre(
        style = "white-space: pre-wrap;", 
        ifelse(is.na(desc) || desc == "", "(empty)", desc)
      ),
      tags$hr(),
      tags$h4("Citation"),
      tags$pre(
        style = "white-space: pre-wrap;", 
        ifelse(is.na(cite) || cite == "", "(empty)", cite)
      )
    )
  })
  
  # Download handler
  output$download_selected <- downloadHandler(
    filename = function() {
      req(input$ref_pick, input$file_pick)
      paste0(gsub("/", "__", input$ref_pick), "__", basename(input$file_pick))
    },
    content = function(file) {
      p <- local_path()
      req(!is.null(p), file.exists(p))
      file.copy(p, file, overwrite = TRUE)
    },
    contentType = "text/csv"
  )
  
  # Types & Missing
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
  
  # Summary
  output$summary_out <- renderPrint({
    summary(dat())
  })
  
  # Head UI
  output$head_ui <- renderUI({
    if (isTRUE(input$use_dt)) {
      DTOutput("head_dt")
    } else {
      tableOutput("head_tbl")
    }
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
  
  # Variable selector for plot
  output$var_ui <- renderUI({
    df <- dat()
    selectInput("plot_var", "Variable", choices = names(df))
  })
  
  # Plot
  output$plot_out <- renderPlot({
    df <- dat()
    req(input$plot_var)
    
    x <- df[[input$plot_var]]
    if (isTRUE(input$drop_na_plot)) x <- x[!is.na(x)]
    
    if (is.numeric(x)) {
      dd <- data.frame(x = x)
      ggplot(dd, aes(x = x)) +
        geom_histogram(bins = 30) +
        labs(x = input$plot_var, y = "Count") +
        theme_minimal()
    } else {
      dd <- data.frame(x = as.factor(x))
      
      if (input$chart_type == "bar") {
        ggplot(dd, aes(x = x)) +
          geom_bar() +
          coord_flip() +
          labs(x = input$plot_var, y = "Count") +
          theme_minimal()
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
  
  # Debug
  output$debug_out <- renderPrint({
    list(
      kaggle_username = Sys.getenv("KAGGLE_USERNAME"),
      kaggle_key_set = nchar(Sys.getenv("KAGGLE_KEY")) > 0,
      meta_path = input$meta_path,
      meta_loaded = !is.null(meta()) && nrow(meta()) > 0,
      cache_dir = cache_dir(),
      picked_ref = input$ref_pick,
      picked_file = input$file_pick,
      local_path = tryCatch(
        local_path(), 
        error = function(e) paste("Error:", conditionMessage(e))
      )
    )
  })
})
