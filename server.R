library(shiny)
library(readr)
library(dplyr)
library(stringr)
library(DT)
library(ggplot2)
library(tools)

if (file.exists("/opt/anaconda3/bin/kaggle")) {
  Sys.setenv(KAGGLE_BIN = "/opt/anaconda3/bin/kaggle")
}


# ====== Kaggle CLI path (yours) ======
# ====== Kaggle runner (local + cloud compatible) ======
# local: use CLI if available; cloud: fallback to python -m kaggle
KAGGLE_BIN <- Sys.getenv("KAGGLE_BIN", unset = Sys.which("kaggle"))

# Prefer python that lives next to kaggle binary (same env)
PY_BIN <- Sys.getenv("PY_BIN", unset = "")

if (!nzchar(PY_BIN) && nzchar(KAGGLE_BIN)) {
  cand <- file.path(dirname(KAGGLE_BIN), "python3")
  if (!file.exists(cand)) cand <- file.path(dirname(KAGGLE_BIN), "python")
  if (file.exists(cand)) PY_BIN <- cand
}

if (!nzchar(PY_BIN)) {
  PY_BIN <- Sys.which("python3")
  if (!nzchar(PY_BIN)) PY_BIN <- Sys.which("python")
}

has_python_kaggle <- function() {
  if (!nzchar(PY_BIN)) return(FALSE)
  out <- system2(PY_BIN, c("-c", "import kaggle; print('ok')"),
                 stdout = TRUE, stderr = TRUE)
  status <- attr(out, "status") %||% 0
  status == 0
}

has_kaggle_creds <- function() {
  user <- Sys.getenv("KAGGLE_USERNAME")
  key  <- Sys.getenv("KAGGLE_KEY")
  json <- file.path(path.expand("~"), ".kaggle", "kaggle.json")
  (nzchar(user) && nzchar(key)) || file.exists(json)
}

ensure_kaggle_auth <- function() {
  # 1) 强制 User-Agent，避免 Connect 上变成 None
  Sys.setenv(
    KAGGLE_HTTP_USER_AGENT = "shiny-connect",
    KAGGLE_USER_AGENT      = "shiny-connect",
    HTTP_USER_AGENT        = "shiny-connect"
  )
  
  # 2) 把 secret variables 写成 kaggle.json（Connect 上 ~ 目录不一定可用/持久）
  user <- Sys.getenv("KAGGLE_USERNAME")
  key  <- Sys.getenv("KAGGLE_KEY")
  
  if (nzchar(user) && nzchar(key)) {
    cfg <- file.path(tempdir(), "kaggle_cfg")
    dir.create(cfg, showWarnings = FALSE, recursive = TRUE)
    
    json <- sprintf('{"username":"%s","key":"%s"}', user, key)
    writeLines(json, file.path(cfg, "kaggle.json"))
    Sys.chmod(file.path(cfg, "kaggle.json"), "600")
    
    Sys.setenv(KAGGLE_CONFIG_DIR = cfg)
  }
}

run_kaggle_py <- function(py_code, args = character(), extra_env = character()) {
  if (!nzchar(PY_BIN)) stop("python not found on this server.")
  script <- tempfile(fileext = ".py")
  writeLines(py_code, script)
  
  out <- system2(PY_BIN, c(script, args), stdout = TRUE, stderr = TRUE, env = extra_env)
  status <- attr(out, "status") %||% 0
  if (status != 0) stop(paste(out, collapse = "\n"))
  out
}


#helper
ensure_sitecustomize <- function() {
  d <- file.path(tempdir(), "py_sitecustomize")
  dir.create(d, showWarnings = FALSE, recursive = TRUE)
  f <- file.path(d, "sitecustomize.py")
  
  if (!file.exists(f)) {
    writeLines(
      c(
        "import os",
        "try:",
        "    import requests",
        "    from requests.sessions import Session",
        "    _old = Session.request",
        "    def _patched(self, method, url, **kwargs):",
        "        headers = kwargs.get('headers') or {}",
        "        ua = headers.get('User-Agent')",
        "        if ua is None:",
        "            headers['User-Agent'] = os.environ.get('HTTP_USER_AGENT') or 'shiny-connect'",
        "            kwargs['headers'] = headers",
        "        return _old(self, method, url, **kwargs)",
        "    Session.request = _patched",
        "except Exception:",
        "    pass"
      ),
      f
    )
  }
  d
}


run_kaggle <- function(args) {
  
  # ensure kaggle.json from secrets (works on Connect Cloud)
  user <- Sys.getenv("KAGGLE_USERNAME")
  key  <- Sys.getenv("KAGGLE_KEY")
  if (nzchar(user) && nzchar(key)) {
    cfg <- file.path(tempdir(), "kaggle_cfg")
    dir.create(cfg, showWarnings = FALSE, recursive = TRUE)
    writeLines(sprintf('{"username":"%s","key":"%s"}', user, key),
               file.path(cfg, "kaggle.json"))
    Sys.chmod(file.path(cfg, "kaggle.json"), "600")
    Sys.setenv(KAGGLE_CONFIG_DIR = cfg)
  }
  
  UA <- "shiny-connect"
  extra_env <- c(
    paste0("HTTP_USER_AGENT=", UA),
    paste0("KAGGLE_USER_AGENT=", UA),
    paste0("KAGGLE_HTTP_USER_AGENT=", UA)
  )
  
  # 1) Try kaggle CLI first (best for local)
  if (nzchar(KAGGLE_BIN)) {
    out <- system2(KAGGLE_BIN, args, stdout = TRUE, stderr = TRUE, env = extra_env)
    status <- attr(out, "status") %||% 0
    
    if (status == 0) return(out)
    
    msg <- paste(out, collapse = "\n")
    
    # If it's the known Connect Cloud UA=None bug, then fallback to python API
    if (grepl("Header part \\(None\\).*User-Agent", msg, perl = TRUE) ||
        grepl("User-Agent', None", msg, fixed = TRUE)) {
      
      if (!has_python_kaggle()) {
        stop(
          "Kaggle CLI hit User-Agent=None bug, and Python doesn't have 'kaggle' installed.\n",
          "Fix options:\n",
          "  (1) Install kaggle into PY_BIN:  ", PY_BIN, " -m pip install kaggle\n",
          "  (2) Or set PY_BIN to the python env that already has kaggle.\n\n",
          msg
        )
      }
      
      # fallback: python -m kaggle
      out2 <- system2(PY_BIN, c("-m", "kaggle", args), stdout = TRUE, stderr = TRUE, env = extra_env)
      status2 <- attr(out2, "status") %||% 0
      if (status2 != 0) stop("python -m kaggle failed.\n", paste(out2, collapse = "\n"))
      return(out2)
    }
    
    # Other CLI failure: just show it
    stop("Kaggle CLI failed.\n", msg)
  }
  
  stop("kaggle CLI not found on this server.")
}





# Helper: default fallback for NULL/NA/""
`%||%` <- function(a, b) {
  if (is.null(a) || length(a) == 0) return(b)
  if (length(a) == 1 && (is.na(a) || a == "")) return(b)
  a
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
  
  # 确保子进程也能拿到 UA
  UA <- "shiny-connect"
  extra_env <- c(
    paste0("HTTP_USER_AGENT=", UA),
    paste0("KAGGLE_USER_AGENT=", UA),
    paste0("KAGGLE_HTTP_USER_AGENT=", UA)
  )
  
  py_code <- "
import os, sys, csv
# patch requests: if User-Agent is None, force a string
try:
    import requests
    from requests.sessions import Session
    _old = Session.request
    def _patched(self, method, url, **kwargs):
        headers = kwargs.get('headers') or {}
        if headers.get('User-Agent') is None:
            headers['User-Agent'] = os.environ.get('HTTP_USER_AGENT') or 'shiny-connect'
            kwargs['headers'] = headers
        return _old(self, method, url, **kwargs)
    Session.request = _patched
except Exception:
    pass

from kaggle.api.kaggle_api_extended import KaggleApi

ref = sys.argv[1]
api = KaggleApi()
api.authenticate()

obj = api.dataset_list_files(ref)
files = getattr(obj, 'files', []) or []

w = csv.writer(sys.stdout)
w.writerow(['name','size'])
for f in files:
    name = getattr(f, 'name', '')
    size = getattr(f, 'totalBytes', None)
    if size is None:
        size = getattr(f, 'size', None)
    if size is None:
        size = ''
    w.writerow([name, size])
"
  
  out <- tryCatch(
    run_kaggle_py(py_code, args = c(ref), extra_env = extra_env),
    error = function(e) {
      warning("Failed to list files from Kaggle via Python API:\n", conditionMessage(e))
      return(character())
    }
  )
  
  if (length(out) == 0) {
    return(data.frame(name = character(), size = character(), stringsAsFactors = FALSE))
  }
  
  df <- readr::read_csv(I(paste(out, collapse = "\n")), show_col_types = FALSE)
  names(df) <- tolower(names(df))
  
  if (!("name" %in% names(df))) {
    warning("Python API output missing 'name'.\nOutput:\n", paste(out, collapse = "\n"))
    return(data.frame(name = character(), size = character(), stringsAsFactors = FALSE))
  }
  
  df %>%
    mutate(name = as.character(.data$name)) %>%
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
  
  UA <- "shiny-connect"
  extra_env <- c(
    paste0("HTTP_USER_AGENT=", UA),
    paste0("KAGGLE_USER_AGENT=", UA),
    paste0("KAGGLE_HTTP_USER_AGENT=", UA)
  )
  
  py_code <- "
import os, sys, zipfile, glob
from kaggle.api.kaggle_api_extended import KaggleApi

# patch requests
try:
    import requests
    from requests.sessions import Session
    _old = Session.request
    def _patched(self, method, url, **kwargs):
        headers = kwargs.get('headers') or {}
        if headers.get('User-Agent') is None:
            headers['User-Agent'] = os.environ.get('HTTP_USER_AGENT') or 'shiny-connect'
            kwargs['headers'] = headers
        return _old(self, method, url, **kwargs)
    Session.request = _patched
except Exception:
    pass

ref  = sys.argv[1]
fn   = sys.argv[2]
outd = sys.argv[3]

os.makedirs(outd, exist_ok=True)

api = KaggleApi()
api.authenticate()

api.dataset_download_file(ref, fn, path=outd, force=True, quiet=True)

# unzip any zip files in outd
for z in glob.glob(os.path.join(outd, '*.zip')):
    with zipfile.ZipFile(z, 'r') as zip_ref:
        zip_ref.extractall(outd)
    try:
        os.remove(z)
    except Exception:
        pass

print('OK')
"
  
  # 运行 python 下载
  tryCatch(
    run_kaggle_py(py_code, args = c(ref, file_in_dataset, out_dir), extra_env = extra_env),
    error = function(e) stop("Kaggle download via Python API failed:\n", conditionMessage(e))
  )
  
  
  if (!file.exists(target_path)) {
    stop(paste0(
      "Kaggle download failed.\n",
      "Args: ", paste(args, collapse = " "),
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
      python_bin = PY_BIN,
      kaggle_user_set = nzchar(Sys.getenv("KAGGLE_USERNAME")),
      kaggle_key_set  = nzchar(Sys.getenv("KAGGLE_KEY")),
      kaggle_user_agent = Sys.getenv("KAGGLE_USER_AGENT"),
      http_user_agent   = Sys.getenv("HTTP_USER_AGENT"),
      meta_path = input$meta_path,
      cache_dir = cache_dir(),
      picked_ref = input$ref_pick,
      picked_file = input$file_pick,
      local_path = tryCatch(local_path(), error = function(e) paste("local_path() error:", conditionMessage(e)))
    )
  })
  
})
