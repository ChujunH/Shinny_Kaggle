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

# Helper: default fallback for NULL/NA/""
`%||%` <- function(a, b) {
  if (is.null(a) || length(a) == 0) return(b)
  if (length(a) == 1 && (is.na(a) || a == "")) return(b)
  a
}

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
  # 1) 强制 User-Agent（给 R / curl / kaggle cli 都留一份）
  Sys.setenv(
    KAGGLE_HTTP_USER_AGENT = "shiny-connect",
    KAGGLE_USER_AGENT      = "shiny-connect",
    HTTP_USER_AGENT        = "shiny-connect"
  )
  
  # 2) 把 secret variables 写成 kaggle.json（并加上 user_agent）
  user <- Sys.getenv("KAGGLE_USERNAME")
  key  <- Sys.getenv("KAGGLE_KEY")
  
  if (nzchar(user) && nzchar(key)) {
    cfg <- file.path(tempdir(), "kaggle_cfg")
    dir.create(cfg, showWarnings = FALSE, recursive = TRUE)
    
    cfg_json <- jsonlite::toJSON(
      list(username = user, key = key, user_agent = "shiny-connect"),
      auto_unbox = TRUE
    )
    cfg_path <- file.path(cfg, "kaggle.json")
    writeLines(cfg_json, cfg_path)
    Sys.chmod(cfg_path, "600")
    
    # 关键：让 kaggle CLI / kaggle python 都从这里读配置
    Sys.setenv(KAGGLE_CONFIG_DIR = cfg)
  }
}

ensure_kaggle_auth()


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


kaggle_auth_handle <- function() {
  user <- Sys.getenv("KAGGLE_USERNAME")
  key  <- Sys.getenv("KAGGLE_KEY")
  if (!nzchar(user) || !nzchar(key)) return(NULL)
  
  h <- curl::new_handle()
  curl::handle_setopt(
    h,
    userpwd   = paste0(user, ":", key),
    httpauth  = 1L,
    useragent = "shiny-connect"
  )
  h
}

kaggle_api_get <- function(path) {
  h <- kaggle_auth_handle()
  url <- paste0("https://www.kaggle.com/api/v1", path)
  res <- curl::curl_fetch_memory(url, handle = h)
  txt <- rawToChar(res$content)
  if (res$status_code >= 400) {
    stop(sprintf("Kaggle API error %s\nURL: %s\nBody:\n%s", res$status_code, url, txt))
  }
  txt
}

kaggle_api_download <- function(path, destfile) {
  h <- kaggle_auth_handle()
  url <- paste0("https://www.kaggle.com/api/v1", path)
  curl::curl_download(url, destfile = destfile, handle = h, quiet = TRUE, mode = "wb")
  destfile
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

# ---------- Kaggle CLI runner (for local fallback) ----------
run_kaggle_cli <- function(args) {
  # ensure config/UA if env vars exist
  ensure_kaggle_auth()
  
  if (!nzchar(KAGGLE_BIN)) {
    stop("kaggle CLI not found. Set KAGGLE_BIN or install kaggle.")
  }
  
  out <- system2(KAGGLE_BIN, args, stdout = TRUE, stderr = TRUE)
  status <- attr(out, "status") %||% 0
  if (status != 0) {
    stop("Kaggle CLI failed.\n", paste(out, collapse = "\n"))
  }
  out
}

# ---------- CLI version: list files ----------
kaggle_list_files_cli <- function(ref) {
  if (!nzchar(KAGGLE_BIN)) stop("kaggle CLI not found (KAGGLE_BIN empty).")
  
  # 给子进程硬塞 UA（本地一般不需要，但无害）
  extra_env <- c(
    "HTTP_USER_AGENT=shiny-connect",
    "KAGGLE_USER_AGENT=shiny-connect",
    "KAGGLE_HTTP_USER_AGENT=shiny-connect"
  )
  
  out <- system2(KAGGLE_BIN, c("datasets", "files", "--csv", ref),
                 stdout = TRUE, stderr = TRUE, env = extra_env)
  status <- attr(out, "status") %||% 0
  if (status != 0) stop(paste(out, collapse = "\n"))
  
  # Kaggle CLI 有时会夹杂 warning，找到真正的 csv header 行
  i <- which(grepl("^name,", out))[1]
  if (is.na(i)) stop("Kaggle --csv output missing 'name' column.\n", paste(out, collapse="\n"))
  
  txt <- paste(out[i:length(out)], collapse = "\n")
  df <- readr::read_csv(txt, show_col_types = FALSE)
  names(df) <- tolower(names(df))
  df
}

kaggle_list_files_py <- function(ref) {
  py_code <- "
import os, sys, json
from kaggle.api.kaggle_api_extended import KaggleApi

ua = os.environ.get('KAGGLE_USER_AGENT') or os.environ.get('HTTP_USER_AGENT') or 'shiny-connect'

api = KaggleApi()
api.authenticate()

# 关键：强制 user_agent 是字符串，避免 None
try:
    api.api_client.configuration.user_agent = ua
except Exception:
    pass

files = api.dataset_list_files(sys.argv[1]).files
out = []
for f in files:
    out.append({
        'name': getattr(f, 'name', ''),
        'size': getattr(f, 'size', None)
    })
print(json.dumps(out))
"
  extra_env <- c(
    "HTTP_USER_AGENT=shiny-connect",
    "KAGGLE_USER_AGENT=shiny-connect",
    "KAGGLE_HTTP_USER_AGENT=shiny-connect"
  )
  out <- run_kaggle_py(py_code, args = c(ref), extra_env = extra_env)
  df <- jsonlite::fromJSON(paste(out, collapse = ""), simplifyDataFrame = TRUE)
  if (is.null(df) || nrow(df) == 0) df <- data.frame(name=character(), size=double())
  df
}
kaggle_download_file_py <- function(ref, filename, out_dir) {
  py_code <- "
import os, sys, json, zipfile, glob
from kaggle.api.kaggle_api_extended import KaggleApi

ua = os.environ.get('KAGGLE_USER_AGENT') or os.environ.get('HTTP_USER_AGENT') or 'shiny-connect'

ref = sys.argv[1]
fn  = sys.argv[2]
out = sys.argv[3]
os.makedirs(out, exist_ok=True)

api = KaggleApi()
api.authenticate()
try:
    api.api_client.configuration.user_agent = ua
except Exception:
    pass

api.dataset_download_file(ref, fn, path=out, force=True, quiet=True)

base = os.path.basename(fn)
cand1 = os.path.join(out, base)
cand2 = os.path.join(out, base + '.zip')

# 有时会下载成 zip
if os.path.exists(cand2) and zipfile.is_zipfile(cand2):
    with zipfile.ZipFile(cand2, 'r') as z:
        z.extractall(out)
    os.remove(cand2)

# 如果目标文件不在，兜底找同名
if not os.path.exists(cand1):
    hits = glob.glob(os.path.join(out, '**', base), recursive=True)
    if hits:
        cand1 = hits[0]

print(json.dumps({'path': cand1}))
"
  extra_env <- c(
    "HTTP_USER_AGENT=shiny-connect",
    "KAGGLE_USER_AGENT=shiny-connect",
    "KAGGLE_HTTP_USER_AGENT=shiny-connect"
  )
  out <- run_kaggle_py(py_code, args = c(ref, filename, out_dir), extra_env = extra_env)
  obj <- jsonlite::fromJSON(paste(out, collapse = ""))
  obj$path
}
kaggle_download_file_cli <- function(ref, filename, out_dir) {
  if (!nzchar(KAGGLE_BIN)) stop("kaggle CLI not found (KAGGLE_BIN empty).")
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  
  out <- system2(KAGGLE_BIN,
                 c("datasets", "download", "-d", ref, "-f", filename, "-p", out_dir, "--force"),
                 stdout = TRUE, stderr = TRUE)
  status <- attr(out, "status") %||% 0
  if (status != 0) stop(paste(out, collapse = "\n"))
  
  # CLI 往往下载成 filename.zip，再解压
  z <- file.path(out_dir, paste0(basename(filename), ".zip"))
  if (file.exists(z)) {
    utils::unzip(z, exdir = out_dir)
    unlink(z)
  }
  file.path(out_dir, basename(filename))
}


kaggle_list_files <- function(ref) {
  ensure_kaggle_auth()
  
  # Connect Cloud：有 kaggle python 包（log 里就是 kaggle==1.8.2）
  if (has_python_kaggle()) {
    df <- kaggle_list_files_py(ref)
  } else {
    df <- kaggle_list_files_cli(ref)
  }
  
  df %>%
    dplyr::mutate(name = as.character(.data$name)) %>%
    dplyr::filter(stringr::str_detect(tolower(.data$name), "\\.(csv|tsv)$")) %>%
    dplyr::arrange(.data$name)
}

kaggle_download_to_temp <- function(ref, file_in_dataset, cache_dir) {
  ensure_kaggle_auth()
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  
  safe_ref <- gsub("[^A-Za-z0-9._-]+", "_", ref)
  out_dir <- file.path(cache_dir, paste0("kaggle_cache_", safe_ref))
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  
  target_path <- file.path(out_dir, basename(file_in_dataset))
  if (file.exists(target_path)) return(target_path)
  
  if (has_python_kaggle()) {
    p <- kaggle_download_file_py(ref, file_in_dataset, out_dir)
  } else {
    p <- kaggle_download_file_cli(ref, file_in_dataset, out_dir)
  }
  
  if (!file.exists(p)) stop("Downloaded but file not found: ", p)
  p
}











ensure_kaggle_auth()

shinyServer(function(input, output, session) {
  
  cache_dir <- reactiveVal(file.path(tempdir(), "kaggle_shiny_cache"))
  
  observeEvent(input$clear_cache, {
    p <- cache_dir()
    if (dir.exists(p)) unlink(p, recursive = TRUE, force = TRUE)
    dir.create(p, showWarnings = FALSE, recursive = TRUE)
  }, ignoreInit = TRUE)
  
  meta <- eventReactive(input$load_meta, {
    shiny::validate(shiny::need(file.exists(input$meta_path), "Metadata CSV not found."))
    readr::read_csv(input$meta_path, show_col_types = FALSE)
  }, ignoreInit = FALSE)
  
  meta_filtered <- reactive({
    df <- meta()
    shiny::validate(
      shiny::need(COL_TABULAR %in% names(df), paste0("Missing column: ", COL_TABULAR)),
      shiny::need(COL_URL %in% names(df), paste0("Missing column: ", COL_URL)),
      shiny::need(COL_ID %in% names(df), paste0("Missing column: ", COL_ID))
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
    shiny::validate(shiny::need(nrow(df) > 0, "No rows after filtering tabular_files == 1."))
    
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
    shiny::validate(shiny::need(nrow(fd) > 0, "No CSV/TSV files found in this dataset (or listing failed)."))
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
      kaggle_config_dir = Sys.getenv("KAGGLE_CONFIG_DIR"),
      kaggle_json_exists = file.exists(file.path(Sys.getenv("KAGGLE_CONFIG_DIR"), "kaggle.json")),
      meta_path = input$meta_path,
      cache_dir = cache_dir(),
      picked_ref = input$ref_pick,
      picked_file = input$file_pick,
      local_path = tryCatch(local_path(), error = function(e) paste("local_path() error:", conditionMessage(e)))
    )
  })
  
})
