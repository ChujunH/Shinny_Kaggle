library(shiny)
library(DT)

shinyUI(
  fluidPage(
    titlePanel("Kaggle Tabular Quick Viewer (tabular_files == 1)"),
    
    sidebarLayout(
      sidebarPanel(
        helpText("1) Load your metadata CSV  2) Filter tabular_files == 1  3) After selecting a dataset, automatically list files inside it  4) Select a CSV, download it to tempdir() (temporary), and display it."),
        tags$hr(),
        
        textInput(
          "meta_path", "Metadata CSV path",
          value = "data/clinical_datasets_summary_prefilter.csv"
        ),
        actionButton("load_meta", "Load metadata"),
        
        tags$hr(),
        uiOutput("dataset_ui"),
        uiOutput("file_ui"),
        
        tags$hr(),
        checkboxInput("use_dt", "Interactive head()", TRUE),
        numericInput("head_n", "Rows in head()", value = 10, min = 1),
        
        tags$hr(),
        h4("Chart"),
        selectInput("chart_type", "Type", choices = c("Bar" = "bar", "Pie" = "pie")),
        uiOutput("var_ui"),
        checkboxInput("drop_na_plot", "Drop NA in plot", TRUE),
        
        tags$hr(),
        h4("Download"),
        helpText("Downloads the currently selected CSV file."),
        downloadButton("download_selected", "Download selected CSV"),
        
        tags$hr(),
        actionButton("clear_cache", "Clear temp cache")
      ),
      
      mainPanel(
        tabsetPanel(
          tabPanel("Filtered metadata", DTOutput("meta_tbl")),
          tabPanel("Description", uiOutput("desc_out")),
          tabPanel("Types & Missing", verbatimTextOutput("type_out")),
          tabPanel("Summary", verbatimTextOutput("summary_out")),
          tabPanel("Head", uiOutput("head_ui")),
          tabPanel("Plot", plotOutput("plot_out", height = 420)),
          tabPanel("Debug", verbatimTextOutput("debug_out"))
        )
      )
    )
  )
)
