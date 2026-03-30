# =============================================================================
# app.R  —  Chatbot Time Savings Dashboard
# Hosted as a Databricks App (deployed via Databricks Apps, NOT shinyApp() local)
#
# Runtime dependencies:
#   - Model results pre-computed by model_job.R run as a weekly Databricks Job
#     Saved to DBFS as an RDS file — app only reads, never re-fits.
#   - SQL Warehouse reachable via the env vars below.
#
# Required environment variables (set in Databricks App config or .Renviron):
#   DATABRICKS_HOST        e.g. "adb-<workspace-id>.azuredatabricks.net"
#   DATABRICKS_HTTP_PATH   e.g. "/sql/1.0/warehouses/<warehouse-id>"
#   DATABRICKS_TOKEN       personal access token or service-principal secret
#   DB_CATALOG             Unity Catalog name          (default: hive_metastore)
#   DB_SCHEMA              schema / database name      (default: default)
#   MODEL_RDS_PATH         DBFS path to model RDS      (default below)
#   USE_SPARKLYR           set to "true" to use sparklyr instead of ODBC
# =============================================================================

library(shiny)
library(DBI)
library(odbc)
library(dplyr)

# sparklyr is optional — only loaded if USE_SPARKLYR=true
USE_SPARKLYR <- identical(Sys.getenv("USE_SPARKLYR"), "true")
if (USE_SPARKLYR) library(sparklyr)

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
DB_HOST    <- Sys.getenv("DATABRICKS_HOST",     "")
DB_HTTP    <- Sys.getenv("DATABRICKS_HTTP_PATH", "")
DB_TOKEN   <- Sys.getenv("DATABRICKS_TOKEN",     "")
DB_CATALOG <- Sys.getenv("DB_CATALOG",  "hive_metastore")
DB_SCHEMA  <- Sys.getenv("DB_SCHEMA",   "default")
MODEL_PATH <- Sys.getenv("MODEL_RDS_PATH",
                          "/dbfs/mnt/models/chatbot_model_results.rds")

# Chunk size for collecting large result sets incrementally (rows per fetch)
CHUNK_SIZE <- as.integer(Sys.getenv("CHUNK_SIZE", "50000"))

# -----------------------------------------------------------------------------
# CONNECTION HELPERS
# -----------------------------------------------------------------------------

# ODBC connection to Databricks SQL Warehouse
odbc_connect <- function() {
  dbConnect(
    odbc::odbc(),
    Driver          = "Simba Spark ODBC Driver",
    Host            = DB_HOST,
    Port            = 443,
    HTTPPath        = DB_HTTP,
    AuthMech        = 3,
    UID             = "token",
    PWD             = DB_TOKEN,
    ThriftTransport = 2
  )
}

# sparklyr connection — used when USE_SPARKLYR=true
# (handles clusters too large for ODBC result sets)
sparklyr_connect <- function() {
  config <- spark_config()
  config$`sparklyr.shell.driver-memory` <- "4g"
  spark_connect(
    method  = "databricks",
    cluster = Sys.getenv("DATABRICKS_CLUSTER_ID", ""),
    config  = config
  )
}

# Returns the active connection (ODBC or sparklyr) for a query block.
# Always call db_disconnect(con) in on.exit().
db_connect <- function() {
  if (USE_SPARKLYR) sparklyr_connect() else odbc_connect()
}

db_disconnect <- function(con) {
  if (USE_SPARKLYR) spark_disconnect(con) else dbDisconnect(con)
}

# -----------------------------------------------------------------------------
# LARGE-DATA QUERY HELPERS
# All aggregation happens in SQL — only small result sets come to R.
# For tables with billions of rows use APPROX_COUNT_DISTINCT to avoid full scans.
# -----------------------------------------------------------------------------

pull_summary_counts <- function() {
  con <- db_connect()
  on.exit(db_disconnect(con), add = TRUE)

  if (USE_SPARKLYR) {
    tbl(con, paste0("`", DB_CATALOG, "`.`", DB_SCHEMA, "`.work_orders")) |>
      summarise(
        n_pre      = approx_count_distinct(if_else(post == 0, wo_number, NA_character_)),
        n_post     = approx_count_distinct(if_else(post == 1, wo_number, NA_character_)),
        n_sessions = approx_count_distinct(if_else(post == 1 & !is.na(session_duration),
                                                   wo_number, NA_character_))
      ) |>
      collect()
  } else {
    dbGetQuery(con, sprintf(
      "SELECT
         APPROX_COUNT_DISTINCT(CASE WHEN post = 0 THEN wo_number END) AS n_pre,
         APPROX_COUNT_DISTINCT(CASE WHEN post = 1 THEN wo_number END) AS n_post,
         APPROX_COUNT_DISTINCT(CASE WHEN post = 1
                                    AND session_duration IS NOT NULL
                               THEN wo_number END)                     AS n_sessions
       FROM %s.%s.work_orders",
      DB_CATALOG, DB_SCHEMA
    ))
  }
}

# Random sample of post-period session durations for histogram.
# LIMIT keeps this small regardless of table size — SQL engine does the work.
pull_session_sample <- function(n = 2000L) {
  con <- db_connect()
  on.exit(db_disconnect(con), add = TRUE)

  if (USE_SPARKLYR) {
    tbl(con, paste0("`", DB_CATALOG, "`.`", DB_SCHEMA, "`.work_orders")) |>
      filter(post == 1, !is.na(session_duration)) |>
      select(session_duration) |>
      sdf_sample(fraction = 0.01, replacement = FALSE) |>
      head(n) |>
      collect() |>
      pull(session_duration)
  } else {
    dbGetQuery(con, sprintf(
      "SELECT session_duration
       FROM %s.%s.work_orders
       WHERE post = 1
         AND session_duration IS NOT NULL
       ORDER BY RAND()
       LIMIT %d",
      DB_CATALOG, DB_SCHEMA, n
    ))$session_duration
  }
}

# Chunked collector — use this if you ever need to pull a large result set
# into R in pieces rather than all at once (e.g. for export or full-data ops).
# Not used by the dashboard directly but available for model_job.R.
collect_in_chunks <- function(con, query, chunk_size = CHUNK_SIZE) {
  rs  <- dbSendQuery(con, query)
  out <- list()
  while (!dbHasCompleted(rs)) {
    chunk <- dbFetch(rs, n = chunk_size)
    if (nrow(chunk) == 0) break
    out <- c(out, list(chunk))
  }
  dbClearResult(rs)
  bind_rows(out)
}

# -----------------------------------------------------------------------------
# LOAD PRE-COMPUTED MODEL RESULTS FROM DBFS
# model_job.R runs weekly as a Databricks Job and writes this file.
# The app reads it once at startup — never re-fits the model.
# -----------------------------------------------------------------------------
load_model_results <- function(path = MODEL_PATH) {
  if (!file.exists(path)) {
    stop(
      "Model results not found at: ", path,
      "\nRun model_job.R as a weekly Databricks Job to generate them."
    )
  }
  readRDS(path)
}

# Load once at app startup
res            <- load_model_results()
delta_R        <- res$delta_R          # posterior draws: minutes saved per lookup
R_manual_avg   <- res$R_manual_avg     # posterior draws: avg manual retrieval (hrs)
mean_session   <- res$mean_session     # scalar: mean observed chatbot session (hrs)
n_sessions     <- res$n_sessions       # scalar: count of post-period sessions
total_hrs      <- res$total_hrs        # posterior draws: total hours saved
model_run_date <- res$model_run_date   # POSIXct: when model_job.R last ran

# Pull summary counts once at startup (tiny query)
summary_counts <- tryCatch(
  pull_summary_counts(),
  error = function(e) {
    warning("DB summary query failed: ", conditionMessage(e))
    data.frame(n_pre = NA_integer_, n_post = NA_integer_, n_sessions = NA_integer_)
  }
)

n_pre_wo  <- summary_counts$n_pre[1]
n_post_wo <- summary_counts$n_post[1]

session_sample <- tryCatch(
  pull_session_sample(),
  error = function(e) {
    warning("DB session sample query failed: ", conditionMessage(e))
    numeric(0)
  }
)


# =============================================================================
# UI
# =============================================================================
ui <- fluidPage(
  tags$head(tags$style(HTML("
    body { background: #f4f4f7; font-family: -apple-system, sans-serif; padding: 10px; }
    .box { background: white; border-radius: 6px; padding: 14px; margin-bottom: 12px;
           box-shadow: 0 1px 2px rgba(0,0,0,0.06); }
    .lbl { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; }
    .val { font-size: 26px; font-weight: 700; color: #222; margin: 4px 0; }
    .sub { font-size: 11px; color: #bbb; }
    .section-title { font-size: 13px; font-weight: 600; color: #666; margin: 16px 0 8px 0;
                     text-transform: uppercase; letter-spacing: 0.5px; }
    td { padding: 6px 10px; font-size: 13px; }
  "))),

  div(style = "max-width: 1100px; margin: auto;",

    h2("Chatbot Time Savings", style = "color:#222; margin-bottom: 2px;"),
    p(style = "color:#999; font-size: 12px; margin-bottom: 16px;",
      paste0(
        "Model run: ", format(model_run_date, "%b %d, %Y %I:%M %p"),
        " | Pre: ",  if (is.na(n_pre_wo))  "N/A" else n_pre_wo,  " WOs",
        " | Post: ", if (is.na(n_post_wo)) "N/A" else n_post_wo, " WOs",
        " | ", n_sessions, " sessions"
      )
    ),

    fluidRow(
      column(3, div(class = "box",
        div(class = "lbl", "Savings per lookup"),
        div(class = "val", paste0(round(median(delta_R), 1), " min")),
        div(class = "sub", paste0(
          "90% CI: [", round(quantile(delta_R, 0.05), 1),
          ", ",        round(quantile(delta_R, 0.95), 1), "]"
        ))
      )),
      column(3, div(class = "box",
        div(class = "lbl", "P(savings > 0)"),
        div(class = "val", paste0(round(mean(delta_R > 0) * 100, 1), "%")),
        div(class = "sub", "Posterior probability")
      )),
      column(3, div(class = "box",
        div(class = "lbl", "Manual retrieval (est.)"),
        div(class = "val", paste0(round(median(R_manual_avg) * 60, 1), " min")),
        div(class = "sub", "Bayesian posterior median")
      )),
      column(3, div(class = "box",
        div(class = "lbl", "Chatbot session (obs.)"),
        div(class = "val", paste0(round(mean_session * 60, 1), " min")),
        div(class = "sub", paste0("Mean of ", n_sessions, " sessions"))
      ))
    ),

    fluidRow(
      column(4, div(class = "box", plotOutput("p1", height = "260px"))),
      column(4, div(class = "box", plotOutput("p2", height = "260px"))),
      column(4, div(class = "box", plotOutput("p3", height = "260px")))
    ),

    div(class = "box",
      div(class = "section-title", "Total Savings"),
      tags$table(
        tags$tr(tags$td("Hours saved (median)"),
                tags$td(style = "font-weight:600;",
                        paste0(round(median(total_hrs), 0), " hrs"))),
        tags$tr(tags$td("90% CI"),
                tags$td(paste0("[", round(quantile(total_hrs, 0.05), 0),
                               ", ", round(quantile(total_hrs, 0.95), 0), "]"))),
        tags$tr(tags$td("Based on"),
                tags$td(paste0(n_sessions, " sessions")))
      )
    )
  )
)


# =============================================================================
# SERVER
# =============================================================================
server <- function(input, output, session) {

  output$p1 <- renderPlot({
    par(mar = c(4, 3, 2, 1))
    hist(delta_R, breaks = 40, col = "#4a7fb5", border = "white",
         main = "Minutes Saved per Lookup", xlab = "minutes", ylab = "",
         cex.main = 0.9)
    abline(v = median(delta_R), col = "red", lwd = 2, lty = 2)
    abline(v = 0, lty = 3)
  })

  output$p2 <- renderPlot({
    par(mar = c(4, 3, 2, 1))
    hist(R_manual_avg * 60, breaks = 40, col = "#d4873f", border = "white",
         main = "Estimated Manual Retrieval", xlab = "minutes", ylab = "",
         cex.main = 0.9)
    abline(v = median(R_manual_avg) * 60, col = "red", lwd = 2, lty = 2)
  })

  output$p3 <- renderPlot({
    par(mar = c(4, 3, 2, 1))
    if (length(session_sample) == 0) {
      plot.new()
      text(0.5, 0.5, "Session data unavailable", cex = 1.2, col = "#999")
    } else {
      hist(session_sample * 60, breaks = 30, col = "#3a8a5c", border = "white",
           main = "Observed Chatbot Sessions", xlab = "minutes", ylab = "",
           cex.main = 0.9)
      abline(v = mean_session * 60, col = "red", lwd = 2, lty = 2)
    }
  })
}

shinyApp(ui, server)
