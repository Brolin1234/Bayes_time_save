library(shiny)
library(rstanarm)
library(dplyr)

# ---------------------------------------------------------------
# DATA — replace with DB pull
# con <- dbConnect(...)
# df <- dbGetQuery(con, "SELECT ... FROM work_orders LEFT JOIN sessions ...")
# ---------------------------------------------------------------

set.seed(42)
n_emp <- 40; n_pre <- 600; n_post <- 400

emp <- data.frame(
  employee_id = paste0("E", sprintf("%03d", 1:n_emp)),
  location = sample(c("BLDG_A","BLDG_B","BLDG_C","BLDG_D"), n_emp, replace=T),
  gs_scale = sample(c(7,9,11,12,13), n_emp, replace=T),
  alpha_L = rnorm(n_emp, 0, 0.3),
  alpha_R = rnorm(n_emp, 0, 0.4)
)

make_wo <- function(n, period, emp) {
  d <- data.frame(
    wo_number = paste0("WO-", period, "-", sprintf("%04d", 1:n)),
    employee_id = sample(emp$employee_id, n, replace=T),
    project_cost = exp(rnorm(n, log(5000), 0.8)),
    wo_start_date = as.Date(ifelse(period=="PRE", "2024-01-01", "2025-01-15")) + sample(0:180, n, replace=T),
    post = ifelse(period=="PRE", 0, 1)
  )
  d <- merge(d, emp, by="employee_id")
  d$true_L <- exp(1.2 + 0.45*log(d$project_cost) - 0.02*d$gs_scale + d$alpha_L + rnorm(n,0,0.25))
  if (period=="PRE") {
    d$true_R <- exp(log(0.25) + 0.08*log(d$project_cost) + d$alpha_R + rnorm(n,0,0.4))
    d$session_duration <- NA
  } else {
    d$true_R <- exp(log(0.05) + 0.03*log(d$project_cost) + rnorm(n,0,0.3))
    d$session_duration <- d$true_R
  }
  d$hours_worked <- d$true_L + d$true_R + abs(rnorm(n,0,0.1))
  d
}

pre <- make_wo(n_pre, "PRE", emp)
post_raw <- make_wo(n_post, "POST", emp)
df <- bind_rows(pre, post_raw) %>%
  select(wo_number, employee_id, project_cost, hours_worked, session_duration,
         post, location, gs_scale, wo_start_date)


# ---------------------------------------------------------------
# MODEL — move to batch job later
# TODO: saveRDS(results, "model_results.rds")
# TODO: replace below with res <- readRDS("model_results.rds")
# ---------------------------------------------------------------

df_post <- df %>% filter(post==1, !is.na(session_duration)) %>%
  mutate(implied_L = hours_worked - session_duration) %>%
  filter(implied_L > 0) %>%
  mutate(log_L = log(implied_L), log_cost = log(project_cost))

df_pre <- df %>% filter(post==0) %>% mutate(log_cost = log(project_cost))

fit <- stan_lmer(
  log_L ~ log_cost + (1 | employee_id),
  data = df_post,
  prior = normal(0,1), prior_intercept = normal(0,2),
  chains = 4, iter = 4000, warmup = 2000,
  seed = 42, refresh = 0
)

L_draws <- posterior_predict(fit, newdata=df_pre, draws=2000)
R_manual <- sweep(-exp(L_draws), 2, df_pre$hours_worked, "+")
R_manual_avg <- rowMeans(R_manual)
mean_session <- mean(df_post$session_duration)
delta_R <- (R_manual_avg - mean_session) * 60

n_sessions <- nrow(df_post)
total_hrs <- delta_R / 60 * n_sessions
model_run_date <- Sys.time()


# ---------------------------------------------------------------
# DASHBOARD
# ---------------------------------------------------------------

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

  div(style="max-width: 1100px; margin: auto;",

    h2("Chatbot Time Savings", style="color:#222; margin-bottom: 2px;"),
    p(style="color:#999; font-size: 12px; margin-bottom: 16px;",
      paste0("Model run: ", format(model_run_date, "%b %d, %Y %I:%M %p"),
             " | Pre: ", nrow(df_pre), " WOs",
             " | Post: ", nrow(df_post), " WOs",
             " | ", n_sessions, " sessions")),

    fluidRow(
      column(3, div(class="box",
        div(class="lbl", "Savings per lookup"),
        div(class="val", paste0(round(median(delta_R),1), " min")),
        div(class="sub", paste0("90% CI: [", round(quantile(delta_R,0.05),1),
                                ", ", round(quantile(delta_R,0.95),1), "]"))
      )),
      column(3, div(class="box",
        div(class="lbl", "P(savings > 0)"),
        div(class="val", paste0(round(mean(delta_R>0)*100,1), "%")),
        div(class="sub", "Posterior probability")
      )),
      column(3, div(class="box",
        div(class="lbl", "Manual retrieval (est.)"),
        div(class="val", paste0(round(median(R_manual_avg)*60,1), " min")),
        div(class="sub", "Bayesian posterior median")
      )),
      column(3, div(class="box",
        div(class="lbl", "Chatbot session (obs.)"),
        div(class="val", paste0(round(mean_session*60,1), " min")),
        div(class="sub", paste0("Mean of ", n_sessions, " sessions"))
      ))
    ),

    fluidRow(
      column(4, div(class="box", plotOutput("p1", height="260px"))),
      column(4, div(class="box", plotOutput("p2", height="260px"))),
      column(4, div(class="box", plotOutput("p3", height="260px")))
    ),

    div(class="box",
      div(class="section-title", "Total Savings"),
      tags$table(
        tags$tr(tags$td("Hours saved (median)"), tags$td(style="font-weight:600;", paste0(round(median(total_hrs),0), " hrs"))),
        tags$tr(tags$td("90% CI"), tags$td(paste0("[", round(quantile(total_hrs,0.05),0), ", ", round(quantile(total_hrs,0.95),0), "]"))),
        tags$tr(tags$td("Based on"), tags$td(paste0(n_sessions, " sessions")))
      )
    )
  )
)

server <- function(input, output, session) {

  output$p1 <- renderPlot({
    par(mar=c(4,3,2,1))
    hist(delta_R, breaks=40, col="#4a7fb5", border="white",
         main="Minutes Saved per Lookup", xlab="minutes", ylab="", cex.main=0.9)
    abline(v=median(delta_R), col="red", lwd=2, lty=2)
    abline(v=0, lty=3)
  })

  output$p2 <- renderPlot({
    par(mar=c(4,3,2,1))
    hist(R_manual_avg*60, breaks=40, col="#d4873f", border="white",
         main="Estimated Manual Retrieval", xlab="minutes", ylab="", cex.main=0.9)
    abline(v=median(R_manual_avg)*60, col="red", lwd=2, lty=2)
  })

  output$p3 <- renderPlot({
    par(mar=c(4,3,2,1))
    hist(df_post$session_duration*60, breaks=30, col="#3a8a5c", border="white",
         main="Observed Chatbot Sessions", xlab="minutes", ylab="", cex.main=0.9)
    abline(v=mean_session*60, col="red", lwd=2, lty=2)
  })
}

shinyApp(ui, server)
