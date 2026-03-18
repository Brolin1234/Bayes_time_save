library(brms)
library(dplyr)
library(ggplot2)
library(knitr)

set.seed(42)


# simulate data 

n_emp <- 40
n_pre <- 600
n_post <- 400

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
    post = ifelse(period == "PRE", 0, 1)
  )
  d <- merge(d, emp, by="employee_id")
  d$true_L <- exp(1.2 + 0.45*log(d$project_cost) - 0.02*d$gs_scale + d$alpha_L + rnorm(n,0,0.25))
  if (period == "PRE") {
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
post <- make_wo(n_post, "POST", emp)

# drop true columns — wouldn't have these in production
df <- bind_rows(pre, post) %>%
  select(wo_number, employee_id, project_cost, hours_worked, session_duration, post, location, gs_scale)


#  1: implied labor in post period 

df_post <- df %>% filter(post==1, !is.na(session_duration)) %>%
  mutate(implied_L = hours_worked - session_duration) %>%
  filter(implied_L > 0) %>%
  mutate(log_L = log(implied_L), log_cost = log(project_cost))


# 2: fit labor model 

fit <- brm(
  log_L ~ log_cost + (1 | employee_id),
  data = df_post,
  family = gaussian(),
  prior = c(prior(normal(0,2), class="Intercept"),
            prior(normal(0,1), class="b"),
            prior(cauchy(0,1), class="sd"),
            prior(cauchy(0,1), class="sigma")),
  chains = 4, iter = 4000, warmup = 2000,
  seed = 42, silent = 2, refresh = 0
)


# 3: predict labor for pre period 

df_pre <- df %>% filter(post==0) %>% mutate(log_cost = log(project_cost))
L_draws <- posterior_epred(fit, newdata = df_pre, allow_new_levels = TRUE, ndraws = 2000)


# 4: recover R_manual, compute delta R 

R_manual <- sweep(-exp(L_draws), 2, df_pre$hours_worked, "+")  # hours_worked - exp(predicted log L)
R_manual_avg <- rowMeans(R_manual)
mean_session <- mean(df_post$session_duration)
delta_R <- (R_manual_avg - mean_session) * 60  # convert to minutes


# results

results <- data.frame(
  Metric = c("Estimated manual retrieval (median)",
             "Observed chatbot session (mean)",
             "Time savings per lookup (median)",
             "90% credible interval",
             "P(savings > 0)",
             paste0("Total hours saved (", nrow(df_post), " lookups)")),
  Value = c(paste0(round(median(R_manual_avg)*60, 1), " min"),
            paste0(round(mean_session*60, 1), " min"),
            paste0(round(median(delta_R), 1), " min"),
            paste0("[", round(quantile(delta_R, 0.05), 1), ", ", round(quantile(delta_R, 0.95), 1), "]"),
            round(mean(delta_R > 0), 3),
            paste0(round(median(delta_R/60 * nrow(df_post)), 1), " hrs"))
)

kable(results, col.names = c("", ""), caption = "Time Savings Estimates")


# plots

par(mfrow=c(1,3), mar=c(4,3,3,1))

# posterior of time savings
hist(delta_R, breaks=40, col="steelblue", border="white",
     main="Minutes Saved per Lookup", xlab="minutes", ylab="")
abline(v=median(delta_R), col="red", lwd=2, lty=2)
abline(v=0, lty=3)

# estimated manual retrieval distribution
hist(R_manual_avg * 60, breaks=40, col="darkorange", border="white",
     main="Estimated Manual Retrieval", xlab="minutes", ylab="")
abline(v=median(R_manual_avg)*60, col="red", lwd=2, lty=2)

# observed chatbot session times
hist(df_post$session_duration * 60, breaks=30, col="forestgreen", border="white",
     main="Observed Chatbot Sessions", xlab="minutes", ylab="")
abline(v=mean(df_post$session_duration)*60, col="red", lwd=2, lty=2)
