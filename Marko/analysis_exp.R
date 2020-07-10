library(rstan)
library(ggplot2)
library(dplyr)

# read the data
d <- read.csv("data/prediction_data/ticket_model/encoded_model_data_development_filtered_hours_all_real-data.csv")

# remove weird data (duration lower than half a day)
d <- d %>% filter(Duration > 0.5)

# build the model
model <- stan_model(file="lognormal.stan")

# prepare data and fit
stan_data <- list(y=d$Duration, n=nrow(d))
fit <- sampling(model,
                data=stan_data)

# extract
extract <- extract(fit)
mu <- mean(extract$mu)
sigma <- mean(extract$sigma)



# compare with developer #6
compare_value <- d[6,]
smaller <- plnorm(compare_value, meanlog=mu, sdlog=sigma)
larger <- 1 - smaller
label <- paste0(format(smaller*100, digits=1),
                "% < ",
                format(compare_value, digits=2),
                " < ",
                format(larger*100, digits=1),
                "%")

# plot
ggplot(data=data.frame(x=c(0, 20)), aes(x=x)) +
  stat_function(fun=dlnorm,
                n=1000,
                args=list(meanlog=mu, sdlog=sigma),
                geom="area",
                fill="#3182bd",
                alpha =0.6) +
  geom_segment(aes(x=compare_value,
                   xend=compare_value,
                   y=0,
                   yend=0.2),
                   size=1.5,
                   color="#ff4e3f",
                   alpha=0.4) +
  geom_text(aes(label=label, x=compare_value, y=0.21)) +
  xlab("days") +
  ylab("density")


