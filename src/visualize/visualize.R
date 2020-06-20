library(ggplot2)
library(tidyr)
library(reshape2)

breakdownByType <- read.csv(file= "../../results/basic_stats/by_type.csv")

issueTypes <- ggplot(breakdownByType) +
      geom_bar(aes(Issue.type, Count), stat = "identity") +
      geom_bar(aes(Issue.type, Count, fill=Resolved), stat = "identity", position = "dodge") +
      xlab("Issue type") + ylab("Count")
issueTypes


breakdownByPriority <- read.csv(file= "../../results/basic_stats/by-priority.csv")

issuePriority <- ggplot(breakdownByPriority) +
      geom_bar(aes(Issue.priority, Count), stat = "identity") +
      geom_bar(aes(Issue.priority, Count, fill=Resolved), stat = "identity", position = "dodge") +
      xlab("Issue priority") + ylab("Count")
issuePriority


resolvedIssues <- read.csv(file = "../../results/basic_stats/resolved-issues.csv")
issueTypes <- unique(resolvedIssues$Issue.Type)
plots <- list()
for (type in issueTypes) {
  data <- resolvedIssues[resolvedIssues$Issue.Type == type,]
  plots[[type]] <- ggplot(data) +
    geom_histogram(aes(x = Watch.Count), binwidth = 1, color="black", fill="red") +
    scale_x_continuous(breaks = seq(0, 35, 1), labels = seq(0, 35, 1)) +
    ylab("Issue count") +
    xlab("People watching") +
    ggtitle(type) +
    theme(plot.title = element_text(hjust = 0.5))
}

plots["Bug"]

unrresolvedIssues <- read.csv(file = "../../results/basic_stats/unresolved-issues.csv")
issueTypes <- sort(unique(unrresolvedIssues$Status))
plots <- list()
for (type in issueTypes) {
  data <- unrresolvedIssues[unrresolvedIssues$Status == type,]
  if (nrow(data) < 30) {
    cat("Ignored because too little data: ", type, nrow(data), fill = TRUE)
    next
  }
  plots[[type]] <- ggplot(data) +
    geom_histogram(aes(x = Changelog.Count), binwidth = 5, color="black", fill="red") +
    # scale_x_continuous(breaks = seq(0, 35, 1), breakdown_labels = seq(0, 35, 1)) +
    ylab("Issue count") +
    xlab("Changelog count") +
    ggtitle(paste0(type, ", (", nrow(data), ")")) +
    theme(plot.title = element_text(hjust = 0.5))
}

plots