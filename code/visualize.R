library(ggplot2)
library(tidyr)
library(reshape2)

breakdownByType <- read.csv(file="../basic_stats/by_type.csv")

issueTypes <- ggplot(breakdownByType) +
      geom_bar(aes(Issue.type, Count), stat = "identity") +
      geom_bar(aes(Issue.type, Count, fill=Resolved), stat = "identity", position = "dodge") +
      xlab("Issue type") + ylab("Count")
issueTypes


breakdownByPriority <- read.csv(file="basic_stats/by-priority.csv")

issuePriority <- ggplot(breakdownByPriority) +
      geom_bar(aes(Issue.priority, Count), stat = "identity") +
      geom_bar(aes(Issue.priority, Count, fill=Resolved), stat = "identity", position = "dodge") +
      xlab("Issue priority") + ylab("Count")
issuePriority


resolvedIssues <- read.csv(file = "basic_stats/resolved-issues.csv")
