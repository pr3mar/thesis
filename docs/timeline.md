# Timelines

## Overview  

All issues on Jira have some kind of activity, which reflect the actions of its users.
Some of these actions are changing an issue's state, to which actions we refer to as transitions.
But this is not the only kind of information we have, and it does not reflect 
the entire state of the issues at a given point in time.

To obtain the entire state of the issue we need to combine two pieces of information: 
- the issue's state
- the issue's assignee

Sorting the combination of the two pieces by the date of its occurrence yields a very simple timeline.
Each timeline item needs to have a date interval on which it is valid, and as a consequence it is
non-overlapping with the rest of the items of the same issue.

## Timeline analysis

Some insights which are available from timeline analysis:
- average time used per:
    - **tl;dr**: dimensions are issue key, developer id, status; metric is time spent  
    - issue
    - issue + status: how much time per status has been spent
    - issue + status + developer: how much time has a developer has spent working on an issue in a given status
    - status + developer: how much time per status has a developer spent
    - issue + developer: how much time per issue has a developer spent
    - developer: how much time a developer has spent in total
    - status: how much time have all the developers spent in a given status
- basic detection of cycles
    - count(unique keys) != count(times in state) 
- basic detection of workflow inconsistencies (e.g. how can an issue be twice in master?)
- identification of stale/obsolete/underused/abused workflow elements
    - very low/high amount of time spent in a particular element
- identification of stale/halted issues
    - very low/high amount of time spent in backlog/on hold/other state
- identification of card complexity
    - there is a high rate of cycles which last long
    - can be used to increase/decrease complexity
- infer collaborators (teams), i.e. people which are co-occurring on issues' timelines frequently
    - (!) without building a network representation
- impact/causality analysis
    - if we include all other activities we can infer the role of each user
    - are there any events which cause things to move forward/halt
        - e.g. comments with mentions, description edits, etc. by PMOs, PMs, team leads, etc. 

Each of these can be calculated for a given time interval (e.g. a quarter/half a year).
Multiple intervals can be compared amongst each other, given that they are either normalized (?), or are of the same length. 
Comparison of multiple intervals can illustrate the life of employees, teams and even the entire company.
It can provide insights about the performance changes over time, and further correlate with events which
are not 

**NOTE**: This timeline analysis does not take into account the fact that a single person works on multiple
issues in parallel. Instead, it assumes that all the timelines are non-overlapping.  

**NOTE**: The time deltas are just an approximation, as no external factors (public holidays, sick days, vacation)
have been taken into account at this point

## Timeline interleaving

So far, we have only looked at the aggregated values of the timelines, and we assumed
that all timelines are independent of each other, but this is clearly not the case.
In this section we are going to try to take into account this fact and rectify the analysis,
or at least visualize how a user's workload looks like.



## Some useful SQL views

```sql
-- 1) avg time in status of a user per issue for a given interval 
SELECT
    KEY,
    ASSIGNEE,
    STATUS,
    COUNT(DISTINCT KEY)             UNIQUEKEYS,
    COUNT(*)                        COUNT,
    AVG(TIMEDELTA) / (60 * 60 * 24) AVG_DAY,
    MAX(TIMEDELTA) / (60 * 60 * 24) MAX_DAYS,
    MIN(TIMEDELTA) / (60 * 60 * 24) MIN_DAYS,
    AVG(TIMEDELTA) / (60 * 60)      AVG_HOUR,
    MAX(TIMEDELTA) / (60 * 60)      MAX_HOUR,
    MIN(TIMEDELTA) / (60 * 60)      MIN_HOUR
FROM TIMELINES
WHERE
    DATEFROM >= '2019-10-01' AND
    DATETO < '2020-01-01'
GROUP BY
    KEY,
    ASSIGNEE,
    STATUS
ORDER BY 1, 4 DESC;
```
