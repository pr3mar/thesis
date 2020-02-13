# Master's Thesis: Data Driven Project Management

The plan:
## Phase 1:
- [x] ~~wip -> Choose complete data to work on~~ Will be updated on demand
- [x] Anonymize the data. Best effort for now, not committing anything with PII
- [x] we are missing the comments, edits --> history of editing, [!] actually we have all of these
- [x] Export the data to MySQL/..., currently stuck on Snowflake, won't do for now, I can query JSONs easily there
- [x] wip -> Script to automatically transform the data into 'analyzable' form
    - [x] queries in [`basic_stats.sql`](code/db/stat_queries.sql) generates some views, see the `.csv`s in `basic_stats/`
    - [x] ~~wip -> explore which fields are useful for analysis of the distributions, e.g. `Watch count` is very insightful [1].~~
    - [x] ~~explore what is the best way to do this, we already have a couple of supplementary tables for transitions~~
    - [x] write queries which to generate views on demand (see the [sqls](code/db/stat_queries.sql)), will be updated on demand
- [ ] WIP: identify additional _dimensions_ and _metrics_ [1][2]
-----------
## Phase 2:
- [ ] Build the user dependency graph -> 2 layer graph [Users] <-> [Tasks]
    - [ ] represent how the users interact with each other through the tickets
- [ ] Build the ticket statistical model
    - [x] distribution of the tickets broken down by issue type, priority, resolution status regarding the #changelog items, #comments, #watchers, #days to resolve, #days to date
- [ ] !!DETECT TICKET REASSIGNMENT!! Status is not changed, but assignee is changed. Try to infer why?
- [ ] build a user "timeline". How many issues is he/she working on and at the same time - Do it per week/month/quarter. Where does it get stuck?
- [ ] Quarter analysis: -> will be useful for
    - how many tickets are completed in a quarter per user/team/overall
    - how many tickets got to QA
- [ ] How often are [issue types] created?

-----------
## Findings so far:
- [1]: `Watch count` is the number of users that are getting updates/notifications of the given issue. It indicates how many people are interested in the development of the issue.
- [2]: By dimensions I mean categorical attributes by which we can break down the data, such as priorities, issue types, status. By dimensions I mean some quantifiable data such as days in status, #watchers, ...
    
    At the moment we only have a single grain of how we can view the data, there are some other views which need to be taken into consideration as well: 
    - histogram of the changelog: TODO: generalize this to all tickets, use (time in status, #times in same status)
        - how many times in same status, how many cycles (CR -> FIXES -> CR -> FIXES, QA -> FIXES -> QA, ...)
    - per users:
        - how many times in which role (owner, developer, lead, ...)
        - how many times a ticket has been returned to him/her
        - is there interaction if he/she has a role defined
    - define completeness of the ticket: how many fields are filled out of all possible. Determine which are often left out, but might be very important.

