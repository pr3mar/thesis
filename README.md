# Master's Thesis: Data Driven Project Management

The plan:
## Phase 1:
- [ ] wip -> Choose complete data to work on
- [ ] ?? Anonymize the data. Best effort for now, not committing anything with PII
- [x] we are missing the comments, edits --> history of editing, [!] actually we have all of these
- [x] Export the data to MySQL/..., currently stuck on Snowflake, won't do for now, I can query JSONs easily there
- [ ] wip -> Script to automatically transform the data into 'analyzable' form
    - [x] queries in [`basic_stats.sql`](code/stat_queries.sql) generates some views, see the `.csv`s in `basic_stats/`
    - [ ] wip -> explore which fields are useful for analysis of the distributions, e.g. `Watch count` is very insightful [1].
    - [ ] explore what is the best way to do this, we already have a couple of supplementary tables for transitions
-----------
## Phase 2:
- [ ] Build the user dependency graph -> 2 layer graph [Users] <-> [Tasks]
    - [ ] represent how the users interact with each other through the tickets
- [ ] Build the ticket statistical model
    - [ ] distribution of the tickets with regards to [define] attribute(s)

-----------
Findings so far:
- [1]: `Watch count` is the number of users that are getting updates/notifications of the given issue. It indicates how many people are interested in the development of the issue.
