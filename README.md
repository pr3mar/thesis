# Master's Thesis: Data Driven Project Management

The plan:
## Phase 1:
- [ ] Choose complete data to work on
- [?] Anonymize the data. 
- [ ] we are missing the comments, edits --> history of editing
- [?] Export the data to MySQL/..., currently stuck on Snowflake
- [ ] Script to automatically transform the data into 'analyzable' form
    - [ ] explore what is the best way to do this, we already have a couple of supplementary tables for transitions
-----------
## Phase 2:
- [ ] Build the user dependency graph -> 2 layer graph [Users] <-> [Tasks]
    - [ ] represent how the users interact with each other through the tickets
- [ ] Build the ticket statistical model
    - [ ] distribution of the tickets with regards to [define] attribute(s)
