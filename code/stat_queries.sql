-- NOTE: Data up to ~2019-11-22

-- get all the resolved issues ordered by their time to resolve
SELECT 
    sha2(ji.key) "Code", 
    ji.fields:issuetype:name::string "Type", 
    ji.fields:resolution:name::string "Resolution", 
    ji.fields:status:name::string "Status", 
    ji.fields:priority:name::string "Priority", 
    ji.changelog:total "Changelog Count", 
    ji.fields:comment:total "Comments Count", 
    ji.fields:watches:watchCount "Watch Count",
    TO_TIMESTAMP_NTZ(ji.fields:created) "Date Created",
    TO_TIMESTAMP_NTZ(ji.fields:resolutiondate) "Date resolved",
    DATEDIFF(days, TO_TIMESTAMP_NTZ(ji.fields:created), TO_TIMESTAMP_NTZ(ji.fields:resolutiondate)) "Days to resolve",
    DATEDIFF(hour, TO_TIMESTAMP_NTZ(ji.fields:created), TO_TIMESTAMP_NTZ(ji.fields:resolutiondate)) "Hours to resolve"
FROM 
    "STITCHDATA"."JIRA"."JIRA_ISSUES" ji
WHERE 
    ji.KEY LIKE 'MAB-%'
    AND ji.fields:issuetype:name IN ('New Feature or Improvement', 'Bug', 'Internal Improvement')
    AND NOT IS_NULL_VALUE(ji.fields:resolution)
ORDER BY 
    "Days to resolve",
    "Watch Count" DESC, 
    "Changelog Count" DESC, 
    "Comments Count" DESC;


-- get all UNresolved issues and their *UP TO DATE* time to resolve
SELECT 
    sha2(ji.key) "Code", 
    ji.fields:issuetype:name::string "Type", 
    ji.fields:status:name::string "Status", 
    ji.fields:priority:name::string "Priority", 
    ji.changelog:total::string "Changelog Count", 
    ji.fields:comment:total "Comments Count", 
    ji.fields:watches:watchCount "Watch Count",
    TO_TIMESTAMP_NTZ(ji.fields:created) "Date Created",
    DATEDIFF(days, TO_TIMESTAMP_NTZ(ji.fields:created), IFF(IS_NULL_VALUE(ji.fields:resolutiondate), CURRENT_TIMESTAMP, TO_TIMESTAMP_NTZ(ji.fields:resolutiondate))) "Days to date"
FROM 
    "STITCHDATA"."JIRA"."JIRA_ISSUES" ji
WHERE 
    ji.KEY LIKE 'MAB-%'
    AND ji.fields:issuetype:name IN ('New Feature or Improvement', 'Bug', 'Internal Improvement')
    AND IS_NULL_VALUE(ji.fields:resolution)
ORDER BY 
    "Watch Count" DESC, 
    "Changelog Count" DESC, 
    "Comments Count" DESC;

-- main view, resolved aggregates
SELECT 
    ji.fields:priority:name "Priority", 
    ji.fields:status:name "Status", 
    ji.fields:resolution:name "Resolution", 
    count(*) "Count",
    AVG(DATEDIFF(days, TO_TIMESTAMP_NTZ(ji.fields:created), IFF(IS_NULL_VALUE(ji.fields:resolutiondate), CURRENT_TIMESTAMP, TO_TIMESTAMP_NTZ(ji.fields:resolutiondate)))) "Avg days to resolve",
    AVG(DATEDIFF(hour, TO_TIMESTAMP_NTZ(ji.fields:created), IFF(IS_NULL_VALUE(ji.fields:resolutiondate), CURRENT_TIMESTAMP, TO_TIMESTAMP_NTZ(ji.fields:resolutiondate)))) "Avg hours to resolve"
FROM 
    "JIRA"."JIRA_ISSUES" ji
WHERE 
    ji.KEY LIKE 'MAB-%'
    AND ji.fields:issuetype:name IN ('New Feature or Improvement', 'Bug', 'Internal Improvement')
    AND NOT IS_NULL_VALUE(ji.fields:resolution)
GROUP BY
    "Priority",
    "Status",
    "Resolution"
ORDER BY
    "Priority",
    "Avg days to resolve",
    "Count" DESC;
