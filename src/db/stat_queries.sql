-- -- NOTE: Data up to 2020-01-04

// main view, resolved
SELECT 
    sha2(ji.key) "Issue id",
    ji.fields:issuetype:name::string "Issue Type",
    ji.fields:priority:name::string "Priority",
    ji.fields:resolution:name::string "Resolution",
    ji.fields:status:name::string "Status",
    ch."Changelog Count",
    com."Comments Count",
    ji.fields:watches:watchCount "Watch Count",
    TO_TIMESTAMP_NTZ(ji.fields:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM') "Date Created",
    TO_TIMESTAMP_NTZ(ji.fields:resolutiondate::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM') "Date resolved",
    DATEDIFF(days, TO_TIMESTAMP_NTZ(ji.fields:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'), TO_TIMESTAMP_NTZ(ji.fields:resolutiondate::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')) "Days to resolve",
    DATEDIFF(hour, TO_TIMESTAMP_NTZ(ji.fields:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'), TO_TIMESTAMP_NTZ(ji.fields:resolutiondate::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')) "Hours to resolve"
FROM
    ISSUES ji
    JOIN (
        SELECT KEY, COUNT(*) "Changelog Count"
        FROM JIRA.CHANGELOGS
        GROUP BY key
    ) ch on ch.key = ji.key
    JOIN (
        SELECT KEY, COUNT(*) "Comments Count"
        FROM JIRA.COMMENTS
        GROUP BY key
    ) com on com.key = ji.key
WHERE
    ji.KEY LIKE 'MAB-%'
    AND ji.fields:issuetype: name IN ('Bug', 'Epic', 'Internal Improvement', 'New Feature or Improvement')
    AND ji.fields:resolutiondate IS NOT NULL
ORDER BY
    "Days to resolve",
    "Watch Count" DESC,
    "Changelog Count" DESC,
    "Comments Count" DESC;

-- main view, UNresolved
SELECT 
    sha2(ji.key) "Issue id",
    ji.fields:issuetype:name::string "Type",
    ji.fields:status:name::string "Status",
    ji.fields:priority:name::string "Priority",
    ch."Changelog Count",
    com."Comments Count",
    ji.fields:watches:watchCount "Watch Count",
    TO_TIMESTAMP_NTZ(ji.fields:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM') "Date Created",
    DATEDIFF(days, TO_TIMESTAMP_NTZ(ji.fields:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'), IFF(ji.fields:resolutiondate IS NULL, CURRENT_TIMESTAMP, TO_TIMESTAMP_NTZ(ji.fields:resolutiondate::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'))) "Days to date"
FROM 
    ISSUES ji
    JOIN (
        SELECT KEY, COUNT(*) "Changelog Count"
        FROM JIRA.CHANGELOGS
        GROUP BY key
    ) ch on ch.key = ji.key
    JOIN (
        SELECT KEY, COUNT(*) "Comments Count"
        FROM JIRA.COMMENTS
        GROUP BY key
    ) com on com.key = ji.key
WHERE 
    ji.KEY LIKE 'MAB-%'
    AND ji.fields:issuetype:name IN ('Bug', 'Epic', 'Internal Improvement', 'New Feature or Improvement')
    AND ji.fields:resolutiondate IS NULL AND ji.fields:resolution:name IS NULL
ORDER BY
    "Watch Count" DESC,
    "Changelog Count" DESC,
    "Comments Count" DESC;

-- main view, resolved aggregates
SELECT 
    ji.fields:priority:name::string "Priority", 
    ji.fields:status:name::string "Status", 
    ji.fields:resolution:name::string "Resolution", 
    count(*) "Count",
    AVG(DATEDIFF(days, TO_TIMESTAMP_NTZ(ji.fields:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'), IFF(ji.fields:resolutiondate IS NULL, CURRENT_TIMESTAMP, TO_TIMESTAMP_NTZ(ji.fields:resolutiondate::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')))) as "Avg days to resolve",
    AVG(DATEDIFF(hour, TO_TIMESTAMP_NTZ(ji.fields:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'), IFF(ji.fields:resolutiondate IS NULL, CURRENT_TIMESTAMP, TO_TIMESTAMP_NTZ(ji.fields:resolutiondate::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')))) as "Avg hours to resolve"
FROM 
    ISSUES ji
WHERE 
    ji.KEY LIKE 'MAB-%'
    AND ji.fields:issuetype:name IN ('Bug', 'Epic', 'Internal Improvement', 'New Feature or Improvement')
    AND ji.fields:resolution IS NOT NULL
GROUP BY
    "Priority",
    "Status",
    "Resolution"
ORDER BY
    "Priority",
    "Avg days to resolve",
    "Count" DESC;

// get all issue types and their counts
SELECT
--     ji.fields:issuetype:name::string "Issue type",
    ji.fields:priority:name::string "Issue priority",
    IFF(ji.fields:resolutiondate IS NULL, 'No', 'Yes') "Resolved",
    count(*) "Count"
FROM
    ISSUES ji
WHERE
    ji.KEY LIKE 'MAB-%'
    AND ji.fields:issuetype:name IN ('Bug', 'Epic', 'Internal Improvement', 'New Feature or Improvement')
GROUP BY
    "Issue priority",
    "Resolved"
ORDER BY
--     "Issue type",
    "Issue priority",
    "Count" DESC;

SELECT ji.key, ji.fields:created, ji.fields:issuetype:name::string, ji.fields:customfield_10004::int "Story Points"
FROM ISSUES ji
WHERE ji.KEY LIKE 'MAB-%' AND ji.fields:customfield_10004 IS NOT NULL;


--
SELECT 
--     sha2(ji.key),
    ji.fields:priority:name::string "Priority", 
--     ji.fields:status:name::string "Status",
--     TO_DATE(ji.fields:duedate) "Due date",
    count(*) "Issues overdue",
    AVG(DATEDIFF(days, TO_DATE(ji.fields:duedate), CURRENT_DATE))"Avg days overdue"
FROM ISSUES ji
WHERE 
    ji.KEY LIKE 'MAB-%'
    AND TO_DATE(ji.fields:duedate) IS NOT NULL
    AND ji.fields:resolutiondate IS NULL
    AND DATEDIFF(days, TO_DATE(ji.fields:duedate), CURRENT_DATE) > 1
    AND ji.fields:status:name::string NOT IN ('Live', 'Done', 'Cancelled')
GROUP BY
    "Priority"
--      "Status"
ORDER BY
    "Priority",
    "Avg days overdue" DESC;
