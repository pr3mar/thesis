-- model choosing the fittest developer
SELECT i.KEY                            TicketKey,
       i.FIELDS:issuetype: name::string IssueType,
       i.FIELDS:priority: name::string  IssuePriority,
       COALESCE(c.NumberOfComments, 0)  NumberOfComments,
       COALESCE(a.AuthoredActivity, 0)  AuthoredActivity,
       label.VALUE::string              Label,
       ARRAY_SIZE(i.FIELDS:labels)      NumberOfLabels,
       d.DegreeOfCycling                DegreeOfCycling,
       ROUND(d.DaysInDevelopment)       DaysInDevelopment,
       d.Developer                      Developer
FROM ISSUES i
         INNER JOIN (
    SELECT t.KEY                           TicketKey,
           t.ASSIGNEE                      Developer,
           COUNT(DISTINCT t.STATUS)        States,
           COUNT(*)                        Transitions,
           (Transitions / States) - 1      DegreeOfCycling,
           SUM(TIMEDELTA) / (60 * 60 * 24) DaysInDevelopment,
           SUM(TIMEDELTA) / (60 * 60)      HoursInDevelopment
    FROM TIMELINES t
    WHERE STATUS IN ('Development', 'Needs CR fixes', 'Needs QA fixes')
      AND t.ASSIGNEE IS NOT NULL
    GROUP BY 1, 2
) d ON d.TicketKey = i.KEY
         INNER JOIN USERS u ON d.Developer = u.USERKEY
         LEFT JOIN (
    SELECT KEY,
           COMMENT:author: key::string Author,
           COUNT(*)                    NumberOfComments
    FROM COMMENTS
    GROUP BY 1, 2
) c ON c.KEY = i.KEY AND d.Developer = c.Author
         LEFT JOIN (
    SELECT KEY,
           USERID   Developer,
           COUNT(*) AuthoredActivity
    FROM CHANGELOGS
    GROUP BY 1, 2
    ORDER BY 3 DESC
) a ON a.KEY = i.KEY AND d.Developer = a.Developer,
     LATERAL FLATTEN(FIELDS:labels) label
WHERE u.ACTIVE = TRUE;