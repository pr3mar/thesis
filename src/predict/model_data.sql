SELECT TicketKey,
       IssueType,
       IssuePriority,
       Component,
       NumberOfComponents,
       Label,
       NumberOfLabels,
       DegreeOfCycling,
       DaysInDevelopment
-- SELECT COUNT( DISTINCT TicketKey)
FROM (SELECT i.KEY                                                                         TicketKey,
             i.FIELDS:issuetype: name::string                                              IssueType,
             i.FIELDS:priority: name::string                                               IssuePriority,
             component.VALUE: name::string                                                 Component,
             ARRAY_SIZE(i.FIELDS:components)                                               NumberOfComponents,
             label.VALUE::string                                                           Label,
             ARRAY_SIZE(i.FIELDS:labels)                                                   NumberOfLabels,
             d.DegreeOfCycling                                                             DegreeOfCycling,
             ROUND(d.DaysInDevelopment) DaysInDevelopment,
             ROUND(d.HoursInDevelopment) HoursInDevelopment,
             convert_timezone('UTC', to_timestamp_tz(i.FIELDS:created::string,
                                                     'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')) DateCreated,
             convert_timezone('UTC', to_timestamp_tz(i.FIELDS:resolutiondate::string,
                                                     'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')) ResolutionDate,
             DATEDIFF('days', DateCreated, ResolutionDate)                                 TimeToResolve
      FROM ISSUES i
               INNER JOIN (
          SELECT t.KEY                      TicketKey,
                 COUNT(DISTINCT t.STATUS)   States,
                 COUNT(*)                   Transitions,
                 (Transitions / States) - 1 DegreeOfCycling
          FROM TIMELINES t
          GROUP BY 1
      ) t ON t.TicketKey = i.KEY
               INNER JOIN (
          SELECT t.KEY                      TicketKey,
                 COUNT(DISTINCT t.STATUS)   States,
                 COUNT(*)                   Transitions,
                 (Transitions / States) - 1 DegreeOfCycling,
                 AVG(TIMEDELTA) / (60 * 60 * 24)   DaysInDevelopment,
                 AVG(TIMEDELTA) / (60 * 60)   HoursInDevelopment
          FROM TIMELINES t
          WHERE STATUS IN ('Development', 'Needs CR fixes', 'Needs QA fixes')
          GROUP BY 1
      ) d ON d.TicketKey = i.KEY,
           LATERAL FLATTEN(FIELDS:components) component,
           LATERAL FLATTEN(FIELDS:labels) label
      WHERE FIELDS:resolutiondate IS NOT NULL AND HoursInDevelopment > 0.5 AND DaysInDevelopment < 30)
--       WHERE FIELDS:resolutiondate IS NOT NULL AND HoursInDevelopment > 2)
ORDER BY DaysInDevelopment DESC;
