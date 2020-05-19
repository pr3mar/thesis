SELECT TicketKey,
       IssueType,
       IssuePriority,
       Component,
       NumberOfComponents,
       Label,
       NumberOfLabels,
       DegreeOfCycling,
       TimeToResolve
FROM (SELECT i.KEY                                                                         TicketKey,
             i.FIELDS:issuetype: name::string                                              IssueType,
             i.FIELDS:priority: name::string                                               IssuePriority,
             component.VALUE: name::string                                                 Component,
             ARRAY_SIZE(i.FIELDS:components)                                               NumberOfComponents,
             label.VALUE::string                                                           Label,
             ARRAY_SIZE(i.FIELDS:labels)                                                   NumberOfLabels,
             t.DegreeOfCycling                                                             DegreeOfCycling,
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
      ) t ON t.TicketKey = i.KEY,
           LATERAL FLATTEN(FIELDS:components) component,
           LATERAL FLATTEN(FIELDS:labels) label
      WHERE FIELDS:resolutiondate IS NOT NULL);
