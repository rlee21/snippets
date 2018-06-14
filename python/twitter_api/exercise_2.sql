-- Write a SQL query which will return a list of people who are currently in the building.
SELECT
    a.person
FROM (SELECT
         person,
         event_timestamp,
         event_name,
         ROW_NUMBER() OVER (PARTITION BY person ORDER BY event_timestamp DESC) AS rownum
      FROM building_entrance_exit_events
      ) a
WHERE a.rownum = 1
AND a.event_name = 'entrance';

