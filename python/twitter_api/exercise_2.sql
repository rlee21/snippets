SELECT
    person
FROM (SELECT
         person,
         event_timestamp,
         event_name,
         ROW_NUMBER() OVER (PARTITION BY person ORDER BY event_timestamp DESC) AS rownum
      FROM building_entrance_exit_events
      ) a
WHERE rownum = 1
AND event_name = 'entrance';

