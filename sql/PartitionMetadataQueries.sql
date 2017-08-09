/* Inserts, Updates, Deletes */
  SELECT *
    FROM all_tab_modifications
   WHERE table_owner = 'DM_OBR' 
       AND table_name = 'WC_OBRTXN_F'
ORDER BY partition_name;
------------------------------------------------------------------------------------------------------------
/* last analyzed */
  SELECT   table_name,
           partition_name,
           global_stats,
           last_analyzed,
           num_rows
    FROM   user_tab_partitions
   WHERE   table_name = 'FACT_PROJ_RESOURCE'
ORDER BY   1, 2, 4 DESC NULLS LAST;

------------------------------------------------------------------------------------------------------------
/* Status of Indexes on Partition Table */    
SELECT i.index_name, p.status, p.partition_name
FROM user_indexes i, user_ind_partitions p 
WHERE i.index_name = p.index_name 
AND i.table_name = 'FACT_PROJ_RESOURCE' 
--AND p.status = 'UNUSABLE'
AND i.index_name NOT IN ('PK_FACT_PROJ_RESOURCE', 'UK_FACT_PROJ_RESOURCE')
ORDER BY i.index_name, p.partition_position;