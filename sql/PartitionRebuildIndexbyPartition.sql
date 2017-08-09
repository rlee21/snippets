/* Update Global Indexes */
DECLARE
   v_last_year         NUMBER := (TO_NUMBER (TO_CHAR (SYSDATE - 1, 'YYYY'))) - 1;
   v_current_year   NUMBER := TO_NUMBER (TO_CHAR (SYSDATE - 1, 'YYYY'));   
   v_table_name     VARCHAR2 (30) := 'FACT_LEDGER';
   v_sql                   VARCHAR2 (4000);
   
BEGIN
   v_sql := 'ALTER TABLE '|| v_table_name || ' ' || 'TRUNCATE PARTITION fdm_yr_' || v_last_year || ' UPDATE GLOBAL INDEXES';
   EXECUTE IMMEDIATE v_sql;

   v_sql := 'ALTER TABLE ' || v_table_name || ' ' || 'TRUNCATE PARTITION fdm_yr_' || v_current_year || ' UPDATE GLOBAL INDEXES';
   EXECUTE IMMEDIATE v_sql;

EXCEPTION
   WHEN OTHERS THEN NULL;

END;
/

/* Rebuild Global Indexes */
DECLARE
   v_index_name       VARCHAR2(30);
   v_partition_name   VARCHAR2(30);   
   v_sql              VARCHAR2(4000);
   CURSOR cur_get_indexes IS
                            SELECT i.index_name, p.partition_name
                              FROM user_indexes i, user_ind_partitions p 
                             WHERE i.index_name = p.index_name 
                               AND i.table_name = 'FACT_PROJ_RESOURCE' AND p.status = 'UNUSABLE'
                               AND i.index_name NOT IN ('PK_FACT_PROJ_RESOURCE', 'UK_FACT_PROJ_RESOURCE')
                            ORDER BY i.index_name, p.partition_position;
       
BEGIN
   OPEN cur_get_indexes;

   LOOP

      FETCH cur_get_indexes INTO v_index_name, v_partition_name;
      EXIT WHEN cur_get_indexes%NOTFOUND;

       BEGIN
        v_sql:= 'ALTER INDEX '||v_index_name||' '||'REBUILD PARTITION '||v_partition_name; 
             
       EXECUTE IMMEDIATE v_sql;

       EXCEPTION 
       WHEN OTHERS THEN NULL;
       END; 
         
   END LOOP;

   CLOSE cur_get_indexes;

END;
/