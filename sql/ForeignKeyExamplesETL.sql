/* Add FK Ref Constraint */
ALTER TABLE FACT_LEDGER ADD (
  CONSTRAINT FK_FACT_LEDGER_DIM_ACCT
 FOREIGN KEY (ACCOUNT_KEY) 
 REFERENCES DIM_ACCOUNT (ACCOUNT_KEY) );
--------------------------------------------------------------------------------
/* Disable Foreign Key Constraints */
DECLARE
   v_table_name   VARCHAR2 (30);  
   v_fk_cons_name      VARCHAR2 (30);
   v_sql    VARCHAR2(4000);
   CURSOR cur_fk_cons IS
                                           SELECT table_name, constraint_name
                                             FROM user_constraints
                                           WHERE constraint_type = 'R' AND status = 'ENABLED'
                                                AND table_name IN ('FACT_PROJ_RESOURCE', 'FACT_LEDGER');
       
BEGIN
   OPEN cur_fk_cons;

   LOOP

      FETCH cur_fk_cons INTO v_table_name, v_fk_cons_name;
      EXIT WHEN cur_fk_cons%NOTFOUND;

       BEGIN
        v_sql:= 'ALTER TABLE '||v_table_name||' '||'disable CONSTRAINT '||v_fk_cons_name; 
       EXECUTE IMMEDIATE v_sql;

       EXCEPTION 
       WHEN OTHERS THEN NULL;
       END; 
         
   END LOOP;

   CLOSE cur_fk_cons;

END;
--------------------------------------------------------------------------------
/* Enable Foreign Key Constraints */
DECLARE
   v_table_name   VARCHAR2 (30);  
   v_fk_cons_name      VARCHAR2 (30);
   v_sql    VARCHAR2(4000);
   CURSOR cur_fk_cons IS
                                           SELECT table_name, constraint_name
                                             FROM user_constraints
                                           WHERE constraint_type = 'R' AND status = 'DISABLED'
                                                AND table_name IN ('FACT_PROJ_RESOURCE', 'FACT_LEDGER');
       
BEGIN
   OPEN cur_fk_cons;

   LOOP

      FETCH cur_fk_cons INTO v_table_name, v_fk_cons_name;
      EXIT WHEN cur_fk_cons%NOTFOUND;

       BEGIN
        v_sql:= 'ALTER TABLE '||v_table_name||' '||'enable CONSTRAINT '||v_fk_cons_name; 
       EXECUTE IMMEDIATE v_sql;

       EXCEPTION 
       WHEN OTHERS THEN NULL;
       END; 
         
   END LOOP;

   CLOSE cur_fk_cons;

END;
--------------------------------------------------------------------------------
/* Query to check for orphans */
   SELECT   LOWER (cons.owner) || '.' || cons.table_name
               AS orphan_fact_table_name,
            LOWER (cols.owner) || '.' || cols.table_name
               AS orphan_dim_table_name,
            cols.column_name AS orphan_column_name
     FROM   user_constraints cons, all_cons_columns cols
    WHERE       cons.r_owner = cols.owner
            AND cons.r_constraint_name = cols.constraint_name
            AND cons.constraint_type = 'R'
            AND cons.status = 'DISABLED'
            AND cons.table_name IN ('FACT_PROJ_RESOURCE', 'FACT_LEDGER');
--------------------------------------------------------------------------------