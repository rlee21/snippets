DROP VIEW EDW.V_LOGS;

/* Formatted on 6/13/2013 8:22:10 AM (QP5 v5.215.12089.38647) */
CREATE OR REPLACE FORCE VIEW EDW.V_LOGS
(
   BATCH_ID,
   PACKAGE_NAME,
   PROGRAM_NAME,
   PROGRAM_STATUS,
   PROGRAM_START_DATE,
   PROGRAM_END_DATE,
   RUNTIME,
   INSERT_COUNT,
   ERROR_MESSAGE,
   COMMENTS
)
AS
     SELECT a.batch_id,
            a.package_name,
            a.program_name,
            a.program_status,
            a.program_start_date,
            a.program_end_date,
            NUMTODSINTERVAL (a.program_end_date - a.program_start_date, 'day')
               AS runtime,
            --              c.delete_count,
            c.insert_count,
            --              c.update_count,
            a.error_message,
            a.comments
       FROM process_status a, process_control b, process_audit c
      WHERE     a.batch_id = b.batch_id(+)
            AND a.batch_id = c.batch_id(+)
            AND TRUNC (a.program_start_date) = TRUNC (SYSDATE)
   --         AND trunc(a.program_start_date) = '05-MAY-2013'
   --         AND a.package_name = 'EDW_LOAD_PKG'
   ORDER BY a.batch_id;
