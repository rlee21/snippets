/* Unpivot Example */
   SELECT
         batch_id,
         org AS deptid,
         account,
         project,
         bip,
         yyyy AS fiscal_year,
         DECODE(month, 'M01', 1,
                       'M02', 2,
                       'M03', 3,
                       'M04', 4,
                       'M05', 5,
                       'M06', 6,
                       'M07', 7,
                       'M08', 8,
                       'M09', 9,
                       'M10', 10,
                       'M11', 11,
                       'M12', 12,
                       NULL) AS accounting_period,
         SUM(CASE WHEN record_type = '4' THEN TO_NUMBER(amount) ELSE 0 END) AS hours,
         SUM(CASE WHEN record_type = '5' THEN TO_NUMBER(amount) ELSE 0 END) AS dollars
    FROM (
           SELECT * FROM (
                           SELECT   
                                 batch_id,
                                 org,
                                 account,
                                 project,
                                 record_type,
                                 bip,
                                 yyyy,
                                 m01,
                                 m02,
                                 m03,
                                 m04,
                                 m05,
                                 m06,
                                 m07,
                                 m08,
                                 m09,
                                 m10,
                                 m11,
                                 m12
                            FROM 
                                 ui_planner_budget_stg
--                           WHERE org = '316'
--                             AND account = '730800'
--                             AND project = '2611'
--                             AND yyyy = '2013'
--                        --     AND record_type = '5'
                                 )
           UNPIVOT EXCLUDE NULLS
           (
             amount
             FOR month
             IN (M01,M02,M03,M04,M05,M06,M07,M08,M09,M10,M11,M12)
           )
   )
   GROUP BY
         batch_id,
         org,
         account,
         project,
         bip,
         yyyy,
         DECODE(month, 'M01', 1,
                       'M02', 2,
                       'M03', 3,
                       'M04', 4,
                       'M05', 5,
                       'M06', 6,
                       'M07', 7,
                       'M08', 8,
                       'M09', 9,
                       'M10', 10,
                       'M11', 11,
                       'M12', 12,
                       NULL)
   ORDER BY batch_id, org, account, project, bip, yyyy, accounting_period;
------------------------------------------------------------------------------------------
 Select Query
UNPIVOT [<INCLUDE | EXCLUDE> NULLS] (<column_list>) FOR (<column_list>)
IN (<column_list>) [AS (<constant_list>)]) 
---------------------------------------------------------------------------------------
SELECT *
FROM   (SELECT customer_id, product_code, quantity
        FROM   pivot_test)
PIVOT  (SUM(quantity) AS sum_quantity FOR (product_code) IN ('A' AS a, 'B' AS b, 'C' AS c))
ORDER BY customer_id;
---------------------------------------------------------------------------------------
WITH "Q_Totals" AS (
      SELECT   
               "DIM_DATE"."ACCOUNTING_PERIOD" "Accounting_Period",
               "DIM_LEDGER"."LEDGER_GROUP" "Ledger",
               SUM("FACT_LEDGER"."DOLLARS") "Dollars"
          FROM "FINANCE_DMART"."DIM_DEPT" "DIM_DEPT4",
               "FINANCE_DMART"."DIM_DATE_LEDGER" "DIM_DATE",
               "FINANCE_DMART"."DIM_LEDGER_GROUP" "DIM_LEDGER",
               "FINANCE_DMART"."FACT_LEDGER" "FACT_LEDGER",
               "FINANCE_DMART"."DIM_ACCOUNT" "DIM_ACCOUNT"
         WHERE "DIM_DATE"."FISCAL_YEAR" = 2013
           AND "DIM_DEPT4"."DEPTID" = '115'
           AND "DIM_LEDGER"."LEDGER_GROUP" IN ('ACTUALS', 'BUDGET', 'ENCUMBRANCES')
           AND "DIM_ACCOUNT"."ACCOUNT" BETWEEN '710000' AND '899999'
           AND "FACT_LEDGER"."DEPT_KEY" = "DIM_DEPT4"."DEPT_KEY"
           AND "FACT_LEDGER"."ACCOUNT_KEY" = "DIM_ACCOUNT"."ACCOUNT_KEY"
           AND "FACT_LEDGER"."DATE_LEDGER_KEY" = "DIM_DATE"."DATE_LEDGER_KEY"
           AND "FACT_LEDGER"."LEDGER_GROUP_KEY" = "DIM_LEDGER"."LEDGER_GROUP_KEY"
      GROUP BY 
               "DIM_DATE"."ACCOUNTING_PERIOD",
               "DIM_LEDGER"."LEDGER_GROUP"
               ),
"Q_Acct_Periods" AS
     (SELECT DISTINCT "DIM_DATE"."ACCOUNTING_PERIOD" "Accounting_Period"
                 FROM "FINANCE_DMART"."DIM_DATE_LEDGER" "DIM_DATE"
                WHERE "DIM_DATE"."ACCOUNTING_PERIOD" > 0
                  AND "DIM_DATE"."FISCAL_YEAR" = 2013),                    
"Q_Running_Totals" AS ( 
SELECT   
         "Q_Acct_Periods"."Accounting_Period",
         "Q_Totals"."Ledger",
         SUM ("Q_Totals"."Dollars") "Dollars"         
    FROM "Q_Totals", "Q_Acct_Periods"
   WHERE "Q_Acct_Periods"."Accounting_Period" >= "Q_Totals"."Accounting_Period"
GROUP BY 
         "Q_Acct_Periods"."Accounting_Period", "Q_Totals"."Ledger"
ORDER BY "Q_Acct_Periods"."Accounting_Period")
SELECT * 
FROM "Q_Running_Totals"
PIVOT ( sum("Dollars") FOR "Accounting_Period" IN (1,2,3,4,5,6,7,8,9,10,11,12))