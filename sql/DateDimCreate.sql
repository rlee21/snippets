  SELECT 
             TO_NUMBER (TO_CHAR (currdate, 'YYYYMMDD')) AS date_key,
             currdate AS calendar_date,
             TO_NUMBER (TO_CHAR (currdate, 'YYYY')) AS year_no,
             TO_NUMBER ( (TO_CHAR (currdate, 'Q'))) AS qtr_no,
             TO_NUMBER (TO_CHAR (currdate, 'MM')) AS month_no,
             TO_CHAR (currdate, 'Month') AS month_name,
             TO_CHAR (currdate, 'Mon') AS month_short,
             TO_NUMBER (TO_CHAR (currdate, 'DD')) AS day_of_month,
             TO_NUMBER (TO_CHAR (currdate + 1, 'IW')) AS week_no,
             TO_NUMBER (TO_CHAR (currdate, 'D')) AS weekday_no,
             TO_CHAR (currdate, 'Day') AS weekday_name,
             SUBSTR (TO_CHAR (currdate, 'Day'), 1, 3) AS weekday_short
  FROM ( 
             SELECT 
                       LEVEL AS n, 
                       TO_DATE ('12/31/2013', 'MM/DD/YYYY') + NUMTODSINTERVAL (LEVEL, 'day') AS currdate
               FROM dual
             CONNECT BY LEVEL <= (52 * 7 * 30) + 43 -- 30 years plus 43 days for each leap year.
           ) 
 ORDER BY 1;
--------------------------------------------------------------------
      SELECT
      to_number(to_char(CurrDate, 'J'))   as Dim_Date_Key,
      CurrDate                            as CALENDAR_DATE,
      to_number(TO_CHAR(CurrDate,'YYYY')) AS Year_NO,
      to_number(TO_CHAR(CurrDate,'DDD'))  AS Day_NO,
      to_number((TO_CHAR(CurrDate,'Q')))  AS qtr_no,
      to_number(TO_CHAR(CurrDate,'MM'))   AS Month_No,
      TO_CHAR(CurrDate,'Month')           AS Month_name,
      TO_CHAR(CurrDate,'Mon')             AS Month_short,
      to_number(TO_CHAR(CurrDate,'DD'))   AS Day_Of_Month,
      to_number(TO_CHAR(CurrDate+1,'IW')) AS Week_No,
      to_number(TO_CHAR(CurrDate,'D'))    AS WEEKDAY_NO,
      TO_CHAR(CurrDate,'Day')             as WEEKDAY_NAME,
      substr(TO_CHAR(CurrDate,'Day'),1,3) as Weekday_short,
      CASE
         WHEN to_number(TO_CHAR(CurrDate,'D')) in (1,7)
         THEN 'WEEKEND' ELSE 'WEEKDAY'
      END                                 AS WEEKDAY_TYPE 
      FROM (
      select level n, to_date('12/26/1994','MM/DD/YYYY') + NUMTODSINTERVAL(level,'day') CurrDate
      from dual
      connect by level <= (52*7*30)+43) -- 30 years plus 43 days for each leap year.
      order by 1;
  
