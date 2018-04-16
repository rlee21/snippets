Select distinct salary from employment e1 where 3= (select count (distinct salary) from employment e2 where e1.salary<=e2.salary)



set curRank:= 0;

# yesterday's date
subdate(current_date, 1)
-----------------------------
SELECT
  @row_num:=IF(@prev_col1=t.col1 AND @prev_col2=t.col2,
               @row_num+1,
               1) AS row_number,
  @dense:=IF(@prev_col1=t.col1 AND @prev_col2=t.col2, 
             IF(@prev_col3=col3, 
                @dense, 
                @dense+1), 
             1) AS dense_rank,
  @rank:=IF(@prev_col1=t.col1 AND @prev_col2=t.col2 AND @prev_col3=col3, 
            @rank, 
            @row_num) AS rank,
  t.*
FROM (SELECT * FROM table1 ORDER BY col1, col2, col3 DESC) t,
     (SELECT @row_num:=1, @dense:=1, @rank:=1, @prev_col1:=NULL, @prev_col2:=NULL, @prev_col3:=NULL) var
-----------------------------     
select 
id, advice_session_id,
@dense_rnk:=IF(@prev_advice_session_id=t.advice_session_id, @dense_rnk, @dense_rnk+1) AS dense_rank
from (
(
SELECT id, advice_session_id
  FROM ocato.financial_event_logs
 WHERE financial_event_reason_id IS NOT NULL
order by advice_session_id desc
) t,
(select @dense_rnk:= 1, @prev_advice_session_id:=NULL) var
)
;
-----------------------------

      select 
         e.Salary
       from (
              select 
                  Id, 
                  Salary, 
                  @curRank:= @curRank + 1 as salary_rnk
               from Employee
            ) e
      where e.salary_rnk = n


# Write your MySQL query statement below
delete from person
where id in (select max(id)
             from person
             where email in (select email
                             from (select email, count(*)
                                    from person
                                   group by email
                                   having count(*) > 1
                                  ) dupes
                             )



select 
id, advice_session_id,
@dense_rnk:=IF(@prev_advice_session_id=t.advice_session_id, @dense_rnk+1, @dense_rnk) AS dense_rank
from (
(
SELECT id, advice_session_id
  FROM ocato.financial_event_logs
 WHERE financial_event_reason_id IS NOT NULL
order by advice_session_id desc
) t,
(select @dense_rnk:=1, @prev_id:=NULL, @prev_advice_session_id:=NULL) var
)
;
SELECT t.id, t.advice_session_id,
-- @rnk:= @rank + (@prev <> (@prev:= t.advice_session_id)) as rank
  case 
    when @prev = t.advice_session_id then @rnk
    when (@prev := t.advice_session_id) is not null then @rnk := @rnk+1
  end as Rank
  FROM ocato.financial_event_logs t,
  (select @rnk:=0, @prev:=NULL) var
 WHERE financial_event_reason_id IS NOT NULL
order by t.advice_session_id desc;
             
            )      