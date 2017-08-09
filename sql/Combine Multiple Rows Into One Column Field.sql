with 
  test as 
    (
      select '001' as id, 'Angelo' as name, 'A' as segment from dual union all
      select '001' as id, 'Angelo' as name, 'D' as segment from dual union all
      select '001' as id, 'Angelo' as name, 'G' as segment from dual union all
      select '002' as id, 'John' as name, 'A' as segment from dual union all
      select '002' as id, 'John' as name, 'B' as segment from dual union all
      select '002' as id, 'John' as name, 'K' as segment from dual union all
      select '002' as id, 'John' as name, 'P' as segment from dual union all
      select '003' as id, 'Chloe' as name, 'Q' as segment from dual union all
      select '003' as id, 'Chloe' as name, 'S' as segment from dual union all
      select '004' as id, 'Dinah' as name, 'Z' as segment from dual
    )
select id, name, 
       max(ltrim(sys_connect_by_path(segment,','),',')) keep
           (dense_rank last order by level) as segment_path
  from (
         select test.*, row_number() over(partition by id order by segment) as rn
           from test
       )
 start with rn = 1
connect by prior id = id
       and prior rn = rn - 1
group by id, name;
