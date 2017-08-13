select case when length(email)>80 then '' else email end email,
case when length(email_2_c)>80 then '' else email_2c end email_2_c
from (
  select 
trim(coalesce(case when (UPPER(professional_email_address_name)) rlike "[A-Z0-9.%+-/!#$%&'*=?`{|}~]@[A-Z0-9.-].[A-Z]{2,4}$" 
              then professional_email_address_name else '' end,'')) as email_2__c,
trim(coalesce(case when (UPPER(email_address)) rlike "[A-Z0-9.%+-/!#$%&'*=?`{|}~]@[A-Z0-9.-].[A-Z]{2,4} $" 
              then email_address else '' end,'')) as email
from 
dm.professional_dimension d 
join src.account_user au
on au.id=cast(d.professional_user_account_id as int)
join src.account_email_address e
on e.id=au.email_address_id
     )z
limit 100;
---------------------------------------------------------------------------------------------
select
case when  (length(professional_email_address_name) <= 80)
             and
           (instr(professional_email_address_name, '@') > 1)
             and
           --1 character before @
           (not rlike(substr(professional_email_address_name,instr(professional_email_address_name,'@')-1, 1), '[.]') = true)
             and 
           --1 character after @
           (not rlike(substr(professional_email_address_name,instr(professional_email_address_name,'@')+1, 1), '[-]') = true)
             and 
           --all characters after @             
           (not rlike(substr(professional_email_address_name,instr(professional_email_address_name,'@')+1 ),'[\~\`\!\@\#\$\%\^\&\*\(\)\<\>\,\;\/\?\=\_\+\{\|\}\']') = true)
             and 
           --very last character              
           (not rlike (substr(professional_email_address_name,length(professional_email_address_name),1),'[\~\`\!\@\#\$\%\^\&\*\(\)\-\<\>\,\;\/\?\=\_\+\{\|\}\']') = true)
      then professional_email_address_name
      else ''
end as professional_email_address_name
from (
      select 'email@-dummy.com' as professional_email_address_name
     ) as foo
---------------------------------------------------------------------------------------------


Find cities containing characters A, B or R at any position:

  SELECT name FROM cities WHERE name RLIKE 'A|B|R';
---------------------------------------------------------------------------------------------
Salesforce email validation:  https://help.salesforce.com/HTViewSolution?id=000001145&language=en_US
---------------------------------------------------------------------------------------------
case 
when rlike (professional_email_address_name,'[a-zA-Z0-9!#$%&*/=?^_+-{|}~]+@[a-zA-Z0-9-]+\.[a-zA-Z0]{2,4}') =1 
		and 
	INSTR( professional_email_address_name, '@')>1 
		and 
	INSTR( SUBSTR(professional_email_address_name, INSTR( professional_email_address_name, '@')+1 ), '_') >=0 
then professional_email_address_name 
end as professional_email_address_name,


case when rlike (user_account_email_address,'[a-zA-Z0-9!#$%&*/=?^_+-{|}~]+@[a-zA-Z0-9-]+\.[a-zA-Z0]{2,4}') =1 and INSTR( user_account_email_address, '@')>1 and INSTR( SUBSTR(user_account_email_address, INSTR( user_account_email_address, '@')+1 ), '_') >=0 then user_account_email_address end as user_account_email_address,

--character position of '@'
INSTR( professional_email_address_name, '@')
--all characters after '@'
SUBSTR(professional_email_address_name, INSTR( professional_email_address_name, '@')+1 )

---------------------------------------------------------------------------------------------
select
 --rlike (professional_email_address_name,'[a-za-z0-9!#$%&*/=?^_+-{|}~]+@[a-za-z0-9-]+\.[a-za-z0]{2,4}') as regexp_all
rlike (professional_email_address_name,'[a-za-z0-9!#$%&\'*/=?^_+-`{|}~]') as regexp_tmp
,instr( professional_email_address_name, '@') as position_of_at
,SUBSTR(professional_email_address_name, 1, INSTR( professional_email_address_name, '@')-1) as chars_before_at
,SUBSTR(professional_email_address_name, INSTR( professional_email_address_name, '@')+1 ) as chars_after_at
,instr( substr(professional_email_address_name, instr( professional_email_address_name, '@')+1 ), '_') as foo2
,case when rlike (professional_email_address_name,'[a-za-z0-9!#$%&*/=?^_+-{|}~]+@[a-za-z0-9-]+\.[a-za-z0]{2,4}') =1 and instr(professional_email_address_name, '@')>1 and instr( substr(professional_email_address_name, instr(professional_email_address_name, '@')+1 ), '_') >=0 then professional_email_address_name end as professional_email_address_name
from (
      select 'email@dummy.com' as professional_email_address_name
     ) as foo
	 
	 
!#$%&'*/=?^_+-`{|}~	 
----------------------------------------------------------------------------------------------------

select
rlike (professional_email_address_name,'[a-za-z0-9!#$%&*/=?^_+-{|}~]+@[a-za-z0-9-]+\.[a-za-z0]{2,4}') as regexp_all
,rlike (professional_email_address_name,'[a-za-z0-9!#$%&\'*/=?^_+-`{|}~]') as regexp_tmp
, rlike (SUBSTR(professional_email_address_name, INSTR( professional_email_address_name, '@')+1 ),'[a-za-z0-9-]') as regexp
,instr( professional_email_address_name, '@') as position_of_at
,SUBSTR(professional_email_address_name, 1, INSTR( professional_email_address_name, '@')-1) as chars_before_at
,SUBSTR(professional_email_address_name, INSTR( professional_email_address_name, '@')+1 ) as chars_after_at
,instr( substr(professional_email_address_name, instr( professional_email_address_name, '@')+1 ), '_') as foo2
,case when rlike (professional_email_address_name,'[a-za-z0-9!#$%&*/=?^_+-{|}~]+@[a-za-z0-9-]+\.[a-za-z0]{2,4}') =1 and instr(professional_email_address_name, '@')>1 and instr( substr(professional_email_address_name, instr(professional_email_address_name, '@')+1 ), '_') >=0 then professional_email_address_name end as professional_email_address_name
from (
      --select 'email@dummy.com' as professional_email_address_name
      select 'a!' as professional_email_address_name  
     ) as foo
------------------------------------------------------------------------------------------------------
select
not rlike(substr(professional_email_address_name,instr(professional_email_address_name,'@')-1, 1), '[.]') as regexp_before
,not rlike(substr(professional_email_address_name,instr(professional_email_address_name,'@')+1 ),'[~`!@#$%^&*()<>,;/?=_+{|}\']') as regexp_after
,not rlike (substr(professional_email_address_name,length(professional_email_address_name),1),'[~`!@#$%^&*()-<>,;/?=_+{|}\']') as tmp
,case 
when (instr(professional_email_address_name, '@') > 1)
      and
     (not rlike(substr(professional_email_address_name,instr(professional_email_address_name,'@')-1, 1), '[.]') = true)
      and 
     (not rlike(substr(professional_email_address_name,instr(professional_email_address_name,'@')+1 ),'[~`!@#$%^&*()<>,;/?=_+{|}\']') = true)
      and 
     (not rlike (substr(professional_email_address_name,length(professional_email_address_name),1),'[~`!@#$%^&*()-<>,;/?=_+{|}\']') = true)
then substr(professional_email_address_name,1,80)
else ' '
end as professional_email_address_name
from (
      select "email@dummy.com-" as professional_email_address_name
     ) as foo	 
------------------------------------------------
select
professional_email_address_name,
case when  (length(professional_email_address_name) <= 80)
             and
           (instr(professional_email_address_name, '@') > 1)
             and
           (not rlike(substr(professional_email_address_name,instr(professional_email_address_name,'@')-1, 1), '[.]') = true)
             and 
           (not rlike(substr(professional_email_address_name,instr(professional_email_address_name,'@')+1 ),'[\~\`\!\@\#\$\%\^\&\*\(\)\<\>\,\;\/\?\=\_\+\{\|\}\']') = true)
             and 
           (not rlike (substr(professional_email_address_name,length(professional_email_address_name),1),'[\~\`\!\@\#\$\%\^\&\*\(\)\-\<\>\,\;\/\?\=\_\+\{\|\}\']') = true)
      then professional_email_address_name
      else ' '
end as professional_email_address_name
from dm.professional_dimension
where professional_id in (
  190502,
226009,
444618,
591179,
640003,
719687,
724730,
773758,
775401,
899140,
938161,
1021673
  )

     