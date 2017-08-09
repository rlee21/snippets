CREATE OR REPLACE PROCEDURE SP_TRUNC_PARTITION(in_table_name IN VARCHAR2, in_company_code IN varchar2, in_dwh_owner varchar2) IS

TBL_NAME varchar2(50) := in_table_name;
COMPANY_CODE varchar2(1) := in_company_code;
DWH_OWNR varchar2(50) := in_dwh_owner;
SQL_STMT  VARCHAR2(4000);
TRUNC_PARTITION_NAME varchar2(50);
PARTITION_EXIST int;

BEGIN
SQL_STMT := 'select distinct b.subobject_name from '||TBL_NAME||' a, all_objects b where dbms_rowid.rowid_object(a.rowid) = b.data_object_id and b.owner ='''||DWH_OWNR||''' 
and a.company_code ='''||COMPANY_CODE||'''';
EXECUTE IMMEDIATE SQL_STMT into TRUNC_PARTITION_NAME;

SQL_STMT := 'SELECT 1 FROM DUAL
WHERE EXISTS(Select 1 from USER_TAB_PARTITIONS Where table_Name='''||TBL_NAME||''' and PARTITION_NAME='''||TRUNC_PARTITION_NAME||''')';
EXECUTE IMMEDIATE SQL_STMT into PARTITION_EXIST;

IF(PARTITION_EXIST = 1 AND TRUNC_PARTITION_NAME IS NOT NULL) THEN 
 EXECUTE IMMEDIATE  'ALTER TABLE '||DWH_OWNR||'.'||TBL_NAME||' TRUNCATE PARTITION '||TRUNC_PARTITION_NAME||' DROP STORAGE';
END IF;

EXCEPTION
WHEN OTHERS THEN NULL;
END;
/
