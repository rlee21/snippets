--SELECT * FROM ALL_DIRECTORIES 

DROP TABLE REV_AK_BALANCER_TDEF_MAPPING CASCADE CONSTRAINTS;

CREATE TABLE REV_AK_BALANCER_TDEF_MAPPING
(
  CUSTOM_TDEF_ID    VARCHAR2(8 BYTE),
  STANDARD_TDEF_ID  VARCHAR2(8 BYTE)
)
ORGANIZATION EXTERNAL
  (  TYPE ORACLE_LOADER
     DEFAULT DIRECTORY UPLOAD_REV
     ACCESS PARAMETERS 
       ( records delimited by newline
        badfile UPLOAD_REV:'rev_ak_balancer_tdef_mapping.bad'  
        nodiscardfile
        logfile UPLOAD_REV:'rev_ak_balancer_tdef_mapping.log'
        fields terminated by ','     
        optionally enclosed by '"'          
        lrtrim 
        (  
          CUSTOM_TDEF_ID,
          STANDARD_TDEF_ID
        )       
                )
     LOCATION (UPLOAD_REV:'AK Balancer tDef Mapping.csv')
  )
REJECT LIMIT 0
NOPARALLEL
NOMONITORING;


GRANT SELECT ON REV_AK_BALANCER_TDEF_MAPPING TO DM_TKR_R;

GRANT SELECT ON REV_AK_BALANCER_TDEF_MAPPING TO DM_TKR_RO;
