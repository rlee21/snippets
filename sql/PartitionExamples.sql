/* Interval Partition Example */
create table obr_txn_f
(
  POS_ID          NUMBER        NOT NULL,
  POS_SHP_ID        NUMBER(2)     NOT NULL,   
  POSTING_DT_WID  NUMBER(8)     NOT NULL,
  OBRDEP_WID      NUMBER           NOT NULL,
  LOAD_DT         DATE          NOT NULL,
  AMOUNT          NUMBER        NOT NULL,
  CRUISE_END_DATE        DATE       NOT NULL,
  SCHED_WID       NUMBER        NOT NULL,
  PAX_WID        NUMBER,
  VOYAGE_WID      NUMBER        NOT NULL
)
TABLESPACE OBR_DATA
PARTITION BY RANGE (CRUISE_END_DATE)
INTERVAL(NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION POS_P1 VALUES LESS THAN (TO_DATE('01-JUL-2008','DD-MON-YYYY')),
PARTITION POS_P2 VALUES LESS THAN (TO_DATE('01-AUG-2008','DD-MON-YYYY'))
);

/* Identifying Interval Partitions */
select distinct uo.subobject_name 
from dm_obr.wc_obrrev_a t, user_objects uo
where dbms_rowid.rowid_object(t.rowid) = uo.data_object_id
and t.cruise_end_date = to_date('05/01/2015','MM/DD/YYYY');

select distinct b.subobject_name 
from dim_shipboard_preferences_stg a, all_objects b
where dbms_rowid.rowid_object(a.rowid) = b.data_object_id
and b.owner = 'DWH_OWNER'
and a.company_code = 'S';

/* Range Partitioning Example */
CREATE TABLE sales_range 
(salesman_id  NUMBER(5), 
salesman_name VARCHAR2(30), 
sales_amount  NUMBER(10), 
sales_date    DATE)
PARTITION BY RANGE(sales_date) 
(
PARTITION sales_jan2000 VALUES LESS THAN(TO_DATE('02/01/2000','DD/MM/YYYY')),
PARTITION sales_feb2000 VALUES LESS THAN(TO_DATE('03/01/2000','DD/MM/YYYY')),
PARTITION sales_mar2000 VALUES LESS THAN(TO_DATE('04/01/2000','DD/MM/YYYY')),
PARTITION sales_apr2000 VALUES LESS THAN(TO_DATE('05/01/2000','DD/MM/YYYY'))
);

/* List Partitioning Example */
CREATE TABLE sales_list
(salesman_id  NUMBER(5), 
salesman_name VARCHAR2(30),
sales_state   VARCHAR2(20),
sales_amount  NUMBER(10), 
sales_date    DATE)
PARTITION BY LIST(sales_state)
(
PARTITION sales_west VALUES('California', 'Hawaii'),
PARTITION sales_east VALUES ('New York', 'Virginia', 'Florida'),
PARTITION sales_central VALUES('Texas', 'Illinois'),
PARTITION sales_other VALUES(DEFAULT)
);


---------------------------------------------------------------------------------------
CREATE BITMAP INDEX IDX_FACT_ACCOUNTING_DT_KEY_01 ON FACT_PROJ_RESOURCE (ACCOUNTING_DT_KEY) LOCAL;
ALTER TABLE fact_proj_resource MODIFY PARTITION fdm_yr_2014 UNUSABLE LOCAL INDEXES;
ALTER TABLE fact_proj_resource MODIFY PARTITION fdm_yr_2014 REBUILD UNUSABLE LOCAL INDEXES;
ALTER TABLE fact_proj_resource ENABLE ROW MOVEMENT;
ALTER TABLE my_table TRUNCATE PARTITION SOURCE_FP update global indexes;
ALTER TABLE table_name exchange partition partition_name with table tmp_sales including indexes without validation;
ALTER TABLE pit_bkng_dtl_f EXCHANGE PARTITION pit_wk_less_9 with table pit_bkng_dtl_f_tmp excluding indexes without validation;
---------------------------------------------------------------------------------------
SELECT i.index_name, p.partition_name
FROM user_indexes i, user_ind_partitions p 
WHERE i.index_name = p.index_name 
AND i.table_name = 'FACT_LEDGER' AND p.status = 'UNUSABLE'
AND i.index_name NOT IN ('PK_FACT_LEDGER', 'UK_FACT_LEDGER')
ORDER BY i.index_name, p.partition_position;

---------------------------------------------------------------------------------------
CREATE TABLE D_VOY_MSTR (
  VOYAGE_KEY                     NUMBER NOT NULL PRIMARY KEY,
  VYD_VOYAGE                     VARCHAR2(5) NOT NULL,
  COMPANY_CODE                   VARCHAR2(1),
  CAL_YEAR                       VARCHAR2(4 BYTE),
  B_SAIL_DATE                      DATE
)
PARTITION BY LIST (COMPANY_CODE)
  SUBPARTITION BY RANGE(B_SAIL_DATE)
  ( 
    PARTITION p2_h_data VALUES ('H')
    (
      SUBPARTITION sub_h_p1 VALUES LESS THAN (TO_DATE('01-JAN-2010','DD-MON-YYYY')),
      SUBPARTITION sub_h_p2 VALUES LESS THAN (TO_DATE('01-JAN-2011','DD-MON-YYYY')),
      SUBPARTITION sub_h_p3 VALUES LESS THAN (TO_DATE('01-JAN-2012','DD-MON-YYYY')),
      SUBPARTITION sub_h_p4 VALUES LESS THAN (TO_DATE('01-JAN-2013','DD-MON-YYYY')),
      SUBPARTITION sub_h_p5 VALUES LESS THAN (TO_DATE('01-JAN-2014','DD-MON-YYYY')),
      SUBPARTITION sub_h_p6 VALUES LESS THAN (TO_DATE('01-JAN-2015','DD-MON-YYYY')),
      SUBPARTITION sub_h_p7 VALUES LESS THAN (TO_DATE('01-JAN-2016','DD-MON-YYYY')),
      SUBPARTITION sub_h_p8 VALUES LESS THAN (MAXVALUE)                  
    ),
    PARTITION p3_s_data VALUES ('S')
    (
      SUBPARTITION sub_s_p1 VALUES LESS THAN (TO_DATE('01-JAN-2010','DD-MON-YYYY')),
      SUBPARTITION sub_s_p2 VALUES LESS THAN (TO_DATE('01-JAN-2011','DD-MON-YYYY')),
      SUBPARTITION sub_s_p3 VALUES LESS THAN (TO_DATE('01-JAN-2012','DD-MON-YYYY')),
      SUBPARTITION sub_s_p4 VALUES LESS THAN (TO_DATE('01-JAN-2013','DD-MON-YYYY')),
     SUBPARTITION sub_s_p5 VALUES LESS THAN (TO_DATE('01-JAN-2014','DD-MON-YYYY')),
      SUBPARTITION sub_s_p6 VALUES LESS THAN (TO_DATE('01-JAN-2015','DD-MON-YYYY')),
      SUBPARTITION sub_s_p7 VALUES LESS THAN (TO_DATE('01-JAN-2016','DD-MON-YYYY')),
      SUBPARTITION sub_s_p8 VALUES LESS THAN (MAXVALUE)    
    ),
    PARTITION p_default VALUES (DEFAULT)  
    (
      SUBPARTITION sub_default_p1 VALUES LESS THAN (TO_DATE('01-JAN-2010','DD-MON-YYYY')),
      SUBPARTITION sub_default_p2 VALUES LESS THAN (TO_DATE('01-JAN-2011','DD-MON-YYYY')),
      SUBPARTITION sub_default_p3 VALUES LESS THAN (TO_DATE('01-JAN-2012','DD-MON-YYYY')),
      SUBPARTITION sub_default_p4 VALUES LESS THAN (TO_DATE('01-JAN-2013','DD-MON-YYYY')),
      SUBPARTITION sub_default_p5 VALUES LESS THAN (TO_DATE('01-JAN-2014','DD-MON-YYYY')),
      SUBPARTITION sub_default_p6 VALUES LESS THAN (TO_DATE('01-JAN-2015','DD-MON-YYYY')),
      SUBPARTITION sub_default_p7 VALUES LESS THAN (TO_DATE('01-JAN-2016','DD-MON-YYYY')),
      SUBPARTITION sub_default_p8 VALUES LESS THAN (MAXVALUE)    
    )    
   )

;

CREATE TABLE D_BOOKING (
  VOYAGE_KEY                    NUMBER NOT NULL,
  B_BKNG_KEY                    NUMBER NOT NULL PRIMARY KEY,
  COMPANY_CODE_KEY              NUMBER          NOT NULL,
  B_BKNG_NBR                    VARCHAR2(7 BYTE) NOT NULL,
  B_VOYAGE                      VARCHAR2(5 BYTE),
  B_SAIL_DATE                   DATE,
  CONSTRAINT d_bk_voyage_fk FOREIGN KEY (VOYAGE_KEY) REFERENCES D_VOY_MSTR
)
PARTITION BY REFERENCE (d_bk_voyage_fk)
;

--------------------------------------------------------------------------------------

ALTER TABLE PARTITION_TEST
 DROP PRIMARY KEY CASCADE;
DROP TABLE PARTITION_TEST CASCADE CONSTRAINTS;

CREATE TABLE PARTITION_TEST
(
  YEAR   NUMBER(4),
  COUNT  NUMBER
)
TABLESPACE HR_DATA
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
NOLOGGING
PARTITION BY RANGE (YEAR) 
(  
  PARTITION YR_PART_MIN VALUES LESS THAN (2011)
    NOLOGGING
    NOCOMPRESS
    TABLESPACE HR_DATA
    PCTFREE    10
    INITRANS   1
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               ),  
  PARTITION YR_PART_2012 VALUES LESS THAN (2013)
    NOLOGGING
    NOCOMPRESS
    TABLESPACE HR_DATA
    PCTFREE    10
    INITRANS   1
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               ),  
  PARTITION YR_PART_2013 VALUES LESS THAN (2014)
    NOLOGGING
    NOCOMPRESS
    TABLESPACE HR_DATA
    PCTFREE    10
    INITRANS   1
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               ),  
  PARTITION YR_PART_2014 VALUES LESS THAN (2015)
    NOLOGGING
    NOCOMPRESS
    TABLESPACE HR_DATA
    PCTFREE    10
    INITRANS   1
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               ),  
  PARTITION YR_PART_MAX VALUES LESS THAN (MAXVALUE)
    NOLOGGING
    NOCOMPRESS
    TABLESPACE HR_DATA
    PCTFREE    10
    INITRANS   1
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
)
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;


CREATE UNIQUE INDEX PK_PART_TEST ON PARTITION_TEST
(YEAR)
LOGGING
TABLESPACE HR_DATA
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           )
NOPARALLEL;


ALTER TABLE PARTITION_TEST ADD (
  CONSTRAINT PK_PART_TEST
 PRIMARY KEY
 (YEAR)
    USING INDEX 
    TABLESPACE HR_DATA
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                PCTINCREASE      0
               ));



------------------------------------------------------------------------------------------------------------------------------

DROP TABLE RDM_TRANS_F1 ;

CREATE TABLE RDM_TRANS_F1
(
  BKNG_ID                        VARCHAR2(10 BYTE) NOT NULL,
  AGENCY_ID                      VARCHAR2(13 BYTE) NOT NULL ,
  AGENCY_NBR                     VARCHAR2(8 BYTE) NOT NULL,
  VOYAGE                         VARCHAR2(5 BYTE) NOT NULL,  
  RPT_YEAR                       VARCHAR2(4 BYTE) NOT NULL ,
  C1                                      VARCHAR2(4 BYTE) NOT NULL 
  )
    TABLESPACE TKR_DATA
    NOLOGGING 
PARTITION BY RANGE (RPT_YEAR)
(                    
  PARTITION TRAN_F_MIN VALUES LESS THAN ( 2008 )
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_DATA
    STORAGE    (
                INITIAL          8M
                NEXT             1M                
               ),
PARTITION TRAN_F_2008 VALUES LESS THAN ( 2009 ) 
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_DATA    
     STORAGE    (
                INITIAL          8M
               NEXT             1M                
               ),                   
  PARTITION TRAN_F_2009 VALUES LESS THAN ( 2010 ) 
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_DATA    
    STORAGE    (
                INITIAL          8M
                NEXT             1M                
               ),                 
  PARTITION TRAN_F_2010 VALUES LESS THAN ( 2011 )
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_DATA
    STORAGE    (
                INITIAL          8M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_2011 VALUES LESS THAN ( 2012 )
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_DATA  
    STORAGE    (
                INITIAL          8M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_2012 VALUES LESS THAN ( 2013)
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_DATA  
    STORAGE    (
                INITIAL          8M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_2013 VALUES LESS THAN ( 2014) 
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_DATA
    STORAGE    (
                INITIAL          8M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_MAX VALUES LESS THAN (MAXVALUE)
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_DATA
    STORAGE    (
                INITIAL          8M
                NEXT             1M                
               )
)
NOCOMPRESS 
NOCACHE
PARALLEL ( DEGREE 8 INSTANCES 1 )
MONITORING;


CREATE INDEX TRANS_F_I1 ON RDM_TRANS_F1
(RPT_YEAR)         
    TABLESPACE TKR_INDX    
    NOLOGGING 
LOCAL (  
  PARTITION TRAN_F_I1_MIN
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_INDX  
    STORAGE    (
                INITIAL         1M
                NEXT             1M                
               ),
  PARTITION TRAN_F_I1_2008
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_INDX
    STORAGE    (
                INITIAL         1M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_I1_2009
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_INDX
    STORAGE    (
                INITIAL         1M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_I1_2010
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_INDX
    STORAGE    (
                INITIAL         1M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_I1_2011
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_INDX
    STORAGE    (
                INITIAL         1M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_I1_2012
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_INDX
    STORAGE    (
                INITIAL         1M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_I1_2013
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_INDX
    STORAGE    (
                INITIAL         1M
                NEXT             1M                
               ),  
  PARTITION TRAN_F_I1_MAX
    NOLOGGING
    NOCOMPRESS 
    TABLESPACE TKR_INDX
    STORAGE    (
            INITIAL         1M
                NEXT             1M
               )
)
PARALLEL ( DEGREE 8 INSTANCES 1 );


CREATE BITMAP INDEX TRAN_F_I2 ON RDM_TRANS_F1
(AGENCY_ID)
  TABLESPACE TKR_INDX    
  NOLOGGING 
LOCAL  (  
  PARTITION TRANS_F_I2_A
     NOLOGGING   
    TABLESPACE TKR_INDX ,  
  PARTITION TRANS_F_I2_B
    NOLOGGING 
    TABLESPACE TKR_INDX , 
  PARTITION TRANS_F_I2_C
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I2_D
    NOLOGGING
    TABLESPACE TKR_INDX , 
  PARTITION TRANS_F_I2_E
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I2_F
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I2_G
    NOLOGGING
    TABLESPACE TKR_INDX , 
  PARTITION TRANS_F_I2_H
    NOLOGGING
    TABLESPACE TKR_INDX  
)
PARALLEL ( DEGREE 8 INSTANCES 1 );

CREATE BITMAP INDEX TRAN_F_I3 ON RDM_TRANS_F1
(BKNG_ID)
  TABLESPACE TKR_INDX  
  NOLOGGING
LOCAL  (  
  PARTITION TRANS_F_I3_A
    NOLOGGING
    TABLESPACE TKR_INDX,   
  PARTITION TRANS_F_I3_B
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I3_C
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I3_D
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I3_E
    NOLOGGING
    TABLESPACE TKR_INDX , 
  PARTITION TRANS_F_I3_F
    NOLOGGING
    TABLESPACE TKR_INDX,
  PARTITION TRANS_F_I3_G
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I3_H
    NOLOGGING
    TABLESPACE TKR_INDX  
)
PARALLEL ( DEGREE 8 INSTANCES 1 );




CREATE BITMAP INDEX TRAN_F_I4 ON RDM_TRANS_F1
(VOYAGE)
  TABLESPACE TKR_INDX  
  NOLOGGING
LOCAL  (  
  PARTITION TRANS_F_I3_A
    NOLOGGING
   TABLESPACE TKR_INDX  ,
  PARTITION TRANS_F_I3_B
    NOLOGGING
    TABLESPACE TKR_INDX ,  
  PARTITION TRANS_F_I3_C
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I3_D
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I3_E
    NOLOGGING
    TABLESPACE TKR_INDX,  
  PARTITION TRANS_F_I3_F
    NOLOGGING
    TABLESPACE TKR_INDX,
  PARTITION TRANS_F_I3_G
    NOLOGGING
    TABLESPACE TKR_INDX , 
  PARTITION TRANS_F_I3_H
    NOLOGGING
    TABLESPACE TKR_INDX   
)
PARALLEL ( DEGREE 8 INSTANCES 1 );


