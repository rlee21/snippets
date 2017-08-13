--------------------------------
/* Delete Duplicate Rows */
--------------------------------
DELETE FROM STG_DWHPTAVOY a
WHERE ROWID > (SELECT MIN(ROWID) FROM STG_DWHPTAVOY b
                WHERE   a.PRT_1_PORT_CODE = b.PRT_1_PORT_CODE  
                and a.PRT_1_EFFECTIVE_DATE = b.PRT_1_EFFECTIVE_DATE
                and a.PRT_1_EMBR_DEMBR_FLAG = b.PRT_1_EMBR_DEMBR_FLAG
                and a.PRT_1_AIR_CITY = b.PRT_1_AIR_CITY
                and a.PRT_1_EARLIEST_HRS = b.PRT_1_EARLIEST_HRS
                and a.PRT_1_IDEAL_HRS    = b.PRT_1_IDEAL_HRS
                and a.PRT_1_LATEST_HRS  = b.PRT_1_LATEST_HRS
                and a.PRT_1_INC_EXCL_FLAG = b.PRT_1_INC_EXCL_FLAG    
                and a.PRT_1_VOYAGE = b.PRT_1_VOYAGE 
                );
COMMIT;