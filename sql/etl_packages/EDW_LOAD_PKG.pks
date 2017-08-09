CREATE OR REPLACE PACKAGE EDW.EDW_LOAD_PKG AS
/******************************************************************************
   NAME:       EDW_LOAD_PKG
   PURPOSE: Container for EDW_LOAD_PKG. 

   REVISIONS:
   Ver        Date        Author     Description
   ---------  ----------  ------     ------------------------------------
   
******************************************************************************/
    PROCEDURE RUN_EDW_LOAD;

    PROCEDURE VOYAGE_PLR_STG_LOAD;

    PROCEDURE VOYAGE_FMS_STG_LOAD;

    PROCEDURE VOYAGE_STG_LOAD;

    PROCEDURE VOYAGE_LOAD;

    PROCEDURE VOYAGE_FIN_PRORATE_STG_LOAD;
    
    PROCEDURE VOYAGE_FIN_PRORATE_LOAD;   

    PROCEDURE VOYAGE_CAL_PRORATE_STG_LOAD;  

    PROCEDURE VOYAGE_CAL_PRORATE_LOAD;  

    PROCEDURE SHIP_ITIN_STG_LOAD;
    
    PROCEDURE SHIP_ITIN_LOAD;         
    
    PROCEDURE SHIP_SCHEDULE_STG_LOAD;

    PROCEDURE SHIP_SCHEDULE_LOAD;

    PROCEDURE AGENCY_STG_LOAD;
    
    PROCEDURE AGENCY_LOAD;
    
    PROCEDURE BKNG_DIM_STG_LOAD;    
    
    PROCEDURE BKNG_DIM_STATUS_LOAD;  
    
    PROCEDURE BKNG_DIM_CHANNEL_LOAD;      

    PROCEDURE BKNG_DIM_GEO_LOAD;
    
    PROCEDURE BKNG_DIM_GUEST_DEM_LOAD;            

    PROCEDURE BKNG_DIM_CABIN_LOAD;       

    PROCEDURE BKNG_DIM_MINI_LOAD;  
    
    PROCEDURE BKNG_FACT_STG_LOAD;    
    
    PROCEDURE BKNG_FACT_LOAD;       
    
    PROCEDURE BKNG_PII_STG_LOAD;

    PROCEDURE BKNG_PII_LOAD;
    
    PROCEDURE BKNG_PROMO_STG_LOAD;
      
    PROCEDURE BKNG_PROMO_LOAD;  
                                                               
END EDW_LOAD_PKG;
/