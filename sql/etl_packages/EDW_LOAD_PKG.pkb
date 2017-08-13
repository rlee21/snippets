CREATE OR REPLACE PACKAGE BODY EDW.EDW_LOAD_PKG AS
/******************************************************************************
   NAME:     EDW_LOAD_PKG
   PURPOSE:  Process to load data in EDW_STG and EDW

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   
 Coding Conventions 
           ------------------                     
           c_     Constant 
           e_     Error  
           in_    Input Parm 
           out_   Output Parm 
           io_    Input / Output parm
           cur_   Cursor 
           rcr_   Reference Cursor 
           cv_    Cursor Variable 
           typ_   Type Declaration 
           row_   Rowtype Cursor Variable
           rec_   Instace of a type declaration
           d_     date variable
           s_     string variable
           no prefix of numeric Variable   
               
           Oracle keywords in Caps
           Function & Procedure names in Caps 
           Variable & other declarations in Mixed Case
           Table / Column names lower Case  
                     
******************************************************************************/

    -- Constants and Pkg level vars    
   cSchema_NM    CONSTANT   Varchar2(30) :=  USER  ;
   cPackage_NM   CONSTANT   Varchar2(100) := 'EDW_LOAD_PKG'  ;
   eErrMsg                  Varchar2(500);
   eErrNo                   Number ;
   sComment                 Varchar2(2000) ;      
   eCALL_FAILED   Exception;
   eINCONSISTENCY Exception; 
    
   PROCEDURE RUN_EDW_LOAD IS
   /***************************************************************************
   NAME: RUN_EDW_LOAD;
   
   PURPOSE: 
      Call procedures to process EDW load.
        
   RETURN: N/A
   
   PARAMETERS:     
        None   
         
   PROCESS STEPS :     
      1) Call procedures in EDW load.
                          
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/

   -- Constants and Procedure level vars
   cProgram_NM   CONSTANT    VARCHAR2(30) := 'RUN_EDW_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                  NUMBER;
   vRowCountDeletes    NUMBER;
   vRowCountUpdates   NUMBER;
   vRowCountInserts     NUMBER;      
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
    
   BEGIN
       
      -- Log program start 
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID  => vBatchID, 
         in_Schema_Name  =>  cSchema_NM,  
         in_Package_Name   => cPackage_NM,
         in_Program_Name  =>cProgram_NM, 
         in_Prog_Status => 'I', 
         in_Comments  => 'Job in Progress', 
         in_Error_Message => 'NA') != 0 THEN                          
         
         RAISE eCALL_FAILED;

      END IF;               
   
   ---------------------------------------------
        VOYAGE_PLR_STG_LOAD;
        VOYAGE_FMS_STG_LOAD;
        VOYAGE_STG_LOAD;
        VOYAGE_LOAD;
        VOYAGE_FIN_PRORATE_STG_LOAD;  
        VOYAGE_FIN_PRORATE_LOAD;   
        VOYAGE_CAL_PRORATE_STG_LOAD;  
        VOYAGE_CAL_PRORATE_LOAD;  
        SHIP_ITIN_STG_LOAD;    
        SHIP_ITIN_LOAD;             
        SHIP_SCHEDULE_STG_LOAD;
        SHIP_SCHEDULE_LOAD;
        AGENCY_STG_LOAD;    
        AGENCY_LOAD;    
        BKNG_DIM_STG_LOAD;     
        BKNG_DIM_STATUS_LOAD;
        BKNG_DIM_CHANNEL_LOAD;
        BKNG_DIM_GEO_LOAD;
        BKNG_DIM_GUEST_DEM_LOAD;
        BKNG_DIM_CABIN_LOAD;
        BKNG_DIM_MINI_LOAD;                        
        BKNG_FACT_STG_LOAD;    
        BKNG_FACT_LOAD;    
        BKNG_PII_STG_LOAD;
        BKNG_PII_LOAD;  
        BKNG_PROMO_STG_LOAD;
        BKNG_PROMO_LOAD;   
   ---------------------------------------------

      vEndDate := SYSDATE ;  -- set end date to sysdate
       
      -- Record Completion in Log    
      sComment := 'Job ran successfully';
                  
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA' ) != 0 THEN
         
         RAISE eCALL_FAILED ;

      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE ) != 0 THEN
         
         RAISE eCALL_FAILED;

      END IF;        
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE eINCONSISTENCY;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE eCALL_FAILED;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );

         RAISE;                  
                     
   END RUN_EDW_LOAD;


   PROCEDURE VOYAGE_PLR_STG_LOAD IS
   /***************************************************************************
   NAME: VOYAGE_PLR_STG_LOAD
       
   PURPOSE: 
      POLAR staging table load for table EDW_VOYAGE_D
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Truncate table
      2) Insert current and future report years.
         
                                          
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM          CONSTANT   VARCHAR2(30) := 'VOYAGE_PLR_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes      NUMBER;
   vRowCountUpdates     NUMBER;
   vRowCountInserts       NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
     
      EXECUTE IMMEDIATE 'TRUNCATE TABLE plr_voyage_ds';
      
      INSERT /*+ append */  INTO plr_voyage_ds
      (
        voyage_unique_id,
        voyage,
        company_cd,
        polar_ship_cd,
        voyage_status,
        voyage_type,
        voyage_xld_dt,
        sail_dt,
        return_dt,
        sea_days,
        from_port,
        to_port,
        trade,
        trade_description,
        subtrade,
        subtrade_description,
        season,
        season_description,
        rpt_qtr,
        rpt_year,
        sail_year,
        fscl_sail_dt,
        voyage_capacity,
        lower_beds_available,
        cabin_version,
        final_pymt_days,
        physical_voyage,
        ship_capacity
      )
      SELECT      
             (vm.vyd_voyage||vm.company_code||vm.sail_year) AS voyage_unique_id,             
             vm.vyd_voyage AS voyage,
             vm.company_code AS company_cd,
             vm.vyd_ship_code AS ship_cd,
             vm.vyd_voyage_status AS voyage_status,
             vm.vyd_voyage_type AS voyage_type,
             vm.voyage_cancel_date AS voyage_xld_dt,
             vm.vyd_sail_date AS sail_dt,
             vm.vyd_return_date AS return_dt,
             vm.vyd_basic_sea_days AS sea_days,
             vm.vyd_basic_from_port AS from_port,
             vm.vyd_basic_to_port AS to_port,
             vm.vyd_trade AS trade,
             tc.trade_description AS trade_description,
             vm.vyd_subtrade AS subtrade,
             sc.subtrade_description AS subtrade_description,
             vm.vyd_season AS season,
             sac.season_desc AS season_description,
             vm.rpt_qtr,
             vm.rpt_year,
             vm.sail_year,
             vm.fiscal_sail_date AS fiscial_sail_dt,
             vm.voyage_capacity,
             vm.bedsavailable AS lower_beds_available,
             vm.vyd_cabin_version AS cabin_version,
             NVL(vs.vrs_fp_nbr_days, 0) AS final_pymt_days,
             vm.physical_voyage_nbr AS physical_voyage,
             SUM(ushp.ship_2_lower_berths) AS ship_capacity
        FROM 
             voyage_mstr@stgdwh1_edwro vm,
             trade_code@stgdwh1_edwro tc,
             subtrade_code@stgdwh1_edwro sc,
             season_code@stgdwh1_edwro sac,
             voyage_route_sector@stgdwh1_edwro vs,
             ushp_config_ship2@stgdwh1_edwro ushp
      WHERE     
             vm.vyd_trade = tc.trade_code
         AND vm.company_code = tc.company_code
         AND vm.company_code = sc.company_code
         AND vm.company_code = sac.company_code
         AND vm.vyd_subtrade = sc.subtrade_code
         AND vm.vyd_season = sac.season_code
         AND vm.vyd_voyage = vs.vrs_voyage(+)
         AND vm.company_code = vs.company_code(+)
         AND vm.vyd_ship_code = ushp.ship_2_key_code(+)
         AND vm.vyd_cabin_version = ushp.ship_2_key_version(+)
         AND vm.company_code = ushp.company_code(+)
         AND vs.vrs_seq_nbr(+) = 1
         AND vm.vyd_ship_code NOT IN ('ZZ', 'LP')
         AND vm.vyd_voyage_status IN ('A', 'H', 'C')
--         AND vm.vyd_voyage = 'D158'
--         AND TO_NUMBER(vm.rpt_year) >= (SELECT TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY')) FROM dual)
         AND TO_NUMBER(vm.rpt_year) >= 2008
     GROUP BY  vm.vyd_voyage, vm.company_code, vm.vyd_ship_code, vm.vyd_voyage_status, vm.vyd_voyage_type,
                     vm.voyage_cancel_date, vm.vyd_sail_date, vm.vyd_return_date, vm.vyd_basic_sea_days,
                     vm.vyd_basic_from_port, vm.vyd_basic_to_port, vm.vyd_trade, tc.trade_description, vm.vyd_subtrade,
                     sc.subtrade_description, vm.vyd_season, sac.season_desc, vm.rpt_qtr, vm.rpt_year, vm.sail_year,
                     vm.fiscal_sail_date, vm.voyage_capacity, vm.bedsavailable, vm.vyd_cabin_version,
                     NVL(vrs_fp_nbr_days, 0), vm.physical_voyage_nbr;      
  
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate
      
        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'PLR_VOYAGE_DS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;         

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count =>   0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END VOYAGE_PLR_STG_LOAD;

   PROCEDURE VOYAGE_FMS_STG_LOAD IS
   /***************************************************************************
   NAME: VOYAGE_FMS_STG_LOAD
       
   PURPOSE: 
      FMS staging table load for table EDW_VOYAGE_D
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Delect records where sailing return date is greater than or equal the last 30 days.     
      2) Insert records where sailing return date is greater than or equal the last 30 days
         
                                          
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM          CONSTANT   VARCHAR2(30) := 'VOYAGE_FMS_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes      NUMBER;
   vRowCountUpdates     NUMBER;
   vRowCountInserts       NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
     
      EXECUTE IMMEDIATE 'TRUNCATE TABLE fms_voyage_ds';
     
      INSERT /*+ append */  INTO fms_voyage_ds
      (
        fms_voyage_id,
        fms_ship_id,
        sail_dt,
        return_dt
      )
      SELECT 
                 scr_id AS fms_voyage_id,
                 scr_shp_id AS fms_ship_id,
                 scr_s_date AS sail_dt,
                 scr_e_date AS return_dt
        FROM fconsol.scr@prdfms_edwro
--      WHERE TO_NUMBER(TO_CHAR (scr_s_date, 'YYYY')) >= (SELECT TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY')) FROM dual);
      WHERE TO_NUMBER(TO_CHAR (scr_s_date, 'YYYY')) >= 2007;
--       WHERE scr_e_date >= TRUNC(sysdate)-30;      
  
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate
      
        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'FMS_VOYAGE_DS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;         

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count =>   0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END VOYAGE_FMS_STG_LOAD;

   PROCEDURE VOYAGE_STG_LOAD IS
   /***************************************************************************
   NAME: VOYAGE_STG_LOAD
       
   PURPOSE: 
      Staging table load for table EDW_VOYAGE_D
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Update records for current and future report years.
      2) Insert records for current and future report years.
         
                                          
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'VOYAGE_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      EXECUTE IMMEDIATE 'TRUNCATE TABLE edw_voyage_ds';

      INSERT /*+ append */  INTO edw_voyage_ds 
      (
        voyage_unique_id,
        voyage,
        fms_voyage_id,
        company_cd,
        cal_year,
        fscl_year,
        fscl_qtr,
        fms_ship_id,
        ship_cd,
        polar_ship_cd,
        sail_dt,
        return_dt,
        cruise_days,
        from_port,
        to_port,
        trade_cd,
        trade_desc,
        subtrade_cd,
        subtrade_desc,
        season_cd,
        season_desc,
        active_flg,
        voyage_type,
        logical_type,
        physical_voyage_flg,
        ship_capacity,
        voyage_xld_dt,
        voyage_capacity,
        lower_beds_available,
        cabin_version,
        final_pymt_days,
        fscl_sail_dt,
        fscl_return_month,
        obr_month_order,
        obr_ship_order,
        obr_end_sort,
        mkt_trade_cd,
        mkt_trade_desc,
        siebel_id        
      )
        WITH fms AS (
        SELECT 
                    fms.fms_voyage_id,
                    fms.fms_ship_id,
                    fms.sail_dt,
                    fms.return_dt,
                    shp.polar_ship_cd,
                    shp.obr_ship_order
          FROM 
                   fms_voyage_ds fms,  
                   edw_ship_d shp                 
         WHERE 
                    fms.fms_ship_id = shp.fms_ship_id          
        )
        SELECT     
            plr.voyage_unique_id,
            plr.voyage,
            NVL(fms.fms_voyage_id,0) fms_voyage_id,
            plr.company_cd,                    
            TO_NUMBER(plr.sail_year) AS cal_year,
            TO_NUMBER(plr.rpt_year) AS fscl_year,
            plr.rpt_qtr AS fscl_qtr,
            fms.fms_ship_id,
            shp.ship_cd,
            plr.polar_ship_cd,
            plr.sail_dt,
            plr.return_dt,
            plr.sea_days AS cruise_days,
            plr.from_port,
            plr.to_port,
            plr.trade AS trade_cd,
            plr.trade_description AS trade_desc,
            plr.subtrade AS subtrade_cd,
            plr.subtrade_description AS subtrade_desc,
            plr.season AS season_cd,
            plr.season_description AS season_desc,
            CASE WHEN plr.voyage_status IN ('A', 'H') THEN 'Y' ELSE 'N' END AS active_flg,
            DECODE (plr.voyage_status,
                'A', 'Standard',
                'B', 'Build Status',
                'C', 'Cancelled',
                'D', 'Dry Dock',
                'H', 'Charter',
                'L', 'Inaugural',
                'I', 'Inactive',
                'E', 'Incident Voyage',
                NULL, 'New Voyage',
                'Unknown') AS voyage_type,
            DECODE (plr.voyage_type,
                'B', 'Back-to-Back',
                'S', 'Segmented',
                'O', 'Overlapping',
                NULL, NULL,
                'Unknown') AS logical_type,
            CASE WHEN plr.physical_voyage = plr.voyage THEN 'Y' ELSE 'N' END AS physical_voyage_flg,
            plr.ship_capacity,
            plr.voyage_xld_dt,
            plr.voyage_capacity,
            plr.lower_beds_available AS lower_beds_available,
            plr.cabin_version,
            plr.final_pymt_days,
            plr.fscl_sail_dt,
            TO_NUMBER(TO_CHAR (fms.return_dt, 'MM')) AS fscl_return_month,
            TO_NUMBER(TO_CHAR (fms.return_dt, 'YYYYMM')) AS obr_month_order,
            fms.obr_ship_order,                    
            TO_NUMBER(TO_CHAR (fms.return_dt, 'YYYYMMDD')) * -1 AS obr_end_sort,
            NULL AS mkt_trade_cd,
            NULL AS mkt_trade_desc,
            NULL AS siebel_id
        FROM 
                  plr_voyage_ds plr,
                  fms,
                  edw_ship_d shp
        WHERE 
                    plr.polar_ship_cd = shp.polar_ship_cd
             AND plr.polar_ship_cd = fms.polar_ship_cd(+)  
             AND plr.sail_dt = fms.sail_dt(+)
             AND plr.return_dt = fms.return_dt(+);
             
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate
      
        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_VOYAGE_DS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;         

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count =>   0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END VOYAGE_STG_LOAD;

   PROCEDURE VOYAGE_LOAD IS
   /***************************************************************************
   NAME: VOYAGE_LOAD
       
   PURPOSE: 
      Load for table EDW_VOYAGE_D
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Update records for current and future report years.
      2) Insert records for current and future report years.
         
                                          
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'VOYAGE_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_voyage_d;    
     
      --Updates/Inserts
      MERGE INTO edw_voyage_d tgt 
      USING (
            SELECT ROWNUM AS row_wid,
                        voyage_unique_id,
                        voyage,
                        fms_voyage_id,
                        company_cd,
                        cal_year,
                        fscl_year,
                        fscl_qtr,
                        fms_ship_id,
                        ship_cd,
                        polar_ship_cd,
                        sail_dt,
                        return_dt,
                        cruise_days,
                        from_port,
                        to_port,
                        trade_cd,
                        trade_desc,
                        subtrade_cd,
                        subtrade_desc,
                        season_cd,
                        season_desc,
                        active_flg,
                        voyage_type,
                        logical_type,
                        physical_voyage_flg,
                        ship_capacity,
                        voyage_xld_dt,
                        voyage_capacity,
                        lower_beds_available,
                        cabin_version,
                        final_pymt_days,
                        fscl_sail_dt,
                        fscl_return_month,
                        obr_month_order,
                        obr_ship_order,
                        obr_end_sort,
                        mkt_trade_cd,
                        mkt_trade_desc,
                        siebel_id              
              FROM edw_voyage_ds
              ) src
          ON (tgt.voyage_unique_id = src.voyage_unique_id)                    
              WHEN MATCHED THEN
                 UPDATE SET
                            tgt.voyage = src.voyage,
                            tgt.fms_voyage_id = src.fms_voyage_id,
                            tgt.company_cd = src.company_cd,
                            tgt.cal_year = src.cal_year,
                            tgt.fscl_year = src.fscl_year,
                            tgt.fscl_qtr = src.fscl_qtr,
                            tgt.fms_ship_id = src.fms_ship_id,
                            tgt.ship_cd = src.ship_cd,
                            tgt.polar_ship_cd = src.polar_ship_cd,
                            tgt.sail_dt = src.sail_dt,
                            tgt.return_dt = src.return_dt,
                            tgt.cruise_days = src.cruise_days,
                            tgt.from_port = src.from_port,
                            tgt.to_port = src.to_port,
                            tgt.trade_cd = src.trade_cd,
                            tgt.trade_desc = src.trade_desc,
                            tgt.subtrade_cd = src.subtrade_cd,
                            tgt.subtrade_desc = src.subtrade_desc,
                            tgt.season_cd = src.season_cd,
                            tgt.season_desc = src.season_desc,
                            tgt.active_flg = src.active_flg,
                            tgt.voyage_type = src.voyage_type,
                            tgt.logical_type = src.logical_type,
                            tgt.physical_voyage_flg = src.physical_voyage_flg,
                            tgt.ship_capacity = src.ship_capacity,
                            tgt.voyage_xld_dt = src.voyage_xld_dt,
                            tgt.voyage_capacity = src.voyage_capacity,
                            tgt.lower_beds_available = src.lower_beds_available,
                            tgt.cabin_version = src.cabin_version,
                            tgt.final_pymt_days = src.final_pymt_days,
                            tgt.fscl_sail_dt = src.fscl_sail_dt,
                            tgt.fscl_return_month = src.fscl_return_month,
                            tgt.obr_month_order = src.obr_month_order,
                            tgt.obr_ship_order = src.obr_ship_order,
                            tgt.obr_end_sort = src.obr_end_sort,
                            tgt.mkt_trade_cd = src.mkt_trade_cd,
                            tgt.mkt_trade_desc = src.mkt_trade_desc,
                            tgt.siebel_id = src.siebel_id
              WHEN NOT MATCHED THEN          
                 INSERT ( 
                                tgt.row_wid,                   
                                tgt.voyage_unique_id,                           
                                tgt.voyage,
                                tgt.fms_voyage_id,
                                tgt.company_cd,
                                tgt.cal_year,
                                tgt.fscl_year,
                                tgt.fscl_qtr,
                                tgt.fms_ship_id,
                                tgt.ship_cd,
                                tgt.polar_ship_cd,
                                tgt.sail_dt,
                                tgt.return_dt,
                                tgt.cruise_days,
                                tgt.from_port,
                                tgt.to_port,
                                tgt.trade_cd,
                                tgt.trade_desc,
                                tgt.subtrade_cd,
                                tgt.subtrade_desc,
                                tgt.season_cd,
                                tgt.season_desc,
                                tgt.active_flg,
                                tgt.voyage_type,
                                tgt.logical_type,
                                tgt.physical_voyage_flg,
                                tgt.ship_capacity,
                                tgt.voyage_xld_dt,
                                tgt.voyage_capacity,
                                tgt.lower_beds_available,
                                tgt.cabin_version,
                                tgt.final_pymt_days,
                                tgt.fscl_sail_dt,
                                tgt.fscl_return_month,
                                tgt.obr_month_order,
                                tgt.obr_ship_order,
                                tgt.obr_end_sort,
                                tgt.mkt_trade_cd,
                                tgt.mkt_trade_desc,
                                tgt.siebel_id)                       
                 VALUES ( src.row_wid + NVL(v_max_wid,0), 
                                src.voyage_unique_id,                           
                                src.voyage,
                                src.fms_voyage_id,
                                src.company_cd,
                                src.cal_year,
                                src.fscl_year,
                                src.fscl_qtr,
                                src.fms_ship_id,
                                src.ship_cd,
                                src.polar_ship_cd,
                                src.sail_dt,
                                src.return_dt,
                                src.cruise_days,
                                src.from_port,
                                src.to_port,
                                src.trade_cd,
                                src.trade_desc,
                                src.subtrade_cd,
                                src.subtrade_desc,
                                src.season_cd,
                                src.season_desc,
                                src.active_flg,
                                src.voyage_type,
                                src.logical_type,
                                src.physical_voyage_flg,
                                src.ship_capacity,
                                src.voyage_xld_dt,
                                src.voyage_capacity,
                                src.lower_beds_available,
                                src.cabin_version,
                                src.final_pymt_days,
                                src.fscl_sail_dt,
                                src.fscl_return_month,
                                src.obr_month_order,
                                src.obr_ship_order,
                                src.obr_end_sort,
                                src.mkt_trade_cd,
                                src.mkt_trade_desc,
                                src.siebel_id);          
  
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate
      
        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_VOYAGE_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;         

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count =>   0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END VOYAGE_LOAD;

   PROCEDURE VOYAGE_FIN_PRORATE_STG_LOAD IS
   /***************************************************************************
   NAME: VOYAGE_FIN_PRORATE_STG_LOAD
       
   PURPOSE: 
      Staging table load for EDW_VOYAGE_FIN_PRORATE_H  
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'VOYAGE_FIN_PRORATE_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
     
      EXECUTE IMMEDIATE 'TRUNCATE TABLE plr_voyage_fin_prorate_hs';

      INSERT /*+ append */  INTO plr_voyage_fin_prorate_hs 
      (
        voyage_prorate_unique_id,
        voyage_unique_id,
        company_cd,
        voyage,
        voyage_physical,
        logical_proration_pct,
        inventory_proration_flg        
      )
  with t1 as (
        select vm.company_code,
                 vm.sail_year,
                 vm.rpt_year,
                 vm.vyd_trade as trade,
                 vm.vyd_subtrade as subtrade,
                 (case
                     when an.ax_voyage is null then vm.physical_voyage_nbr
                     else an.ax_to_voyage
                  end)
                    as voyage_physical,
                 (case when an.ax_voyage is null then vm.vyd_voyage else an.ax_voyage end)
                    as voyage, 
                 vm.vyd_voyage_status as voyage_status, 
                 vm.vyd_sail_date as sail_date,
                 vm.vyd_return_date as return_date,       
                 case
                    when an.ax_voyage_days = 0
                    then
                       0
                    else
                       (case
                           when an.ax_voyage is null then 1
                           else (an.ax_prorate_days / an.ax_voyage_days)
                        end)
                 end
                    as voyage_proration
            from voyage_mstr@stgdwh1_edwro vm, abr_newxref@stgdwh1_edwro an
           where   vm.vyd_voyage = an.ax_voyage(+)
                and vm.vyd_voyage_status IN ('A', 'H', 'C')
                and vm.vyd_ship_code NOT IN ('ZZ', 'LP')
                 --Fiscal year is hardcoded instead of using sysdate because some Fall/Dec physical voyages have segments assigned to different fiscal years. i.e.T155, T155A to 2011. T155B, T155C, T155D to 2012.
                 and vm.rpt_year >= '2007'
        order by vm.company_code, vm.rpt_year, voyage
        ),
        t2 as (
        select
            distinct
            t1.voyage_physical,
            case when t1.voyage_physical = t1.voyage then t1.sail_date else t1.return_date end as part_sail_date
        from t1
        where t1.voyage_status = 'A'
        order by part_sail_date
        ),
        t3 as (
        select
            t2.voyage_physical,
            t2.part_sail_date,
            lead(t2.part_sail_date) over (partition by t2.voyage_physical order by t2.voyage_physical, t2.part_sail_date) as part_return_date
        from t2
        ),
        t4 as (
        select
            t3.voyage_physical,
            t3.part_sail_date,
            t3.part_return_date,
            vm.vyd_sail_date as sail_date,
            vm.vyd_return_date as return_date,
            sum(
            case
                when vm.vyd_trade = 'L' and vm.vyd_voyage <> vm.physical_voyage_nbr then 0
                when vm.vyd_sail_date < t3.part_return_date and vm.vyd_return_date > t3.part_sail_date
                    then
                        case
                            when bp.b_p_passenger_id = '999' then bp.passenger_count
                            when bp.b_p_use in ('S', 'D') then 1
                            when bp.b_p_use in ('I', 'X') then 2
                            else 0
                        end
                else 0
            end
            ) as lower_bed_count
        from
            t3,
            bkng_mstr@stgdwh1_edwro bm,
            voyage_mstr@stgdwh1_edwro vm,
            bkng_psngr@stgdwh1_edwro bp
        where
            t3.voyage_physical = vm.physical_voyage_nbr
            and bm.company_code = vm.company_code
            and bm.company_code = bp.company_code
            and bm.b_voyage = vm.vyd_voyage
            and bm.b_bkng_nbr = bp.b_bkng_nbr
            and bp.b_p_passenger_status = 'A'
            and t3.part_return_date is not null
        group by t3.voyage_physical, t3.part_sail_date, t3.part_return_date, vm.vyd_sail_date, vm.vyd_return_date
        order by t3.part_sail_date, sail_date
        ),
        t5 as (
        select
            t4.voyage_physical,
            t4.part_sail_date,
            t4.part_return_date,
            sum(t4.lower_bed_count) as lower_bed_count
        from
            t4
        group by t4.voyage_physical, t4.part_sail_date, t4.part_return_date
        ),
        t6 as (
        select
            t5.voyage_physical,
            t5.part_sail_date,
            t5.part_return_date,
            t5.lower_bed_count,
            dense_rank() over (partition by t5.voyage_physical order by t5.lower_bed_count desc, t5.part_sail_date) as part_rank
        from
            t5
        ),
        t7 as (
        select
            *
        from
            t6
        where
            t6.part_rank = 1
        )
        select
            (t1.voyage||t1.voyage_physical||t1.company_code) AS voyage_prorate_unique_id,
            (t1.voyage||t1.company_code||t1.sail_year) AS voyage_unique_id,      
            t1.company_code AS company_cd,
            t1.voyage,
            t1.voyage_physical,            
            t1.voyage_proration AS logical_proration_pct,
            case
                when t1.voyage_status = 'H' then 1
                when t1.voyage = t1.voyage_physical then 1
                when t1.trade = 'L' and t1.voyage <> t1.voyage_physical then 0
                when t1.sail_date < t7.part_return_date and t1.return_date > t7.part_sail_date then 1
                else 0
            end as inventory_proration_flg
        from
            t1,
            t7
        where
                  t1.voyage_physical = t7.voyage_physical(+);
      
      vRowCountInserts := SQL%ROWCOUNT;  
      
       COMMIT;          

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'PLR_VOYAGE_FIN_PRORATE_HS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count =>   0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END VOYAGE_FIN_PRORATE_STG_LOAD;

   PROCEDURE VOYAGE_FIN_PRORATE_LOAD IS
   /***************************************************************************
   NAME: VOYAGE_FIN_PRORATE_LOAD
       
   PURPOSE: 
      Load for table VOYAGE_FIN_PRORATE_LOAD
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'VOYAGE_FIN_PRORATE_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_voyage_fin_prorate_h;    
     
      --Updates/Inserts
      MERGE INTO edw_voyage_fin_prorate_h tgt 
      USING (SELECT
                            ROWNUM AS row_wid,
                            stg.voyage_prorate_unique_id,
                            stg.voyage_unique_id,
                            NVL(voy1.row_wid,0) AS voyage_wid,
                            NVL(voy2.row_wid,0) AS voyage_physical_wid,   
                            stg.company_cd,
                            stg.voyage,
                            stg.voyage_physical,
                            stg.logical_proration_pct,
                            stg.inventory_proration_flg
                      FROM plr_voyage_fin_prorate_hs stg,
                               edw_voyage_d voy1,
                               edw_voyage_d voy2 
                 WHERE stg.voyage_unique_id = voy1.voyage_unique_id(+)
                     AND stg.voyage_physical = voy2.voyage(+)
                     AND stg.company_cd = voy2.company_cd(+) ) src
      ON (tgt.voyage_prorate_unique_id = src.voyage_prorate_unique_id)                    
      WHEN MATCHED THEN
         UPDATE SET
            tgt.voyage_wid = src.voyage_wid,
            tgt.voyage_physical_wid = src.voyage_physical_wid,
            tgt.voyage = src.voyage,
            tgt.voyage_physical = src.voyage_physical,
            tgt.logical_proration_pct = src.logical_proration_pct,
            tgt.inventory_proration_flg = src.inventory_proration_flg         
      WHEN NOT MATCHED THEN          
         INSERT (  tgt.row_wid,
                        tgt.voyage_prorate_unique_id,
                        tgt.voyage_wid,
                        tgt.voyage_physical_wid,
                        tgt.voyage,
                        tgt.voyage_physical,
                        tgt.logical_proration_pct,
                        tgt.inventory_proration_flg)                      
         VALUES ((src.row_wid + NVL(v_max_wid,0)), 
                        src.voyage_prorate_unique_id,
                        src.voyage_wid,
                        src.voyage_physical_wid,
                        src.voyage,
                        src.voyage_physical,
                        src.logical_proration_pct,
                        src.inventory_proration_flg);              
  
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_VOYAGE_FIN_PRORATE_H'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END VOYAGE_FIN_PRORATE_LOAD;

   PROCEDURE VOYAGE_CAL_PRORATE_STG_LOAD IS
   /***************************************************************************
   NAME: VOYAGE_CAL_PRORATE_STG_LOAD
       
   PURPOSE: 
      Staging table load for EDW_VOYAGE_CAL_PRORATE_H  
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'VOYAGE_CAL_PRORATE_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
     
      EXECUTE IMMEDIATE 'TRUNCATE TABLE plr_voyage_cal_prorate_hs';

      INSERT /*+ append */  INTO plr_voyage_cal_prorate_hs 
      (
            voyage_prorate_unique_id,
            voyage_unique_id,            
            schedule_week,
            company_cd,
            voyage_physical,            
            fin_fscl_year,
            fin_fscl_qtr,
            fin_qtr_proration_pct
      )
    with t1 as (
    select
        company_code,       
        sch_polar_voyage,
        sch_ship_code,
        MAX(sch_week) as sch_week
    from sailing_schedule@stgdwh1_edwro
    where sch_fiscal_year >= '2007'
    and sch_polar_voyage not like '%?%'
    group by company_code, sch_polar_voyage,sch_ship_code
    )
    select
        (ss.sch_polar_voyage||ss.company_code|| ss.sch_week||ss.sch_qtr) AS voyage_prorate_unique_id,
        (ss.sch_polar_voyage||ss.company_code||TO_CHAR (ss.sch_sail_date, 'YYYY')) AS voyage_unique_id,
        ss.sch_week as schedule_week,
        ss.company_code AS company_cd,
        ss.sch_polar_voyage as voyage_physical,
        to_number(ss.sch_fiscal_year) as fin_fscl_year,             
        to_number(substr(ss.sch_qtr,2,1)) as fin_fscl_qtr,         
        ss.sch_qtr_split_ratio as fin_qtr_proration_pct
    from
        t1,
        sailing_schedule@stgdwh1_edwro ss
    where
        t1.company_code = ss.company_code
        and  t1.sch_polar_voyage = ss.sch_polar_voyage
        and t1.sch_ship_code=ss.sch_ship_code
        and t1.sch_week=ss.sch_week;
      
      vRowCountInserts := SQL%ROWCOUNT;  
      
       COMMIT;          

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'PLR_VOYAGE_CAL_PRORATE_HS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count =>   0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END VOYAGE_CAL_PRORATE_STG_LOAD;

   PROCEDURE VOYAGE_CAL_PRORATE_LOAD IS
   /***************************************************************************
   NAME: VOYAGE_CAL_PRORATE_LOAD
       
   PURPOSE: 
      Load for table VOYAGE_CAL_PRORATE_LOAD
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'VOYAGE_CAL_PRORATE_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_voyage_cal_prorate_h;    
     
      --Updates/Inserts
      MERGE INTO edw_voyage_cal_prorate_h tgt 
      USING (
                 SELECT 
                            ROWNUM AS row_wid,
                            stg.voyage_prorate_unique_id,
                            stg.voyage_unique_id,
                            NVL(voy.row_wid,0) AS voyage_physical_wid,
                            stg.schedule_week,
                            stg.company_cd,
                            stg.voyage_physical,
                            stg.fin_fscl_year,
                            stg.fin_fscl_qtr,
                            stg.fin_qtr_proration_pct                  
                  FROM plr_voyage_cal_prorate_hs stg,
                           edw_voyage_d voy 
                 WHERE stg.voyage_unique_id = voy.voyage_unique_id(+)
                     AND stg.rowid IN (SELECT max(rowid) FROM plr_voyage_cal_prorate_hs 
                                               GROUP BY voyage_prorate_unique_id) ) src
      ON (tgt.voyage_prorate_unique_id = src.voyage_prorate_unique_id)                    
      WHEN MATCHED THEN
         UPDATE SET
                        tgt.voyage_physical_wid = src.voyage_physical_wid,
                        tgt.schedule_week = src.schedule_week,
                        tgt.voyage_physical = src.voyage_physical,
                        tgt.fin_fscl_year = src.fin_fscl_year,
                        tgt.fin_fscl_qtr = src.fin_fscl_qtr,
                        tgt.fin_qtr_proration_pct = src.fin_qtr_proration_pct
      WHEN NOT MATCHED THEN          
         INSERT (  tgt.row_wid,
                        tgt.voyage_prorate_unique_id,
                        tgt.voyage_physical_wid,
                        tgt.schedule_week,
                        tgt.voyage_physical,
                        tgt.fin_fscl_year,
                        tgt.fin_fscl_qtr,
                        tgt.fin_qtr_proration_pct)                       
         VALUES ((src.row_wid + NVL(v_max_wid,0)), 
                        src.voyage_prorate_unique_id,
                        src.voyage_physical_wid,
                        src.schedule_week,
                        src.voyage_physical,
                        src.fin_fscl_year,
                        src.fin_fscl_qtr,
                        src.fin_qtr_proration_pct);              
  
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_VOYAGE_CAL_PRORATE_H'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END VOYAGE_CAL_PRORATE_LOAD;

   PROCEDURE SHIP_ITIN_STG_LOAD IS
   /***************************************************************************
   NAME: SHIP_ITIN_STG_LOAD
       
   PURPOSE: 
      Staging table load for EDW_SHIP_ITIN_D 
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'SHIP_ITIN_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
     
      EXECUTE IMMEDIATE 'TRUNCATE TABLE plr_ship_itin_ds';

      INSERT /*+ append */  INTO plr_ship_itin_ds
      (
            ship_itin_unique_id,
            voyage_unique_id,                    
            voyage,
            seq_nbr,
            polar_ship_cd,
            port_cd,
            port_name,
            port_cntry_cd,
            port_cntry_name,
            port_nbr,
            port_dt,
            boarding_time,
            sail_time,
            arrival_time,
            port1_cd,
            port2_cd,
            port3_cd,
            cross_dt_line_cd
      )
     SELECT 
            ( vi.vyd_voyage||vi.vyd_sequence_nbr||vi.company_code) AS ship_itin_unique_id,
            ( vi.vyd_voyage||vi.company_code||vm.sail_year) AS voyage_unique_id,
            vi.vyd_voyage AS voyage,
            vi.vyd_sequence_nbr AS seq_nbr,
            vm.vyd_ship_code AS polar_ship_cd,
            vi.vyd_port_nmn AS port_cd,
            CASE WHEN vi.vyd_port_nmn = 'XXX' THEN 'AT SEA' ELSE port_name END AS port_name,
            pn.port_country AS port_cntry_code,
            cc.cct_country_name AS port_cntry_name,
            vi.vyd_port_nbr AS port_nbr,
            vi.vyd_port_date AS port_dt,
            vi.vyd_board_time AS boarding_time,
            vi.vyd_sail_time AS sail_time,
            vi.vyd_arrv_time AS arrival_time,
            vi.vyd_port_flag1 AS port1_cd,
            vi.vyd_port_flag2 AS port2_cd,
            vi.vyd_port_flag3 AS port3_cd,
            vi.vyd_cross_date_line AS cross_dt_line_cd
       FROM voyage_itinerary@stgdwh1_edwro vi,
                voyage_mstr@stgdwh1_edwro vm,
                port_name@stgdwh1_edwro pn,
                country_code@stgdwh1_edwro cc                
      WHERE  vi.vyd_voyage = vm.vyd_voyage
            AND vi.vyd_port_nmn = pn.port_code
            AND pn.port_country = cc.cct_country_code(+)
            AND vi.company_code = vm.company_code
            AND vi.company_code = pn.company_code
            AND pn.company_code = cc.company_code(+)
            AND vm.vyd_ship_code NOT IN ('ZZ', 'LP')
            AND vm.vyd_voyage_status IN ('A', 'H', 'C')
--            AND TO_NUMBER(vm.rpt_year) >= (SELECT TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY')) FROM dual)            
           AND TO_NUMBER(vm.rpt_year) >= 2008
           ;
      
      vRowCountInserts := SQL%ROWCOUNT;        

         COMMIT;  
             
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'PLR_SHIP_ITIN_DS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0 ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END SHIP_ITIN_STG_LOAD;

   PROCEDURE SHIP_ITIN_LOAD IS
   /***************************************************************************
   NAME: SHIP_ITIN_LOAD
       
   PURPOSE: 
      Load for EDW_SHIP_ITIN_D
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'SHIP_ITIN_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_ship_itin_d;    
     
      --Updates/Inserts
      MERGE INTO edw_ship_itin_d tgt 
      USING (     
                 SELECT ROWNUM AS row_wid,
                            stg.ship_itin_unique_id,
                            stg.voyage_unique_id,
                            voy.row_wid AS voyage_wid,
                            stg.voyage,
                            stg.seq_nbr,
                            stg.polar_ship_cd,
                            stg.port_cd,
                            stg.port_name,
                            stg.port_cntry_cd,
                            stg.port_cntry_name,
                            stg.port_nbr,
                            stg.port_dt,
                            stg.boarding_time,
                            stg.sail_time,
                            stg.arrival_time,
                            stg.port1_cd,
                            stg.port2_cd,
                            stg.port3_cd,
                            stg.cross_dt_line_cd
                   FROM plr_ship_itin_ds stg,
                             edw_voyage_d voy 
                 WHERE stg.voyage_unique_id = voy.voyage_unique_id 
                 ) src
      ON (tgt.ship_itin_unique_id = src.ship_itin_unique_id)                    
      WHEN MATCHED THEN
         UPDATE SET
                tgt.voyage_wid = src.voyage_wid,
                tgt.voyage = src.voyage,
                tgt.seq_nbr = src.seq_nbr,
                tgt.polar_ship_cd = src.polar_ship_cd,
                tgt.port_cd = src.port_cd,
                tgt.port_name = src.port_name,
                tgt.port_cntry_cd = src.port_cntry_cd,
                tgt.port_cntry_name = src.port_cntry_name,
                tgt.port_nbr = src.port_nbr,
                tgt.port_dt = src.port_dt,
                tgt.boarding_time = src.boarding_time,
                tgt.sail_time = src.sail_time,
                tgt.arrival_time = src.arrival_time,
                tgt.port1_cd = src.port1_cd,
                tgt.port2_cd = src.port2_cd,
                tgt.port3_cd = src.port3_cd,
                tgt.cross_dt_line_cd = src.cross_dt_line_cd      
      WHEN NOT MATCHED THEN          
         INSERT (  tgt.row_wid,
                        tgt.ship_itin_unique_id,
                        tgt.voyage_wid,
                        tgt.voyage,
                        tgt.seq_nbr,
                        tgt.polar_ship_cd,
                        tgt.port_cd,
                        tgt.port_name,
                        tgt.port_cntry_cd,
                        tgt.port_cntry_name,
                        tgt.port_nbr,
                        tgt.port_dt,
                        tgt.boarding_time,
                        tgt.sail_time,
                        tgt.arrival_time,
                        tgt.port1_cd,
                        tgt.port2_cd,
                        tgt.port3_cd,
                        tgt.cross_dt_line_cd)                      
         VALUES ((src.row_wid + NVL(v_max_wid,0)), 
                        src.ship_itin_unique_id,
                        src.voyage_wid,
                        src.voyage,
                        src.seq_nbr,
                        src.polar_ship_cd,
                        src.port_cd,
                        src.port_name,
                        src.port_cntry_cd,
                        src.port_cntry_name,
                        src.port_nbr,
                        src.port_dt,
                        src.boarding_time,
                        src.sail_time,
                        src.arrival_time,
                        src.port1_cd,
                        src.port2_cd,
                        src.port3_cd,
                        src.cross_dt_line_cd);              
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_SHIP_ITIN_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END SHIP_ITIN_LOAD;

   PROCEDURE SHIP_SCHEDULE_STG_LOAD IS
   /***************************************************************************
   NAME: SHIP_SCHEDULE_STG_LOAD
       
   PURPOSE: 
      Staging table oad for table EDW_SHIP_SCHEDULE_D 
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'SHIP_SCHEDULE_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
   
      EXECUTE IMMEDIATE 'TRUNCATE TABLE fms_ship_schedule_ds';

      INSERT /*+ append */  INTO fms_ship_schedule_ds
      (
        ship_schedule_unique_id,
        voyage_unique_id,
        voyage,
        cruise_dt,
        cruise_day,
        complete_pct,
        day_nbr,
        cruise_dt_wid
      )
    WITH all_days AS (
    SELECT 
                (vi.vyd_voyage||vi.vyd_port_date||vi.company_code) AS ship_schedule_unique_id,
                (vi.vyd_voyage||vi.company_code||vm.sail_year) AS voyage_unique_id,
                vi.vyd_voyage AS voyage,
                vm.vyd_sail_date AS cruise_start_dt,
                vm.vyd_return_date AS cruise_end_dt,
                vi.vyd_port_date AS cruise_dt,
                ROW_NUMBER() OVER (PARTITION BY vi.vyd_voyage, vi.company_code, vm.sail_year, vi.vyd_port_date ORDER BY vi.vyd_sequence_nbr) AS UNIQUE_DAY 
           FROM 
                    voyage_itinerary@stgdwh1_edwro vi,
                    voyage_mstr@stgdwh1_edwro vm            
          WHERE  vi.vyd_voyage = vm.vyd_voyage
                AND vi.company_code = vm.company_code
                AND vm.vyd_ship_code NOT IN ('ZZ', 'LP')
                AND vm.vyd_voyage_status IN ('A', 'H', 'C')
                AND vm.vyd_return_date <> vi.vyd_port_date
    --            AND TO_NUMBER(vm.rpt_year) >= (SELECT TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY')) FROM dual)            
                AND TO_NUMBER(vm.rpt_year) >= 2008
    --           AND vm.vyd_voyage = 'E140'           
    ORDER BY vi.vyd_voyage, vi.vyd_sequence_nbr, vi.company_code, vm.sail_year
    ),
    unique_day AS (
    SELECT    
                ship_schedule_unique_id,
                voyage_unique_id,
                voyage,
                cruise_start_dt,
                cruise_end_dt,
                cruise_dt,
                ROW_NUMBER() OVER (PARTITION BY voyage_unique_id ORDER BY cruise_dt) AS seq_nbr_revised
      FROM all_days
    WHERE unique_day = 1
    )
    SELECT        
                ship_schedule_unique_id,
                voyage_unique_id,
                voyage,
                cruise_dt,
                TO_CHAR (seq_nbr_revised) || ' Of '|| TO_CHAR (ROUND ( (cruise_end_dt - cruise_start_dt), 0)) AS cruise_day,
--                 (CAST(cruise_dt - cruise_start_dt + 1 as INT) / CAST(NVL(cruise_end_dt,MAX(cruise_dt) OVER (PARTITION BY voyage_unique_id)) - cruise_start_dt AS INT) ) COMPLETE_PCT,
                CASE WHEN CAST(NVL(cruise_end_dt,MAX(cruise_dt) OVER (PARTITION BY voyage_unique_id)) - cruise_start_dt AS INT) = 0
                                   THEN 0 ELSE (CAST(cruise_dt - cruise_start_dt + 1 as INT) / CAST(NVL(cruise_end_dt,MAX(cruise_dt) OVER (PARTITION BY voyage_unique_id)) - cruise_start_dt AS INT) ) END AS COMPLETE_PCT,  
                CAST(cruise_dt - cruise_start_dt + 1 AS INT) AS day_nbr,
                TO_CHAR(cruise_dt,'YYYYMMDD') AS cruise_dt_wid
    FROM unique_day;

      vRowCountInserts := SQL%ROWCOUNT; 
        
      COMMIT;       
                       
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'FMS_SHIP_SCHEDULE_DS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END SHIP_SCHEDULE_STG_LOAD;

   PROCEDURE SHIP_SCHEDULE_LOAD IS
   /***************************************************************************
   NAME: SHIP_SCHEDULE_LOAD
       
   PURPOSE: 
      Load for table EDW_SHIP_SCHEDULE_D 
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/

   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'SHIP_SCHEDULE_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_ship_schedule_d;    

      --Updates/Inserts
      MERGE INTO edw_ship_schedule_d tgt 
      USING (
            SELECT ROWNUM AS row_wid,
                        stg.ship_schedule_unique_id,
                        voy.row_wid AS voyage_wid,
                        stg.voyage,
                        stg.cruise_dt,
                        stg.cruise_day,
                        stg.complete_pct,
                        stg.day_nbr,
                        stg.cruise_dt_wid            
              FROM fms_ship_schedule_ds stg,
                       edw_voyage_d voy
            WHERE stg.voyage_unique_id = voy.voyage_unique_id 
                ) src
       ON (tgt.ship_schedule_unique_id = src.ship_schedule_unique_id)                    
      WHEN MATCHED THEN
         UPDATE SET
                        tgt.voyage_wid = src.voyage_wid,
                        tgt.voyage = src.voyage,
                        tgt.cruise_dt = src.cruise_dt,
                        tgt.cruise_day = src.cruise_day,
                        tgt.complete_pct = src.complete_pct,
                        tgt.day_nbr = src.day_nbr,
                        tgt.cruise_dt_wid = src.cruise_dt_wid      
      WHEN NOT MATCHED THEN          
         INSERT (  tgt.row_wid,
                        tgt.ship_schedule_unique_id,
                        tgt.voyage_wid,
                        tgt.voyage,
                        tgt.cruise_dt,
                        tgt.cruise_day,
                        tgt.complete_pct,
                        tgt.day_nbr,
                        tgt.cruise_dt_wid)                      
         VALUES ((src.row_wid + NVL(v_max_wid,0)), 
                        src.ship_schedule_unique_id,
                        src.voyage_wid,
                        src.voyage,
                        src.cruise_dt,
                        src.cruise_day,
                        src.complete_pct,
                        src.day_nbr,
                        src.cruise_dt_wid);              

      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;  
                   
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_SHIP_SCHEDULE_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END SHIP_SCHEDULE_LOAD;

   PROCEDURE AGENCY_STG_LOAD IS
   /***************************************************************************
   NAME: AGENCY_STG_LOAD
       
   PURPOSE: 
      Staging table load for EDW_AGENCY_D
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'AGENCY_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
       
      EXECUTE IMMEDIATE 'TRUNCATE TABLE plr_agency_ds';

      INSERT /*+ append */  INTO plr_agency_ds
      (
        agency_unique_id,
        agency_nbr,
        company_cd,
        agency_name,
        addr1,
        addr2,
        addr3,
        city,
        state,
        zip,
        cntry_name,
        phone,
        fax,
        email,
        agency_contact,
        agency_status_cd,
        comm_withhold_cd,
        region_cd,
        region_desc,
        region_district_cd,
        bdm_name,
        bdr_name,
        brc_all_cd,
        brc_alaska_cd,
        brc_canal_cd,
        brc_carib_cd,
        brc_europe_cd,
        brc_long_cd,
        brc_volume_cd,
        brc_voyage_cd,
        addl_bkng_cntry1_cd,
        addl_bkng_cntry2_cd,
        addl_bkng_cntry3_cd,
        addl_bkng_cntry4_cd,
        addl_bkng_cntry5_cd,
        arc_nbr,
        agency_nbr_legacy,
        cntry_cd,
        agency_create_dt,
        finance_cd,
        pymt_method_cd,
        onesource_status_cd
      )
     SELECT 
            (am.agy1_agency_id || am.company_code) AS agency_unique_id,
            am.agy1_agency_id AS agency_nbr,
            am.company_code AS company_cd,
            am.agy1_name AS agency_name,
            am.agy1_address_1 AS addr1,
            am.agy1_address_2 AS addr2,
            am.agy1_address_3 AS addr3,
            am.agy1_city AS city,
            am.agy1_state AS state,
            am.agy1_zip AS zip,
            am.agy1_country_name AS cntry_name,
            am.agy1_telephone AS phone,
            am.agy1_fax_telephone_intl AS fax,
            ao.agy2_email_address AS email,
            am.agy1_contact AS agency_contact,
            ao.agy2_agency_status AS agency_status_cd,
            ao.agy2_withhold_commission AS comm_withhold_cd,
            ao.agy2_region AS region_cd,
            dr.dsm_region_description AS region_desc,
            ao.agent_region_district AS region_district_cd,
            dm.dsm_name AS bdm_name,
            dm.telesales_rep AS bdr_name,
            ao.brc_class_all AS brc_all_cd,
            ao.brc_class_alaska AS brc_alaska_cd,
            ao.brc_class_canal AS brc_canal_cd,
            ao.brc_class_carib AS brc_carib_cd,
            ao.brc_class_europe AS brc_europe_cd,
            ao.brc_class_long AS brc_long_cd,
            ao.brc_class_volume AS brc_volume_cd,
            ao.brc_class_voyage AS brc_voyage_cd,
            ao.agy2_addl_bkng_cntry_cd1 AS addl_bkng_cntry1_cd,
            ao.agy2_addl_bkng_cntry_cd2 AS addl_bkng_cntry2_cd,
            ao.agy2_addl_bkng_cntry_cd3 AS addl_bkng_cntry3_cd,
            ao.agy2_addl_bkng_cntry_cd4 AS addl_bkng_cntry4_cd,
            ao.agy2_addl_bkng_cntry_cd5 AS addl_bkng_cntry5_cd,
            am.agy1_arc_number AS arc_nbr,
            ao.agy2_orig_agency_company_id AS agency_nbr_legacy,
            am.agy1_country_code AS cntry_cd,
            am.agy1_create_date AS agency_create_dt,
            ao.agy2_finance_type AS finance_cd,
            ao.agy2_pmt_method_status AS pymt_method_cd,
            am.agy1_onesource_status AS onesource_status_cd
       FROM 
            agent_mstr@stgdwh1_edwro am,
            agent_oc@stgdwh1_edwro ao,
            dsm_region@stgdwh1_edwro dr,
            dsm_mstr@stgdwh1_edwro dm
      WHERE  am.agy1_agency_id = ao.agy2_agency_id
            AND am.agy1_agency_base_code = ao.agy2_agency_base_code
            AND am.company_code = ao.company_code
            AND am.company_code = dm.company_code
            AND ao.agy2_region = dr.dsm_region_code
            AND ao.agent_region_district = dm.dsm_code;
     
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;          
       
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'PLR_AGENCY_DS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END AGENCY_STG_LOAD;

   PROCEDURE AGENCY_LOAD IS
   /***************************************************************************
   NAME: AGENCY_LOAD
       
   PURPOSE: 
      Load for table EDW_AGENCY_D
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'AGENCY_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_agency_d;    
     
      --Updates/Inserts
      MERGE INTO edw_agency_d tgt 
      USING (
        SELECT ROWNUM AS row_wid,
                    agency_unique_id,
                    agency_nbr,
                    company_cd,
                    agency_name,
                    addr1,
                    addr2,
                    addr3,
                    city,
                    state,
                    zip,
                    cntry_name,
                    phone,
                    fax,
                    email,
                    agency_contact,
                    agency_status_cd,
                    comm_withhold_cd,
                    region_cd,
                    region_desc,
                    region_district_cd,
                    bdm_name,
                    bdr_name,
                    brc_all_cd,
                    brc_alaska_cd,
                    brc_canal_cd,
                    brc_carib_cd,
                    brc_europe_cd,
                    brc_long_cd,
                    brc_volume_cd,
                    brc_voyage_cd,
                    addl_bkng_cntry1_cd,
                    addl_bkng_cntry2_cd,
                    addl_bkng_cntry3_cd,
                    addl_bkng_cntry4_cd,
                    addl_bkng_cntry5_cd,
                    arc_nbr,
                    agency_nbr_legacy,
                    cntry_cd,
                    agency_create_dt,
                    finance_cd,
                    pymt_method_cd,
                    onesource_status_cd
                    FROM plr_agency_ds 
              ) src
      ON (tgt.agency_unique_id = src.agency_unique_id)                    
      WHEN MATCHED THEN
         UPDATE SET
                    tgt.agency_nbr = src.agency_nbr,
                    tgt.company_cd = src.company_cd,
                    tgt.agency_name = src.agency_name,
                    tgt.addr1 = src.addr1,
                    tgt.addr2 = src.addr2,
                    tgt.addr3 = src.addr3,
                    tgt.city = src.city,
                    tgt.state = src.state,
                    tgt.zip = src.zip,
                    tgt.cntry_name = src.cntry_name,
                    tgt.phone = src.phone,
                    tgt.fax = src.fax,
                    tgt.email = src.email,
                    tgt.agency_contact = src.agency_contact,
                    tgt.agency_status_cd = src.agency_status_cd,
                    tgt.comm_withhold_cd = src.comm_withhold_cd,
                    tgt.region_cd = src.region_cd,
                    tgt.region_desc = src.region_desc,
                    tgt.region_district_cd = src.region_district_cd,
                    tgt.bdm_name = src.bdm_name,
                    tgt.bdr_name = src.bdr_name,
                    tgt.brc_all_cd = src.brc_all_cd,
                    tgt.brc_alaska_cd = src.brc_alaska_cd,
                    tgt.brc_canal_cd = src.brc_canal_cd,
                    tgt.brc_carib_cd = src.brc_carib_cd,
                    tgt.brc_europe_cd = src.brc_europe_cd,
                    tgt.brc_long_cd = src.brc_long_cd,
                    tgt.brc_volume_cd = src.brc_volume_cd,
                    tgt.brc_voyage_cd = src.brc_voyage_cd,
                    tgt.addl_bkng_cntry1_cd = src.addl_bkng_cntry1_cd,
                    tgt.addl_bkng_cntry2_cd = src.addl_bkng_cntry2_cd,
                    tgt.addl_bkng_cntry3_cd = src.addl_bkng_cntry3_cd,
                    tgt.addl_bkng_cntry4_cd = src.addl_bkng_cntry4_cd,
                    tgt.addl_bkng_cntry5_cd = src.addl_bkng_cntry5_cd,
                    tgt.arc_nbr = src.arc_nbr,
                    tgt.agency_nbr_legacy = src.agency_nbr_legacy,
                    tgt.cntry_cd = src.cntry_cd,
                    tgt.agency_create_dt = src.agency_create_dt,
                    tgt.finance_cd = src.finance_cd,
                    tgt.pymt_method_cd = src.pymt_method_cd,
                    tgt.onesource_status_cd = src.onesource_status_cd
      WHEN NOT MATCHED THEN          
         INSERT (  tgt.row_wid,
                        tgt.agency_unique_id,
                        tgt.agency_nbr,
                        tgt.company_cd,
                        tgt.agency_name,
                        tgt.addr1,
                        tgt.addr2,
                        tgt.addr3,
                        tgt.city,
                        tgt.state,
                        tgt.zip,
                        tgt.cntry_name,
                        tgt.phone,
                        tgt.fax,
                        tgt.email,
                        tgt.agency_contact,
                        tgt.agency_status_cd,
                        tgt.comm_withhold_cd,
                        tgt.region_cd,
                        tgt.region_desc,
                        tgt.region_district_cd,
                        tgt.bdm_name,
                        tgt.bdr_name,
                        tgt.brc_all_cd,
                        tgt.brc_alaska_cd,
                        tgt.brc_canal_cd,
                        tgt.brc_carib_cd,
                        tgt.brc_europe_cd,
                        tgt.brc_long_cd,
                        tgt.brc_volume_cd,
                        tgt.brc_voyage_cd,
                        tgt.addl_bkng_cntry1_cd,
                        tgt.addl_bkng_cntry2_cd,
                        tgt.addl_bkng_cntry3_cd,
                        tgt.addl_bkng_cntry4_cd,
                        tgt.addl_bkng_cntry5_cd,
                        tgt.arc_nbr,
                        tgt.agency_nbr_legacy,
                        tgt.cntry_cd,
                        tgt.agency_create_dt,
                        tgt.finance_cd,
                        tgt.pymt_method_cd,
                        tgt.onesource_status_cd)                       
         VALUES ((src.row_wid + NVL(v_max_wid,0)), 
                        src.agency_unique_id,
                        src.agency_nbr,
                        src.company_cd,
                        src.agency_name,
                        src.addr1,
                        src.addr2,
                        src.addr3,
                        src.city,
                        src.state,
                        src.zip,
                        src.cntry_name,
                        src.phone,
                        src.fax,
                        src.email,
                        src.agency_contact,
                        src.agency_status_cd,
                        src.comm_withhold_cd,
                        src.region_cd,
                        src.region_desc,
                        src.region_district_cd,
                        src.bdm_name,
                        src.bdr_name,
                        src.brc_all_cd,
                        src.brc_alaska_cd,
                        src.brc_canal_cd,
                        src.brc_carib_cd,
                        src.brc_europe_cd,
                        src.brc_long_cd,
                        src.brc_volume_cd,
                        src.brc_voyage_cd,
                        src.addl_bkng_cntry1_cd,
                        src.addl_bkng_cntry2_cd,
                        src.addl_bkng_cntry3_cd,
                        src.addl_bkng_cntry4_cd,
                        src.addl_bkng_cntry5_cd,
                        src.arc_nbr,
                        src.agency_nbr_legacy,
                        src.cntry_cd,
                        src.agency_create_dt,
                        src.finance_cd,
                        src.pymt_method_cd,
                        src.onesource_status_cd);              
  
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_AGENCY_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END AGENCY_LOAD;

   PROCEDURE BKNG_DIM_STG_LOAD IS
   /***************************************************************************
   NAME: BKNG_DIM_STG_LOAD
       
   PURPOSE: 
      Staging table load for table EDW_BKNG_D 
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_DIM_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
    
      EXECUTE IMMEDIATE 'TRUNCATE TABLE plr_bkng_ds';

      INSERT /*+ append */  INTO plr_bkng_ds
      (
        bkng_unique_id,
        bkng_id,
        bkng_nbr,
        bkng_party_nbr,
        company_cd,
        title,
        first_name,
        last_name,
        suffix,
        home_state,
        home_zip,
        nationality,
        home_cntry,
        age,
        cpp_cd,
        concession_cd,
        direct_flg,
        earmark_right,
        earmark_left,
        link_id,
        link_seq_flg,
        travel_with_id,
        bkng_status_cd,
        bkng_party_status_cd,
        bkng_open_dt,
        bkng_confirm_dt,
        bkng_party_open_dt,
        bkng_xld_cd,
        bkng_xld_dt,
        bkng_party_xld_dt,
--        open_user_id,
--        reassign_user_id,
        prod_desc,
        air_flg,
        air_city,
        currency_cd,
        primary_promo_id,
        primary_promo_desc,
        promo_comm_cd,
        bkng_channel,
--        cabin_nbr,
        category_cd,
        meta_cd,
        sub_meta_cd,
        apo_alt_cd,
        apo_cd,
        apo_meta_cd,
        apo_sub_meta_cd,
        cabin_use_cd,
        bkng_contact,
        deposit_rcvd_dt,
        final_pymt_rcvd_dt,
--        wholesaler_flg,
--        secondary_agency_nbr,
--        primary_agency_nbr,
--        comm_withheld_cd,
        primary_source,
        secondary_source,
        intl_source,
        first_name_initial,
        non_intl_party_flg,
        no_home_cntry_flg,
        air_home_city,
        air_cntry,
        direct_first_name,
        direct_last_name,
        direct_home_addr1,
        direct_home_addr2,
        direct_home_city,
        direct_home_state,
        direct_home_zip,
        direct_home_phone,
        meta_long_desc,
        third_source,
        bkng_source,
        bkng_type,
        channel_type,
        gender_cd,
        promo1_id,
        promo2_id,
        promo3_id,
        promo4_id,
        promo5_id,
        promo6_id,
        promo7_id,
        promo8_id,
        promo9_id,
        promo10_id,
        promo11_id,
        promo12_id,
        air_fee_uk_pymt_dt,
--        xld_user_id,
--        open_user_name,
        bkng_sub_origin_cd,
        bkng_origin_cd,
        waitlist_remarks,
--        reassign_user_name,
        activate_cd,
        balance_due_dt,
        call_back_cd,
        call_back_dt,
        dining_cd,
        dining_room_cd,
        option_dt,
        dining_time_cd,
        dining_time_confirm_flg,
        dining_time_waitlist_cd,
        cpp_final_pymt_dt,
--        change_user_id,
        ct_alloc_method_cd,
        ct_alloc_band_cd,
--        change_user_name,
        fare_type                
      )
     SELECT 
            bkng_unique_id,
            bkng_id,
            bkng_nbr,
            bkng_party_nbr,
            company_cd,
            title,
            first_name,
            last_name,
            suffix,
            home_state,
            home_zip,
            nationality,
            home_cntry,
            age,
            cpp_cd,
            concession_cd,
            direct_flg,
            earmark_right,
            earmark_left,
            link_id,
            link_seq_flg,
            travel_with_id,
            bkng_status_cd,
            bkng_party_status_cd,
            bkng_open_dt,
            bkng_confirm_dt,
            bkng_party_open_dt,
            bkng_xld_cd,
            bkng_xld_dt,
            bkng_party_xld_dt,
--            open_user_id,
--            reassign_user_id,
            prod_desc,
            air_flg,
            air_city,
            currency_cd,
            primary_promo_id,
            primary_promo_desc,
            promo_comm_cd,
            bkng_channel,
--            cabin_nbr,
            category_cd,
            meta_cd,
            sub_meta_cd,
            apo_alt_cd,
            apo_cd,
            apo_meta_cd,
            apo_sub_meta_cd,
            cabin_use_cd,
            bkng_contact,
            deposit_rcvd_dt,
            final_pymt_rcvd_dt,
--            wholesaler_flg,
--            secondary_agency_nbr,
--            primary_agency_nbr,
--            comm_withheld_cd,
            primary_source,
            secondary_source,
            intl_source,
            first_name_initial,
            non_intl_party_flg,
            no_home_cntry_flg,
            air_home_city,
            air_cntry,
            direct_first_name,
            direct_last_name,
            direct_home_addr1,
            direct_home_addr2,
            direct_home_city,
            direct_home_state,
            direct_home_zip,
            direct_home_phone,
            meta_long_desc,
            third_source,
            bkng_source,
            bkng_type,
            channel_type,
            gender_cd,
            promo1_id,
            promo2_id,
            promo3_id,
            promo4_id,
            promo5_id,
            promo6_id,
            promo7_id,
            promo8_id,
            promo9_id,
            promo10_id,
            promo11_id,
            promo12_id,
            air_fee_uk_pymt_dt,
--            xld_user_id,
--            open_user_name,
            bkng_sub_origin_cd,
            bkng_origin_cd,
            waitlist_remarks,
--            reassign_user_name,
            activate_cd,
            balance_due_dt,
            call_back_cd,
            call_back_dt,
            dining_cd,
            dining_room_cd,
            option_dt,
            dining_time_cd,
            dining_time_confirm_flg,
            dining_time_waitlist_cd,
            cpp_final_pymt_dt,
--            change_user_id,
            ct_alloc_method_cd,
            ct_alloc_band_cd,
--            change_user_name,
            fare_type
       FROM 
            dm_tkr.v_edw_bkng_d_daily_dev@stgdwh1_edwro;
--            dm_tkr.v_edw_bkng_d_wkly_dev@stgdwh1_edwro;

      vRowCountInserts := SQL%ROWCOUNT;

      COMMIT;
             
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'PLR_BKNG_DS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_DIM_STG_LOAD;

   PROCEDURE BKNG_DIM_STATUS_LOAD IS
   /***************************************************************************
   NAME: BKNG_DIM_STATUS_LOAD
       
   PURPOSE: 
      Load for table EDW_BKNG_STATUS_D 
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_DIM_STATUS_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_bkng_status_d;    
     
      --Inserts
      INSERT /*+ append */  INTO edw_bkng_status_d    
      (
         row_wid,
         bkng_status_cd,
         bkng_party_status_cd
       )
      SELECT ROWNUM + NVL(v_max_wid,0) AS row_wid,
                  bkng_status_cd,
                  bkng_party_status_cd
       FROM (
                  SELECT
                          stg.bkng_status_cd,
                          stg.bkng_party_status_cd
                   FROM plr_bkng_ds stg 
                  WHERE NOT EXISTS (SELECT 1 FROM edw_bkng_status_d tgt 
                                                  WHERE NVL(tgt.bkng_status_cd, 'x') = NVL(stg.bkng_status_cd, 'x') 
                                                      AND NVL(tgt.bkng_party_status_cd, 'x') = NVL(stg.bkng_party_status_cd, 'x'))       
                 GROUP BY stg.bkng_status_cd, stg.bkng_party_status_cd
                 );
              
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_BKNG_STATUS_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_DIM_STATUS_LOAD;

   PROCEDURE BKNG_DIM_CHANNEL_LOAD IS
   /***************************************************************************
   NAME: BKNG_DIM_CHANNEL_LOAD
       
   PURPOSE: 
      Load for table EDW_BKNG_CHANNEL_D  
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_DIM_CHANNEL_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_bkng_channel_d;    
     
      --Inserts
      INSERT /*+ append */  INTO edw_bkng_channel_d     
      (
         row_wid,
         bkng_channel,
         bkng_source,
         bkng_type,
         channel_type,
         bkng_sub_origin_cd,
         bkng_origin_cd,
         fare_type
       )
      SELECT ROWNUM + NVL(v_max_wid,0) AS row_wid,
                  bkng_channel,
                  bkng_source,
                  bkng_type,
                  channel_type,
                  bkng_sub_origin_cd,
                  bkng_origin_cd,
                  fare_type
         FROM (      
                      SELECT
                              stg.bkng_channel,
                              stg.bkng_source,
                              stg.bkng_type,
                              stg.channel_type,
                              stg.bkng_sub_origin_cd,
                              stg.bkng_origin_cd,
                              stg.fare_type
                       FROM plr_bkng_ds stg 
                      WHERE NOT EXISTS (SELECT 1 FROM edw_bkng_channel_d tgt 
                                                     WHERE NVL(tgt.bkng_channel, 'x') = NVL(stg.bkng_channel, 'x') 
                                                          AND NVL(tgt.bkng_source, 'x') = NVL(stg.bkng_source, 'x')
                                                          AND NVL(tgt.bkng_type, 'x') = NVL(stg.bkng_type, 'x')
                                                          AND NVL(tgt.channel_type, 'x') = NVL(stg.channel_type, 'x')
                                                          AND NVL(tgt.bkng_sub_origin_cd, 'x') = NVL(stg.bkng_sub_origin_cd, 'x')
                                                          AND NVL(tgt.bkng_origin_cd, 'x') = NVL(stg.bkng_origin_cd, 'x')                                        
                                                          AND NVL(tgt.fare_type, 'x') = NVL(stg.fare_type, 'x'))       
                     GROUP BY stg.bkng_channel, stg.bkng_source, stg.bkng_type, stg.channel_type, stg.bkng_sub_origin_cd, stg.bkng_origin_cd, stg.fare_type
                   );
              
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_BKNG_CHANNEL_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_DIM_CHANNEL_LOAD;

   PROCEDURE BKNG_DIM_GEO_LOAD IS
   /***************************************************************************
   NAME: BKNG_DIM_GEO_LOAD
       
   PURPOSE: 
      Load for table EDW_BKNG_GEO_D    
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_DIM_GEO_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_bkng_geo_d;    
     
      --Inserts
      INSERT /*+ append */  INTO edw_bkng_geo_d       
      (
        row_wid,
        air_city,
        primary_source,
        secondary_source,
        intl_source,
        air_home_city,
        air_cntry,
        third_source,
        nationality,
        home_cntry,
        home_state,
        direct_home_state
       )
      SELECT ROWNUM + NVL(v_max_wid,0) AS row_wid,
                air_city,
                primary_source,
                secondary_source,
                intl_source,
                air_home_city,
                air_cntry,
                third_source,
                nationality,
                home_cntry,
                home_state,
                direct_home_state  
          FROM (               
                      SELECT
                            stg.air_city,
                            stg.primary_source,
                            stg.secondary_source,
                            stg.intl_source,
                            stg.air_home_city,
                            stg.air_cntry,
                            stg.third_source,
                            stg.nationality,
                            stg.home_cntry,
                            stg.home_state,
                            stg.direct_home_state
                       FROM plr_bkng_ds stg 
                      WHERE NOT EXISTS (SELECT 1 FROM edw_bkng_geo_d tgt 
                                                      WHERE NVL(tgt.air_city, 'x') = NVL(stg.air_city, 'x')
                                                          AND NVL(tgt.primary_source, 'x') = NVL(stg.primary_source, 'x')  
                                                          AND NVL(tgt.secondary_source, 'x') = NVL(stg.secondary_source, 'x')
                                                          AND NVL(tgt.intl_source, 'x') = NVL(stg.intl_source, 'x')
                                                          AND NVL(tgt.air_home_city, 'x') = NVL(stg.air_home_city, 'x')  
                                                          AND NVL(tgt.air_cntry, 'x') = NVL(stg.air_cntry, 'x')
                                                          AND NVL(tgt.third_source, 'x') = NVL(stg.third_source, 'x')
                                                          AND NVL(tgt.nationality, 'x') = NVL(stg.nationality, 'x')  
                                                          AND NVL(tgt.home_cntry, 'x') = NVL(stg.home_cntry, 'x')
                                                          AND NVL(tgt.home_state, 'x') = NVL(stg.home_state, 'x')
                                                          AND NVL(tgt.direct_home_state, 'x') = NVL(stg.direct_home_state, 'x') )       
                     GROUP BY stg.air_city,stg.primary_source, stg.secondary_source, stg.intl_source, stg.air_home_city,
                                     stg.air_cntry, stg.third_source, stg.nationality, stg.home_cntry, stg.home_state, stg.direct_home_state
                     );
              
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_BKNG_GEO_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_DIM_GEO_LOAD;

   PROCEDURE BKNG_DIM_GUEST_DEM_LOAD IS
   /***************************************************************************
   NAME: BKNG_DIM_GUEST_DEM_LOAD
       
   PURPOSE: 
      Load for table EDW_BKNG_GUEST_DEMOG_D     
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_DIM_GUEST_DEM_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_bkng_guest_demog_d;    
     
      --Inserts
      INSERT /*+ append */  INTO edw_bkng_guest_demog_d        
      (
        row_wid,
        age,
        gender_cd
       )
      SELECT ROWNUM + NVL(v_max_wid,0) AS row_wid,
                  age,
                  gender_cd
         FROM (      
                      SELECT
                            stg.age,
                            stg.gender_cd
                       FROM plr_bkng_ds stg 
                      WHERE NOT EXISTS (SELECT 1 FROM edw_bkng_guest_demog_d tgt 
                                                      WHERE NVL(tgt.age, 0) = NVL(stg.age, 0)
                                                          AND NVL(tgt.gender_cd, 'x') = NVL(stg.gender_cd, 'x') )       
                     GROUP BY stg.age, stg.gender_cd
                  );
              
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_BKNG_GUEST_DEMOG_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_DIM_GUEST_DEM_LOAD;

   PROCEDURE BKNG_DIM_CABIN_LOAD IS
   /***************************************************************************
   NAME: BKNG_DIM_CABIN_LOAD
       
   PURPOSE: 
      Load for table EDW_BKNG_CABIN_D      
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_DIM_CABIN_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_bkng_cabin_d;    
     
      --Inserts
      INSERT /*+ append */  INTO edw_bkng_cabin_d        
      (
        row_wid,
        category_cd,
        meta_cd,
        sub_meta_cd,
        apo_alt_cd,
        apo_cd,
        apo_meta_cd,
        apo_sub_meta_cd,
        cabin_use_cd,
        meta_long_desc
       )
      SELECT ROWNUM + NVL(v_max_wid,0) AS row_wid,
                  category_cd,
                  meta_cd,
                  sub_meta_cd,
                  apo_alt_cd,
                  apo_cd,
                  apo_meta_cd,
                  apo_sub_meta_cd,
                  cabin_use_cd,
                  meta_long_desc 
         FROM (
                      SELECT
                            stg.category_cd,
                            stg.meta_cd,
                            stg.sub_meta_cd,
                            stg.apo_alt_cd,
                            stg.apo_cd,
                            stg.apo_meta_cd,
                            stg.apo_sub_meta_cd,
                            stg.cabin_use_cd,
                            stg.meta_long_desc
                       FROM plr_bkng_ds stg 
                      WHERE NOT EXISTS (SELECT 1 FROM edw_bkng_cabin_d tgt 
                                                      WHERE NVL(tgt.category_cd, 'x') = NVL(stg.category_cd, 'x')
                                                          AND NVL(tgt.meta_cd, 'x') = NVL(stg.meta_cd, 'x')
                                                          AND NVL(tgt.sub_meta_cd, 'x') = NVL(stg.sub_meta_cd, 'x')
                                                          AND NVL(tgt.apo_alt_cd, 'x') = NVL(stg.apo_alt_cd, 'x')
                                                          AND NVL(tgt.apo_cd, 'x') = NVL(stg.apo_cd, 'x')
                                                          AND NVL(tgt.apo_meta_cd, 'x') = NVL(stg.apo_meta_cd, 'x')
                                                          AND NVL(tgt.apo_sub_meta_cd, 'x') = NVL(stg.apo_sub_meta_cd, 'x')
                                                          AND NVL(tgt.cabin_use_cd, 'x') = NVL(stg.cabin_use_cd, 'x')
                                                          AND NVL(tgt.meta_long_desc, 'x') = NVL(stg.meta_long_desc, 'x') )       
                     GROUP BY stg.category_cd, stg.meta_cd, stg.sub_meta_cd, stg.apo_alt_cd,
                                     stg.apo_cd, stg.apo_meta_cd, stg.apo_sub_meta_cd, stg.cabin_use_cd, stg.meta_long_desc
                     );
              
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_BKNG_CABIN_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_DIM_CABIN_LOAD;

   PROCEDURE BKNG_DIM_MINI_LOAD IS
   /***************************************************************************
   NAME: BKNG_DIM_MINI_LOAD
       
   PURPOSE: 
      Load for table EDW_BKNG_MINI_D        
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_DIM_MINI_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           

      SELECT MAX(row_wid) INTO v_max_wid FROM edw_bkng_mini_d;    
     
      --Inserts
      INSERT /*+ append */  INTO edw_bkng_mini_d        
      (
        row_wid,
        activate_cd,
        cpp_cd,
        concession_cd,
        direct_flg,
        bkng_xld_cd,
        air_flg,
        currency_cd,
        promo_comm_cd,
        non_intl_party_flg,
        no_home_cntry_flg
       )
      SELECT ROWNUM + NVL(v_max_wid,0) AS row_wid,
                  activate_cd,
                  cpp_cd,
                  concession_cd,
                  direct_flg,
                  bkng_xld_cd,
                  air_flg,
                  currency_cd,
                  promo_comm_cd,
                  non_intl_party_flg,
                  no_home_cntry_flg   
          FROM (   
                      SELECT
                            stg.activate_cd,
                            stg.cpp_cd,
                            stg.concession_cd,
                            stg.direct_flg,
                            stg.bkng_xld_cd,
                            stg.air_flg,
                            stg.currency_cd,
                            stg.promo_comm_cd,
                            stg.non_intl_party_flg,
                            stg.no_home_cntry_flg
                       FROM plr_bkng_ds stg 
                      WHERE NOT EXISTS (SELECT 1 FROM edw_bkng_mini_d tgt 
                                                      WHERE NVL(tgt.activate_cd, 'x') = NVL(stg.activate_cd, 'x')
                                                          AND NVL(tgt.cpp_cd, 'x') = NVL(stg.cpp_cd, 'x')
                                                          AND NVL(tgt.concession_cd, 'x') = NVL(stg.concession_cd, 'x')
                                                          AND NVL(tgt.direct_flg, 'x') = NVL(stg.direct_flg, 'x')
                                                          AND NVL(tgt.bkng_xld_cd, 'x') = NVL(stg.bkng_xld_cd, 'x')
                                                          AND NVL(tgt.air_flg, 'x') = NVL(stg.air_flg, 'x')
                                                          AND NVL(tgt.currency_cd, 'x') = NVL(stg.currency_cd, 'x')
                                                          AND NVL(tgt.promo_comm_cd, 'x') = NVL(stg.promo_comm_cd, 'x')
                                                          AND NVL(tgt.non_intl_party_flg, 9) = NVL(stg.non_intl_party_flg, 9)
                                                          AND NVL(tgt.no_home_cntry_flg, 9) = NVL(stg.no_home_cntry_flg, 9) )       
                     GROUP BY stg.activate_cd, stg.cpp_cd, stg.concession_cd, stg.direct_flg, stg.bkng_xld_cd,
                                     stg.air_flg, stg.currency_cd, stg.promo_comm_cd, stg.non_intl_party_flg, stg.no_home_cntry_flg
                     );
              
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_BKNG_MINI_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_DIM_MINI_LOAD;

   PROCEDURE BKNG_FACT_STG_LOAD IS
   /***************************************************************************
   NAME: BKNG_FACT_STG_LOAD
       
   PURPOSE: 
      Staging table load for table EDW_BKNG_F 
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_FACT_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
    
      EXECUTE IMMEDIATE 'TRUNCATE TABLE plr_bkng_fs';

      INSERT /*+ append */  INTO plr_bkng_fs
      (
        voyage_unique_id,    agency_unique_id,    group_unique_id,    guest_unique_id,    bkng_unique_id,    bkng_id, bkng_nbr, bkng_party_nbr,
        guest_count,    guest_count_qualified,    lower_bed_count,    lower_bed_count_alt,    gross_rev_native,    gross_rev_usd_blended,    gross_rev_usd_local,    comm_rev_native,
        comm_rev_usd_blended,    comm_rev_usd_local,    ct_rev_native,    ct_rev_usd_blended,    ct_rev_usd_local,    ct_net_rev_native,    ct_net_rev_usd_blended,    ct_net_rev_usd_local,    cta_rev_native,    cta_rev_usd_blended,
        cta_rev_usd_local,    cta_rev_usd_qualified,    cta_net_rev_native,    cta_net_rev_usd_blended,    cta_net_rev_usd_local,    air_rev_native,    air_rev_usd_blended,    air_rev_usd_local,    comm_native,    comm_usd_blended,
        comm_usd_local,    base_comm_native,    base_comm_usd_blended,    base_comm_usd_local,    over_comm_native,    over_comm_usd_blended,    over_comm_usd_local,    air_comm_native,    air_comm_usd_blended,    air_comm_usd_local,
        bonus_comm_native,    bonus_comm_usd_blended,    bonus_comm_usd_local,    wholesaler_comm_native,    wholesaler_comm_usd_blended,    wholesaler_comm_usd_local,    air_fee_native,    air_fee_usd_blended,    air_fee_usd_local,    air_fee_tax_native,
        air_fee_tax_usd_blended,    air_fee_tax_usd_local,    air_dev_fee_native,    air_dev_fee_usd_blended,    air_dev_fee_usd_local,    ncf_native,    ncf_usd_blended,    ncf_usd_local,    govt_fee_native,    govt_fee_usd_blended,
        govt_fee_usd_local,    service_fee_native,    service_fee_usd_blended,    service_fee_usd_local,    fuel_supp_native,    fuel_supp_usd_blended,    fuel_supp_usd_local,    fcd_native,    fcd_usd_blended,    fcd_usd_local,
        cpp_rev_native,    cpp_rev_usd_blended,    cpp_rev_usd_local,    cpp_comm_native,    cpp_comm_usd_blended,    cpp_comm_usd_local,    package_rev_native,    package_rev_usd_blended,    package_rev_usd_local,    package_comm_native,
        package_comm_usd_blended,    package_comm_usd_local,    land_mods_native,    land_mods_usd_blended,    land_mods_usd_local,    land_mods_fee_native,    land_mods_fee_usd_blended,    land_mods_fee_usd_local,    act_tc_cost_native,    act_tc_cost_usd_blended,
        act_tc_cost_usd_local,    fuel_ta_fee_native,    fuel_ta_fee_usd_local,    std_land_cost_native,    std_land_cost_usd_local,    act_air_cost_native,    act_air_cost_usd_local,    obc_usd,    obc_native,    act_gap_cost_usd,
        act_gap_cost_native,    act_cocktail_cost_usd,    act_cocktail_cost_native,    act_amenities_cost_usd,    act_amenities_cost_native,    est_gap_cost_usd,    est_gap_cost_native,    est_tc_cost_usd,    est_tc_cost_native,    est_over_comm_usd,
        est_over_comm_native,    tax_margin_usd,    tax_margin_native,    akt_ship_cost_usd,    akt_supp_cost_usd,    akt_ncf_usd,    akt_tax_rev_usd,    akt_tax_cost_usd,    act_caso_cost_usd,    act_caso_cost_native,
        act_loy_air_dev_native,    act_loy_air_dev_usd_blended,    act_loy_air_dev_usd_local,    ntr_fin_usd,    ntr_sls_usd,    ntr_sls_native,    fare_native,    fare_usd_blended,    fare_usd_local,    disc_native,
        disc_usd_blended,    disc_usd_local,    indep_lower_bed_count,    indep_ntr_fin_usd,    group_lower_bed_count,    group_ntr_fin_usd,    crs_lower_bed_count,    crs_ntr_fin_usd,    far_lower_bed_count,    tour_lower_bed_count,
        tour_ntr_fin_usd,    extra_guest_count,    single_guest_count,    air_guest_count,    air_add_on_native,    air_add_on_usd_blended,    air_add_on_usd_local,    air_supp_native,    air_supp_usd_blended,    air_supp_usd_local,
        pcd,    olbd,    npr_usd_blended,    net_tour_fare_native,    net_tour_fare_usd_blended,    net_crs_fare_native,    net_crs_fare_usd_blended,    net_crs_fare_percent,    ntr_fin_native,    air_fee_uk_usd,
        air_fee_uk_native,    rtce_guest_count,    tmod_comm_native,    tmod_comm_usd_blended,    tmod_comm_usd_local,    port_charges_native,    port_charges_usd_blended,    port_charges_usd_local,    land_charges_native,    land_charges_usd_blended,
        land_charges_usd_local,    fuel_ta_fee_usd_blended,    land_mods_cost_native,    land_mods_cost_usd_blended,    land_mods_cost_usd_local,    package_fee_native,    package_fee_usd_blended,    package_fee_usd_local,    double_guest_count,    share_guest_count,
        air_fee_base_comm_native,    air_fee_base_comm_usd_blended,    air_fee_over_comm_native,    air_fee_over_comm_usd_blended,    fuel_supp_base_comm_native,    fuel_supp_base_comm_usd_blend,    fuel_supp_over_comm_native,    fuel_supp_over_comm_usd_blend,    govt_fee_base_comm_native,    govt_fee_base_comm_usd_blended,
        govt_fee_over_comm_native,    govt_fee_over_comm_usd_blended,    ncf_base_comm_native,    ncf_base_comm_usd_blended,    ncf_over_comm_native,    ncf_over_comm_usd_blended, blended_conv_rate, local_per_base_rate, cost_conv_rate                
      )
     SELECT 
        voyage_unique_id,    agency_unique_id,    group_unique_id,    guest_unique_id,    bkng_unique_id,    bkng_id, bkng_nbr, bkng_party_nbr,
        guest_count,    guest_count_qualified,    lower_bed_count,    lower_bed_count_alt,    gross_rev_native,    gross_rev_usd_blended,    gross_rev_usd_local,    comm_rev_native,
        comm_rev_usd_blended,    comm_rev_usd_local,    ct_rev_native,    ct_rev_usd_blended,    ct_rev_usd_local,    ct_net_rev_native,    ct_net_rev_usd_blended,    ct_net_rev_usd_local,    cta_rev_native,    cta_rev_usd_blended,
        cta_rev_usd_local,    cta_rev_usd_qualified,    cta_net_rev_native,    cta_net_rev_usd_blended,    cta_net_rev_usd_local,    air_rev_native,    air_rev_usd_blended,    air_rev_usd_local,    comm_native,    comm_usd_blended,
        comm_usd_local,    base_comm_native,    base_comm_usd_blended,    base_comm_usd_local,    over_comm_native,    over_comm_usd_blended,    over_comm_usd_local,    air_comm_native,    air_comm_usd_blended,    air_comm_usd_local,
        bonus_comm_native,    bonus_comm_usd_blended,    bonus_comm_usd_local,    wholesaler_comm_native,    wholesaler_comm_usd_blended,    wholesaler_comm_usd_local,    air_fee_native,    air_fee_usd_blended,    air_fee_usd_local,    air_fee_tax_native,
        air_fee_tax_usd_blended,    air_fee_tax_usd_local,    air_dev_fee_native,    air_dev_fee_usd_blended,    air_dev_fee_usd_local,    ncf_native,    ncf_usd_blended,    ncf_usd_local,    govt_fee_native,    govt_fee_usd_blended,
        govt_fee_usd_local,    service_fee_native,    service_fee_usd_blended,    service_fee_usd_local,    fuel_supp_native,    fuel_supp_usd_blended,    fuel_supp_usd_local,    fcd_native,    fcd_usd_blended,    fcd_usd_local,
        cpp_rev_native,    cpp_rev_usd_blended,    cpp_rev_usd_local,    cpp_comm_native,    cpp_comm_usd_blended,    cpp_comm_usd_local,    package_rev_native,    package_rev_usd_blended,    package_rev_usd_local,    package_comm_native,
        package_comm_usd_blended,    package_comm_usd_local,    land_mods_native,    land_mods_usd_blended,    land_mods_usd_local,    land_mods_fee_native,    land_mods_fee_usd_blended,    land_mods_fee_usd_local,    act_tc_cost_native,    act_tc_cost_usd_blended,
        act_tc_cost_usd_local,    fuel_ta_fee_native,    fuel_ta_fee_usd_local,    std_land_cost_native,    std_land_cost_usd_local,    act_air_cost_native,    act_air_cost_usd_local,    obc_usd,    obc_native,    act_gap_cost_usd,
        act_gap_cost_native,    act_cocktail_cost_usd,    act_cocktail_cost_native,    act_amenities_cost_usd,    act_amenities_cost_native,    est_gap_cost_usd,    est_gap_cost_native,    est_tc_cost_usd,    est_tc_cost_native,    est_over_comm_usd,
        est_over_comm_native,    tax_margin_usd,    tax_margin_native,    akt_ship_cost_usd,    akt_supp_cost_usd,    akt_ncf_usd,    akt_tax_rev_usd,    akt_tax_cost_usd,    act_caso_cost_usd,    act_caso_cost_native,
        act_loy_air_dev_native,    act_loy_air_dev_usd_blended,    act_loy_air_dev_usd_local,    ntr_fin_usd,    ntr_sls_usd,    ntr_sls_native,    fare_native,    fare_usd_blended,    fare_usd_local,    disc_native,
        disc_usd_blended,    disc_usd_local,    indep_lower_bed_count,    indep_ntr_fin_usd,    group_lower_bed_count,    group_ntr_fin_usd,    crs_lower_bed_count,    crs_ntr_fin_usd,    far_lower_bed_count,    tour_lower_bed_count,
        tour_ntr_fin_usd,    extra_guest_count,    single_guest_count,    air_guest_count,    air_add_on_native,    air_add_on_usd_blended,    air_add_on_usd_local,    air_supp_native,    air_supp_usd_blended,    air_supp_usd_local,
        pcd,    olbd,    npr_usd_blended,    net_tour_fare_native,    net_tour_fare_usd_blended,    net_crs_fare_native,    net_crs_fare_usd_blended,    net_crs_fare_percent,    ntr_fin_native,    air_fee_uk_usd,
        air_fee_uk_native,    rtce_guest_count,    tmod_comm_native,    tmod_comm_usd_blended,    tmod_comm_usd_local,    port_charges_native,    port_charges_usd_blended,    port_charges_usd_local,    land_charges_native,    land_charges_usd_blended,
        land_charges_usd_local,    fuel_ta_fee_usd_blended,    land_mods_cost_native,    land_mods_cost_usd_blended,    land_mods_cost_usd_local,    package_fee_native,    package_fee_usd_blended,    package_fee_usd_local,    double_guest_count,    share_guest_count,
        air_fee_base_comm_native,    air_fee_base_comm_usd_blended,    air_fee_over_comm_native,    air_fee_over_comm_usd_blended,    fuel_supp_base_comm_native,    fuel_supp_base_comm_usd_blend,    fuel_supp_over_comm_native,    fuel_supp_over_comm_usd_blend,    govt_fee_base_comm_native,    govt_fee_base_comm_usd_blended,
        govt_fee_over_comm_native,    govt_fee_over_comm_usd_blended,    ncf_base_comm_native,    ncf_base_comm_usd_blended,    ncf_over_comm_native,    ncf_over_comm_usd_blended, blended_conv_rate, local_per_base_rate, cost_conv_rate
       FROM 
            dm_tkr.v_edw_bkng_f_daily_dev@stgdwh1_edwro;
--            dm_tkr.v_edw_bkng_f_wkly_dev@stgdwh1_edwro;            

      vRowCountInserts := SQL%ROWCOUNT;

      COMMIT;
             
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'PLR_BKNG_FS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_FACT_STG_LOAD;

   PROCEDURE BKNG_FACT_LOAD IS
   /***************************************************************************
   NAME: BKNG_FACT_LOAD
       
   PURPOSE: 
      Load for table EDW_BKNG_F 
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_FACT_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
    
      --Updates/Inserts
      MERGE INTO edw_bkng_f tgt 
      USING (SELECT /*+ PARALLEL */
                           stg.bkng_unique_id,
                           stg.bkng_id,
                           stg.bkng_nbr,
                           stg.bkng_party_nbr,
                           voy.row_wid AS voyage_wid,
                           agy.row_wid AS agency_wid,
                           0 AS group_wid,
                           NVL(pax.guest_wid,0) AS guest_wid,
                            stg.guest_count,
                            stg.guest_count_qualified,
                            stg.lower_bed_count,
                            stg.lower_bed_count_alt,
                            stg.gross_rev_native,
                            stg.gross_rev_usd_blended,
                            stg.gross_rev_usd_local,
                            stg.comm_rev_native,
                            stg.comm_rev_usd_blended,
                            stg.comm_rev_usd_local,
                            stg.ct_rev_native,
                            stg.ct_rev_usd_blended,
                            stg.ct_rev_usd_local,
                            stg.ct_net_rev_native,
                            stg.ct_net_rev_usd_blended,
                            stg.ct_net_rev_usd_local,
                            stg.cta_rev_native,
                            stg.cta_rev_usd_blended,
                            stg.cta_rev_usd_local,
                            stg.cta_rev_usd_qualified,
                            stg.cta_net_rev_native,
                            stg.cta_net_rev_usd_blended,
                            stg.cta_net_rev_usd_local,
                            stg.air_rev_native,
                            stg.air_rev_usd_blended,
                            stg.air_rev_usd_local,
                            stg.comm_native,
                            stg.comm_usd_blended,
                            stg.comm_usd_local,
                            stg.base_comm_native,
                            stg.base_comm_usd_blended,
                            stg.base_comm_usd_local,
                            stg.over_comm_native,
                            stg.over_comm_usd_blended,
                            stg.over_comm_usd_local,
                            stg.air_comm_native,
                            stg.air_comm_usd_blended,
                            stg.air_comm_usd_local,
                            stg.bonus_comm_native,
                            stg.bonus_comm_usd_blended,
                            stg.bonus_comm_usd_local,
                            stg.wholesaler_comm_native,
                            stg.wholesaler_comm_usd_blended,
                            stg.wholesaler_comm_usd_local,
                            stg.air_fee_native,
                            stg.air_fee_usd_blended,
                            stg.air_fee_usd_local,
                            stg.air_fee_tax_native,
                            stg.air_fee_tax_usd_blended,
                            stg.air_fee_tax_usd_local,
                            stg.air_dev_fee_native,
                            stg.air_dev_fee_usd_blended,
                            stg.air_dev_fee_usd_local,
                            stg.ncf_native,
                            stg.ncf_usd_blended,
                            stg.ncf_usd_local,
                            stg.govt_fee_native,
                            stg.govt_fee_usd_blended,
                            stg.govt_fee_usd_local,
                            stg.service_fee_native,
                            stg.service_fee_usd_blended,
                            stg.service_fee_usd_local,
                            stg.fuel_supp_native,
                            stg.fuel_supp_usd_blended,
                            stg.fuel_supp_usd_local,
                            stg.fcd_native,
                            stg.fcd_usd_blended,
                            stg.fcd_usd_local,
                            stg.cpp_rev_native,
                            stg.cpp_rev_usd_blended,
                            stg.cpp_rev_usd_local,
                            stg.cpp_comm_native,
                            stg.cpp_comm_usd_blended,
                            stg.cpp_comm_usd_local,
                            stg.package_rev_native,
                            stg.package_rev_usd_blended,
                            stg.package_rev_usd_local,
                            stg.package_comm_native,
                            stg.package_comm_usd_blended,
                            stg.package_comm_usd_local,
                            stg.land_mods_native,
                            stg.land_mods_usd_blended,
                            stg.land_mods_usd_local,
                            stg.land_mods_fee_native,
                            stg.land_mods_fee_usd_blended,
                            stg.land_mods_fee_usd_local,
                            stg.act_tc_cost_native,
                            stg.act_tc_cost_usd_blended,
                            stg.act_tc_cost_usd_local,
                            stg.fuel_ta_fee_native,
                            stg.fuel_ta_fee_usd_local,
                            stg.std_land_cost_native,
                            stg.std_land_cost_usd_local,
                            stg.act_air_cost_native,
                            stg.act_air_cost_usd_local,
                            stg.obc_usd,
                            stg.obc_native,
                            stg.act_gap_cost_usd,
                            stg.act_gap_cost_native,
                            stg.act_cocktail_cost_usd,
                            stg.act_cocktail_cost_native,
                            stg.act_amenities_cost_usd,
                            stg.act_amenities_cost_native,
                            stg.est_gap_cost_usd,
                            stg.est_gap_cost_native,
                            stg.est_tc_cost_usd,
                            stg.est_tc_cost_native,
                            stg.est_over_comm_usd,
                            stg.est_over_comm_native,
                            stg.tax_margin_usd,
                            stg.tax_margin_native,
                            stg.akt_ship_cost_usd,
                            stg.akt_supp_cost_usd,
                            stg.akt_ncf_usd,
                            stg.akt_tax_rev_usd,
                            stg.akt_tax_cost_usd,
                            stg.act_caso_cost_usd,
                            stg.act_caso_cost_native,
                            stg.act_loy_air_dev_native,
                            stg.act_loy_air_dev_usd_blended,
                            stg.act_loy_air_dev_usd_local,
                            stg.ntr_fin_usd,
                            stg.ntr_sls_usd,
                            stg.ntr_sls_native,
                            stg.fare_native,
                            stg.fare_usd_blended,
                            stg.fare_usd_local,
                            stg.disc_native,
                            stg.disc_usd_blended,
                            stg.disc_usd_local,
                            stg.indep_lower_bed_count,
                            stg.indep_ntr_fin_usd,
                            stg.group_lower_bed_count,
                            stg.group_ntr_fin_usd,
                            stg.crs_lower_bed_count,
                            stg.crs_ntr_fin_usd,
                            stg.far_lower_bed_count,
                            stg.tour_lower_bed_count,
                            stg.tour_ntr_fin_usd,
                            stg.extra_guest_count,
                            stg.single_guest_count,
                            stg.air_guest_count,
                            stg.air_add_on_native,
                            stg.air_add_on_usd_blended,
                            stg.air_add_on_usd_local,
                            stg.air_supp_native,
                            stg.air_supp_usd_blended,
                            stg.air_supp_usd_local,
                            stg.pcd,
                            stg.olbd,
                            stg.npr_usd_blended,
                            stg.net_tour_fare_native,
                            stg.net_tour_fare_usd_blended,
                            stg.net_crs_fare_native,
                            stg.net_crs_fare_usd_blended,
                            stg.net_crs_fare_percent,
                            stg.ntr_fin_native,
                            stg.air_fee_uk_usd,
                            stg.air_fee_uk_native,
                            stg.rtce_guest_count,
                            stg.tmod_comm_native,
                            stg.tmod_comm_usd_blended,
                            stg.tmod_comm_usd_local,
                            stg.port_charges_native,
                            stg.port_charges_usd_blended,
                            stg.port_charges_usd_local,
                            stg.land_charges_native,
                            stg.land_charges_usd_blended,
                            stg.land_charges_usd_local,
                            stg.fuel_ta_fee_usd_blended,
                            stg.land_mods_cost_native,
                            stg.land_mods_cost_usd_blended,
                            stg.land_mods_cost_usd_local,
                            stg.package_fee_native,
                            stg.package_fee_usd_blended,
                            stg.package_fee_usd_local,
                            stg.double_guest_count,
                            stg.share_guest_count,
                            stg.air_fee_base_comm_native,
                            stg.air_fee_base_comm_usd_blended,
                            stg.air_fee_over_comm_native,
                            stg.air_fee_over_comm_usd_blended,
                            stg.fuel_supp_base_comm_native,
                            stg.fuel_supp_base_comm_usd_blend,
                            stg.fuel_supp_over_comm_native,
                            stg.fuel_supp_over_comm_usd_blend,
                            stg.govt_fee_base_comm_native,
                            stg.govt_fee_base_comm_usd_blended,
                            stg.govt_fee_over_comm_native,
                            stg.govt_fee_over_comm_usd_blended,
                            stg.ncf_base_comm_native,
                            stg.ncf_base_comm_usd_blended,
                            stg.ncf_over_comm_native,
                            stg.ncf_over_comm_usd_blended,
                            stg.blended_conv_rate,
                            stg.local_per_base_rate,
                            stg.cost_conv_rate,
                            edw_bkng_cabin_d.row_wid AS bkng_cabin_wid,
                            edw_bkng_channel_d.row_wid AS bkng_channel_wid,
                            edw_bkng_geo_d.row_wid AS bkng_geo_wid,
                            edw_bkng_guest_demog_d.row_wid AS bkng_guest_demog_wid,
                            edw_bkng_mini_d.row_wid AS bkng_mini_wid,
                            edw_bkng_status_d.row_wid AS bkng_status_wid,
                            TO_CHAR(bkg.bkng_open_dt,'YYYYMMDD') AS bkng_open_dt_wid,
                            TO_CHAR(bkg.bkng_confirm_dt,'YYYYMMDD') AS bkng_confirm_dt_wid,
                            TO_CHAR(bkg.bkng_xld_dt,'YYYYMMDD') AS bkng_xld_dt_wid,
                            TO_CHAR(bkg.bkng_party_open_dt,'YYYYMMDD') AS bkng_party_open_dt_wid,
                            TO_CHAR(bkg.bkng_party_xld_dt,'YYYYMMDD') AS bkng_party_xld_dt_wid,
                            TO_CHAR(bkg.deposit_rcvd_dt,'YYYYMMDD') AS bkng_deposit_rcvd_dt_wid,
                            TO_CHAR(bkg.final_pymt_rcvd_dt,'YYYYMMDD') AS final_pymt_rcvd_dt_wid,
                            TO_CHAR(bkg.air_fee_uk_pymt_dt,'YYYYMMDD') AS air_fee_uk_pymt_dt_wid,
                            TO_CHAR(bkg.balance_due_dt,'YYYYMMDD') AS balance_due_dt_wid,
                            TO_CHAR(bkg.call_back_dt,'YYYYMMDD') AS call_back_dt_wid,
                            TO_CHAR(bkg.option_dt,'YYYYMMDD') AS option_dt_wid,
                            TO_CHAR(bkg.cpp_final_pymt_dt,'YYYYMMDD') AS cpp_final_pymt_dt_wid                      
                      FROM 
                           plr_bkng_fs stg,
                           plr_bkng_ds bkg,
                           edw_voyage_d voy,
                           edw_agency_d agy,
                           edw_bkng_status_d,
                           edw_bkng_geo_d,
                           edw_bkng_guest_demog_d,
                           edw_bkng_channel_d,
                           edw_bkng_mini_d,
                           edw_bkng_cabin_d,                 
--                           (SELECT DISTINCT bkng_id, guest_wid
--                              FROM edw_bkng_cruise_hist_f
--                             WHERE guest_wid > 0) pax
                           edw_bkng_cruise_hist_f pax
                     WHERE 
                               stg.bkng_unique_id = bkg.bkng_unique_id
                        AND stg.voyage_unique_id = voy.voyage_unique_id
                        AND stg.agency_unique_id = agy.agency_unique_id
                        AND NVL(bkg.category_cd, 'x') = NVL(edw_bkng_cabin_d.category_cd, 'x')
                        AND NVL(bkg.meta_cd, 'x') = NVL(edw_bkng_cabin_d.meta_cd, 'x')
                        AND NVL(bkg.sub_meta_cd, 'x') = NVL(edw_bkng_cabin_d.sub_meta_cd, 'x')
                        AND NVL(bkg.apo_alt_cd, 'x') = NVL(edw_bkng_cabin_d.apo_alt_cd, 'x')
                        AND NVL(bkg.apo_cd, 'x') = NVL(edw_bkng_cabin_d.apo_cd, 'x')
                        AND NVL(bkg.apo_meta_cd, 'x') = NVL(edw_bkng_cabin_d.apo_meta_cd, 'x')
                        AND NVL(bkg.apo_sub_meta_cd, 'x') = NVL(edw_bkng_cabin_d.apo_sub_meta_cd, 'x')
                        AND NVL(bkg.cabin_use_cd, 'x') = NVL(edw_bkng_cabin_d.cabin_use_cd, 'x')
                        AND NVL(bkg.meta_long_desc, 'x') = NVL(edw_bkng_cabin_d.meta_long_desc, 'x')
                        AND NVL(bkg.bkng_channel, 'x') = NVL(edw_bkng_channel_d.bkng_channel, 'x')
                        AND NVL(bkg.bkng_source, 'x') = NVL(edw_bkng_channel_d.bkng_source, 'x')
                        AND NVL(bkg.bkng_type, 'x') = NVL(edw_bkng_channel_d.bkng_type, 'x')
                        AND NVL(bkg.channel_type, 'x') = NVL(edw_bkng_channel_d.channel_type, 'x')
                        AND NVL(bkg.bkng_sub_origin_cd, 'x') = NVL(edw_bkng_channel_d.bkng_sub_origin_cd, 'x')
                        AND NVL(bkg.bkng_origin_cd, 'x') = NVL(edw_bkng_channel_d.bkng_origin_cd, 'x')
                        AND NVL(bkg.fare_type, 'x') = NVL(edw_bkng_channel_d.fare_type, 'x')
                        AND NVL(bkg.air_city, 'x') = NVL(edw_bkng_geo_d.air_city, 'x')
                        AND NVL(bkg.primary_source, 'x') = NVL(edw_bkng_geo_d.primary_source, 'x')
                        AND NVL(bkg.secondary_source, 'x') = NVL(edw_bkng_geo_d.secondary_source, 'x')
                        AND NVL(bkg.intl_source, 'x') = NVL(edw_bkng_geo_d.intl_source, 'x')
                        AND NVL(bkg.air_home_city, 'x') = NVL(edw_bkng_geo_d.air_home_city, 'x')
                        AND NVL(bkg.air_cntry, 'x') = NVL(edw_bkng_geo_d.air_cntry, 'x')
                        AND NVL(bkg.third_source, 'x') = NVL(edw_bkng_geo_d.third_source, 'x')
                        AND NVL(bkg.nationality, 'x') = NVL(edw_bkng_geo_d.nationality, 'x')
                        AND NVL(bkg.home_cntry, 'x') = NVL(edw_bkng_geo_d.home_cntry, 'x')
                        AND NVL(bkg.home_state, 'x') = NVL(edw_bkng_geo_d.home_state, 'x')
                        AND NVL(bkg.direct_home_state, 'x') = NVL(edw_bkng_geo_d.direct_home_state, 'x')
                        AND NVL(bkg.age, 0) = NVL(edw_bkng_guest_demog_d.age, 0)
                        AND NVL(bkg.gender_cd, 'x') = NVL(edw_bkng_guest_demog_d.gender_cd, 'x')
                        AND NVL(bkg.activate_cd, 'x') = NVL(edw_bkng_mini_d.activate_cd, 'x')
                        AND NVL(bkg.cpp_cd, 'x') = NVL(edw_bkng_mini_d.cpp_cd, 'x')
                        AND NVL(bkg.concession_cd, 'x') = NVL(edw_bkng_mini_d.concession_cd, 'x')
                        AND NVL(bkg.direct_flg, 'x') = NVL(edw_bkng_mini_d.direct_flg, 'x')
                        AND NVL(bkg.bkng_xld_cd, 'x') = NVL(edw_bkng_mini_d.bkng_xld_cd, 'x')
                        AND NVL(bkg.air_flg, 'x') = NVL(edw_bkng_mini_d.air_flg, 'x')
                        AND NVL(bkg.currency_cd, 'x') = NVL(edw_bkng_mini_d.currency_cd, 'x')
                        AND NVL(bkg.promo_comm_cd, 'x') = NVL(edw_bkng_mini_d.promo_comm_cd, 'x')
                        AND NVL(bkg.non_intl_party_flg, 9) = NVL(edw_bkng_mini_d.non_intl_party_flg, 9)
                        AND NVL(bkg.no_home_cntry_flg, 9) = NVL(edw_bkng_mini_d.no_home_cntry_flg, 9)
                        AND NVL(bkg.bkng_status_cd, 'x') = NVL(edw_bkng_status_d.bkng_status_cd, 'x')
                        AND NVL(bkg.bkng_party_status_cd, 'x') = NVL(edw_bkng_status_d.bkng_party_status_cd, 'x')                
                        AND stg.bkng_id = pax.bkng_id(+) ) src
      ON (tgt.bkng_unique_id = src.bkng_unique_id)                    
      WHEN MATCHED THEN
         UPDATE SET
                        tgt.bkng_id = src.bkng_id,
                        tgt.bkng_nbr = src.bkng_nbr,
                        tgt.bkng_party_nbr = src.bkng_party_nbr,
                        tgt.voyage_wid = src.voyage_wid,
                        tgt.agency_wid = src.agency_wid,
                        tgt.group_wid = src.group_wid,
                        tgt.guest_wid = src.guest_wid,
                        tgt.guest_count = src.guest_count,
                        tgt.guest_count_qualified = src.guest_count_qualified,
                        tgt.lower_bed_count = src.lower_bed_count,
                        tgt.lower_bed_count_alt = src.lower_bed_count_alt,
                        tgt.gross_rev_native = src.gross_rev_native,
                        tgt.gross_rev_usd_blended = src.gross_rev_usd_blended,
                        tgt.gross_rev_usd_local = src.gross_rev_usd_local,
                        tgt.comm_rev_native = src.comm_rev_native,
                        tgt.comm_rev_usd_blended = src.comm_rev_usd_blended,
                        tgt.comm_rev_usd_local = src.comm_rev_usd_local,
                        tgt.ct_rev_native = src.ct_rev_native,
                        tgt.ct_rev_usd_blended = src.ct_rev_usd_blended,
                        tgt.ct_rev_usd_local = src.ct_rev_usd_local,
                        tgt.ct_net_rev_native = src.ct_net_rev_native,
                        tgt.ct_net_rev_usd_blended = src.ct_net_rev_usd_blended,
                        tgt.ct_net_rev_usd_local = src.ct_net_rev_usd_local,
                        tgt.cta_rev_native = src.cta_rev_native,
                        tgt.cta_rev_usd_blended = src.cta_rev_usd_blended,
                        tgt.cta_rev_usd_local = src.cta_rev_usd_local,
                        tgt.cta_rev_usd_qualified = src.cta_rev_usd_qualified,
                        tgt.cta_net_rev_native = src.cta_net_rev_native,
                        tgt.cta_net_rev_usd_blended = src.cta_net_rev_usd_blended,
                        tgt.cta_net_rev_usd_local = src.cta_net_rev_usd_local,
                        tgt.air_rev_native = src.air_rev_native,
                        tgt.air_rev_usd_blended = src.air_rev_usd_blended,
                        tgt.air_rev_usd_local = src.air_rev_usd_local,
                        tgt.comm_native = src.comm_native,
                        tgt.comm_usd_blended = src.comm_usd_blended,
                        tgt.comm_usd_local = src.comm_usd_local,
                        tgt.base_comm_native = src.base_comm_native,
                        tgt.base_comm_usd_blended = src.base_comm_usd_blended,
                        tgt.base_comm_usd_local = src.base_comm_usd_local,
                        tgt.over_comm_native = src.over_comm_native,
                        tgt.over_comm_usd_blended = src.over_comm_usd_blended,
                        tgt.over_comm_usd_local = src.over_comm_usd_local,
                        tgt.air_comm_native = src.air_comm_native,
                        tgt.air_comm_usd_blended = src.air_comm_usd_blended,
                        tgt.air_comm_usd_local = src.air_comm_usd_local,
                        tgt.bonus_comm_native = src.bonus_comm_native,
                        tgt.bonus_comm_usd_blended = src.bonus_comm_usd_blended,
                        tgt.bonus_comm_usd_local = src.bonus_comm_usd_local,
                        tgt.wholesaler_comm_native = src.wholesaler_comm_native,
                        tgt.wholesaler_comm_usd_blended = src.wholesaler_comm_usd_blended,
                        tgt.wholesaler_comm_usd_local = src.wholesaler_comm_usd_local,
                        tgt.air_fee_native = src.air_fee_native,
                        tgt.air_fee_usd_blended = src.air_fee_usd_blended,
                        tgt.air_fee_usd_local = src.air_fee_usd_local,
                        tgt.air_fee_tax_native = src.air_fee_tax_native,
                        tgt.air_fee_tax_usd_blended = src.air_fee_tax_usd_blended,
                        tgt.air_fee_tax_usd_local = src.air_fee_tax_usd_local,
                        tgt.air_dev_fee_native = src.air_dev_fee_native,
                        tgt.air_dev_fee_usd_blended = src.air_dev_fee_usd_blended,
                        tgt.air_dev_fee_usd_local = src.air_dev_fee_usd_local,
                        tgt.ncf_native = src.ncf_native,
                        tgt.ncf_usd_blended = src.ncf_usd_blended,
                        tgt.ncf_usd_local = src.ncf_usd_local,
                        tgt.govt_fee_native = src.govt_fee_native,
                        tgt.govt_fee_usd_blended = src.govt_fee_usd_blended,
                        tgt.govt_fee_usd_local = src.govt_fee_usd_local,
                        tgt.service_fee_native = src.service_fee_native,
                        tgt.service_fee_usd_blended = src.service_fee_usd_blended,
                        tgt.service_fee_usd_local = src.service_fee_usd_local,
                        tgt.fuel_supp_native = src.fuel_supp_native,
                        tgt.fuel_supp_usd_blended = src.fuel_supp_usd_blended,
                        tgt.fuel_supp_usd_local = src.fuel_supp_usd_local,
                        tgt.fcd_native = src.fcd_native,
                        tgt.fcd_usd_blended = src.fcd_usd_blended,
                        tgt.fcd_usd_local = src.fcd_usd_local,
                        tgt.cpp_rev_native = src.cpp_rev_native,
                        tgt.cpp_rev_usd_blended = src.cpp_rev_usd_blended,
                        tgt.cpp_rev_usd_local = src.cpp_rev_usd_local,
                        tgt.cpp_comm_native = src.cpp_comm_native,
                        tgt.cpp_comm_usd_blended = src.cpp_comm_usd_blended,
                        tgt.cpp_comm_usd_local = src.cpp_comm_usd_local,
                        tgt.package_rev_native = src.package_rev_native,
                        tgt.package_rev_usd_blended = src.package_rev_usd_blended,
                        tgt.package_rev_usd_local = src.package_rev_usd_local,
                        tgt.package_comm_native = src.package_comm_native,
                        tgt.package_comm_usd_blended = src.package_comm_usd_blended,
                        tgt.package_comm_usd_local = src.package_comm_usd_local,
                        tgt.land_mods_native = src.land_mods_native,
                        tgt.land_mods_usd_blended = src.land_mods_usd_blended,
                        tgt.land_mods_usd_local = src.land_mods_usd_local,
                        tgt.land_mods_fee_native = src.land_mods_fee_native,
                        tgt.land_mods_fee_usd_blended = src.land_mods_fee_usd_blended,
                        tgt.land_mods_fee_usd_local = src.land_mods_fee_usd_local,
                        tgt.act_tc_cost_native = src.act_tc_cost_native,
                        tgt.act_tc_cost_usd_blended = src.act_tc_cost_usd_blended,
                        tgt.act_tc_cost_usd_local = src.act_tc_cost_usd_local,
                        tgt.fuel_ta_fee_native = src.fuel_ta_fee_native,
                        tgt.fuel_ta_fee_usd_local = src.fuel_ta_fee_usd_local,
                        tgt.std_land_cost_native = src.std_land_cost_native,
                        tgt.std_land_cost_usd_local = src.std_land_cost_usd_local,
                        tgt.act_air_cost_native = src.act_air_cost_native,
                        tgt.act_air_cost_usd_local = src.act_air_cost_usd_local,
                        tgt.obc_usd = src.obc_usd,
                        tgt.obc_native = src.obc_native,
                        tgt.act_gap_cost_usd = src.act_gap_cost_usd,
                        tgt.act_gap_cost_native = src.act_gap_cost_native,
                        tgt.act_cocktail_cost_usd = src.act_cocktail_cost_usd,
                        tgt.act_cocktail_cost_native = src.act_cocktail_cost_native,
                        tgt.act_amenities_cost_usd = src.act_amenities_cost_usd,
                        tgt.act_amenities_cost_native = src.act_amenities_cost_native,
                        tgt.est_gap_cost_usd = src.est_gap_cost_usd,
                        tgt.est_gap_cost_native = src.est_gap_cost_native,
                        tgt.est_tc_cost_usd = src.est_tc_cost_usd,
                        tgt.est_tc_cost_native = src.est_tc_cost_native,
                        tgt.est_over_comm_usd = src.est_over_comm_usd,
                        tgt.est_over_comm_native = src.est_over_comm_native,
                        tgt.tax_margin_usd = src.tax_margin_usd,
                        tgt.tax_margin_native = src.tax_margin_native,
                        tgt.akt_ship_cost_usd = src.akt_ship_cost_usd,
                        tgt.akt_supp_cost_usd = src.akt_supp_cost_usd,
                        tgt.akt_ncf_usd = src.akt_ncf_usd,
                        tgt.akt_tax_rev_usd = src.akt_tax_rev_usd,
                        tgt.akt_tax_cost_usd = src.akt_tax_cost_usd,
                        tgt.act_caso_cost_usd = src.act_caso_cost_usd,
                        tgt.act_caso_cost_native = src.act_caso_cost_native,
                        tgt.act_loy_air_dev_native = src.act_loy_air_dev_native,
                        tgt.act_loy_air_dev_usd_blended = src.act_loy_air_dev_usd_blended,
                        tgt.act_loy_air_dev_usd_local = src.act_loy_air_dev_usd_local,
                        tgt.ntr_fin_usd = src.ntr_fin_usd,
                        tgt.ntr_sls_usd = src.ntr_sls_usd,
                        tgt.ntr_sls_native = src.ntr_sls_native,
                        tgt.fare_native = src.fare_native,
                        tgt.fare_usd_blended = src.fare_usd_blended,
                        tgt.fare_usd_local = src.fare_usd_local,
                        tgt.disc_native = src.disc_native,
                        tgt.disc_usd_blended = src.disc_usd_blended,
                        tgt.disc_usd_local = src.disc_usd_local,
                        tgt.indep_lower_bed_count = src.indep_lower_bed_count,
                        tgt.indep_ntr_fin_usd = src.indep_ntr_fin_usd,
                        tgt.group_lower_bed_count = src.group_lower_bed_count,
                        tgt.group_ntr_fin_usd = src.group_ntr_fin_usd,
                        tgt.crs_lower_bed_count = src.crs_lower_bed_count,
                        tgt.crs_ntr_fin_usd = src.crs_ntr_fin_usd,
                        tgt.far_lower_bed_count = src.far_lower_bed_count,
                        tgt.tour_lower_bed_count = src.tour_lower_bed_count,
                        tgt.tour_ntr_fin_usd = src.tour_ntr_fin_usd,
                        tgt.extra_guest_count = src.extra_guest_count,
                        tgt.single_guest_count = src.single_guest_count,
                        tgt.air_guest_count = src.air_guest_count,
                        tgt.air_add_on_native = src.air_add_on_native,
                        tgt.air_add_on_usd_blended = src.air_add_on_usd_blended,
                        tgt.air_add_on_usd_local = src.air_add_on_usd_local,
                        tgt.air_supp_native = src.air_supp_native,
                        tgt.air_supp_usd_blended = src.air_supp_usd_blended,
                        tgt.air_supp_usd_local = src.air_supp_usd_local,
                        tgt.pcd = src.pcd,
                        tgt.olbd = src.olbd,
                        tgt.npr_usd_blended = src.npr_usd_blended,
                        tgt.net_tour_fare_native = src.net_tour_fare_native,
                        tgt.net_tour_fare_usd_blended = src.net_tour_fare_usd_blended,
                        tgt.net_crs_fare_native = src.net_crs_fare_native,
                        tgt.net_crs_fare_usd_blended = src.net_crs_fare_usd_blended,
                        tgt.net_crs_fare_percent = src.net_crs_fare_percent,
                        tgt.ntr_fin_native = src.ntr_fin_native,
                        tgt.air_fee_uk_usd = src.air_fee_uk_usd,
                        tgt.air_fee_uk_native = src.air_fee_uk_native,
                        tgt.rtce_guest_count = src.rtce_guest_count,
                        tgt.tmod_comm_native = src.tmod_comm_native,
                        tgt.tmod_comm_usd_blended = src.tmod_comm_usd_blended,
                        tgt.tmod_comm_usd_local = src.tmod_comm_usd_local,
                        tgt.port_charges_native = src.port_charges_native,
                        tgt.port_charges_usd_blended = src.port_charges_usd_blended,
                        tgt.port_charges_usd_local = src.port_charges_usd_local,
                        tgt.land_charges_native = src.land_charges_native,
                        tgt.land_charges_usd_blended = src.land_charges_usd_blended,
                        tgt.land_charges_usd_local = src.land_charges_usd_local,
                        tgt.fuel_ta_fee_usd_blended = src.fuel_ta_fee_usd_blended,
                        tgt.land_mods_cost_native = src.land_mods_cost_native,
                        tgt.land_mods_cost_usd_blended = src.land_mods_cost_usd_blended,
                        tgt.land_mods_cost_usd_local = src.land_mods_cost_usd_local,
                        tgt.package_fee_native = src.package_fee_native,
                        tgt.package_fee_usd_blended = src.package_fee_usd_blended,
                        tgt.package_fee_usd_local = src.package_fee_usd_local,
                        tgt.double_guest_count = src.double_guest_count,
                        tgt.share_guest_count = src.share_guest_count,
                        tgt.air_fee_base_comm_native = src.air_fee_base_comm_native,
                        tgt.air_fee_base_comm_usd_blended = src.air_fee_base_comm_usd_blended,
                        tgt.air_fee_over_comm_native = src.air_fee_over_comm_native,
                        tgt.air_fee_over_comm_usd_blended = src.air_fee_over_comm_usd_blended,
                        tgt.fuel_supp_base_comm_native = src.fuel_supp_base_comm_native,
                        tgt.fuel_supp_base_comm_usd_blend = src.fuel_supp_base_comm_usd_blend,
                        tgt.fuel_supp_over_comm_native = src.fuel_supp_over_comm_native,
                        tgt.fuel_supp_over_comm_usd_blend = src.fuel_supp_over_comm_usd_blend,
                        tgt.govt_fee_base_comm_native = src.govt_fee_base_comm_native,
                        tgt.govt_fee_base_comm_usd_blended = src.govt_fee_base_comm_usd_blended,
                        tgt.govt_fee_over_comm_native = src.govt_fee_over_comm_native,
                        tgt.govt_fee_over_comm_usd_blended = src.govt_fee_over_comm_usd_blended,
                        tgt.ncf_base_comm_native = src.ncf_base_comm_native,
                        tgt.ncf_base_comm_usd_blended = src.ncf_base_comm_usd_blended,
                        tgt.ncf_over_comm_native = src.ncf_over_comm_native,
                        tgt.ncf_over_comm_usd_blended = src.ncf_over_comm_usd_blended,
                        tgt.blended_conv_rate = src.blended_conv_rate,
                        tgt.local_per_base_rate = src.local_per_base_rate,
                        tgt.cost_conv_rate = src.cost_conv_rate,                                                
                        tgt.bkng_cabin_wid = src.bkng_cabin_wid,
                        tgt.bkng_channel_wid = src.bkng_channel_wid,
                        tgt.bkng_geo_wid = src.bkng_geo_wid,
                        tgt.bkng_guest_demog_wid = src.bkng_guest_demog_wid,
                        tgt.bkng_mini_wid = src.bkng_mini_wid,
                        tgt.bkng_status_wid = src.bkng_status_wid,
                        tgt.bkng_open_dt_wid = src.bkng_open_dt_wid,
                        tgt.bkng_confirm_dt_wid = src.bkng_confirm_dt_wid,
                        tgt.bkng_xld_dt_wid = src.bkng_xld_dt_wid,
                        tgt.bkng_party_open_dt_wid = src.bkng_party_open_dt_wid,
                        tgt.bkng_party_xld_dt_wid = src.bkng_party_xld_dt_wid,
                        tgt.bkng_deposit_rcvd_dt_wid = src.bkng_deposit_rcvd_dt_wid,
                        tgt.final_pymt_rcvd_dt_wid = src.final_pymt_rcvd_dt_wid,
                        tgt.air_fee_uk_pymt_dt_wid = src.air_fee_uk_pymt_dt_wid,
                        tgt.balance_due_dt_wid = src.balance_due_dt_wid,
                        tgt.call_back_dt_wid = src.call_back_dt_wid,
                        tgt.option_dt_wid = src.option_dt_wid,
                        tgt.cpp_final_pymt_dt_wid = src.cpp_final_pymt_dt_wid                       
      WHEN NOT MATCHED THEN          
         INSERT (  tgt.bkng_unique_id,
                        tgt.bkng_id,
                        tgt.bkng_nbr,
                        tgt.bkng_party_nbr,
                        tgt.voyage_wid,
                        tgt.agency_wid,
                        tgt.group_wid,
                        tgt.guest_wid,
                        tgt.guest_count,
                        tgt.guest_count_qualified,
                        tgt.lower_bed_count,
                        tgt.lower_bed_count_alt,
                        tgt.gross_rev_native,
                        tgt.gross_rev_usd_blended,
                        tgt.gross_rev_usd_local,
                        tgt.comm_rev_native,
                        tgt.comm_rev_usd_blended,
                        tgt.comm_rev_usd_local,
                        tgt.ct_rev_native,
                        tgt.ct_rev_usd_blended,
                        tgt.ct_rev_usd_local,
                        tgt.ct_net_rev_native,
                        tgt.ct_net_rev_usd_blended,
                        tgt.ct_net_rev_usd_local,
                        tgt.cta_rev_native,
                        tgt.cta_rev_usd_blended,
                        tgt.cta_rev_usd_local,
                        tgt.cta_rev_usd_qualified,
                        tgt.cta_net_rev_native,
                        tgt.cta_net_rev_usd_blended,
                        tgt.cta_net_rev_usd_local,
                        tgt.air_rev_native,
                        tgt.air_rev_usd_blended,
                        tgt.air_rev_usd_local,
                        tgt.comm_native,
                        tgt.comm_usd_blended,
                        tgt.comm_usd_local,
                        tgt.base_comm_native,
                        tgt.base_comm_usd_blended,
                        tgt.base_comm_usd_local,
                        tgt.over_comm_native,
                        tgt.over_comm_usd_blended,
                        tgt.over_comm_usd_local,
                        tgt.air_comm_native,
                        tgt.air_comm_usd_blended,
                        tgt.air_comm_usd_local,
                        tgt.bonus_comm_native,
                        tgt.bonus_comm_usd_blended,
                        tgt.bonus_comm_usd_local,
                        tgt.wholesaler_comm_native,
                        tgt.wholesaler_comm_usd_blended,
                        tgt.wholesaler_comm_usd_local,
                        tgt.air_fee_native,
                        tgt.air_fee_usd_blended,
                        tgt.air_fee_usd_local,
                        tgt.air_fee_tax_native,
                        tgt.air_fee_tax_usd_blended,
                        tgt.air_fee_tax_usd_local,
                        tgt.air_dev_fee_native,
                        tgt.air_dev_fee_usd_blended,
                        tgt.air_dev_fee_usd_local,
                        tgt.ncf_native,
                        tgt.ncf_usd_blended,
                        tgt.ncf_usd_local,
                        tgt.govt_fee_native,
                        tgt.govt_fee_usd_blended,
                        tgt.govt_fee_usd_local,
                        tgt.service_fee_native,
                        tgt.service_fee_usd_blended,
                        tgt.service_fee_usd_local,
                        tgt.fuel_supp_native,
                        tgt.fuel_supp_usd_blended,
                        tgt.fuel_supp_usd_local,
                        tgt.fcd_native,
                        tgt.fcd_usd_blended,
                        tgt.fcd_usd_local,
                        tgt.cpp_rev_native,
                        tgt.cpp_rev_usd_blended,
                        tgt.cpp_rev_usd_local,
                        tgt.cpp_comm_native,
                        tgt.cpp_comm_usd_blended,
                        tgt.cpp_comm_usd_local,
                        tgt.package_rev_native,
                        tgt.package_rev_usd_blended,
                        tgt.package_rev_usd_local,
                        tgt.package_comm_native,
                        tgt.package_comm_usd_blended,
                        tgt.package_comm_usd_local,
                        tgt.land_mods_native,
                        tgt.land_mods_usd_blended,
                        tgt.land_mods_usd_local,
                        tgt.land_mods_fee_native,
                        tgt.land_mods_fee_usd_blended,
                        tgt.land_mods_fee_usd_local,
                        tgt.act_tc_cost_native,
                        tgt.act_tc_cost_usd_blended,
                        tgt.act_tc_cost_usd_local,
                        tgt.fuel_ta_fee_native,
                        tgt.fuel_ta_fee_usd_local,
                        tgt.std_land_cost_native,
                        tgt.std_land_cost_usd_local,
                        tgt.act_air_cost_native,
                        tgt.act_air_cost_usd_local,
                        tgt.obc_usd,
                        tgt.obc_native,
                        tgt.act_gap_cost_usd,
                        tgt.act_gap_cost_native,
                        tgt.act_cocktail_cost_usd,
                        tgt.act_cocktail_cost_native,
                        tgt.act_amenities_cost_usd,
                        tgt.act_amenities_cost_native,
                        tgt.est_gap_cost_usd,
                        tgt.est_gap_cost_native,
                        tgt.est_tc_cost_usd,
                        tgt.est_tc_cost_native,
                        tgt.est_over_comm_usd,
                        tgt.est_over_comm_native,
                        tgt.tax_margin_usd,
                        tgt.tax_margin_native,
                        tgt.akt_ship_cost_usd,
                        tgt.akt_supp_cost_usd,
                        tgt.akt_ncf_usd,
                        tgt.akt_tax_rev_usd,
                        tgt.akt_tax_cost_usd,
                        tgt.act_caso_cost_usd,
                        tgt.act_caso_cost_native,
                        tgt.act_loy_air_dev_native,
                        tgt.act_loy_air_dev_usd_blended,
                        tgt.act_loy_air_dev_usd_local,
                        tgt.ntr_fin_usd,
                        tgt.ntr_sls_usd,
                        tgt.ntr_sls_native,
                        tgt.fare_native,
                        tgt.fare_usd_blended,
                        tgt.fare_usd_local,
                        tgt.disc_native,
                        tgt.disc_usd_blended,
                        tgt.disc_usd_local,
                        tgt.indep_lower_bed_count,
                        tgt.indep_ntr_fin_usd,
                        tgt.group_lower_bed_count,
                        tgt.group_ntr_fin_usd,
                        tgt.crs_lower_bed_count,
                        tgt.crs_ntr_fin_usd,
                        tgt.far_lower_bed_count,
                        tgt.tour_lower_bed_count,
                        tgt.tour_ntr_fin_usd,
                        tgt.extra_guest_count,
                        tgt.single_guest_count,
                        tgt.air_guest_count,
                        tgt.air_add_on_native,
                        tgt.air_add_on_usd_blended,
                        tgt.air_add_on_usd_local,
                        tgt.air_supp_native,
                        tgt.air_supp_usd_blended,
                        tgt.air_supp_usd_local,
                        tgt.pcd,
                        tgt.olbd,
                        tgt.npr_usd_blended,
                        tgt.net_tour_fare_native,
                        tgt.net_tour_fare_usd_blended,
                        tgt.net_crs_fare_native,
                        tgt.net_crs_fare_usd_blended,
                        tgt.net_crs_fare_percent,
                        tgt.ntr_fin_native,
                        tgt.air_fee_uk_usd,
                        tgt.air_fee_uk_native,
                        tgt.rtce_guest_count,
                        tgt.tmod_comm_native,
                        tgt.tmod_comm_usd_blended,
                        tgt.tmod_comm_usd_local,
                        tgt.port_charges_native,
                        tgt.port_charges_usd_blended,
                        tgt.port_charges_usd_local,
                        tgt.land_charges_native,
                        tgt.land_charges_usd_blended,
                        tgt.land_charges_usd_local,
                        tgt.fuel_ta_fee_usd_blended,
                        tgt.land_mods_cost_native,
                        tgt.land_mods_cost_usd_blended,
                        tgt.land_mods_cost_usd_local,
                        tgt.package_fee_native,
                        tgt.package_fee_usd_blended,
                        tgt.package_fee_usd_local,
                        tgt.double_guest_count,
                        tgt.share_guest_count,
                        tgt.air_fee_base_comm_native,
                        tgt.air_fee_base_comm_usd_blended,
                        tgt.air_fee_over_comm_native,
                        tgt.air_fee_over_comm_usd_blended,
                        tgt.fuel_supp_base_comm_native,
                        tgt.fuel_supp_base_comm_usd_blend,
                        tgt.fuel_supp_over_comm_native,
                        tgt.fuel_supp_over_comm_usd_blend,
                        tgt.govt_fee_base_comm_native,
                        tgt.govt_fee_base_comm_usd_blended,
                        tgt.govt_fee_over_comm_native,
                        tgt.govt_fee_over_comm_usd_blended,
                        tgt.ncf_base_comm_native,
                        tgt.ncf_base_comm_usd_blended,
                        tgt.ncf_over_comm_native,
                        tgt.ncf_over_comm_usd_blended,
                        tgt.blended_conv_rate,
                        tgt.local_per_base_rate,
                        tgt.cost_conv_rate,  
                        tgt.bkng_cabin_wid,
                        tgt.bkng_channel_wid,
                        tgt.bkng_geo_wid,
                        tgt.bkng_guest_demog_wid,
                        tgt.bkng_mini_wid,
                        tgt.bkng_status_wid,
                        tgt.bkng_open_dt_wid,
                        tgt.bkng_confirm_dt_wid,
                        tgt.bkng_xld_dt_wid,
                        tgt.bkng_party_open_dt_wid,
                        tgt.bkng_party_xld_dt_wid,
                        tgt.bkng_deposit_rcvd_dt_wid,
                        tgt.final_pymt_rcvd_dt_wid,
                        tgt.air_fee_uk_pymt_dt_wid,
                        tgt.balance_due_dt_wid,
                        tgt.call_back_dt_wid,
                        tgt.option_dt_wid,
                        tgt.cpp_final_pymt_dt_wid)                       
         VALUES ( src.bkng_unique_id,
                        src.bkng_id,
                        src.bkng_nbr,
                        src.bkng_party_nbr,
                        src.voyage_wid,
                        src.agency_wid,
                        src.group_wid,
                        src.guest_wid,
                        src.guest_count,
                        src.guest_count_qualified,
                        src.lower_bed_count,
                        src.lower_bed_count_alt,
                        src.gross_rev_native,
                        src.gross_rev_usd_blended,
                        src.gross_rev_usd_local,
                        src.comm_rev_native,
                        src.comm_rev_usd_blended,
                        src.comm_rev_usd_local,
                        src.ct_rev_native,
                        src.ct_rev_usd_blended,
                        src.ct_rev_usd_local,
                        src.ct_net_rev_native,
                        src.ct_net_rev_usd_blended,
                        src.ct_net_rev_usd_local,
                        src.cta_rev_native,
                        src.cta_rev_usd_blended,
                        src.cta_rev_usd_local,
                        src.cta_rev_usd_qualified,
                        src.cta_net_rev_native,
                        src.cta_net_rev_usd_blended,
                        src.cta_net_rev_usd_local,
                        src.air_rev_native,
                        src.air_rev_usd_blended,
                        src.air_rev_usd_local,
                        src.comm_native,
                        src.comm_usd_blended,
                        src.comm_usd_local,
                        src.base_comm_native,
                        src.base_comm_usd_blended,
                        src.base_comm_usd_local,
                        src.over_comm_native,
                        src.over_comm_usd_blended,
                        src.over_comm_usd_local,
                        src.air_comm_native,
                        src.air_comm_usd_blended,
                        src.air_comm_usd_local,
                        src.bonus_comm_native,
                        src.bonus_comm_usd_blended,
                        src.bonus_comm_usd_local,
                        src.wholesaler_comm_native,
                        src.wholesaler_comm_usd_blended,
                        src.wholesaler_comm_usd_local,
                        src.air_fee_native,
                        src.air_fee_usd_blended,
                        src.air_fee_usd_local,
                        src.air_fee_tax_native,
                        src.air_fee_tax_usd_blended,
                        src.air_fee_tax_usd_local,
                        src.air_dev_fee_native,
                        src.air_dev_fee_usd_blended,
                        src.air_dev_fee_usd_local,
                        src.ncf_native,
                        src.ncf_usd_blended,
                        src.ncf_usd_local,
                        src.govt_fee_native,
                        src.govt_fee_usd_blended,
                        src.govt_fee_usd_local,
                        src.service_fee_native,
                        src.service_fee_usd_blended,
                        src.service_fee_usd_local,
                        src.fuel_supp_native,
                        src.fuel_supp_usd_blended,
                        src.fuel_supp_usd_local,
                        src.fcd_native,
                        src.fcd_usd_blended,
                        src.fcd_usd_local,
                        src.cpp_rev_native,
                        src.cpp_rev_usd_blended,
                        src.cpp_rev_usd_local,
                        src.cpp_comm_native,
                        src.cpp_comm_usd_blended,
                        src.cpp_comm_usd_local,
                        src.package_rev_native,
                        src.package_rev_usd_blended,
                        src.package_rev_usd_local,
                        src.package_comm_native,
                        src.package_comm_usd_blended,
                        src.package_comm_usd_local,
                        src.land_mods_native,
                        src.land_mods_usd_blended,
                        src.land_mods_usd_local,
                        src.land_mods_fee_native,
                        src.land_mods_fee_usd_blended,
                        src.land_mods_fee_usd_local,
                        src.act_tc_cost_native,
                        src.act_tc_cost_usd_blended,
                        src.act_tc_cost_usd_local,
                        src.fuel_ta_fee_native,
                        src.fuel_ta_fee_usd_local,
                        src.std_land_cost_native,
                        src.std_land_cost_usd_local,
                        src.act_air_cost_native,
                        src.act_air_cost_usd_local,
                        src.obc_usd,
                        src.obc_native,
                        src.act_gap_cost_usd,
                        src.act_gap_cost_native,
                        src.act_cocktail_cost_usd,
                        src.act_cocktail_cost_native,
                        src.act_amenities_cost_usd,
                        src.act_amenities_cost_native,
                        src.est_gap_cost_usd,
                        src.est_gap_cost_native,
                        src.est_tc_cost_usd,
                        src.est_tc_cost_native,
                        src.est_over_comm_usd,
                        src.est_over_comm_native,
                        src.tax_margin_usd,
                        src.tax_margin_native,
                        src.akt_ship_cost_usd,
                        src.akt_supp_cost_usd,
                        src.akt_ncf_usd,
                        src.akt_tax_rev_usd,
                        src.akt_tax_cost_usd,
                        src.act_caso_cost_usd,
                        src.act_caso_cost_native,
                        src.act_loy_air_dev_native,
                        src.act_loy_air_dev_usd_blended,
                        src.act_loy_air_dev_usd_local,
                        src.ntr_fin_usd,
                        src.ntr_sls_usd,
                        src.ntr_sls_native,
                        src.fare_native,
                        src.fare_usd_blended,
                        src.fare_usd_local,
                        src.disc_native,
                        src.disc_usd_blended,
                        src.disc_usd_local,
                        src.indep_lower_bed_count,
                        src.indep_ntr_fin_usd,
                        src.group_lower_bed_count,
                        src.group_ntr_fin_usd,
                        src.crs_lower_bed_count,
                        src.crs_ntr_fin_usd,
                        src.far_lower_bed_count,
                        src.tour_lower_bed_count,
                        src.tour_ntr_fin_usd,
                        src.extra_guest_count,
                        src.single_guest_count,
                        src.air_guest_count,
                        src.air_add_on_native,
                        src.air_add_on_usd_blended,
                        src.air_add_on_usd_local,
                        src.air_supp_native,
                        src.air_supp_usd_blended,
                        src.air_supp_usd_local,
                        src.pcd,
                        src.olbd,
                        src.npr_usd_blended,
                        src.net_tour_fare_native,
                        src.net_tour_fare_usd_blended,
                        src.net_crs_fare_native,
                        src.net_crs_fare_usd_blended,
                        src.net_crs_fare_percent,
                        src.ntr_fin_native,
                        src.air_fee_uk_usd,
                        src.air_fee_uk_native,
                        src.rtce_guest_count,
                        src.tmod_comm_native,
                        src.tmod_comm_usd_blended,
                        src.tmod_comm_usd_local,
                        src.port_charges_native,
                        src.port_charges_usd_blended,
                        src.port_charges_usd_local,
                        src.land_charges_native,
                        src.land_charges_usd_blended,
                        src.land_charges_usd_local,
                        src.fuel_ta_fee_usd_blended,
                        src.land_mods_cost_native,
                        src.land_mods_cost_usd_blended,
                        src.land_mods_cost_usd_local,
                        src.package_fee_native,
                        src.package_fee_usd_blended,
                        src.package_fee_usd_local,
                        src.double_guest_count,
                        src.share_guest_count,
                        src.air_fee_base_comm_native,
                        src.air_fee_base_comm_usd_blended,
                        src.air_fee_over_comm_native,
                        src.air_fee_over_comm_usd_blended,
                        src.fuel_supp_base_comm_native,
                        src.fuel_supp_base_comm_usd_blend,
                        src.fuel_supp_over_comm_native,
                        src.fuel_supp_over_comm_usd_blend,
                        src.govt_fee_base_comm_native,
                        src.govt_fee_base_comm_usd_blended,
                        src.govt_fee_over_comm_native,
                        src.govt_fee_over_comm_usd_blended,
                        src.ncf_base_comm_native,
                        src.ncf_base_comm_usd_blended,
                        src.ncf_over_comm_native,
                        src.ncf_over_comm_usd_blended,
                        src.blended_conv_rate,
                        src.local_per_base_rate,
                        src.cost_conv_rate,                          
                        src.bkng_cabin_wid,
                        src.bkng_channel_wid,
                        src.bkng_geo_wid,
                        src.bkng_guest_demog_wid,
                        src.bkng_mini_wid,
                        src.bkng_status_wid,
                        src.bkng_open_dt_wid,
                        src.bkng_confirm_dt_wid,
                        src.bkng_xld_dt_wid,
                        src.bkng_party_open_dt_wid,
                        src.bkng_party_xld_dt_wid,
                        src.bkng_deposit_rcvd_dt_wid,
                        src.final_pymt_rcvd_dt_wid,
                        src.air_fee_uk_pymt_dt_wid,
                        src.balance_due_dt_wid,
                        src.call_back_dt_wid,
                        src.option_dt_wid,
                        src.cpp_final_pymt_dt_wid);              
  
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;    
             
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_BKNG_F'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_FACT_LOAD;

   PROCEDURE BKNG_PII_STG_LOAD IS
   /***************************************************************************
   NAME: BKNG_PII_STG_LOAD
       
   PURPOSE: 
      Staging table load for EDW_BKNG_PII_D 
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_PII_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
       
      EXECUTE IMMEDIATE 'TRUNCATE TABLE plr_bkng_pii_ds';

      INSERT /*+ append */  INTO plr_bkng_pii_ds
      (
        bkng_unique_id,
        bkng_id,
        bkng_nbr,
        bkng_party_nbr,
        group_id,
        home_addr1,
        home_addr2,
        home_addr3,
        home_city,
        home_state,
        home_zip,
        home_cntry_cd,
        home_phone,
        home_cell_phone,
        home_email,
        emrg_name,
        emrg_relation,
        emrg_city,
        emrg_state,
        emrg_cntry_cd,
        emrg_phone_day,
        emrg_phone_eve,
        emrg_email,
        emrg_assist_company,
        emrg_assist_phone,
        birth_dt,
        passport_nbr,
        passport_expire_dt,
        passport_issued_at,
        passport_authority,
        alien_resident_id,
        visa_nbr,
        visa_issue_dt,
        visa_expire_dt,
        visa_issued_at,
        pif_nationality_cd,
        birth_cntry_cd,
        residence_cntry_cd
      )
     SELECT 
            bkng_unique_id,
            bkng_id,
            bkng_nbr,
            bkng_party_nbr,
            group_id,
            home_addr1,
            home_addr2,
            home_addr3,
            home_city,
            home_state,
            home_zip,
            home_cntry_cd,
            home_phone,
            home_cell_phone,
            home_email,
            emrg_name,
            emrg_relation,
            emrg_city,
            emrg_state,
            emrg_cntry_cd,
            emrg_phone_day,
            emrg_phone_eve,
            emrg_email,
            emrg_assist_company,
            emrg_assist_phone,
            birth_dt,
            passport_nbr,
            passport_expire_dt,
            passport_issued_at,
            passport_authority,
            alien_resident_id,
            visa_nbr,
            visa_issue_dt,
            visa_expire_dt,
            visa_issued_at,
            pif_nationality_cd,
            birth_cntry_cd,
            residence_cntry_cd
       FROM 
            dm_tkr.v_edw_bkng_pii_d_daily_dev@stgdwh1_edwro;
--            dm_tkr.v_edw_bkng_pii_d_wkly_dev@stgdwh1_edwro;            
     
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;          
       
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'PLR_BKNG_PII_DS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_PII_STG_LOAD;

   PROCEDURE BKNG_PII_LOAD IS
   /***************************************************************************
   NAME: BKNG_PII_LOAD
       
   PURPOSE: 
      Load for table EDW_BKNG_PII_D  
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_PII_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
     
      --Updates/Inserts
      MERGE INTO edw_bkng_pii_d tgt 
      USING (SELECT 
                            stg.bkng_unique_id,
                            stg.bkng_id,
                            stg.bkng_nbr,
                            stg.bkng_party_nbr,
                            stg.group_id,
                            stg.home_addr1,
                            stg.home_addr2,
                            stg.home_addr3,
                            stg.home_city,
                            stg.home_state,
                            stg.home_zip,
                            stg.home_cntry_cd,
                            stg.home_phone,
                            stg.home_cell_phone,
                            stg.home_email,
                            stg.emrg_name,
                            stg.emrg_relation,
                            stg.emrg_city,
                            stg.emrg_state,
                            stg.emrg_cntry_cd,
                            stg.emrg_phone_day,
                            stg.emrg_phone_eve,
                            stg.emrg_email,
                            stg.emrg_assist_company,
                            stg.emrg_assist_phone,
                            stg.birth_dt,
                            stg.passport_nbr,
                            stg.passport_expire_dt,
                            stg.passport_issued_at,
                            stg.passport_authority,
                            stg.alien_resident_id,
                            stg.visa_nbr,
                            stg.visa_issue_dt,
                            stg.visa_expire_dt,
                            stg.visa_issued_at,
                            stg.pif_nationality_cd,
                            stg.birth_cntry_cd,
                            stg.residence_cntry_cd                     
                    FROM plr_bkng_pii_ds stg ) src
      ON (tgt.bkng_unique_id = src.bkng_unique_id)                    
      WHEN MATCHED THEN
         UPDATE SET
                        tgt.bkng_id = src.bkng_id,
                        tgt.bkng_nbr = src.bkng_nbr,
                        tgt.bkng_party_nbr = src.bkng_party_nbr,
                        tgt.group_id = src.group_id,
                        tgt.home_addr1 = src.home_addr1,
                        tgt.home_addr2 = src.home_addr2,
                        tgt.home_addr3 = src.home_addr3,
                        tgt.home_city = src.home_city,
                        tgt.home_state = src.home_state,
                        tgt.home_zip = src.home_zip,
                        tgt.home_cntry_cd = src.home_cntry_cd,
                        tgt.home_phone = src.home_phone,
                        tgt.home_cell_phone = src.home_cell_phone,
                        tgt.home_email = src.home_email,
                        tgt.emrg_name = src.emrg_name,
                        tgt.emrg_relation = src.emrg_relation,
                        tgt.emrg_city = src.emrg_city,
                        tgt.emrg_state = src.emrg_state,
                        tgt.emrg_cntry_cd = src.emrg_cntry_cd,
                        tgt.emrg_phone_day = src.emrg_phone_day,
                        tgt.emrg_phone_eve = src.emrg_phone_eve,
                        tgt.emrg_email = src.emrg_email,
                        tgt.emrg_assist_company = src.emrg_assist_company,
                        tgt.emrg_assist_phone = src.emrg_assist_phone,
                        tgt.birth_dt = src.birth_dt,
                        tgt.passport_nbr = src.passport_nbr,
                        tgt.passport_expire_dt = src.passport_expire_dt,
                        tgt.passport_issued_at = src.passport_issued_at,
                        tgt.passport_authority = src.passport_authority,
                        tgt.alien_resident_id = src.alien_resident_id,
                        tgt.visa_nbr = src.visa_nbr,
                        tgt.visa_issue_dt = src.visa_issue_dt,
                        tgt.visa_expire_dt = src.visa_expire_dt,
                        tgt.visa_issued_at = src.visa_issued_at,
                        tgt.pif_nationality_cd = src.pif_nationality_cd,
                        tgt.birth_cntry_cd = src.birth_cntry_cd,
                        tgt.residence_cntry_cd = src.residence_cntry_cd
      WHEN NOT MATCHED THEN          
         INSERT (  tgt.bkng_unique_id,
                        tgt.bkng_id,
                        tgt.bkng_nbr,
                        tgt.bkng_party_nbr,
                        tgt.group_id,
                        tgt.home_addr1,
                        tgt.home_addr2,
                        tgt.home_addr3,
                        tgt.home_city,
                        tgt.home_state,
                        tgt.home_zip,
                        tgt.home_cntry_cd,
                        tgt.home_phone,
                        tgt.home_cell_phone,
                        tgt.home_email,
                        tgt.emrg_name,
                        tgt.emrg_relation,
                        tgt.emrg_city,
                        tgt.emrg_state,
                        tgt.emrg_cntry_cd,
                        tgt.emrg_phone_day,
                        tgt.emrg_phone_eve,
                        tgt.emrg_email,
                        tgt.emrg_assist_company,
                        tgt.emrg_assist_phone,
                        tgt.birth_dt,
                        tgt.passport_nbr,
                        tgt.passport_expire_dt,
                        tgt.passport_issued_at,
                        tgt.passport_authority,
                        tgt.alien_resident_id,
                        tgt.visa_nbr,
                        tgt.visa_issue_dt,
                        tgt.visa_expire_dt,
                        tgt.visa_issued_at,
                        tgt.pif_nationality_cd,
                        tgt.birth_cntry_cd,
                        tgt.residence_cntry_cd)                       
         VALUES ( src.bkng_unique_id,
                        src.bkng_id,
                        src.bkng_nbr,
                        src.bkng_party_nbr,
                        src.group_id,
                        src.home_addr1,
                        src.home_addr2,
                        src.home_addr3,
                        src.home_city,
                        src.home_state,
                        src.home_zip,
                        src.home_cntry_cd,
                        src.home_phone,
                        src.home_cell_phone,
                        src.home_email,
                        src.emrg_name,
                        src.emrg_relation,
                        src.emrg_city,
                        src.emrg_state,
                        src.emrg_cntry_cd,
                        src.emrg_phone_day,
                        src.emrg_phone_eve,
                        src.emrg_email,
                        src.emrg_assist_company,
                        src.emrg_assist_phone,
                        src.birth_dt,
                        src.passport_nbr,
                        src.passport_expire_dt,
                        src.passport_issued_at,
                        src.passport_authority,
                        src.alien_resident_id,
                        src.visa_nbr,
                        src.visa_issue_dt,
                        src.visa_expire_dt,
                        src.visa_issued_at,
                        src.pif_nationality_cd,
                        src.birth_cntry_cd,
                        src.residence_cntry_cd);              
  
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_BKNG_PII_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_PII_LOAD;

   PROCEDURE BKNG_PROMO_STG_LOAD IS
   /***************************************************************************
   NAME: BKNG_PROMO_STG_LOAD
       
   PURPOSE: 
      Staging table load for EDW_BKNG_PROMO_D  
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_PROMO_STG_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
       
      EXECUTE IMMEDIATE 'TRUNCATE TABLE plr_bkng_promo_ds';

      INSERT /*+ append */  INTO plr_bkng_promo_ds 
      (
        bkng_unique_id,
        bkng_id,
        bkng_nbr,
        bkng_party_nbr,
        company_cd,
        primary_promo_id,
        primary_promo_desc,
        promo1_id,
        promo2_id,
        promo3_id,
        promo4_id,
        promo5_id,
        promo6_id,
        promo7_id,
        promo8_id,
        promo9_id,
        promo10_id,
        promo11_id,
        promo12_id
      )
     SELECT 
        bkng_unique_id,
        bkng_id,
        bkng_nbr,
        bkng_party_nbr,
        company_cd,
        primary_promo_id,
        primary_promo_desc,
        promo1_id,
        promo2_id,
        promo3_id,
        promo4_id,
        promo5_id,
        promo6_id,
        promo7_id,
        promo8_id,
        promo9_id,
        promo10_id,
        promo11_id,
        promo12_id
       FROM 
            dm_tkr.v_edw_bkng_promo_d_daily_dev@stgdwh1_edwro;
--            dm_tkr.v_edw_bkng_promo_d_wkly_dev@stgdwh1_edwro;            
     
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;          
       
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'PLR_BKNG_PROMO_DS'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_PROMO_STG_LOAD;

   PROCEDURE BKNG_PROMO_LOAD IS
   /***************************************************************************
   NAME: BKNG_PROMO_LOAD
       
   PURPOSE: 
      Load for table EDW_BKNG_PROMO_D   
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/05/2013  REL           Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'BKNG_PROMO_LOAD';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
   v_max_wid     NUMBER;
    
   BEGIN
      -- Log program start 
      IF  EDW.COMMON_JOBS.LOG_JOB (
        io_Batch_ID => vBatchID, 
        in_Schema_Name => cSchema_NM,  
        in_Package_Name => cPackage_NM,
        in_Program_Name => cProgram_NM, 
        in_Prog_Status => 'I',  
        in_Comments  => 'Job in Progress', 
        in_Error_Message   => 'NA' ) != 0                                         
      THEN                  
         RAISE eCALL_FAILED ;
      END IF;           
     
      --Updates/Inserts
      MERGE INTO edw_bkng_promo_d tgt 
      USING (SELECT 
                            stg.bkng_unique_id,
                            stg.bkng_id,
                            stg.bkng_nbr,
                            stg.bkng_party_nbr,
                            stg.company_cd,
                            stg.primary_promo_id,
                            stg.primary_promo_desc,
                            stg.promo1_id,
                            stg.promo2_id,
                            stg.promo3_id,
                            stg.promo4_id,
                            stg.promo5_id,
                            stg.promo6_id,
                            stg.promo7_id,
                            stg.promo8_id,
                            stg.promo9_id,
                            stg.promo10_id,
                            stg.promo11_id,
                            stg.promo12_id                     
                    FROM plr_bkng_promo_ds stg ) src
      ON (tgt.bkng_unique_id = src.bkng_unique_id)                    
      WHEN MATCHED THEN
         UPDATE SET
                        tgt.bkng_id = src.bkng_id,
                        tgt.bkng_nbr = src.bkng_nbr,
                        tgt.bkng_party_nbr = src.bkng_party_nbr,
                        tgt.company_cd = src.company_cd,
                        tgt.primary_promo_id = src.primary_promo_id,
                        tgt.primary_promo_desc = src.primary_promo_desc,
                        tgt.promo1_id = src.promo1_id,
                        tgt.promo2_id = src.promo2_id,
                        tgt.promo3_id = src.promo3_id,
                        tgt.promo4_id = src.promo4_id,
                        tgt.promo5_id = src.promo5_id,
                        tgt.promo6_id = src.promo6_id,
                        tgt.promo7_id = src.promo7_id,
                        tgt.promo8_id = src.promo8_id,
                        tgt.promo9_id = src.promo9_id,
                        tgt.promo10_id = src.promo10_id,
                        tgt.promo11_id = src.promo11_id,
                        tgt.promo12_id = src.promo12_id
      WHEN NOT MATCHED THEN          
         INSERT (  tgt.bkng_unique_id,
                        tgt.bkng_id,
                        tgt.bkng_nbr,
                        tgt.bkng_party_nbr,
                        tgt.company_cd,
                        tgt.primary_promo_id,
                        tgt.primary_promo_desc,
                        tgt.promo1_id,
                        tgt.promo2_id,
                        tgt.promo3_id,
                        tgt.promo4_id,
                        tgt.promo5_id,
                        tgt.promo6_id,
                        tgt.promo7_id,
                        tgt.promo8_id,
                        tgt.promo9_id,
                        tgt.promo10_id,
                        tgt.promo11_id,
                        tgt.promo12_id)                       
         VALUES ( src.bkng_unique_id,
                        src.bkng_id,
                        src.bkng_nbr,
                        src.bkng_party_nbr,
                        src.company_cd,
                        src.primary_promo_id,
                        src.primary_promo_desc,
                        src.promo1_id,
                        src.promo2_id,
                        src.promo3_id,
                        src.promo4_id,
                        src.promo5_id,
                        src.promo6_id,
                        src.promo7_id,
                        src.promo8_id,
                        src.promo9_id,
                        src.promo10_id,
                        src.promo11_id,
                        src.promo12_id);              
  
      vRowCountInserts := SQL%ROWCOUNT;
      
      COMMIT;      
         
      vEndDate := SYSDATE ;  -- set end date to sysdate

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'EDW'
         ,TabName        => 'EDW_BKNG_PROMO_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;   
        
      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF EDW.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF EDW.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF EDW.COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D', 
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCountInserts,
          in_Target_Cnt => vRowCountInserts,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,  
          in_Insert_Count =>  vRowCountInserts,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      

            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_Parm => vStartDate, 
            in_End_DT_Parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := EDW.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := EDW.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_Parm => vStartDate,  
            in_End_DT_Parm => SYSDATE
         );
         RAISE;

END BKNG_PROMO_LOAD;
---------------------------------------------------------------------------------------------------------------------------- 
END  EDW_LOAD_PKG;
/