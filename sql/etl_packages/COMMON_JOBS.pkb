CREATE OR REPLACE PACKAGE BODY EDW.COMMON_JOBS AS
/******************************************************************************
   NAME:       COMMON_JOBS
   PURPOSE: Container for shared procedures

   REVISIONS:
   Ver        Date                Author         Description
   ---------  ----------         --------------  ------------------------------------

******************************************************************************/
   eSQL_FAILED Exception ;
   eCall_FAILED Exception ;
   eErrMsg             Varchar2(500);        
   cSchema_NM     CONSTANT  Varchar2(15) := 'EDW'  ;
   cPackage_NM     CONSTANT Varchar2(50) := 'COMMON_JOBS'  ;
   
   

 FUNCTION LOG_AUDIT_RESULT
/******************************************************************************
   NAME:       LOG_AUDIT_RESULTS
   PURPOSE:  Insert row in PRICWES_AUDIT to record a test result     
      
   PARMS:   
         io_Batch_ID          In Out Number  ,    
        in_Audit_Type       In Varchar2 := NULL ,
        in_Load_Type        In Varchar2 := NULL,
        in_Start_Dat          In  date  := NULL,
        in_End_Date           In  date  := NULL,,
        in_Source_Cnt       In Number     := NULL,
        in_Target_Cnt        In Number     := NULL,
        in_Audit_OK           In Number     := NULL,
        in_Comments         In Varchar2   := NULL,
        in_Error_Message   In Varchar2   := NULL  )

   REVISIONS:
   Ver        Date                Author         Description
   ---------  ----------         --------------  ------------------------------------
   1.0        11/07/2011      HDH       1. Created this function
   1.1        10/18/2012      HDH       1. Add additional columns 
   ******************************************************************************/
      ( in_Batch_ID          In Number  ,    
        in_Audit_Type       In Varchar2,
        in_Load_Type        In Varchar2,
        in_Start_Date          In  date ,
        in_End_Date           In  date  ,
        in_Source_Cnt       In Number   ,
        in_Target_Cnt        In Number  ,
        in_Audit_OK           In Number,
        in_Audit_Comments In Varchar2 ,   
        in_Insert_Count      In Number := NULL  ,  --optional 
        in_Update_Count      In Number := NULL  ,  --optional,  
        in_Delete_Count         In Number := NULL    --optional
         )
    RETURN NUMBER 
    IS 
--            eSQL_FAILED Exception ;
            BatchID Number ;        
            
    BEGIN
                
                            
        Insert Into 
              process_audit
             (  batch_id,              audit_type,        load_type_parm    ,
                start_date_parm,      end_date_parm ,   source_count    ,
                target_count,        audit_ok ,         
                comments ,
                 insert_count   ,  update_count  ,  delete_count
                 )
          Values
              ( in_Batch_ID,       in_Audit_Type,      in_load_Type,   -- Type is daily
                in_Start_Date,     in_End_Date  ,      in_Source_Cnt,
                in_Target_Cnt,     in_Audit_OK,   
                in_Audit_Comments   ,
                in_insert_count   ,  in_update_count  ,  in_delete_count             
              )     ;
              
            -- test sql            
            IF SQL%FOUND = FALSE THEN   
                RAISE eSQL_FAILED ;                                           
            END IF;

                     
        COMMIT ;   
        RETURN 0  ;
             
    EXCEPTION
        WHEN eSQL_FAILED THEN            
            eErrMsg :=  SQLERRM;                                                      
            dbms_output.put_line('common - log audit results failed - Sql Failed');
            dbms_output.put_line(eErrMsg);
            
            ROLLBACK ;                  
            RETURN -1 ;
            
        WHEN OTHERS THEN
           eErrMsg :=  SQLERRM;                                                      
          
        dbms_output.put_line('Common - log audit reuslts - Other Error ');      
        dbms_output.put_line(eErrMsg);
        
            ROLLBACK;     
            RETURN -1 ;
           
                  
    END ;
   
   
   FUNCTION LOG_JOB
/******************************************************************************
   NAME:       LOG_JOB
   PURPOSE:  Record start and finish OR Errors from data warehouse procedures in the PROCESS_STATUS table. 
                 Insert/Update rows in Process_Status . Also grabs PK from Sequence.    
   
   NOTES:   Remember to  pass 0 for a new record   
         !! needed -1  0  key record in table as placeholders for specific errors conditions  else it won't be good !!    
   
   PARMS:   
        io_Batch_ID   0 passed for new inserts, other vals ID rows to update.        
         Other parms are insert / update in target table           
        in_Schema_Name   
        in_Package_Name  
        in_Program_Name  
        in_Prog_Status   
        in_Comments      
        in_Error_Message 

   REVISIONS:
   Ver        Date                Author         Description
   ---------  ----------         --------------  ------------------------------------
   1.0        08/31/2011      HDH       1. Created this package body.
   1.1        10/18/2012      HDH       1. Add additional columns
******************************************************************************/
      ( io_Batch_ID             IN OUT Number  ,
        in_Schema_Name     IN   Varchar2 ,
        in_Package_Name     IN Varchar2,
        in_Program_Name    IN  Varchar2,
        in_Prog_Status          IN Varchar2,
        in_Comments           IN Varchar2  , 
        in_Error_Message     IN Varchar2   )

    RETURN NUMBER 
    IS 
--            eSQL_FAILED Exception ;
            BatchID Number ;        
            
    BEGIN
                
        IF io_Batch_ID = 0 THEN   
        -- Get batch abnd  insert as in Process 
                       
            io_Batch_ID  := Batch_Seq.nextval  ; -- assign new PK for Job 
                        
            Insert Into 
                process_status                  -- Insert w/ In process status
                (   batch_id,  schema_name ,  package_name ,
                    program_name,  program_start_date,   program_end_date ,
                    comments ,  error_message ,  program_status                 )       
            Values    
                (   io_Batch_ID,   in_Schema_Name  ,    in_Package_Name ,              
                    in_Program_Name ,   SYSDATE,  NULL ,    
                    in_Comments   ,  in_Error_Message , in_Prog_Status    );
                    
            -- test sql            
            IF SQL%FOUND = FALSE THEN   
                RAISE eSQL_FAILED ;                                           
            END IF;

            
        ELSE -- Update using io_Batch 
                 
            Update process_status
            Set                 
                program_end_date = SYSDATE ,
                comments =  in_Comments, 
                error_message = in_Error_Message,
                program_status =   in_Prog_Status               
            Where 
                Batch_Id = io_Batch_ID ;                     
                                   
            -- test sql            
            IF SQL%FOUND = FALSE THEN   
                RAISE eSQL_FAILED ;                                           
            END IF;

        END IF ;
         
        COMMIT ;   
        RETURN 0  ;
             
    EXCEPTION
        WHEN eSQL_FAILED THEN
        --dbms_output.put_line('Common - Log Job  - Sql Failed ');
            ROLLBACK ;                  
            RETURN -1 ;
            
        WHEN OTHERS THEN
        --dbms_output.put_line('Common - Log Job - other error ');      
            ROLLBACK;     
            RETURN -1 ;
           
                  
    END ;
        
        
   FUNCTION LOG_PROC_CTRL 
/******************************************************************************
   NAME:       LOG_PROC_CTRL
   PURPOSE:  Record date parameters passed to a data warehouse procedures in the PROCESS_CONTROL table.
                    Used to control date sequence for extract program.  Insert/Update rows in 
                    Process Control table     
   
   NOTES:     Insert or update mode is passed as an  parm  
   
   PARMS:   
        io_Batch_ID   0 passed for new inserts, other vals ID rows to update.
        -- Other parms are valeue to insert / update in target table           
        in_Schema_Name   
        in_Package_Name  
        in_Program_Name  
        in_Prog_Status   
        in_Comments      
        in_Error_Message 

   REVISIONS:
   Ver        Date                Author         Description
   ---------  ----------         --------------  ------------------------------------
   1.0        08/31/2011      HDH       1. Created this package body.
   1.1        10/18/2012      HDH       1. Add additional columns
******************************************************************************/   
      ( in_Batch_ID             IN  Number  ,
        in_Schema_Name     IN   Varchar2 ,
        in_Package_Name     IN Varchar2  ,
        in_Program_Name    IN  Varchar2  ,
        in_Load_Type            IN Varchar2,
        in_Prog_Status          IN Varchar2 ,
        in_Start_DT_Parm     In Date ,
        in_End_DT_Parm     In Date     )
    RETURN NUMBER 
    IS 
            BatchID Number ;        
               
    BEGIN
                           
        -- Get batch and insert as in Process 
                                  
        Insert Into 
              process_control
             (
              schema_name,          package_name ,              program_name ,
              batch_id     ,              type_parm    ,
              start_date_parm ,          end_date_parm   ,              program_status   )
          Values
              (
                in_Schema_Name,    in_Package_Name,           in_Program_Name  ,  
                in_Batch_ID,             in_Load_Type  ,
                in_Start_DT_Parm ,   in_End_DT_Parm   ,      in_Prog_Status
              )     ;
                                                                            
            -- test sql            
            IF SQL%FOUND = FALSE THEN   
                RAISE eSQL_FAILED ;                                           
            END IF;
         
        COMMIT ;   
        RETURN 0  ;
             
    EXCEPTION
        WHEN eSQL_FAILED THEN
        dbms_output.put_line('Common - Log proc ctrl  - Sql Failed');
            ROLLBACK ;                    
            RETURN -1 ;
            
        WHEN OTHERS THEN
            eErrMsg := SQLERRM ;
            dbms_output.put_line('Common - log procedure ctrl  - Other error '||eErrMsg);      
        
            ROLLBACK;     
            RETURN -1 ;
           
                  
    END ;
-----------------------

END COMMON_JOBS;
/