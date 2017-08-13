CREATE OR REPLACE PACKAGE EDW.COMMON_JOBS AS
/******************************************************************************
   NAME:       COMMON_JOBS
   PURPOSE:  Container for shared functions.

   REVISIONS:
   Ver        Date                Author        Description
   ---------  ----------        ---------------  ------------------------------------
******************************************************************************/

  FUNCTION LOG_JOB
      ( io_Batch_ID             IN OUT Number  ,
        in_Schema_Name     IN   Varchar2 := NULL ,
        in_Package_Name     IN Varchar2 := NULL,
        in_Program_Name    IN  Varchar2 := NULL,
        in_Prog_Status          IN Varchar2  ,
        in_Comments           IN Varchar2   := NULL,
        in_Error_Message     IN Varchar2   := NULL  )
    RETURN NUMBER;
        
  FUNCTION LOG_PROC_CTRL 
     (   in_Batch_ID             IN  Number  ,
        in_Schema_Name     IN   Varchar2 ,
        in_Package_Name     IN Varchar2  ,
        in_Program_Name    IN  Varchar2  ,
        in_Load_Type            IN Varchar2,
        in_Prog_Status          IN Varchar2 ,
        in_Start_DT_Parm     In Date ,
        in_End_DT_Parm     In Date   )
    RETURN NUMBER;    
        
    
      FUNCTION LOG_AUDIT_RESULT
      ( in_Batch_ID          In Number  ,    
        in_Audit_Type       In Varchar2 := NULL ,
        in_Load_Type        In Varchar2 := NULL,
        in_Start_Date          In  date  := NULL,
        in_End_Date           In  date  := NULL,
        in_Source_Cnt       In Number     := NULL,
        in_Target_Cnt        In Number     := NULL,
        in_Audit_OK           In Number     := NULL,
        in_Audit_Comments    In Varchar2 := NULL,
        in_Insert_Count      In Number := NULL  ,  --optional 
        in_Update_Count      In Number := NULL  ,  --optional,  
        in_Delete_Count         In Number := NULL    )
    RETURN NUMBER;

    

END COMMON_JOBS;
/