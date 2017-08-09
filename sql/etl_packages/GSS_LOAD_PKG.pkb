CREATE OR REPLACE PACKAGE BODY DM_TKR.GSS_LOAD_PKG AS
/******************************************************************************
   NAME:     GSS_Load_Pkg
   PURPOSE:  Process to load GSS Result

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   
 Coding Conventions 
           ------------------                     
           c_     Constant 
           e_     Error  
           in_    Input               parm 
           out_   Output               parm 
           io_    Input / Output               parm
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
   cPackage_NM   CONSTANT   Varchar2(100) := 'GSS_LOAD_PKG'  ;
   eErrMsg                  Varchar2(500);
   eErrNo                   Number ;
   sComment                 Varchar2(2000) ;      
   eCALL_FAILED   Exception;
   eINCONSISTENCY Exception; 


   PROCEDURE Load_GSS_Result IS
   /***************************************************************************
   NAME: Load_GSS_Result
       
   PURPOSE: 
      Load GSS Results
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     07/12/2013  Venky         Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'LOAD_GSS_RESULT';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
    
   BEGIN
      -- Log program start 
      IF  DM_INT.COMMON_JOBS.LOG_JOB (
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

      -- Call individual procedures
      Load_GSS_Result_Stg;
      Load_GSS_Result_Daily;
      Load_GSS_Question;
      Load_GSS_Response;
                  
      vEndDate := SYSDATE ;  -- set end date to sysdate

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF DM_INT.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_parm  => vStartDate,
         in_End_DT_parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;
            
      EXCEPTION 

      WHEN eINCONSISTENCY THEN                    
         ROLLBACK ;                         
                         
         -- Record Error
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_parm => vStartDate,  
            in_End_DT_parm => SYSDATE
         );
         RAISE;

   END Load_GSS_Result;


   PROCEDURE Load_GSS_Result_Stg IS
   /***************************************************************************
   NAME: Load_GSS_Result_Stg
       
   PURPOSE: 
      Load for table GSS_Result_Stg
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Truncates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/26/2013  Venky         Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'LOAD_GSS_RESULT_STG';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
    
   BEGIN
      -- Log program start 
      IF  DM_INT.COMMON_JOBS.LOG_JOB (
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

      EXECUTE IMMEDIATE 'TRUNCATE TABLE GSS_Result_Stg'; 

      --HAL Inserts
      INSERT into GSS_Result_Stg (
         COMPANY_CODE,
         VOYAGE_ID,
         BKNG_NBR,
         PASSENGER_ID,
         MARINER_ID,
         parm1,
         parm2,
         parm3,
         parm4,
         parm5,
         parm6,
         parm7,
         parm8,
         parm9,
         parm10,
         parm11,
         parm12,
         parm13,
         parm14,
         parm15,
         parm16,
         parm17,
         parm18,
         parm19,
         parm20,
         parm21,
         parm22,
         parm23,
         parm24,
         parm25,
         parm26,
         parm27,
         parm28,
         parm29,
         parm30,
         parm31,
         parm32,
         parm33,
         parm34,
         parm35,
         parm36,
         parm37,
         parm38,
         parm39,
         parm40,
         parm41,
         parm42,
         parm43,
         parm44,
         parm45,
         parm46,
         parm47,
         parm48,
         parm49,
         parm50,
         parm51,
         parm52,
         parm53,
         parm54,
         parm55,
         parm56,
         parm57,
         parm58,
         parm59,
         parm60,
         parm61,
         parm62,
         parm63,
         parm64,
         parm65,
         parm66,
         parm67,
         parm68,
         parm69,
         parm70,
         parm71,
         parm72,
         parm73,
         parm74,
         parm75,
         parm76,
         parm77,
         parm78,
         parm79,
         parm80,
         parm81,
         parm82,
         parm83,
         parm84,
         parm85,
         parm86,
         parm87,
         parm88,
         parm89,
         parm90,
         parm91,
         parm92,
         parm93,
         parm94,
         parm95,
         parm96,
         parm97,
         parm98,
         parm99,
         parm100,
         parm101,
         parm102,
         parm103,
         parm104,
         parm105,
         parm106,
         parm107,
         parm108,
         parm109,
         parm110,
         parm111,
         parm112,
         parm113,
         parm114,
         parm115,
         parm116,
         parm117,
         parm118,
         parm119,
         parm120,
         parm121,
         parm122,
         parm123,
         parm124,
         parm125,
         parm126,
         parm127,
         parm128,
         parm129,
         parm130,
         parm131,
         parm132,
         parm133,
         parm134,
         parm135,
         parm136,
         parm137,
         parm138,
         parm139,
         parm140,
         parm141,
         parm142,
         parm143,
         parm144,
         parm145,
         parm146,
         parm147,
         parm148,
         parm149,
         parm150,
         parm151,
         parm152,
         parm153,
         parm154,
         parm155,
         parm156,
         parm157,
         parm158,
         parm159,
         parm160,
         parm161,
         parm162,
         parm163,
         parm164,
         parm165,
         parm166,
         parm167,
         parm168,
         parm169,
         parm170,
         parm171,
         parm172,
         parm173,
         parm174,
         parm175,
         parm176,
         parm177,
         parm178,
         parm179,
         parm180,
         parm181,
         parm182,
         parm183,
         parm184,
         parm185,
         parm186,
         parm187,
         parm188,
         parm189,
         parm190,
         parm191,
         parm192,
         parm193,
         parm194,
         parm195,
         parm196,
         parm197,
         parm198,
         parm199,
         parm200,
         parm201,
         parm202,
         parm203,
         parm204,
         parm205,
         parm206,
         parm207,
         parm208,
         parm209,
         parm210,
         parm211,
         parm212,
         parm213,
         parm214,
         parm215,
         parm216,
         parm217,
         parm218,
         parm219,
         parm220,
         parm221,
         parm222,
         parm223,
         parm224,
         parm225,
         parm226,
         parm227,
         parm228,
         parm229,
         parm230,
         parm231,
         parm232,
         parm233,
         parm234,
         parm235,
         parm236,
         parm237,
         parm238,
         parm239,
         parm240,
         parm241,
         parm242,
         parm243,
         parm244,
         parm245,
         parm246,
         parm247,
         parm248,
         parm249,
         parm250,
         LOAD_DATE
      )
      SELECT 'H',
             VOYAGE_ID,
             BKNG_NBR,
             PASSENGER_ID,
             MARINER_ID,
             parm1,
             parm2,
             parm3,
             parm4,
             parm5,
             parm6,
             parm7,
             parm8,
             parm9,
             parm10,
             parm11,
             parm12,
             parm13,
             parm14,
             parm15,
             parm16,
             parm17,
             parm18,
             parm19,
             parm20,
             parm21,
             parm22,
             parm23,
             parm24,
             parm25,
             parm26,
             parm27,
             parm28,
             parm29,
             parm30,
             parm31,
             parm32,
             parm33,
             parm34,
             parm35,
             parm36,
             parm37,
             parm38,
             parm39,
             parm40,
             parm41,
             parm42,
             parm43,
             parm44,
             parm45,
             parm46,
             parm47,
             parm48,
             parm49,
             parm50,
             parm51,
             parm52,
             parm53,
             parm54,
             parm55,
             parm56,
             parm57,
             parm58,
             parm59,
             parm60,
             parm61,
             parm62,
             parm63,
             parm64,
             parm65,
             parm66,
             parm67,
             parm68,
             parm69,
             parm70,
             parm71,
             parm72,
             parm73,
             parm74,
             parm75,
             parm76,
             parm77,
             parm78,
             parm79,
             parm80,
             parm81,
             parm82,
             parm83,
             parm84,
             parm85,
             parm86,
             parm87,
             parm88,
             parm89,
             parm90,
             parm91,
             parm92,
             parm93,
             parm94,
             parm95,
             parm96,
             parm97,
             parm98,
             parm99,
             parm100,
             parm101,
             parm102,
             parm103,
             parm104,
             parm105,
             parm106,
             parm107,
             parm108,
             parm109,
             parm110,
             parm111,
             parm112,
             parm113,
             parm114,
             parm115,
             parm116,
             parm117,
             parm118,
             parm119,
             parm120,
             parm121,
             parm122,
             parm123,
             parm124,
             parm125,
             parm126,
             parm127,
             parm128,
             parm129,
             parm130,
             parm131,
             parm132,
             parm133,
             parm134,
             parm135,
             parm136,
             parm137,
             parm138,
             parm139,
             parm140,
             parm141,
             parm142,
             parm143,
             parm144,
             parm145,
             parm146,
             parm147,
             parm148,
             parm149,
             parm150,
             parm151,
             parm152,
             parm153,
             parm154,
             parm155,
             parm156,
             parm157,
             parm158,
             parm159,
             parm160,
             parm161,
             parm162,
             parm163,
             parm164,
             parm165,
             parm166,
             parm167,
             parm168,
             parm169,
             parm170,
             parm171,
             parm172,
             parm173,
             parm174,
             parm175,
             parm176,
             parm177,
             parm178,
             parm179,
             parm180,
             parm181,
             parm182,
             parm183,
             parm184,
             parm185,
             parm186,
             parm187,
             parm188,
             parm189,
             parm190,
             parm191,
             parm192,
             parm193,
             parm194,
             parm195,
             parm196,
             parm197,
             parm198,
             parm199,
             parm200,
             parm201,
             parm202,
             parm203,
             parm204,
             parm205,
             parm206,
             parm207,
             parm208,
             parm209,
             parm210,
             parm211,
             parm212,
             parm213,
             parm214,
             parm215,
             parm216,
             parm217,
             parm218,
             parm219,
             parm220,
             parm221,
             parm222,
             parm223,
             parm224,
             parm225,
             parm226,
             parm227,
             parm228,
             parm229,
             parm230,
             parm231,
             parm232,
             parm233,
             parm234,
             parm235,
             parm236,
             parm237,
             parm238,
             parm239,
             parm240,
             parm241,
             parm242,
             parm243,
             parm244,
             parm245,
             parm246,
             parm247,
             parm248,
             parm249,
             parm250,
             SYSDATE
        FROM gss_result_stg_hal_ext;
                   
      vRowCountInserts := SQL%ROWCOUNT;

      --HAL Inserts
      INSERT into GSS_Result_Stg (
         COMPANY_CODE,
         VOYAGE_ID,
         BKNG_NBR,
         PASSENGER_ID,
         MARINER_ID,
         parm1,
         parm2,
         parm3,
         parm4,
         parm5,
         parm6,
         parm7,
         parm8,
         parm9,
         parm10,
         parm11,
         parm12,
         parm13,
         parm14,
         parm15,
         parm16,
         parm17,
         parm18,
         parm19,
         parm20,
         parm21,
         parm22,
         parm23,
         parm24,
         parm25,
         parm26,
         parm27,
         parm28,
         parm29,
         parm30,
         parm31,
         parm32,
         parm33,
         parm34,
         parm35,
         parm36,
         parm37,
         parm38,
         parm39,
         parm40,
         parm41,
         parm42,
         parm43,
         parm44,
         parm45,
         parm46,
         parm47,
         parm48,
         parm49,
         parm50,
         parm51,
         parm52,
         parm53,
         parm54,
         parm55,
         parm56,
         parm57,
         parm58,
         parm59,
         parm60,
         parm61,
         parm62,
         parm63,
         parm64,
         parm65,
         parm66,
         parm67,
         parm68,
         parm69,
         parm70,
         parm71,
         parm72,
         parm73,
         parm74,
         parm75,
         parm76,
         parm77,
         parm78,
         parm79,
         parm80,
         parm81,
         parm82,
         parm83,
         parm84,
         parm85,
         parm86,
         parm87,
         parm88,
         parm89,
         parm90,
         parm91,
         parm92,
         parm93,
         parm94,
         parm95,
         parm96,
         parm97,
         parm98,
         parm99,
         parm100,
         parm101,
         parm102,
         parm103,
         parm104,
         parm105,
         parm106,
         parm107,
         parm108,
         parm109,
         parm110,
         parm111,
         parm112,
         parm113,
         parm114,
         parm115,
         parm116,
         parm117,
         parm118,
         parm119,
         parm120,
         parm121,
         parm122,
         parm123,
         parm124,
         parm125,
         parm126,
         parm127,
         parm128,
         parm129,
         parm130,
         parm131,
         parm132,
         parm133,
         parm134,
         parm135,
         parm136,
         parm137,
         parm138,
         parm139,
         parm140,
         parm141,
         parm142,
         parm143,
         parm144,
         parm145,
         parm146,
         parm147,
         parm148,
         parm149,
         parm150,
         parm151,
         parm152,
         parm153,
         parm154,
         parm155,
         parm156,
         parm157,
         parm158,
         parm159,
         parm160,
         parm161,
         parm162,
         parm163,
         parm164,
         parm165,
         parm166,
         parm167,
         parm168,
         parm169,
         parm170,
         parm171,
         parm172,
         parm173,
         parm174,
         parm175,
         parm176,
         parm177,
         parm178,
         parm179,
         parm180,
         parm181,
         parm182,
         parm183,
         parm184,
         parm185,
         parm186,
         parm187,
         parm188,
         parm189,
         parm190,
         parm191,
         parm192,
         parm193,
         parm194,
         parm195,
         parm196,
         parm197,
         parm198,
         parm199,
         parm200,
         parm201,
         parm202,
         parm203,
         parm204,
         parm205,
         parm206,
         parm207,
         parm208,
         parm209,
         parm210,
         parm211,
         parm212,
         parm213,
         parm214,
         parm215,
         parm216,
         parm217,
         parm218,
         parm219,
         parm220,
         parm221,
         parm222,
         parm223,
         parm224,
         parm225,
         parm226,
         parm227,
         parm228,
         parm229,
         parm230,
         parm231,
         parm232,
         parm233,
         parm234,
         parm235,
         parm236,
         parm237,
         parm238,
         parm239,
         parm240,
         parm241,
         parm242,
         parm243,
         parm244,
         parm245,
         parm246,
         parm247,
         parm248,
         parm249,
         parm250,
         LOAD_DATE
      )
      SELECT 'S',
             VOYAGE_ID,
             BKNG_NBR,
             PASSENGER_ID,
             MARINER_ID,
             parm1,
             parm2,
             parm3,
             parm4,
             parm5,
             parm6,
             parm7,
             parm8,
             parm9,
             parm10,
             parm11,
             parm12,
             parm13,
             parm14,
             parm15,
             parm16,
             parm17,
             parm18,
             parm19,
             parm20,
             parm21,
             parm22,
             parm23,
             parm24,
             parm25,
             parm26,
             parm27,
             parm28,
             parm29,
             parm30,
             parm31,
             parm32,
             parm33,
             parm34,
             parm35,
             parm36,
             parm37,
             parm38,
             parm39,
             parm40,
             parm41,
             parm42,
             parm43,
             parm44,
             parm45,
             parm46,
             parm47,
             parm48,
             parm49,
             parm50,
             parm51,
             parm52,
             parm53,
             parm54,
             parm55,
             parm56,
             parm57,
             parm58,
             parm59,
             parm60,
             parm61,
             parm62,
             parm63,
             parm64,
             parm65,
             parm66,
             parm67,
             parm68,
             parm69,
             parm70,
             parm71,
             parm72,
             parm73,
             parm74,
             parm75,
             parm76,
             parm77,
             parm78,
             parm79,
             parm80,
             parm81,
             parm82,
             parm83,
             parm84,
             parm85,
             parm86,
             parm87,
             parm88,
             parm89,
             parm90,
             parm91,
             parm92,
             parm93,
             parm94,
             parm95,
             parm96,
             parm97,
             parm98,
             parm99,
             parm100,
             parm101,
             parm102,
             parm103,
             parm104,
             parm105,
             parm106,
             parm107,
             parm108,
             parm109,
             parm110,
             parm111,
             parm112,
             parm113,
             parm114,
             parm115,
             parm116,
             parm117,
             parm118,
             parm119,
             parm120,
             parm121,
             parm122,
             parm123,
             parm124,
             parm125,
             parm126,
             parm127,
             parm128,
             parm129,
             parm130,
             parm131,
             parm132,
             parm133,
             parm134,
             parm135,
             parm136,
             parm137,
             parm138,
             parm139,
             parm140,
             parm141,
             parm142,
             parm143,
             parm144,
             parm145,
             parm146,
             parm147,
             parm148,
             parm149,
             parm150,
             parm151,
             parm152,
             parm153,
             parm154,
             parm155,
             parm156,
             parm157,
             parm158,
             parm159,
             parm160,
             parm161,
             parm162,
             parm163,
             parm164,
             parm165,
             parm166,
             parm167,
             parm168,
             parm169,
             parm170,
             parm171,
             parm172,
             parm173,
             parm174,
             parm175,
             parm176,
             parm177,
             parm178,
             parm179,
             parm180,
             parm181,
             parm182,
             parm183,
             parm184,
             parm185,
             parm186,
             parm187,
             parm188,
             parm189,
             parm190,
             parm191,
             parm192,
             parm193,
             parm194,
             parm195,
             parm196,
             parm197,
             parm198,
             parm199,
             parm200,
             parm201,
             parm202,
             parm203,
             parm204,
             parm205,
             parm206,
             parm207,
             parm208,
             parm209,
             parm210,
             parm211,
             parm212,
             parm213,
             parm214,
             parm215,
             parm216,
             parm217,
             parm218,
             parm219,
             parm220,
             parm221,
             parm222,
             parm223,
             parm224,
             parm225,
             parm226,
             parm227,
             parm228,
             parm229,
             parm230,
             parm231,
             parm232,
             parm233,
             parm234,
             parm235,
             parm236,
             parm237,
             parm238,
             parm239,
             parm240,
             parm241,
             parm242,
             parm243,
             parm244,
             parm245,
             parm246,
             parm247,
             parm248,
             parm249,
             parm250,
             SYSDATE
        FROM gss_result_stg_sbn_ext;

      vRowCountInserts := vRowCountInserts + SQL%ROWCOUNT;
        
      vEndDate := SYSDATE ;  -- set end date to sysdate

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF DM_INT.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_parm  => vStartDate,
         in_End_DT_parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF DM_INT.COMMON_JOBS.LOG_AUDIT_RESULT (
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
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_parm => vStartDate,  
            in_End_DT_parm => SYSDATE
         );
         RAISE;

   END Load_GSS_Result_Stg;

   PROCEDURE Load_GSS_Result_Daily IS
   /***************************************************************************
   NAME: Load_GSS_Result_Daily
       
   PURPOSE: 
      Load for table Load_GSS_Result_Daily
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Truncates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/26/2013  Venky         Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'LOAD_GSS_RESULT_DAILY';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
    
   BEGIN
      -- Log program start 
      IF  DM_INT.COMMON_JOBS.LOG_JOB (
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

      EXECUTE IMMEDIATE 'TRUNCATE TABLE GSS_Result_Daily'; 

      --Inserts
      INSERT into GSS_Result_Daily (
         COMPANY_CODE,
         VOYAGE_ID,
         BKNG_NBR,
         PASSENGER_ID,
         MARINER_ID,
         LOAD_DATE,
         PARM,
         RESPONSE 
      )
      SELECT COMPANY_CODE,
             VOYAGE_ID,
             BKNG_NBR,
             PASSENGER_ID,
             MARINER_ID,
             LOAD_DATE,
             PARM,
             RESPONSE  
        FROM gss_result_stg
        UNPIVOT INCLUDE NULLS (response for parm in (
              parm1 as 1,
              parm2 as 2,
              parm3 as 3,
              parm4 as 4,
              parm5 as 5,
              parm6 as 6,
              parm7 as 7,
              parm8 as 8,
              parm9 as 9,
              parm10 as 10,
              parm11 as 11,
              parm12 as 12,
              parm13 as 13,
              parm14 as 14,
              parm15 as 15,
              parm16 as 16,
              parm17 as 17,
              parm18 as 18,
              parm19 as 19,
              parm20 as 20,
              parm21 as 21,
              parm22 as 22,
              parm23 as 23,
              parm24 as 24,
              parm25 as 25,
              parm26 as 26,
              parm27 as 27,
              parm28 as 28,
              parm29 as 29,
              parm30 as 30,
              parm31 as 31,
              parm32 as 32,
              parm33 as 33,
              parm34 as 34,
              parm35 as 35,
              parm36 as 36,
              parm37 as 37,
              parm38 as 38,
              parm39 as 39,
              parm40 as 40,
              parm41 as 41,
              parm42 as 42,
              parm43 as 43,
              parm44 as 44,
              parm45 as 45,
              parm46 as 46,
              parm47 as 47,
              parm48 as 48,
              parm49 as 49,
              parm50 as 50,
              parm51 as 51,
              parm52 as 52,
              parm53 as 53,
              parm54 as 54,
              parm55 as 55,
              parm56 as 56,
              parm57 as 57,
              parm58 as 58,
              parm59 as 59,
              parm60 as 60,
              parm61 as 61,
              parm62 as 62,
              parm63 as 63,
              parm64 as 64,
              parm65 as 65,
              parm66 as 66,
              parm67 as 67,
              parm68 as 68,
              parm69 as 69,
              parm70 as 70,
              parm71 as 71,
              parm72 as 72,
              parm73 as 73,
              parm74 as 74,
              parm75 as 75,
              parm76 as 76,
              parm77 as 77,
              parm78 as 78,
              parm79 as 79,
              parm80 as 80,
              parm81 as 81,
              parm82 as 82,
              parm83 as 83,
              parm84 as 84,
              parm85 as 85,
              parm86 as 86,
              parm87 as 87,
              parm88 as 88,
              parm89 as 89,
              parm90 as 90,
              parm91 as 91,
              parm92 as 92,
              parm93 as 93,
              parm94 as 94,
              parm95 as 95,
              parm96 as 96,
              parm97 as 97,
              parm98 as 98,
              parm99 as 99,
              parm100 as 100,
              parm101 as 101,
              parm102 as 102,
              parm103 as 103,
              parm104 as 104,
              parm105 as 105,
              parm106 as 106,
              parm107 as 107,
              parm108 as 108,
              parm109 as 109,
              parm110 as 110,
              parm111 as 111,
              parm112 as 112,
              parm113 as 113,
              parm114 as 114,
              parm115 as 115,
              parm116 as 116,
              parm117 as 117,
              parm118 as 118,
              parm119 as 119,
              parm120 as 120,
              parm121 as 121,
              parm122 as 122,
              parm123 as 123,
              parm124 as 124,
              parm125 as 125,
              parm126 as 126,
              parm127 as 127,
              parm128 as 128,
              parm129 as 129,
              parm130 as 130,
              parm131 as 131,
              parm132 as 132,
              parm133 as 133,
              parm134 as 134,
              parm135 as 135,
              parm136 as 136,
              parm137 as 137,
              parm138 as 138,
              parm139 as 139,
              parm140 as 140,
              parm141 as 141,
              parm142 as 142,
              parm143 as 143,
              parm144 as 144,
              parm145 as 145,
              parm146 as 146,
              parm147 as 147,
              parm148 as 148,
              parm149 as 149,
              parm150 as 150,
              parm151 as 151,
              parm152 as 152,
              parm153 as 153,
              parm154 as 154,
              parm155 as 155,
              parm156 as 156,
              parm157 as 157,
              parm158 as 158,
              parm159 as 159,
              parm160 as 160,
              parm161 as 161,
              parm162 as 162,
              parm163 as 163,
              parm164 as 164,
              parm165 as 165,
              parm166 as 166,
              parm167 as 167,
              parm168 as 168,
              parm169 as 169,
              parm170 as 170,
              parm171 as 171,
              parm172 as 172,
              parm173 as 173,
              parm174 as 174,
              parm175 as 175,
              parm176 as 176,
              parm177 as 177,
              parm178 as 178,
              parm179 as 179,
              parm180 as 180,
              parm181 as 181,
              parm182 as 182,
              parm183 as 183,
              parm184 as 184,
              parm185 as 185,
              parm186 as 186,
              parm187 as 187,
              parm188 as 188,
              parm189 as 189,
              parm190 as 190,
              parm191 as 191,
              parm192 as 192,
              parm193 as 193,
              parm194 as 194,
              parm195 as 195,
              parm196 as 196,
              parm197 as 197,
              parm198 as 198,
              parm199 as 199,
              parm200 as 200,
              parm201 as 201,
              parm202 as 202,
              parm203 as 203,
              parm204 as 204,
              parm205 as 205,
              parm206 as 206,
              parm207 as 207,
              parm208 as 208,
              parm209 as 209,
              parm210 as 210,
              parm211 as 211,
              parm212 as 212,
              parm213 as 213,
              parm214 as 214,
              parm215 as 215,
              parm216 as 216,
              parm217 as 217,
              parm218 as 218,
              parm219 as 219,
              parm220 as 220,
              parm221 as 221,
              parm222 as 222,
              parm223 as 223,
              parm224 as 224,
              parm225 as 225,
              parm226 as 226,
              parm227 as 227,
              parm228 as 228,
              parm229 as 229,
              parm230 as 230,
              parm231 as 231,
              parm232 as 232,
              parm233 as 233,
              parm234 as 234,
              parm235 as 235,
              parm236 as 236,
              parm237 as 237,
              parm238 as 238,
              parm239 as 239,
              parm240 as 240,
              parm241 as 241,
              parm242 as 242,
              parm243 as 243,
              parm244 as 244,
              parm245 as 245,
              parm246 as 246,
              parm247 as 247,
              parm248 as 248,
              parm249 as 249,
              parm250 as 250
          ));
                   
      vRowCountInserts := SQL%ROWCOUNT;
        
      vEndDate := SYSDATE ;  -- set end date to sysdate

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF DM_INT.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_parm  => vStartDate,
         in_End_DT_parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF DM_INT.COMMON_JOBS.LOG_AUDIT_RESULT (
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
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_parm => vStartDate,  
            in_End_DT_parm => SYSDATE
         );
         RAISE;

   END Load_GSS_Result_Daily;

   PROCEDURE Load_GSS_Question IS
   /***************************************************************************
   NAME: Load_GSS_Question
       
   PURPOSE: 
      Load for table Load_GSS_Question
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/26/2013  Venky         Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'LOAD_GSS_QUESTION';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
    
   BEGIN
      -- Log program start 
      IF  DM_INT.COMMON_JOBS.LOG_JOB (
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

      --Inserts
      INSERT into GSS_Question_D (
         ROW_WID,
         COMPANY_CODE,
         Question,
         LOAD_DATE
       )
       SELECT QTN_SEQ.NEXTVAL,
              q.company_code,
              DBMS_LOB.SUBSTR(q.response, 500) Question,
              q.Load_Date
         FROM gss_result_daily q
        WHERE q.voyage_id = 'Voyage ID'
          AND q.response NOT LIKE 'PORT\_%' ESCAPE '\' 
          AND q.response NOT LIKE '[=e_holland_port\_%]' ESCAPE '\'
           AND NOT EXISTS (SELECT 1
                             FROM GSS_Question_D  gq
                             WHERE gq.company_code = q.company_code
                               AND gq.question = DBMS_LOB.SUBSTR(q.response, 500));

      vRowCountInserts := SQL%ROWCOUNT;

      --Insert Ports
      INSERT into GSS_Question_D (
         ROW_WID,
         COMPANY_CODE,
         Question,
         LOAD_DATE
       )
       WITH Port_Questions AS (
       SELECT  q.company_code,
               DBMS_LOB.SUBSTR(r.response, 500) Port,
               MAX(r.load_date) load_date
          FROM GSS_Result_Daily q,
               GSS_Result_Daily r
         WHERE q.parm = r.parm
           AND q.company_code = r.company_code
           AND q.response LIKE 'PORT\_%' ESCAPE '\' 
           AND DBMS_LOB.SUBSTR(r.response, 500) IS NOT NULL
           AND q.voyage_id = 'Voyage ID'
           AND r.voyage_id <> 'Voyage ID'
           AND NOT EXISTS (SELECT 1
                             FROM GSS_Question_D  gq
                             WHERE gq.company_code = q.company_code
                               AND gq.question = DBMS_LOB.SUBSTR(r.response, 500))
         GROUP BY q.company_code,
                  DBMS_LOB.SUBSTR(r.response, 500))
          SELECT QTN_SEQ.NEXTVAL,
                 company_code,
                 Port,
                 load_date
            FROM Port_Questions;
       
      vRowCountInserts := vRowCountInserts + SQL%ROWCOUNT;
                  
      vEndDate := SYSDATE ;  -- set end date to sysdate

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF DM_INT.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_parm  => vStartDate,
         in_End_DT_parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF DM_INT.COMMON_JOBS.LOG_AUDIT_RESULT (
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
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_parm => vStartDate,  
            in_End_DT_parm => SYSDATE
         );
         RAISE;

   END Load_GSS_Question;

   PROCEDURE Load_GSS_Response IS
   /***************************************************************************
   NAME: Load_GSS_Response
       
   PURPOSE: 
      Load for table Load_GSS_Response
         
   RETURN: N/A
       
   PARAMETERS:      
      None   
          
   PROCESS STEPS :      
      1) Updates
      2) Inserts
                                                  
   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     06/26/2013  Venky         Created procedure.

   **************************************************************************/


   -- Constants and Procedure level vars
   cProgram_NM     CONSTANT   VARCHAR2(30) := 'LOAD_GSS_RESPONSE';
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCountDeletes                 NUMBER;
   vRowCountUpdates                 NUMBER;
   vRowCountInserts                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;   
   d_EOB_Data_Date            Date;
    
   BEGIN
      -- Log program start 
      IF  DM_INT.COMMON_JOBS.LOG_JOB (
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

      --Merges
      MERGE INTO GSS_Response_f tgt 
      USING (
          SELECT q.company_code,
                 SUBSTR(r.voyage_id, 1, INSTR(r.voyage_id, '-') - 1) Voyage,
                 r.bkng_nbr,
                 CASE WHEN LENGTH(r.passenger_id) = 1 THEN  '0'||r.passenger_id 
                      ELSE SUBSTR(r.passenger_id, 1, 3) 
                 END passenger_id,
                 r.mariner_id,
                 gq.row_wid question_wid,
                 --q.response question,
                 r.response,
                 r.load_date
            FROM GSS_Result_Daily q,
                 GSS_Result_Daily r,
                 GSS_Question_D gq
           WHERE q.parm = r.parm
             AND q.company_code = r.company_code
             AND q.company_code = gq.company_code
             AND DBMS_LOB.SUBSTR(q.response, 500) = gq.question
             AND q.response NOT LIKE 'PORT\_%' ESCAPE '\' 
             AND q.response NOT LIKE '[=e_holland_port\_%]' ESCAPE '\'
             AND q.voyage_id = 'Voyage ID'
             AND r.voyage_id <> 'Voyage ID') src
      ON (tgt.company_code = src.company_code AND
          tgt.Voyage = src.Voyage AND
          tgt.bkng_nbr = src.bkng_nbr AND
          tgt.passenger_id = src.passenger_id AND
          tgt.question_wid = src.question_wid)
      WHEN MATCHED THEN
         UPDATE SET tgt.response = src.response,
                    tgt.last_update = src.load_date
      WHEN NOT MATCHED THEN
         INSERT (tgt.company_code,
                 tgt.Voyage,
                 tgt.bkng_nbr,
                 tgt.passenger_id,
                 tgt.question_wid,
                 tgt.mariner_id,
                 tgt.response,
                 tgt.load_date)
         VALUES (src.company_code,
                 src.Voyage,
                 src.bkng_nbr,
                 src.passenger_id,
                 src.question_wid,
                 src.mariner_id,
                 src.response,
                 src.load_date);

      vRowCountInserts := SQL%ROWCOUNT;

      --Merge Port Responses
      MERGE INTO GSS_Response_f tgt 
      USING (
          WITH Port AS(
             SELECT r.company_code,
                    r.Voyage_id,
                    r.bkng_nbr,
                    r.passenger_id,
                    r.mariner_id,
                    DBMS_LOB.SUBSTR(q.response, 500) question,
                    DBMS_LOB.SUBSTR(r.response, 500) response,
                    r.load_date
               FROM GSS_Result_Daily q,
                    GSS_Result_Daily r
              WHERE q.parm = r.parm
                AND q.company_code = r.company_code
                AND q.response LIKE 'PORT\_%' ESCAPE '\' 
                AND q.voyage_id = 'Voyage ID'
                AND r.voyage_id <> 'Voyage ID'),
          Port_Response AS(
             SELECT r.company_code,
                    r.Voyage_id,
                    r.bkng_nbr,
                    r.passenger_id,
                    r.mariner_id,
                    UPPER(DBMS_LOB.SUBSTR(q.response, DBMS_LOB.GETLENGTH(q.response) - 13, 13)) question,
                    r.response,
                    r.load_date
               FROM GSS_Result_Daily q,
                    GSS_Result_Daily r
              WHERE q.parm = r.parm
                AND q.company_code = r.company_code
                AND q.response LIKE '[=e_holland_port\_%]' ESCAPE '\'
                AND q.voyage_id = 'Voyage ID'
                AND r.voyage_id <> 'Voyage ID')
          SELECT p.company_code,
                 SUBSTR(p.voyage_id, 1, INSTR(p.voyage_id, '-') - 1) Voyage,
                 p.bkng_nbr,
                 CASE WHEN LENGTH(p.passenger_id) = 1 THEN  '0'||p.passenger_id 
                      ELSE SUBSTR(p.passenger_id, 1, 3) 
                 END passenger_id,
                 p.mariner_id,
                 gq.row_wid question_wid,
                 -- p.response question,
                  pr.response, 
                  pr.load_date 
             FROM Port p,
                  Port_Response pr,
                  GSS_Question_D gq
            WHERE p.company_code = pr.company_code
              AND p.voyage_id = pr.voyage_id
              AND p.bkng_nbr = pr.bkng_nbr
              AND p.passenger_id = pr.passenger_id
              AND p.question = pr.question
              AND p.response IS NOT NULL
              AND p.company_code = gq.company_code
              AND p.response = gq.question) src
      ON (tgt.company_code = src.company_code AND
          tgt.Voyage = src.Voyage AND
          tgt.bkng_nbr = src.bkng_nbr AND
          tgt.passenger_id = src.passenger_id AND
          tgt.question_wid = src.question_wid)
      WHEN MATCHED THEN
         UPDATE SET tgt.response = src.response,
                    tgt.last_update = src.load_date
      WHEN NOT MATCHED THEN
         INSERT (tgt.company_code,
                 tgt.Voyage,
                 tgt.bkng_nbr,
                 tgt.passenger_id,
                 tgt.question_wid,
                 tgt.mariner_id,
                 tgt.response,
                 tgt.load_date)
         VALUES (src.company_code,
                 src.Voyage,
                 src.bkng_nbr,
                 src.passenger_id,
                 src.question_wid,
                 src.mariner_id,
                 src.response,
                 src.load_date);

      vRowCountInserts := vRowCountInserts + SQL%ROWCOUNT;
                  
      vEndDate := SYSDATE ;  -- set end date to sysdate

      -- Record Completion in Log    
        
      sComment := 'Job ran successfully';
        
      IF DM_INT.COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID, 
         in_Prog_Status => 'C', 
         in_Comments => sComment, 
         in_Error_Message => 'NA'
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;        
                                                 
      -- Record Completion in Control File
      IF DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID, 
         in_Schema_Name => cSchema_NM, 
         in_Package_Name => cPackage_NM, 
         in_Program_Name => cProgram_NM, 
         in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
         in_Start_DT_parm  => vStartDate,
         in_End_DT_parm =>  SYSDATE
         ) != 0 THEN
         
         RAISE eCALL_FAILED ;                  
      END IF;

       -- Record Completion in Audit File
       IF DM_INT.COMMON_JOBS.LOG_AUDIT_RESULT (
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
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'F', 
            in_Comments => 'Job Failed', 
            in_Error_Message => eErrMsg 
         );
                                                
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'F' , 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
              
      WHEN eCALL_FAILED THEN
         ROLLBACK;
         
         eErrMsg := 'User Defined - Error in called sub-program';                          
         vBatchID:=-1;
         
         -- Record Error against placeholder w/ batch of -1 since batch not recorded
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID, 
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ', 
            in_Error_Message => eErrMsg
         );

         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID, 
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM, 
            in_Load_Type => 'D', 
            in_Prog_Status => 'X', 
            in_Start_DT_parm => vStartDate, 
            in_End_DT_parm => SYSDATE
         );

         RAISE;
            
      WHEN OTHERS THEN
         ROLLBACK;
         dbms_output.put_line('Other error') ; 
         eErrMsg :=  SQLERRM ;                                              
          
         --  record error w/ the assigned batch ID
         vDummy := DM_INT.COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,   
            in_Prog_Status => 'F',  
            in_Comments => 'Job Failed',  
            in_Error_Message => eErrMsg 
         );
          
         vDummy := DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
            in_Batch_ID => vBatchID,  
            in_Schema_Name => cSchema_NM, 
            in_Package_Name => cPackage_NM, 
            in_Program_Name => cProgram_NM,   
            in_Load_Type => 'D' ,    
            in_Prog_Status => 'F' ,
            in_Start_DT_parm => vStartDate,  
            in_End_DT_parm => SYSDATE
         );
         RAISE;

   END Load_GSS_Response;


END GSS_LOAD_PKG;
/