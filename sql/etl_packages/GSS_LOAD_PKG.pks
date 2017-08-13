CREATE OR REPLACE PACKAGE DM_TKR.GSS_LOAD_PKG AS
/******************************************************************************
   NAME:    GSS_LOAD_PKG
   PURPOSE: Container for GSS_LOAD_PKG. 

   REVISIONS:
   Ver        Date        Author     Description
   ---------  ----------  ------     ------------------------------------
   
******************************************************************************/

    PROCEDURE Load_GSS_Result;

    PROCEDURE Load_GSS_Result_Stg;

    PROCEDURE Load_GSS_Result_Daily;

    PROCEDURE Load_GSS_Question;

    PROCEDURE Load_GSS_Response;
                                                               
END GSS_LOAD_PKG;
/