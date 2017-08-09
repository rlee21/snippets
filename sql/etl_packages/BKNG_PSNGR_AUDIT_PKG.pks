CREATE OR REPLACE PACKAGE DWH_OWNER.Bkng_Psngr_Audit_Pkg AS
/******************************************************************************
   NAME:       Bkng_Psngr_Audit_Pkg
   PURPOSE: Container for Booking Passenger Audit scripts.

   REVISIONS:
   Ver        Date        Author     Description
   ---------  ----------  ------     ------------------------------------

******************************************************************************/
    PROCEDURE Run_Bkng_Psngr_Audit;

    PROCEDURE Get_Bkng_Psngr_Daily;

    PROCEDURE Update_Bkng_Psngr_Audit;


END Bkng_Psngr_Audit_Pkg;
/