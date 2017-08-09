CREATE OR REPLACE PACKAGE BODY DWH_OWNER.Bkng_Psngr_Audit_Pkg AS
/******************************************************************************
   NAME:     Bkng_Psngr_Audit_Pkg
   PURPOSE:  Process to audit Booking Passenger changes

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
           Function  names in Caps
           Variable  declarations in Mixed Case
           Table / Column names lower Case

******************************************************************************/

    -- Constants and Pkg level vars
   cSchema_NM    CONSTANT   Varchar2(15) :=  USER  ;
   cPackage_NM   CONSTANT   Varchar2(50) := 'BKNG_PSNGR_AUDIT_PKG'  ;
   eErrMsg                  Varchar2(500);
   eErrNo                   Number ;
   sComment                 Varchar2(2000) ;
   eCALL_FAILED   Exception;
   eINCONSISTENCY Exception;



   PROCEDURE Run_Bkng_Psngr_Audit IS
   /***************************************************************************
   NAME: Run_Bkng_Psngr_Audit
   PURPOSE:
      Call procedures to process promo pricing daily audit

   RETURN: N/A

   PARAMETERS:
        None

   PROCESS STEPS :
      1) Call procedures to load daily audit

   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     10/24/2012  cxo           initial set up
   BI-2293 05/30/2013  gxg           add 4 new BP GST fields
   BI-2686 06/13/2013  gxg           add new field rev_adj_promo_item
   BI-3619 08/04/2013  hcd           add new fields  others for POLAR R13.2

   **************************************************************************/

   -- Constants and Procedure level vars
   cProgram_NM   CONSTANT   Varchar(30) := 'RUN_BKNG_PSNGR_AUDIT' ;
   vBatchID                 Number := 0;
   vDummy                   Number ;
   vRowCount                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;

   BEGIN

      -- Log program start
      IF COMMON_JOBS.LOG_JOB (
         io_Batch_ID  => vBatchID,
         in_Schema_Name  =>  cSchema_NM,
         in_Package_Name   => cPackage_NM,
         in_Program_Name  =>cProgram_NM,
         in_Prog_Status => 'I',
         in_Comments  => 'Job in Progress',
         in_Error_Message => 'NA') != 0 THEN

         RAISE eCALL_FAILED;

      END IF;

      ----------------------------------
      Get_Bkng_Psngr_Daily;
      ----------------------------------

      BEGIN
        DBMS_STATS.GATHER_TABLE_STATS (
            OwnName        => 'DWH_OWNER'
           ,TabName        => 'BKNG_PSNGR_DAILY'
           ,Degree         => 6
           ,Cascade        => TRUE);
      END;

      ----------------------------------
      Update_Bkng_Psngr_Audit;
      ----------------------------------


      vEndDate := SYSDATE ;  -- set end date to sysdate

      -- Record Completion in Log
      sComment := 'Job ran successfully';

      IF COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID,
         in_Prog_Status => 'C',
         in_Comments => sComment,
         in_Error_Message => 'NA' ) != 0 THEN

         RAISE eCALL_FAILED ;

      END IF;

      -- Record Completion in Control File
      IF COMMON_JOBS.LOG_PROC_CTRL (
         in_Batch_ID => vBatchID,
         in_Schema_Name => cSchema_NM,
         in_Package_Name => cPackage_NM,
         in_Program_Name => cProgram_NM,
         in_Load_Type  => 'D', in_Prog_Status  => 'C' ,
         in_Start_DT_Parm  => vStartDate, in_End_DT_Parm =>  SYSDATE ) != 0 THEN

         RAISE eCALL_FAILED ;

      END IF;

      EXCEPTION

      WHEN eINCONSISTENCY THEN
         ROLLBACK ;

         -- Record Error
         vDummy := COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID,
            in_Prog_Status => 'F',
            in_Comments => 'Job Failed',
            in_Error_Message => eErrMsg
         );

         vDummy := COMMON_JOBS.LOG_PROC_CTRL (
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

         -- Record Error against palceholder w/ batch of -1 since batch not recorded
         vDummy := COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID,
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ',
            in_Error_Message => eErrMsg
         );

         vDummy := COMMON_JOBS.LOG_PROC_CTRL (
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
         vDummy := COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,
            in_Prog_Status => 'F',
            in_Comments => 'Job Failed',
            in_Error_Message => eErrMsg
         );

         vDummy := COMMON_JOBS.LOG_PROC_CTRL (
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

   END Run_Bkng_Psngr_Audit;


   PROCEDURE Get_Bkng_Psngr_Daily IS
   /***************************************************************************
   NAME: Get_Bkng_Psngr_Daily

   PURPOSE:
      Load rows for BKNG_PSNGR_DAILY

   RETURN: N/A

   PARAMETERS:
      None

   PROCESS STEPS :
      1) Truncate table
      2) Insert current daily rows


   REVISIONS:
   Ver      Date       Author(s)     Description
   ------  ----------  ----------    ------------------------------------------
   1.0     10/24/2012  cxo           initial set up
   BI-2293 05/30/2013  gxg           add 4 new BP GST fields

   **************************************************************************/


-- Constants and Procedure level vars
   cProgram_NM     CONSTANT   Varchar(30) := 'GET_BKNG_PSNGR_DAILY' ;
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCount                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;
   d_EOB_Data_Date            Date;

   BEGIN
      -- Log program start
      IF  COMMON_JOBS.LOG_JOB (
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

      --Get system data date
      BEGIN

         SELECT eob_data_date
           INTO d_EOB_Data_Date
           FROM DW_SYSTEM_PARMS;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            d_EOB_Data_Date := NULL;

      END;

      IF d_EOB_Data_Date IS NULL THEN
         eErrMsg :=  'No EOB Data Date found in dw_system_parms' ;
         RAISE eINCONSISTENCY;
      END IF;

      -- Delete from audit table if it has already run
      DELETE FROM bkng_psngr_audit
      WHERE effective_date = d_EOB_Data_Date;

      -- Reset the dates for previous runs
      UPDATE bkng_psngr_audit
         SET curr_rec_ind = 'Y',
             expiration_date = to_date('12/31/9999', 'MM/DD/YYYY')
       WHERE expiration_date = d_EOB_Data_Date;


      EXECUTE IMMEDIATE 'truncate table BKNG_PSNGR_DAILY';

      INSERT INTO bkng_psngr_daily (
         eob_data_date,
         b_p_data_date,
         bkng_id,
         b_bkng_nbr,
         b_voyage,
         b_p_passenger_id,
         source_system,
         b_p_use,
         b_p_passenger_status,
         b_p_conc_cd,
         b_p_pax_type,
         b_p_berth,
         b_p_count_for_tc_flag,
         b_p_pcl_netrev,
         net_rev,
         b_p_air_cost,
         b_p_seafare,
         b_p_land_charges,
         b_p_actual_air_cost,
         b_p_fcc_prot_comm,
         b_p_air_fees,
         b_p_onboard_credit_amt,
         b_p_port_charges,
         b_p_govt_fees,
         b_p_discount,
         b_p_packages,
         b_p_package_fees,
         b_p_waiver,
         b_p_land_price,
         b_p_land_mods,
         std_land_cost,
         b_p_air_add_on,
         b_p_air_supplement,
         pcom_netf_bkg_amt,
         pcom_netf_base_amt,
         pcom_netf_ovr_amt,
         pcom_land_bkg_amt,
         pcom_land_base_amt,
         pcom_land_ovr_amt,
         pcom_air_add_bkg_amt,
         pcom_air_add_base_amt,
         pcom_air_add_ovr_amt,
         pcom_air_upg_bkg_amt,
         pcom_air_upg_base_amt,
         pcom_air_upg_ovr_amt,
         pcom_air_dev_bkg_amt,
         pcom_air_dev_base_amt,
         pcom_air_dev_ovr_amt,
         pcom_pkgs_bkg_amt,
         pcom_pkgs_base_amt,
         pcom_pkgs_ovr_amt,
         pcom_xfer_bkg_amt,
         pcom_xfer_base_amt,
         pcom_xfer_ovr_amt,
         pcom_shr_ex_bkg_amt,
         pcom_shr_ex_base_amt,
         pcom_shr_ex_ovr_amt,
         pcom_tmod_bkg_amt,
         pcom_tmod_base_amt,
         pcom_tmod_ovr_amt,
         pcom_lmod_bkg_amt,
         pcom_lmod_base_amt,
         pcom_lmod_ovr_amt,
         pcom_ldev_bkg_amt,
         pcom_ldev_base_amt,
         pcom_ldev_ovr_amt,
         pcom_ssr_bkg_amt,
         pcom_ssr_base_amt,
         pcom_ssr_ovr_amt,
         pcom_tvl_prot_bkg_amt,
         pcom_tvl_prot_base_amt,
         pcom_tvl_prot_ovr_amt,
         pcom_svc_fee_bkg_amt,
         pcom_svc_fee_base_amt,
         pcom_svc_fee_ovr_amt,
         pcom_cancel_bkg_amt,
         pcom_cancel_base_amt,
         pcom_cancel_bkg_ovr_amt,
         pcom_cancel_ovr_amt,
         pcom_bonus_comm_bkg_amt,
         pcom_bonus_comm_fcc_amt,
         pcom_bonus_comm_amt,
         b_p_fare,
         b_p_promo_sob,
         tax_margin,
         tax_cost_amount,
         ct_supplemental_cost,
         ct_supplemental_revenue,
         ct_ship_cost_amount,
         ct_nda,
         ct_tax_rev,
         ct_tax_cost,
         ct_base_category_tariff,
         b_local_per_base_rate,
         net_ticket_revenue,
         passenger_count,
         rev_adj_act_ammenities,
         rev_adj_act_gap,
         rev_adj_cocktail_pty,
         rev_adj_proj_gap,
         rev_adj_proj_tc,
         b_p_air_fees_taxes_to,
         b_p_air_fees_taxes_fr,
         b_p_wholesaler_comm_portion,
         b_p_fcc_discount,
         b_category,
         b_p_apo,
         b_p_fuel_surcharge,
         fuel_ta_fee,
         blended_conversion_rate,
         b_p_tours_land_cost,
         b_p_waiver_flag,
         voyage_status,
         rev_adj_act_caso,
         rev_adj_act_loyalty_air_dev,
         b_p_land_dev_fee,
         b_p_land_mods_fees,
         b_p_pax_gift,
         b_p_service_fees,
         b_p_landex,
         b_p_airdev_fee,
         b_p_gross,
         rev_adj_proj_ovr_comm_amt,
         company_code,
         net_cruise_fare,
         net_cruise_fare_percent,
         net_tour_fare,
         ntr_fin_usd,
         ntr_sls_usd,
         ntr_sls_bkng_curr,
         npr_usd,
         ntr_fin_bkng_curr,
         ct_method,
         ct_band,
         rev_adj_uk_air_fee_usd_curr ,
         rev_adj_uk_air_fee_bkng_curr,
         pcom_ncfc_base_amt          ,
         pcom_ncfc_ovr_amt           ,
         pcom_air_fee_base_amt       ,
         pcom_air_fee_ovr_amt        ,
         pcom_gov_fee_base_amt       ,
         pcom_gov_fee_ovr_amt        ,
         pcom_vsa_fee_base_amt       ,
         pcom_vsa_fee_ovr_amt        ,
         pcom_fuel_su_base_amt       ,
         pcom_fuel_su_ovr_amt ,
         cal_year,
 --        b_p_cruise_gst_au_amt,
 --        b_p_cruise_gst_nz_amt,
 --        b_p_air_gst_au_amt,
 --        b_p_air_gst_nz_amt,
         rev_adj_promo_item,
         rev_adj_gst_cruise,
         rev_adj_gst_air
      )
      WITH q1 as (
         SELECT
         d_eob_data_date,
         b_p_data_date,
         bkng_id,
         b_bkng_nbr,
         b_voyage,
         b_p_passenger_id,
         source_system,
         b_p_use,
         b_p_passenger_status,
         b_p_conc_cd,
         b_p_pax_type,
         b_p_berth,
         b_p_count_for_tc_flag,
         b_p_pcl_netrev,
         net_rev,
         b_p_air_cost,
         b_p_seafare,
         b_p_land_charges,
         b_p_actual_air_cost,
         b_p_fcc_prot_comm,
         b_p_air_fees,
         b_p_onboard_credit_amt,
         b_p_port_charges,
         b_p_govt_fees,
         b_p_discount,
         b_p_packages,
         b_p_package_fees,
         b_p_waiver,
         b_p_land_price,
         b_p_land_mods,
         std_land_cost,
         b_p_air_add_on,
         b_p_air_supplement,
         pcom_netf_bkg_amt,
         pcom_netf_base_amt,
         pcom_netf_ovr_amt,
         pcom_land_bkg_amt,
         pcom_land_base_amt,
         pcom_land_ovr_amt,
         pcom_air_add_bkg_amt,
         pcom_air_add_base_amt,
         pcom_air_add_ovr_amt,
         pcom_air_upg_bkg_amt,
         pcom_air_upg_base_amt,
         pcom_air_upg_ovr_amt,
         pcom_air_dev_bkg_amt,
         pcom_air_dev_base_amt,
         pcom_air_dev_ovr_amt,
         pcom_pkgs_bkg_amt,
         pcom_pkgs_base_amt,
         pcom_pkgs_ovr_amt,
         pcom_xfer_bkg_amt,
         pcom_xfer_base_amt,
         pcom_xfer_ovr_amt,
         pcom_shr_ex_bkg_amt,
         pcom_shr_ex_base_amt,
         pcom_shr_ex_ovr_amt,
         pcom_tmod_bkg_amt,
         pcom_tmod_base_amt,
         pcom_tmod_ovr_amt,
         pcom_lmod_bkg_amt,
         pcom_lmod_base_amt,
         pcom_lmod_ovr_amt,
         pcom_ldev_bkg_amt,
         pcom_ldev_base_amt,
         pcom_ldev_ovr_amt,
         pcom_ssr_bkg_amt,
         pcom_ssr_base_amt,
         pcom_ssr_ovr_amt,
         pcom_tvl_prot_bkg_amt,
         pcom_tvl_prot_base_amt,
         pcom_tvl_prot_ovr_amt,
         pcom_svc_fee_bkg_amt,
         pcom_svc_fee_base_amt,
         pcom_svc_fee_ovr_amt,
         pcom_cancel_bkg_amt,
         pcom_cancel_base_amt,
         pcom_cancel_bkg_ovr_amt,
         pcom_cancel_ovr_amt,
         pcom_bonus_comm_bkg_amt,
         pcom_bonus_comm_fcc_amt,
         pcom_bonus_comm_amt,
         b_p_fare,
         b_p_promo_sob,
         tax_margin,
         tax_cost_amount,
         ct_supplemental_cost,
         ct_supplemental_revenue,
         ct_ship_cost_amount,
         b_local_per_base_rate,
         net_ticket_revenue,
         passenger_count,
         rev_adj_act_ammenities,
         rev_adj_act_gap,
         rev_adj_cocktail_pty,
         rev_adj_proj_gap,
         rev_adj_proj_tc,
         b_p_air_fees_taxes_to,
         b_p_air_fees_taxes_fr,
         b_p_wholesaler_comm_portion,
         b_p_fcc_discount,
         b_category,
         b_p_apo,
         b_p_fuel_surcharge,
         fuel_ta_fee,
         blended_conversion_rate,
         b_p_tours_land_cost,
         b_p_waiver_flag,
         voyage_status,
         rev_adj_act_caso,
         rev_adj_act_loyalty_air_dev,
         b_p_land_dev_fee,
         b_p_land_mods_fees,
         b_p_pax_gift,
         b_p_service_fees,
         b_p_landex,
         b_p_airdev_fee,
         b_p_gross,
         rev_adj_proj_ovr_comm_amt,
         company_code,
         net_cruise_fare,
         net_cruise_fare_percent,
         net_tour_fare,
         ntr_fin_usd,
         ntr_sls_usd,
         ntr_sls_bkng_curr,
         npr_usd,
         ntr_fin_bkng_curr,
         ct_method,
         ct_band,
         rev_adj_uk_air_fee_usd_curr,
         rev_adj_uk_air_fee_bkng_curr,
         pcom_ncfc_base_amt,
         pcom_ncfc_ovr_amt,
         pcom_air_fee_base_amt,
         pcom_air_fee_ovr_amt,
         pcom_gov_fee_base_amt,
         pcom_gov_fee_ovr_amt,
         pcom_vsa_fee_base_amt,
         pcom_vsa_fee_ovr_amt,
         pcom_fuel_su_base_amt,
         pcom_fuel_su_ovr_amt,
         cal_year,
 --        b_p_cruise_gst_au_amt,
 --        b_p_cruise_gst_nz_amt,
 --        b_p_air_gst_au_amt,
 --        b_p_air_gst_nz_amt,
         rev_adj_promo_item,
         rev_adj_gst_cruise,
         rev_adj_gst_air
         FROM v_bkng_psngr_daily )
      SELECT
         d_eob_data_date,
         b_p_data_date,
         bkng_id,
         b_bkng_nbr,
         b_voyage,
         b_p_passenger_id,
         source_system,
         b_p_use,
         b_p_passenger_status,
         b_p_conc_cd,
         b_p_pax_type,
         b_p_berth,
         b_p_count_for_tc_flag,
         nvl(b_p_pcl_netrev, 0),
         nvl(net_rev, 0),
         nvl(b_p_air_cost, 0),
         nvl(b_p_seafare, 0),
         nvl(b_p_land_charges, 0),
         nvl(b_p_actual_air_cost, 0),
         nvl(b_p_fcc_prot_comm, 0),
         nvl(b_p_air_fees, 0),
         nvl(b_p_onboard_credit_amt, 0),
         nvl(b_p_port_charges, 0),
         nvl(b_p_govt_fees, 0),
         nvl(b_p_discount, 0),
         nvl(b_p_packages, 0),
         nvl(b_p_package_fees, 0),
         nvl(b_p_waiver, 0),
         nvl(b_p_land_price, 0),
         nvl(b_p_land_mods, 0),
         nvl(std_land_cost, 0),
         nvl(b_p_air_add_on, 0),
         nvl(b_p_air_supplement, 0),
         nvl(pcom_netf_bkg_amt, 0),
         nvl(pcom_netf_base_amt, 0),
         nvl(pcom_netf_ovr_amt, 0),
         nvl(pcom_land_bkg_amt, 0),
         nvl(pcom_land_base_amt, 0),
         nvl(pcom_land_ovr_amt, 0),
         nvl(pcom_air_add_bkg_amt, 0),
         nvl(pcom_air_add_base_amt, 0),
         nvl(pcom_air_add_ovr_amt, 0),
         nvl(pcom_air_upg_bkg_amt, 0),
         nvl(pcom_air_upg_base_amt, 0),
         nvl(pcom_air_upg_ovr_amt, 0),
         nvl(pcom_air_dev_bkg_amt, 0),
         nvl(pcom_air_dev_base_amt, 0),
         nvl(pcom_air_dev_ovr_amt, 0),
         nvl(pcom_pkgs_bkg_amt, 0),
         nvl(pcom_pkgs_base_amt, 0),
         nvl(pcom_pkgs_ovr_amt, 0),
         nvl(pcom_xfer_bkg_amt, 0),
         nvl(pcom_xfer_base_amt, 0),
         nvl(pcom_xfer_ovr_amt, 0),
         nvl(pcom_shr_ex_bkg_amt, 0),
         nvl(pcom_shr_ex_base_amt, 0),
         nvl(pcom_shr_ex_ovr_amt, 0),
         nvl(pcom_tmod_bkg_amt, 0),
         nvl(pcom_tmod_base_amt, 0),
         nvl(pcom_tmod_ovr_amt, 0),
         nvl(pcom_lmod_bkg_amt, 0),
         nvl(pcom_lmod_base_amt, 0),
         nvl(pcom_lmod_ovr_amt, 0),
         nvl(pcom_ldev_bkg_amt, 0),
         nvl(pcom_ldev_base_amt, 0),
         nvl(pcom_ldev_ovr_amt, 0),
         nvl(pcom_ssr_bkg_amt, 0),
         nvl(pcom_ssr_base_amt, 0),
         nvl(pcom_ssr_ovr_amt, 0),
         nvl(pcom_tvl_prot_bkg_amt, 0),
         nvl(pcom_tvl_prot_base_amt, 0),
         nvl(pcom_tvl_prot_ovr_amt, 0),
         nvl(pcom_svc_fee_bkg_amt, 0),
         nvl(pcom_svc_fee_base_amt, 0),
         nvl(pcom_svc_fee_ovr_amt, 0),
         nvl(pcom_cancel_bkg_amt, 0),
         nvl(pcom_cancel_base_amt, 0),
         nvl(pcom_cancel_bkg_ovr_amt, 0),
         nvl(pcom_cancel_ovr_amt, 0),
         nvl(pcom_bonus_comm_bkg_amt, 0),
         nvl(pcom_bonus_comm_fcc_amt, 0),
         nvl(pcom_bonus_comm_amt, 0),
         nvl(b_p_fare, 0),
         b_p_promo_sob,
         nvl(tax_margin, 0),
         nvl(tax_cost_amount, 0),
         nvl(ct_supplemental_cost, 0),
         nvl(ct_supplemental_revenue, 0),
         nvl(ct_ship_cost_amount, 0),
         nvl(ctrf.ct_nda, 0),
         nvl(ctrf.ct_tax_rev, 0),
         nvl(ctrf.ct_tax_cost, 0),
         nvl(ctrf.ct_base_category_tariff, 0),
         nvl(b_local_per_base_rate, 0),
         nvl(net_ticket_revenue, 0),
         nvl(passenger_count, 0),
         nvl(rev_adj_act_ammenities, 0),
         nvl(rev_adj_act_gap, 0),
         nvl(rev_adj_cocktail_pty, 0),
         nvl(rev_adj_proj_gap, 0),
         nvl(rev_adj_proj_tc, 0),
         nvl(b_p_air_fees_taxes_to, 0),
         nvl(b_p_air_fees_taxes_fr, 0),
         nvl(b_p_wholesaler_comm_portion, 0),
         nvl(b_p_fcc_discount, 0),
         b_category,
         b_p_apo,
         nvl(b_p_fuel_surcharge, 0),
         nvl(fuel_ta_fee, 0),
         nvl(blended_conversion_rate, 0),
         nvl(b_p_tours_land_cost, 0),
         b_p_waiver_flag,
         voyage_status,
         nvl(rev_adj_act_caso, 0),
         nvl(rev_adj_act_loyalty_air_dev, 0),
         nvl(b_p_land_dev_fee, 0),
         nvl(b_p_land_mods_fees, 0),
         nvl(b_p_pax_gift, 0),
         nvl(b_p_service_fees, 0),
         nvl(b_p_landex, 0),
         nvl(b_p_airdev_fee, 0),
         nvl(b_p_gross, 0),
         nvl(rev_adj_proj_ovr_comm_amt, 0),
         q1.company_code,
         nvl(net_cruise_fare, 0),
         nvl(net_cruise_fare_percent, 0),
         nvl(net_tour_fare, 0),
         nvl(ntr_fin_usd, 0),
         nvl(ntr_sls_usd, 0),
         nvl(ntr_sls_bkng_curr, 0),
         nvl(npr_usd, 0),
         nvl(ntr_fin_bkng_curr, 0),
         ct_method,
         ct_band,
         nvl(rev_adj_uk_air_fee_usd_curr,0),
         nvl(rev_adj_uk_air_fee_bkng_curr,0),
         nvl(pcom_ncfc_base_amt, 0),
         nvl(pcom_ncfc_ovr_amt, 0),
         nvl(pcom_air_fee_base_amt, 0),
         nvl(pcom_air_fee_ovr_amt, 0),
         nvl(pcom_gov_fee_base_amt, 0),
         nvl(pcom_gov_fee_ovr_amt, 0),
         nvl(pcom_vsa_fee_base_amt, 0),
         nvl(pcom_vsa_fee_ovr_amt, 0),
         nvl(pcom_fuel_su_base_amt, 0),
         nvl(pcom_fuel_su_ovr_amt, 0),
         cal_year,
   --      nvl(b_p_cruise_gst_au_amt, 0),
   --     nvl(b_p_cruise_gst_nz_amt, 0),
   --      nvl(b_p_air_gst_au_amt, 0),
   --      nvl(b_p_air_gst_nz_amt, 0),
         nvl(rev_adj_promo_item, 0),
         nvl(rev_adj_gst_cruise, 0),
         nvl(rev_adj_gst_air, 0)
        FROM q1, CRUISE_TOUR_REVENUE_FACTOR ctrf
       WHERE q1.b_voyage=ctrf.ct_voyage(+)
         AND q1.company_code=ctrf.company_code(+)
         ;

      vRowCount := SQL%ROWCOUNT ;

      vEndDate := SYSDATE ;  -- set end date to sysdate

      -- Record Completion in Log

       sComment:= 'Job ran successfully - # of records inserted into BKNG_PSNGR_DAILY: '||vRowCount||'  '||to_char(sysdate,'mm/dd/yyyy hh24:mi:ss');

      IF COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID,
         in_Prog_Status => 'C',
         in_Comments => sComment,
         in_Error_Message => 'NA'
         ) != 0 THEN

         RAISE eCALL_FAILED ;
      END IF;

      -- Record Completion in Control File
      IF COMMON_JOBS.LOG_PROC_CTRL (
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
       IF COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D',
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => vRowCount,
          in_Target_Cnt => vRowCount,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,
          in_Insert_Count =>  vRowCount,
          in_Update_Count  => 0,
          in_Delete_Count => 0  ) != 0
        THEN
            RAISE eCALL_FAILED ;
        END IF;


      EXCEPTION

      WHEN eINCONSISTENCY THEN
         ROLLBACK ;

         -- Record Error
         vDummy := COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID,
            in_Prog_Status => 'F',
            in_Comments => 'Job Failed',
            in_Error_Message => eErrMsg
         );

         vDummy := COMMON_JOBS.LOG_PROC_CTRL (
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

         -- Record Error against palceholder w/ batch of -1 since batch not recorded
         vDummy := COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID,
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ',
            in_Error_Message => eErrMsg
         );

         vDummy := COMMON_JOBS.LOG_PROC_CTRL (
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
         vDummy := COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,
            in_Prog_Status => 'F',
            in_Comments => 'Job Failed',
            in_Error_Message => eErrMsg
         );

         vDummy := COMMON_JOBS.LOG_PROC_CTRL (
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


END Get_Bkng_Psngr_Daily;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------

   PROCEDURE  Update_Bkng_Psngr_Audit IS
   /****************************************************************
   NAME: Update_Bkng_Psngr_Audit

   PURPOSE:
            Load  rows for  BKNG_PSNGR_AUDIT.
            Input: BKNG_PSNGR_DAILY

            Hold daily inserts and updates. There are no deletes.
            This procedure records this activity in bkng_psngr_audit.

   RETURN:N/A

   PARAMETERS: None

   REVISIONS:
   Ver     Date        Author  Description
   ------  ----------  ------  ---------------------------------------------
   1.0     10/24/2012  cxo     initial set up
   BI-2293 05/30/2013  gxg     add 4 new BP GST fields
   ************************************************************/


   cProgram_NM  CONSTANT  Varchar2(30) := 'UPDATE_BKNG_PSNGR_AUDIT'  ;
   vBatchID                  NUMBER := 0;
   vDummy                    NUMBER;
   vRowCount                 NUMBER;
   vStartDate                DATE := SYSDATE;
   vEndDate                  DATE := SYSDATE;

   BEGIN

      -- Log program start
      IF  COMMON_JOBS.LOG_JOB (
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

      -- Step 1 Insert updates
      vRowCount := 0;

      FOR rec IN (
       SELECT /*+ PARALLEL */
              a.bkng_id,
              a.b_bkng_nbr,            a.b_p_land_mods,         a.pcom_shr_ex_base_amt,    a.pcom_bonus_comm_amt,         a.b_p_fuel_surcharge,
              a.b_voyage,              a.std_land_cost,         a.pcom_shr_ex_ovr_amt,     a.b_p_fare,                    a.fuel_ta_fee,
              a.b_p_passenger_id,      a.b_p_air_add_on,        a.pcom_tmod_bkg_amt,       a.b_p_promo_sob,               a.blended_conversion_rate,
              a.source_system,         a.b_p_air_supplement,    a.pcom_tmod_base_amt,      a.tax_margin,                  a.b_p_tours_land_cost,
              a.b_p_use,               a.pcom_netf_bkg_amt,     a.pcom_tmod_ovr_amt,       a.tax_cost_amount,             a.b_p_waiver_flag,
              a.b_p_passenger_status,  a.pcom_netf_base_amt,    a.pcom_lmod_bkg_amt,       a.ct_supplemental_cost,        a.voyage_status,
              a.b_p_conc_cd,           a.pcom_netf_ovr_amt,     a.pcom_lmod_base_amt,      a.ct_supplemental_revenue,     a.rev_adj_act_caso,
              a.b_p_pax_type,          a.pcom_land_bkg_amt,     a.pcom_lmod_ovr_amt,       a.ct_ship_cost_amount,         a.rev_adj_act_loyalty_air_dev,
              a.b_p_berth,             a.pcom_land_base_amt,    a.pcom_ldev_bkg_amt,       a.ct_nda,                      a.b_p_land_dev_fee,
              a.b_p_count_for_tc_flag, a.pcom_land_ovr_amt,     a.pcom_ldev_base_amt,      a.ct_tax_rev,                  a.b_p_land_mods_fees,
              a.b_p_pcl_netrev,        a.pcom_air_add_bkg_amt,  a.pcom_ldev_ovr_amt,       a.ct_tax_cost,                 a.b_p_pax_gift,
              a.net_rev,               a.pcom_air_add_base_amt, a.pcom_ssr_bkg_amt,        a.ct_base_category_tariff,     a.b_p_service_fees,
              a.b_p_air_cost,          a.pcom_air_add_ovr_amt,  a.pcom_ssr_base_amt,       a.b_local_per_base_rate,       a.b_p_landex,
              a.b_p_seafare,           a.pcom_air_upg_bkg_amt,  a.pcom_ssr_ovr_amt,        a.net_ticket_revenue,          a.b_p_airdev_fee,
              a.b_p_land_charges,      a.pcom_air_upg_base_amt, a.pcom_tvl_prot_bkg_amt,   a.passenger_count,             a.b_p_gross,
              a.b_p_actual_air_cost,   a.pcom_air_upg_ovr_amt,  a.pcom_tvl_prot_base_amt,  a.rev_adj_act_ammenities,      a.rev_adj_proj_ovr_comm_amt,
              a.b_p_fcc_prot_comm,     a.pcom_air_dev_bkg_amt,  a.pcom_tvl_prot_ovr_amt,   a.rev_adj_act_gap,             a.company_code,
              a.b_p_air_fees,          a.pcom_air_dev_base_amt, a.pcom_svc_fee_bkg_amt,    a.rev_adj_cocktail_pty,        a.net_cruise_fare,
              a.b_p_onboard_credit_amt,a.pcom_air_dev_ovr_amt,  a.pcom_svc_fee_base_amt,   a.rev_adj_proj_gap,            a.net_cruise_fare_percent,
              a.b_p_port_charges,      a.pcom_pkgs_bkg_amt,     a.pcom_svc_fee_ovr_amt,    a.rev_adj_proj_tc,             a.net_tour_fare,
              a.b_p_govt_fees,         a.pcom_pkgs_base_amt,    a.pcom_cancel_bkg_amt,     a.b_p_air_fees_taxes_to,       a.ntr_fin_usd,
              a.b_p_discount,          a.pcom_pkgs_ovr_amt,     a.pcom_cancel_base_amt,    a.b_p_air_fees_taxes_fr,       a.ntr_sls_usd,
              a.b_p_packages,          a.pcom_xfer_bkg_amt,     a.pcom_cancel_bkg_ovr_amt, a.b_p_wholesaler_comm_portion, a.ntr_sls_bkng_curr,
              a.b_p_package_fees,      a.pcom_xfer_base_amt,    a.pcom_cancel_ovr_amt,     a.b_p_fcc_discount,            a.npr_usd,
              a.b_p_waiver,            a.pcom_xfer_ovr_amt,     a.pcom_bonus_comm_bkg_amt, a.b_category,                  a.ntr_fin_bkng_curr,
              a.b_p_land_price,        a.pcom_shr_ex_bkg_amt,   a.pcom_bonus_comm_fcc_amt, a.b_p_apo,                     a.ct_method,
              a.ct_band,               a.rev_adj_uk_air_fee_usd_curr ,                     a.pcom_ncfc_ovr_amt   ,        a.pcom_gov_fee_base_amt,
              a.pcom_vsa_fee_ovr_amt , a.rev_adj_uk_air_fee_bkng_curr,                     a.pcom_air_fee_base_amt,       a.pcom_gov_fee_ovr_amt,
              a.pcom_fuel_su_base_amt, a.pcom_ncfc_base_amt,    a.pcom_air_fee_ovr_amt,    a.pcom_vsa_fee_base_amt,       a.pcom_fuel_su_ovr_amt,
              a.cal_year,
  --           a.b_p_cruise_gst_au_amt,
  --           a.b_p_cruise_gst_nz_amt,
  --           a.b_p_air_gst_au_amt,
  --           a.b_p_air_gst_nz_amt,
              a.rev_adj_promo_item,
              a.rev_adj_gst_cruise,
              a.rev_adj_gst_air,
              dly.eob_data_date
         FROM bkng_psngr_audit a,
              bkng_psngr_daily dly
        WHERE a.curr_rec_ind='Y'
          AND a.expiration_date=to_date('12/31/9999', 'MM/DD/YYYY')
          AND dly.b_bkng_nbr=a.b_bkng_nbr
          AND dly.b_voyage=a.b_voyage
          AND dly.b_p_passenger_id=a.b_p_passenger_id
          AND dly.company_code=a.company_code
       MINUS
       SELECT
              d.bkng_id,
              d.b_bkng_nbr,            d.b_p_land_mods,         d.pcom_shr_ex_base_amt,    d.pcom_bonus_comm_amt,         d.b_p_fuel_surcharge,
              d.b_voyage,              d.std_land_cost,         d.pcom_shr_ex_ovr_amt,     d.b_p_fare,                    d.fuel_ta_fee,
              d.b_p_passenger_id,      d.b_p_air_add_on,        d.pcom_tmod_bkg_amt,       d.b_p_promo_sob,               d.blended_conversion_rate,
              d.source_system,         d.b_p_air_supplement,    d.pcom_tmod_base_amt,      d.tax_margin,                  d.b_p_tours_land_cost,
              d.b_p_use,               d.pcom_netf_bkg_amt,     d.pcom_tmod_ovr_amt,       d.tax_cost_amount,             d.b_p_waiver_flag,
              d.b_p_passenger_status,  d.pcom_netf_base_amt,    d.pcom_lmod_bkg_amt,       d.ct_supplemental_cost,        d.voyage_status,
              d.b_p_conc_cd,           d.pcom_netf_ovr_amt,     d.pcom_lmod_base_amt,      d.ct_supplemental_revenue,     d.rev_adj_act_caso,
              d.b_p_pax_type,          d.pcom_land_bkg_amt,     d.pcom_lmod_ovr_amt,       d.ct_ship_cost_amount,         d.rev_adj_act_loyalty_air_dev,
              d.b_p_berth,             d.pcom_land_base_amt,    d.pcom_ldev_bkg_amt,       d.ct_nda,                      d.b_p_land_dev_fee,
              d.b_p_count_for_tc_flag, d.pcom_land_ovr_amt,     d.pcom_ldev_base_amt,      d.ct_tax_rev,                  d.b_p_land_mods_fees,
              d.b_p_pcl_netrev,        d.pcom_air_add_bkg_amt,  d.pcom_ldev_ovr_amt,       d.ct_tax_cost,                 d.b_p_pax_gift,
              d.net_rev,               d.pcom_air_add_base_amt, d.pcom_ssr_bkg_amt,        d.ct_base_category_tariff,     d.b_p_service_fees,
              d.b_p_air_cost,          d.pcom_air_add_ovr_amt,  d.pcom_ssr_base_amt,       d.b_local_per_base_rate,       d.b_p_landex,
              d.b_p_seafare,           d.pcom_air_upg_bkg_amt,  d.pcom_ssr_ovr_amt,        d.net_ticket_revenue,          d.b_p_airdev_fee,
              d.b_p_land_charges,      d.pcom_air_upg_base_amt, d.pcom_tvl_prot_bkg_amt,   d.passenger_count,             d.b_p_gross,
              d.b_p_actual_air_cost,   d.pcom_air_upg_ovr_amt,  d.pcom_tvl_prot_base_amt,  d.rev_adj_act_ammenities,      d.rev_adj_proj_ovr_comm_amt,
              d.b_p_fcc_prot_comm,     d.pcom_air_dev_bkg_amt,  d.pcom_tvl_prot_ovr_amt,   d.rev_adj_act_gap,             d.company_code,
              d.b_p_air_fees,          d.pcom_air_dev_base_amt, d.pcom_svc_fee_bkg_amt,    d.rev_adj_cocktail_pty,        d.net_cruise_fare,
              d.b_p_onboard_credit_amt,d.pcom_air_dev_ovr_amt,  d.pcom_svc_fee_base_amt,   d.rev_adj_proj_gap,            d.net_cruise_fare_percent,
              d.b_p_port_charges,      d.pcom_pkgs_bkg_amt,     d.pcom_svc_fee_ovr_amt,    d.rev_adj_proj_tc,             d.net_tour_fare,
              d.b_p_govt_fees,         d.pcom_pkgs_base_amt,    d.pcom_cancel_bkg_amt,     d.b_p_air_fees_taxes_to,       d.ntr_fin_usd,
              d.b_p_discount,          d.pcom_pkgs_ovr_amt,     d.pcom_cancel_base_amt,    d.b_p_air_fees_taxes_fr,       d.ntr_sls_usd,
              d.b_p_packages,          d.pcom_xfer_bkg_amt,     d.pcom_cancel_bkg_ovr_amt, d.b_p_wholesaler_comm_portion, d.ntr_sls_bkng_curr,
              d.b_p_package_fees,      d.pcom_xfer_base_amt,    d.pcom_cancel_ovr_amt,     d.b_p_fcc_discount,            d.npr_usd,
              d.b_p_waiver,            d.pcom_xfer_ovr_amt,     d.pcom_bonus_comm_bkg_amt, d.b_category,                  d.ntr_fin_bkng_curr,
              d.b_p_land_price,        d.pcom_shr_ex_bkg_amt,   d.pcom_bonus_comm_fcc_amt, d.b_p_apo,                     d.ct_method,
              d.ct_band,               d.rev_adj_uk_air_fee_usd_curr ,                     d.pcom_ncfc_ovr_amt   ,        d.pcom_gov_fee_base_amt,
              d.pcom_vsa_fee_ovr_amt , d.rev_adj_uk_air_fee_bkng_curr,                     d.pcom_air_fee_base_amt,       d.pcom_gov_fee_ovr_amt,
              d.pcom_fuel_su_base_amt, d.pcom_ncfc_base_amt,    d.pcom_air_fee_ovr_amt,    d.pcom_vsa_fee_base_amt,       d.pcom_fuel_su_ovr_amt,
              d.cal_year,
 --             d.b_p_cruise_gst_au_amt,
 --             d.b_p_cruise_gst_nz_amt,
 --             d.b_p_air_gst_au_amt,
 --             d.b_p_air_gst_nz_amt,
              d.rev_adj_promo_item,
              d.rev_adj_gst_cruise,
              d.rev_adj_gst_air,
              d.eob_data_date
         FROM bkng_psngr_daily d) LOOP

           UPDATE bkng_psngr_audit
              SET expiration_date = trunc(rec.eob_data_date),
                  curr_rec_ind = 'N'
            WHERE b_bkng_nbr = rec.b_bkng_nbr
              AND b_voyage = rec.b_voyage
              AND b_p_passenger_id = rec.b_p_passenger_id
              AND company_code = rec.company_code
              AND curr_rec_ind = 'Y'
              AND expiration_date = to_date('12/31/9999', 'MM/DD/YYYY');

           INSERT INTO bkng_psngr_audit (
                staging_key ,
                batch_id   ,
                load_dt    ,
                curr_rec_ind,
                effective_date  ,
                expiration_date ,
                  -----Data Cols \/  ---
                eob_data_date,
                b_p_data_date,
                bkng_id,
                b_bkng_nbr,            b_p_land_mods,         pcom_shr_ex_base_amt,    pcom_bonus_comm_amt,         b_p_fuel_surcharge,
                b_voyage,              std_land_cost,         pcom_shr_ex_ovr_amt,     b_p_fare,                    fuel_ta_fee,
                b_p_passenger_id,      b_p_air_add_on,        pcom_tmod_bkg_amt,       b_p_promo_sob,               blended_conversion_rate,
                source_system,         b_p_air_supplement,    pcom_tmod_base_amt,      tax_margin,                  b_p_tours_land_cost,
                b_p_use,               pcom_netf_bkg_amt,     pcom_tmod_ovr_amt,       tax_cost_amount,             b_p_waiver_flag,
                b_p_passenger_status,  pcom_netf_base_amt,    pcom_lmod_bkg_amt,       ct_supplemental_cost,        voyage_status,
                b_p_conc_cd,           pcom_netf_ovr_amt,     pcom_lmod_base_amt,      ct_supplemental_revenue,     rev_adj_act_caso,
                b_p_pax_type,          pcom_land_bkg_amt,     pcom_lmod_ovr_amt,       ct_ship_cost_amount,         rev_adj_act_loyalty_air_dev,
                b_p_berth,             pcom_land_base_amt,    pcom_ldev_bkg_amt,       ct_nda,                      b_p_land_dev_fee,
                b_p_count_for_tc_flag, pcom_land_ovr_amt,     pcom_ldev_base_amt,      ct_tax_rev,                  b_p_land_mods_fees,
                b_p_pcl_netrev,        pcom_air_add_bkg_amt,  pcom_ldev_ovr_amt,       ct_tax_cost,                 b_p_pax_gift,
                net_rev,               pcom_air_add_base_amt, pcom_ssr_bkg_amt,        ct_base_category_tariff,     b_p_service_fees,
                b_p_air_cost,          pcom_air_add_ovr_amt,  pcom_ssr_base_amt,       b_local_per_base_rate,       b_p_landex,
                b_p_seafare,           pcom_air_upg_bkg_amt,  pcom_ssr_ovr_amt,        net_ticket_revenue,          b_p_airdev_fee,
                b_p_land_charges,      pcom_air_upg_base_amt, pcom_tvl_prot_bkg_amt,   passenger_count,             b_p_gross,
                b_p_actual_air_cost,   pcom_air_upg_ovr_amt,  pcom_tvl_prot_base_amt,  rev_adj_act_ammenities,      rev_adj_proj_ovr_comm_amt,
                b_p_fcc_prot_comm,     pcom_air_dev_bkg_amt,  pcom_tvl_prot_ovr_amt,   rev_adj_act_gap,             company_code,
                b_p_air_fees,          pcom_air_dev_base_amt, pcom_svc_fee_bkg_amt,    rev_adj_cocktail_pty,        net_cruise_fare,
                b_p_onboard_credit_amt,pcom_air_dev_ovr_amt,  pcom_svc_fee_base_amt,   rev_adj_proj_gap,            net_cruise_fare_percent,
                b_p_port_charges,      pcom_pkgs_bkg_amt,     pcom_svc_fee_ovr_amt,    rev_adj_proj_tc,             net_tour_fare,
                b_p_govt_fees,         pcom_pkgs_base_amt,    pcom_cancel_bkg_amt,     b_p_air_fees_taxes_to,       ntr_fin_usd,
                b_p_discount,          pcom_pkgs_ovr_amt,     pcom_cancel_base_amt,    b_p_air_fees_taxes_fr,       ntr_sls_usd,
                b_p_packages,          pcom_xfer_bkg_amt,     pcom_cancel_bkg_ovr_amt, b_p_wholesaler_comm_portion, ntr_sls_bkng_curr,
                b_p_package_fees,      pcom_xfer_base_amt,    pcom_cancel_ovr_amt,     b_p_fcc_discount,            npr_usd,
                b_p_waiver,            pcom_xfer_ovr_amt,     pcom_bonus_comm_bkg_amt, b_category,                  ntr_fin_bkng_curr,
                b_p_land_price,        pcom_shr_ex_bkg_amt,   pcom_bonus_comm_fcc_amt, b_p_apo,                     ct_method,
                ct_band,               rev_adj_uk_air_fee_usd_curr ,                   pcom_ncfc_ovr_amt   ,        pcom_gov_fee_base_amt,
                pcom_vsa_fee_ovr_amt , rev_adj_uk_air_fee_bkng_curr,                   pcom_air_fee_base_amt,       pcom_gov_fee_ovr_amt,
                pcom_fuel_su_base_amt, pcom_ncfc_base_amt,    pcom_air_fee_ovr_amt,    pcom_vsa_fee_base_amt,       pcom_fuel_su_ovr_amt,
                cal_year,
  --              b_p_cruise_gst_au_amt,
  --              b_p_cruise_gst_nz_amt,
  --              b_p_air_gst_au_amt,
  --              b_p_air_gst_nz_amt,
                rev_adj_promo_item,
                rev_adj_gst_cruise,
                rev_adj_gst_air
              )
              SELECT
                    bpa_seq.nextval,
                    vBatchID,
                    trunc(SYSDATE),
                    'Y',
                    trunc(d.eob_data_date),
                    to_date('12/31/9999', 'MM/DD/YYYY') ,  -- EOD
                    ---- data cols \/ ----
                    d.eob_data_date,
                    d.b_p_data_date,
                    d.bkng_id,
                    d.b_bkng_nbr,            d.b_p_land_mods,         d.pcom_shr_ex_base_amt,    d.pcom_bonus_comm_amt,         d.b_p_fuel_surcharge,
                    d.b_voyage,              d.std_land_cost,         d.pcom_shr_ex_ovr_amt,     d.b_p_fare,                    d.fuel_ta_fee,
                    d.b_p_passenger_id,      d.b_p_air_add_on,        d.pcom_tmod_bkg_amt,       d.b_p_promo_sob,               d.blended_conversion_rate,
                    d.source_system,         d.b_p_air_supplement,    d.pcom_tmod_base_amt,      d.tax_margin,                  d.b_p_tours_land_cost,
                    d.b_p_use,               d.pcom_netf_bkg_amt,     d.pcom_tmod_ovr_amt,       d.tax_cost_amount,             d.b_p_waiver_flag,
                    d.b_p_passenger_status,  d.pcom_netf_base_amt,    d.pcom_lmod_bkg_amt,       d.ct_supplemental_cost,        d.voyage_status,
                    d.b_p_conc_cd,           d.pcom_netf_ovr_amt,     d.pcom_lmod_base_amt,      d.ct_supplemental_revenue,     d.rev_adj_act_caso,
                    d.b_p_pax_type,          d.pcom_land_bkg_amt,     d.pcom_lmod_ovr_amt,       d.ct_ship_cost_amount,         d.rev_adj_act_loyalty_air_dev,
                    d.b_p_berth,             d.pcom_land_base_amt,    d.pcom_ldev_bkg_amt,       d.ct_nda,                      d.b_p_land_dev_fee,
                    d.b_p_count_for_tc_flag, d.pcom_land_ovr_amt,     d.pcom_ldev_base_amt,      d.ct_tax_rev,                  d.b_p_land_mods_fees,
                    d.b_p_pcl_netrev,        d.pcom_air_add_bkg_amt,  d.pcom_ldev_ovr_amt,       d.ct_tax_cost,                 d.b_p_pax_gift,
                    d.net_rev,               d.pcom_air_add_base_amt, d.pcom_ssr_bkg_amt,        d.ct_base_category_tariff,     d.b_p_service_fees,
                    d.b_p_air_cost,          d.pcom_air_add_ovr_amt,  d.pcom_ssr_base_amt,       d.b_local_per_base_rate,       d.b_p_landex,
                    d.b_p_seafare,           d.pcom_air_upg_bkg_amt,  d.pcom_ssr_ovr_amt,        d.net_ticket_revenue,          d.b_p_airdev_fee,
                    d.b_p_land_charges,      d.pcom_air_upg_base_amt, d.pcom_tvl_prot_bkg_amt,   d.passenger_count,             d.b_p_gross,
                    d.b_p_actual_air_cost,   d.pcom_air_upg_ovr_amt,  d.pcom_tvl_prot_base_amt,  d.rev_adj_act_ammenities,      d.rev_adj_proj_ovr_comm_amt,
                    d.b_p_fcc_prot_comm,     d.pcom_air_dev_bkg_amt,  d.pcom_tvl_prot_ovr_amt,   d.rev_adj_act_gap,             d.company_code,
                    d.b_p_air_fees,          d.pcom_air_dev_base_amt, d.pcom_svc_fee_bkg_amt,    d.rev_adj_cocktail_pty,        d.net_cruise_fare,
                    d.b_p_onboard_credit_amt,d.pcom_air_dev_ovr_amt,  d.pcom_svc_fee_base_amt,   d.rev_adj_proj_gap,            d.net_cruise_fare_percent,
                    d.b_p_port_charges,      d.pcom_pkgs_bkg_amt,     d.pcom_svc_fee_ovr_amt,    d.rev_adj_proj_tc,             d.net_tour_fare,
                    d.b_p_govt_fees,         d.pcom_pkgs_base_amt,    d.pcom_cancel_bkg_amt,     d.b_p_air_fees_taxes_to,       d.ntr_fin_usd,
                    d.b_p_discount,          d.pcom_pkgs_ovr_amt,     d.pcom_cancel_base_amt,    d.b_p_air_fees_taxes_fr,       d.ntr_sls_usd,
                    d.b_p_packages,          d.pcom_xfer_bkg_amt,     d.pcom_cancel_bkg_ovr_amt, d.b_p_wholesaler_comm_portion, d.ntr_sls_bkng_curr,
                    d.b_p_package_fees,      d.pcom_xfer_base_amt,    d.pcom_cancel_ovr_amt,     d.b_p_fcc_discount,            d.npr_usd,
                    d.b_p_waiver,            d.pcom_xfer_ovr_amt,     d.pcom_bonus_comm_bkg_amt, d.b_category,                  d.ntr_fin_bkng_curr,
                    d.b_p_land_price,        d.pcom_shr_ex_bkg_amt,   d.pcom_bonus_comm_fcc_amt, d.b_p_apo,                     d.ct_method,
                    d.ct_band,               d.rev_adj_uk_air_fee_usd_curr ,                     d.pcom_ncfc_ovr_amt   ,        d.pcom_gov_fee_base_amt,
                    d.pcom_vsa_fee_ovr_amt , d.rev_adj_uk_air_fee_bkng_curr,                     d.pcom_air_fee_base_amt,       d.pcom_gov_fee_ovr_amt,
                    d.pcom_fuel_su_base_amt, d.pcom_ncfc_base_amt,    d.pcom_air_fee_ovr_amt,    d.pcom_vsa_fee_base_amt,       d.pcom_fuel_su_ovr_amt,
                    d.cal_year,
   --                 d.b_p_cruise_gst_au_amt,
   --                 d.b_p_cruise_gst_nz_amt,
   --                 d.b_p_air_gst_au_amt,
   --                 d.b_p_air_gst_nz_amt,
                    d.rev_adj_promo_item,
                    d.rev_adj_gst_cruise,
                    d.rev_adj_gst_air
                 FROM bkng_psngr_daily d
                WHERE d.b_bkng_nbr=rec.b_bkng_nbr
                  AND d.b_voyage=rec.b_voyage
                  AND d.b_p_passenger_id=rec.b_p_passenger_id
                  AND d.company_code=rec.company_code
                    ;

         vRowCount :=SQL%ROWCOUNT + vRowCount;

      END LOOP;

      sComment:=   ' Updates '||vRowCount;

      vDummy := vRowCount;

      -- Step 2 Insert New
      vRowCount := 0;

    INSERT INTO bkng_psngr_audit (
                staging_key ,
                batch_id   ,
                load_dt    ,
                curr_rec_ind,
                effective_date  ,
                expiration_date ,
                -----Data Cols \/  ---
                eob_data_date,
                b_p_data_date,
                bkng_id,
                b_bkng_nbr,            b_p_land_mods,         pcom_shr_ex_base_amt,    pcom_bonus_comm_amt,         b_p_fuel_surcharge,
                b_voyage,              std_land_cost,         pcom_shr_ex_ovr_amt,     b_p_fare,                    fuel_ta_fee,
                b_p_passenger_id,      b_p_air_add_on,        pcom_tmod_bkg_amt,       b_p_promo_sob,               blended_conversion_rate,
                source_system,         b_p_air_supplement,    pcom_tmod_base_amt,      tax_margin,                  b_p_tours_land_cost,
                b_p_use,               pcom_netf_bkg_amt,     pcom_tmod_ovr_amt,       tax_cost_amount,             b_p_waiver_flag,
                b_p_passenger_status,  pcom_netf_base_amt,    pcom_lmod_bkg_amt,       ct_supplemental_cost,        voyage_status,
                b_p_conc_cd,           pcom_netf_ovr_amt,     pcom_lmod_base_amt,      ct_supplemental_revenue,     rev_adj_act_caso,
                b_p_pax_type,          pcom_land_bkg_amt,     pcom_lmod_ovr_amt,       ct_ship_cost_amount,         rev_adj_act_loyalty_air_dev,
                b_p_berth,             pcom_land_base_amt,    pcom_ldev_bkg_amt,       ct_nda,                      b_p_land_dev_fee,
                b_p_count_for_tc_flag, pcom_land_ovr_amt,     pcom_ldev_base_amt,      ct_tax_rev,                  b_p_land_mods_fees,
                b_p_pcl_netrev,        pcom_air_add_bkg_amt,  pcom_ldev_ovr_amt,       ct_tax_cost,                 b_p_pax_gift,
                net_rev,               pcom_air_add_base_amt, pcom_ssr_bkg_amt,        ct_base_category_tariff,     b_p_service_fees,
                b_p_air_cost,          pcom_air_add_ovr_amt,  pcom_ssr_base_amt,       b_local_per_base_rate,       b_p_landex,
                b_p_seafare,           pcom_air_upg_bkg_amt,  pcom_ssr_ovr_amt,        net_ticket_revenue,          b_p_airdev_fee,
                b_p_land_charges,      pcom_air_upg_base_amt, pcom_tvl_prot_bkg_amt,   passenger_count,             b_p_gross,
                b_p_actual_air_cost,   pcom_air_upg_ovr_amt,  pcom_tvl_prot_base_amt,  rev_adj_act_ammenities,      rev_adj_proj_ovr_comm_amt,
                b_p_fcc_prot_comm,     pcom_air_dev_bkg_amt,  pcom_tvl_prot_ovr_amt,   rev_adj_act_gap,             company_code,
                b_p_air_fees,          pcom_air_dev_base_amt, pcom_svc_fee_bkg_amt,    rev_adj_cocktail_pty,        net_cruise_fare,
                b_p_onboard_credit_amt,pcom_air_dev_ovr_amt,  pcom_svc_fee_base_amt,   rev_adj_proj_gap,            net_cruise_fare_percent,
                b_p_port_charges,      pcom_pkgs_bkg_amt,     pcom_svc_fee_ovr_amt,    rev_adj_proj_tc,             net_tour_fare,
                b_p_govt_fees,         pcom_pkgs_base_amt,    pcom_cancel_bkg_amt,     b_p_air_fees_taxes_to,       ntr_fin_usd,
                b_p_discount,          pcom_pkgs_ovr_amt,     pcom_cancel_base_amt,    b_p_air_fees_taxes_fr,       ntr_sls_usd,
                b_p_packages,          pcom_xfer_bkg_amt,     pcom_cancel_bkg_ovr_amt, b_p_wholesaler_comm_portion, ntr_sls_bkng_curr,
                b_p_package_fees,      pcom_xfer_base_amt,    pcom_cancel_ovr_amt,     b_p_fcc_discount,            npr_usd,
                b_p_waiver,            pcom_xfer_ovr_amt,     pcom_bonus_comm_bkg_amt, b_category,                  ntr_fin_bkng_curr,
                b_p_land_price,        pcom_shr_ex_bkg_amt,   pcom_bonus_comm_fcc_amt, b_p_apo,                     ct_method,
                ct_band,               rev_adj_uk_air_fee_usd_curr ,                   pcom_ncfc_ovr_amt   ,        pcom_gov_fee_base_amt,
                pcom_vsa_fee_ovr_amt , rev_adj_uk_air_fee_bkng_curr,                   pcom_air_fee_base_amt,       pcom_gov_fee_ovr_amt,
                pcom_fuel_su_base_amt, pcom_ncfc_base_amt,    pcom_air_fee_ovr_amt,    pcom_vsa_fee_base_amt,       pcom_fuel_su_ovr_amt,
                cal_year,
 --               b_p_cruise_gst_au_amt,
 --               b_p_cruise_gst_nz_amt,
 --               b_p_air_gst_au_amt,
 --               b_p_air_gst_nz_amt,
                rev_adj_promo_item,
                rev_adj_gst_cruise,
                rev_adj_gst_air
                )
            Select
                    bpa_seq.nextval,
                    vBatchID,
                    trunc(SYSDATE),
                    'Y',
                    trunc(eob_data_date),
                    to_date('12/31/9999', 'MM/DD/YYYY') , -- EOD 12/31/9999
                    -- data cols \/
                    d.eob_data_date,
                    d.b_p_data_date,
                    d.bkng_id,
                    d.b_bkng_nbr,            d.b_p_land_mods,         d.pcom_shr_ex_base_amt,    d.pcom_bonus_comm_amt,         d.b_p_fuel_surcharge,
                    d.b_voyage,              d.std_land_cost,         d.pcom_shr_ex_ovr_amt,     d.b_p_fare,                    d.fuel_ta_fee,
                    d.b_p_passenger_id,      d.b_p_air_add_on,        d.pcom_tmod_bkg_amt,       d.b_p_promo_sob,               d.blended_conversion_rate,
                    d.source_system,         d.b_p_air_supplement,    d.pcom_tmod_base_amt,      d.tax_margin,                  d.b_p_tours_land_cost,
                    d.b_p_use,               d.pcom_netf_bkg_amt,     d.pcom_tmod_ovr_amt,       d.tax_cost_amount,             d.b_p_waiver_flag,
                    d.b_p_passenger_status,  d.pcom_netf_base_amt,    d.pcom_lmod_bkg_amt,       d.ct_supplemental_cost,        d.voyage_status,
                    d.b_p_conc_cd,           d.pcom_netf_ovr_amt,     d.pcom_lmod_base_amt,      d.ct_supplemental_revenue,     d.rev_adj_act_caso,
                    d.b_p_pax_type,          d.pcom_land_bkg_amt,     d.pcom_lmod_ovr_amt,       d.ct_ship_cost_amount,         d.rev_adj_act_loyalty_air_dev,
                    d.b_p_berth,             d.pcom_land_base_amt,    d.pcom_ldev_bkg_amt,       d.ct_nda,                      d.b_p_land_dev_fee,
                    d.b_p_count_for_tc_flag, d.pcom_land_ovr_amt,     d.pcom_ldev_base_amt,      d.ct_tax_rev,                  d.b_p_land_mods_fees,
                    d.b_p_pcl_netrev,        d.pcom_air_add_bkg_amt,  d.pcom_ldev_ovr_amt,       d.ct_tax_cost,                 d.b_p_pax_gift,
                    d.net_rev,               d.pcom_air_add_base_amt, d.pcom_ssr_bkg_amt,        d.ct_base_category_tariff,     d.b_p_service_fees,
                    d.b_p_air_cost,          d.pcom_air_add_ovr_amt,  d.pcom_ssr_base_amt,       d.b_local_per_base_rate,       d.b_p_landex,
                    d.b_p_seafare,           d.pcom_air_upg_bkg_amt,  d.pcom_ssr_ovr_amt,        d.net_ticket_revenue,          d.b_p_airdev_fee,
                    d.b_p_land_charges,      d.pcom_air_upg_base_amt, d.pcom_tvl_prot_bkg_amt,   d.passenger_count,             d.b_p_gross,
                    d.b_p_actual_air_cost,   d.pcom_air_upg_ovr_amt,  d.pcom_tvl_prot_base_amt,  d.rev_adj_act_ammenities,      d.rev_adj_proj_ovr_comm_amt,
                    d.b_p_fcc_prot_comm,     d.pcom_air_dev_bkg_amt,  d.pcom_tvl_prot_ovr_amt,   d.rev_adj_act_gap,             d.company_code,
                    d.b_p_air_fees,          d.pcom_air_dev_base_amt, d.pcom_svc_fee_bkg_amt,    d.rev_adj_cocktail_pty,        d.net_cruise_fare,
                    d.b_p_onboard_credit_amt,d.pcom_air_dev_ovr_amt,  d.pcom_svc_fee_base_amt,   d.rev_adj_proj_gap,            d.net_cruise_fare_percent,
                    d.b_p_port_charges,      d.pcom_pkgs_bkg_amt,     d.pcom_svc_fee_ovr_amt,    d.rev_adj_proj_tc,             d.net_tour_fare,
                    d.b_p_govt_fees,         d.pcom_pkgs_base_amt,    d.pcom_cancel_bkg_amt,     d.b_p_air_fees_taxes_to,       d.ntr_fin_usd,
                    d.b_p_discount,          d.pcom_pkgs_ovr_amt,     d.pcom_cancel_base_amt,    d.b_p_air_fees_taxes_fr,       d.ntr_sls_usd,
                    d.b_p_packages,          d.pcom_xfer_bkg_amt,     d.pcom_cancel_bkg_ovr_amt, d.b_p_wholesaler_comm_portion, d.ntr_sls_bkng_curr,
                    d.b_p_package_fees,      d.pcom_xfer_base_amt,    d.pcom_cancel_ovr_amt,     d.b_p_fcc_discount,            d.npr_usd,
                    d.b_p_waiver,            d.pcom_xfer_ovr_amt,     d.pcom_bonus_comm_bkg_amt, d.b_category,                  d.ntr_fin_bkng_curr,
                    d.b_p_land_price,        d.pcom_shr_ex_bkg_amt,   d.pcom_bonus_comm_fcc_amt, d.b_p_apo,                     d.ct_method,
                    d.ct_band,               d.rev_adj_uk_air_fee_usd_curr ,                     d.pcom_ncfc_ovr_amt   ,        d.pcom_gov_fee_base_amt,
                    d.pcom_vsa_fee_ovr_amt , d.rev_adj_uk_air_fee_bkng_curr,                     d.pcom_air_fee_base_amt,       d.pcom_gov_fee_ovr_amt,
                    d.pcom_fuel_su_base_amt, d.pcom_ncfc_base_amt,    d.pcom_air_fee_ovr_amt,    d.pcom_vsa_fee_base_amt,       d.pcom_fuel_su_ovr_amt,
                    d.cal_year,
   --                 d.b_p_cruise_gst_au_amt,
   --                 d.b_p_cruise_gst_nz_amt,
   --                 d.b_p_air_gst_au_amt,
   --                 d.b_p_air_gst_nz_amt,
                    d.rev_adj_promo_item,
                    d.rev_adj_gst_cruise,
                    d.rev_adj_gst_air
               FROM bkng_psngr_daily d
              WHERE NOT EXISTS (SELECT 1
                                  FROM  bkng_psngr_audit a
                                 WHERE d.b_bkng_nbr=a.b_bkng_nbr
                                   AND d.b_voyage=a.b_voyage
                                   AND d.b_p_passenger_id=a.b_p_passenger_id
                                   AND d.company_code=a.company_code);

      vRowCount := SQL%ROWCOUNT ;
      sComment:= sComment||' Inserts '||vRowCount;

      vEndDate := SYSDATE ;  -- set end date to sysdate

     -- Record Completion in Log

      IF COMMON_JOBS.LOG_JOB (
         io_Batch_ID => vBatchID,
         in_Prog_Status => 'C',
         in_Comments => sComment,
         in_Error_Message => 'NA'
         ) != 0 THEN

         RAISE eCALL_FAILED ;
      END IF;

      -- Record Completion in Control File
      IF COMMON_JOBS.LOG_PROC_CTRL (
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
       IF COMMON_JOBS.LOG_AUDIT_RESULT (
          in_Batch_ID => vBatchID,
          in_Audit_Type => cProgram_NM,
          in_Load_Type  => 'D',
          in_Start_Date  => vStartDate,
          in_End_Date =>  SYSDATE,
          in_Source_Cnt => 0,
          in_Target_Cnt => 0,
          in_Audit_OK => 0,
          in_Audit_Comments => sComment,
          in_Insert_Count =>  vRowCount,
          in_Update_Count  => vDummy,
          in_Delete_Count => 0  ) != 0
        THEN
            RAISE eCALL_FAILED ;
        END IF;

      EXCEPTION

      WHEN eINCONSISTENCY THEN
         ROLLBACK ;

         -- Record Error
         vDummy := COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID,
            in_Prog_Status => 'F',
            in_Comments => 'Job Failed',
            in_Error_Message => eErrMsg
         );

         vDummy := COMMON_JOBS.LOG_PROC_CTRL (
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

         -- Record Error against palceholder w/ batch of -1 since batch not recorded
         vDummy := COMMON_JOBS.LOG_JOB (
            io_Batch_ID => vBatchID,
            in_Prog_Status => 'X',
            in_Comments => 'Job Failed logged to placeholder  ',
            in_Error_Message => eErrMsg
         );

         vDummy := COMMON_JOBS.LOG_PROC_CTRL (
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
         vDummy := COMMON_JOBS.LOG_JOB (
            io_Batch_ID  => vBatchID,
            in_Prog_Status => 'F',
            in_Comments => 'Job Failed',
            in_Error_Message => eErrMsg
         );

         vDummy := COMMON_JOBS.LOG_PROC_CTRL (
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

   END Update_Bkng_Psngr_Audit;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------

END  Bkng_Psngr_Audit_Pkg ;
/