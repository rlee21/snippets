CREATE OR REPLACE PACKAGE BODY DM_TKR.RDM_LOAD AS
/***********************************************************************************************************
   NAME:       RDM_LOAD
   PURPOSE:  PL/SQL code to Load RDM data marts

   REVISIONS:
   Ver        Date             Author          Description
   ---------  ----------  --------------  -----------------------------------------------------------------
  
*************************************************************************************************************/

    /* Package Variables */
    cSchema_NM    CONSTANT Varchar2(15) := 'DM_TKR';
    cPackage_NM    CONSTANT Varchar2(50) := 'RDM_LOAD';      
    eErrMsg            Varchar2(500);
    eErrNo              Number ;
    dBeg_Date         Date := '01-Jan-1900';
    dEnd_Date         Date := '01-Jan-1900';
    sComment         Varchar2(2000); 
    dStartTime        Date;     
    eCALL_FAILED    Exception;
    eBAD_PARM       Exception; 
    x                       Number ;
    eNO_INDEX         Exception;  
    PRAGMA exception_init(eNO_INDEX,-1418);
    eINCONSISTENCY Exception; 

   PROCEDURE START_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'START_LOAD';
        BatchID Number := 0;
         
  BEGIN
  
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
   sComment  := 'Revenue Data Mart load has started ';
        
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            COMMIT; 
            RAISE;             
        
END START_LOAD;    
        
   PROCEDURE DATE_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'DATE_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_date_d';              

        INSERT INTO rdm_date_d
        (
        eob_data_date,
        sail_year,
        rpt_year,
        eob_data_date_stly,
        eob_data_date_prior_year,
        eob_data_date_prior_friday
        )        
        SELECT eob_data_date,
          TO_CHAR (eob_data_date, 'YYYY') AS sail_year,
            TO_CHAR (
                             CASE
                                     WHEN TO_NUMBER (TO_CHAR (eob_data_date, 'MM')) = 12
                                        THEN TO_NUMBER (TO_CHAR (eob_data_date, 'YYYY')) + 1
                             ELSE  TO_NUMBER (TO_CHAR (eob_data_date, 'YYYY'))
                             END)
          AS rpt_year,
          (eob_data_date - 364) AS eob_data_date_stly,
          (eob_data_date - 365) AS eob_data_date_py,
          TO_DATE (
             eob_data_date
             - (DECODE (TO_CHAR (eob_data_date, 'D'),
                        1, 2,
                        2, 3,
                        3, 4,
                        4, 5,
                        5, 6,
                        6, 7,
                        7, 1,
                        0)))
             AS eob_data_date_prior_friday
     FROM dw_system_parms@tstdwh1_halw_dwh;
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_date_d Inserts: '||x;
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END DATE_LOAD;    
   PROCEDURE GROUP_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'GROUP_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF; 

        DELETE FROM rdm_group_d
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        COMMIT;
         
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_group_d NOLOGGING';
               
--        EXECUTE IMMEDIATE 'ALTER TABLE group_rates NOLOGGING';
--        EXECUTE IMMEDIATE 'TRUNCATE TABLE group_rates'; 
        
        INSERT /*+ append */  INTO rdm_group_d
        (
        group_id,
        company_code,
        voyage,
        group_code,
        rpt_year,
        group_agency_nbr,
        group_type,
        group_status,
        group_sail_date,
        group_open_date,
        group_cancel_date,
        group_name,
        group_currency_code,
        group_tour_company_code,
        group_tour_tdef_id,
        group_open_userid,
        cruise_lower_beds_offered,
        cruise_lower_beds_contracted,
        cruise_lower_beds_forecasted,
        cruise_lower_beds_booked,
        cruise_guests_booked,
        tour_lower_beds_offered,
        tour_lower_beds_contracted,
        tour_lower_beds_forecasted,
        tour_lower_beds_booked,
        tour_guests_booked,
        total_lower_beds_offered,
        total_lower_beds_offered_lw,
        total_lower_beds_contracted,
        total_lower_beds_forecasted,
        total_lower_beds_booked,
        total_guests_booked,
        auto_refund_flag,
        group_left_ear,
        group_right_ear,
        unapplied_receipts,
        multi_agency_flag,
        group_seating_confirm_flag,
        group_final_paymt_date,
        gross_receipts,
        group_concession_code,
        group_tc_guest_count,
        group_deposit_waived_flag,
        tour_conductor_type,
        cocktail_party_code,
        group_recall_date_1,
        group_recall_date_2,
        group_recall_date_3,
        group_tour_id,
        group_affinity_type,
        tour_conductor_ratio,
        amenity_points_max,
        amenity_points_used,
        group_exclusive_flag,
        group_transmit_mode,
        group_finalized_flag,
        agent_first_name,
        agent_last_name,
        tc_application_done_flg,
        tc_avg_gross_amt,
        tc_nbr_earned,
        tc_guests_booked,
        contract_print_dt,
        deposit1_due_dt,
        deposit2_due_dt,
        deposit3_due_dt,
        deposit1_amt,
        deposit2_amt,
        deposit3_amt,
        dining_seats_blocked,
        dining_seats_booked,
        group_open_user_name
        )        
        WITH tdate AS (SELECT eob_data_date AS rptdate FROM dw_system_parms@tstdwh1_halw_dwh),
        t1 as (   
         select   
            company_code,
            gdin_voyage,
            gdin_grp_id,
            sum(gdin_grp_block) as gdin_grp_block,
            sum(gdin_grp_book) as gdin_grp_book
         from group_dining@tstdwh1_halw_dwh
         group by
            company_code,
            gdin_voyage,
            gdin_grp_id)    
         SELECT 
               gm.grm_voyage || gm.grm_group_code AS GROUP_ID,
                gm.company_code AS company_code,
                gm.grm_voyage AS voyage,
                gm.grm_group_code AS group_code,
                vm.rpt_year AS rpt_year,
                gm.grm_agent_code AS group_agency_nbr,
                gm.grm_group_type AS group_type,
                gm.grm_status_of_group AS group_status,
                gm.grm_sailing_date AS group_sail_date,
                gm.grm_open_date AS group_open_date,
                gm.grm_cancel_date AS group_cancel_date,
                gm.grm_group_name AS group_name,
                gm.grm_currency_code AS group_currency_code,
                gm.grm_tour_company_code AS group_tour_company_code,
                gm.grm_tour_tdef_id AS group_tour_tdef_id,
                gm.grm_open_userid AS group_open_userid,
                CASE
                   WHEN gm.grm_tour_company_code IS NULL THEN gm.grm_total_offered
                   ELSE 0
                END
                   AS CRUISE_LOWER_BEDS_OFFERED,
                CASE
                   WHEN gm.grm_tour_company_code IS NULL
                   THEN
                      gm.grm_total_contracted
                   ELSE
                      0
                END
                   AS CRUISE_LOWER_BEDS_CONTRACTED,
                CASE
                   WHEN gm.grm_tour_company_code IS NULL THEN gm.grm_total_forecast
                   ELSE 0
                END
                   AS CRUISE_LOWER_BEDS_FORECASTED,
                CASE
                   WHEN gm.grm_tour_company_code IS NULL THEN gm.grm_total_booked
                   ELSE 0
                END
                   AS CRUISE_LOWER_BEDS_BOOKED,
                CASE
                   WHEN gm.grm_tour_company_code IS NULL THEN gm.grm_pgrs_booked
                   ELSE 0
                END
                   AS CRUISE_GUESTS_BOOKED,
                CASE
                   WHEN gm.grm_tour_company_code IS NOT NULL
                   THEN
                      gm.grm_total_offered
                   ELSE
                      0
                END
                   AS TOUR_LOWER_BEDS_OFFERED,
                CASE
                   WHEN gm.grm_tour_company_code IS NOT NULL
                   THEN
                      gm.grm_total_contracted
                   ELSE
                      0
                END
                   AS TOUR_LOWER_BEDS_CONTRACTED,
                CASE
                   WHEN gm.grm_tour_company_code IS NOT NULL
                   THEN
                      gm.grm_total_forecast
                   ELSE
                      0
                END
                   AS TOUR_LOWER_BEDS_FORECASTED,
                CASE
                   WHEN gm.grm_tour_company_code IS NOT NULL
                   THEN
                      gm.grm_total_booked
                   ELSE
                      0
                END
                   AS TOUR_LOWER_BEDS_BOOKED,
                CASE
                   WHEN gm.grm_tour_company_code IS NOT NULL
                   THEN
                      gm.grm_pgrs_booked
                   ELSE
                      0
                END
                   AS TOUR_GUESTS_BOOKED,
                gm.grm_total_offered AS TOTAL_LOWER_BEDS_OFFERED,
                NVL (
                   SUM (
                      CASE
                         WHEN gma.effective_date <= rptdate - 7
                              AND gma.expiration_date > rptdate - 7
                         THEN
                            gma.grm_total_offered
                         ELSE
                            0
                      END),
                   0)
                   TOTAL_LOWER_BEDS_OFFERED_LW,
                gm.grm_total_contracted AS total_lower_beds_contracted,
                gm.grm_total_forecast AS total_lower_beds_forecasted,
                gm.grm_total_booked AS total_lower_beds_booked,
                gm.grm_pgrs_booked AS total_guests_booked,
                gm.grm_auto_refund_flag AS auto_refund_flag,
                gm.grm_left_ear AS group_left_ear,
                gm.grm_right_ear AS group_right_ear,
                gm.grm_unapplied_receipts AS unapplied_receipts,
                gm.grm_multi_agency_flag AS multi_agency_flag,
                gm.grm_seating_confirm_flag AS group_seating_confirm_flag,
                gm.grm_final_payment_date AS group_final_paymt_date,
                gm.grm_gross_receipts AS gross_receipts,
                gm.grm_concession_code AS group_concession_code,
                gm.grm_tour_cond_pax_cnt AS group_tc_guest_count,
                gm.grm_deposit_waived_flag AS group_deposit_waived_flag,
                gm.grm_tour_conductor_type AS tour_conductor_type,
                gm.grm_cocktail_party_flag AS cocktail_party_code,
                gm.grm_recall_date_1 AS group_recall_date_1,
                gm.grm_recall_date_2 AS group_recall_date_2,
                gm.grm_recall_date_3 AS group_recall_date_3,
                gm.grm_tour_unique AS group_tour_id,
                gm.grm_affinity_type AS group_affinity_type,
                gm.grm_tour_conductor_ratio AS tour_conductor_ratio,
                gm.grm_max_amenity_pts AS amenity_points_max,
                gm.grm_used_amenity_pts AS amenity_points_used,
                gm.grm_exclusive_flag AS group_exclusive_flag,
                gm.grm_transmit_mode AS group_transmit_mode,
                gm.grm_finalized_flag AS group_finalized_flag,
                gm.grm_agent_forename as agent_first_name,
                gm.grm_agent_surname as agent_last_name,
                gm.grm_btch_tc_appl_done_flag as tc_application_done_flg,
                gm.grm_capture_tc_avg_grs_amt as tc_avg_gross_amt,
                gm.grm_capture_tc_earned as tc_nbr_earned,
                gm.grm_capture_tc_pgrs_booked as tc_guests_booked,
                gm.grm_contract_print_date as contract_print_dt,
                gm.grm_deposit_option_date1 as deposit1_due_dt,
                gm.grm_deposit_option_date2 as deposit2_due_dt,
                gm.grm_deposit_option_date3 as deposit3_due_dt,
                gm.grm_deposit_requested1 as deposit1_amt,
                gm.grm_deposit_requested2 as deposit2_amt,
                gm.grm_deposit_requested3 as deposit3_amt,
                NVL(t1.gdin_grp_block,0) as dining_seats_blocked,
                NVL(t1.gdin_grp_book,0) as dining_seats_booked,
                ud.user_name as group_open_user_name
           FROM group_mstr@tstdwh1_halw_dwh gm,
                group_mstr_audit@tstdwh1_halw_dwh gma,
                voyage_mstr@tstdwh1_halw_dwh vm,
                user_details@tstdwh1_halw_dwh ud,
                tdate,
                t1
          WHERE   gm.grm_voyage = gma.grm_voyage
                AND gm.grm_group_code = gma.grm_group_code
                AND gm.company_code = gma.company_code
                AND gm.grm_voyage = vm.vyd_voyage
                AND gm.company_code = vm.company_code
                AND gm.grm_open_userid = ud.userid_polar(+)
                AND gm.company_code = t1.company_code(+)
                AND gm.grm_voyage = t1.gdin_voyage(+)
                AND gm.grm_group_code = t1.gdin_grp_id(+)
                AND vm.rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL)            
                AND ( (gma.effective_date <= rptdate
                       AND gma.expiration_date > rptdate)
                     OR (gma.effective_date <= rptdate - 7
                         AND gma.expiration_date > rptdate - 7))
       GROUP BY gm.grm_voyage, gm.grm_group_code, gm.company_code, vm.rpt_year, gm.grm_agent_code, gm.grm_group_type,
                      gm.grm_status_of_group, gm.grm_sailing_date, gm.grm_open_date, gm.grm_cancel_date, gm.grm_group_name, gm.grm_currency_code,
                      gm.grm_tour_company_code, gm.grm_tour_tdef_id, gm.grm_open_userid, gm.grm_total_offered, gm.grm_total_contracted, gm.grm_total_forecast,
                      gm.grm_total_booked, gm.grm_pgrs_booked, gm.grm_auto_refund_flag, gm.grm_left_ear, gm.grm_right_ear, gm.grm_unapplied_receipts,
                      gm.grm_multi_agency_flag, gm.grm_seating_confirm_flag, gm.grm_final_payment_date, gm.grm_gross_receipts, gm.grm_concession_code,
                      gm.grm_tour_cond_pax_cnt, gm.grm_deposit_waived_flag, gm.grm_tour_conductor_type, gm.grm_cocktail_party_flag, gm.grm_recall_date_1,
                     gm.grm_recall_date_2, gm.grm_recall_date_3, gm.grm_tour_unique, gm.grm_affinity_type, gm.grm_tour_conductor_ratio, gm.grm_max_amenity_pts,
                     gm.grm_used_amenity_pts, gm.grm_exclusive_flag, gm.grm_transmit_mode, gm.grm_finalized_flag, gm.grm_agent_forename, gm.grm_agent_surname,
                     gm.grm_btch_tc_appl_done_flag, gm.grm_capture_tc_avg_grs_amt, gm.grm_capture_tc_earned, gm.grm_capture_tc_pgrs_booked, gm.grm_contract_print_date, 
                     gm.grm_deposit_option_date1, gm.grm_deposit_option_date2, gm.grm_deposit_option_date3, gm.grm_deposit_requested1, gm.grm_deposit_requested2,
                     gm.grm_deposit_requested3, t1.gdin_grp_block, t1.gdin_grp_book, ud.user_name;
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_group_d Inserts: '||x;  
       
--        INSERT INTO group_rates
--        SELECT * FROM dm_tkr.group_rates_tst@tstdwh1_halw_dwh; 
--        
--        x:=SQL%ROWCOUNT; 
--        sComment:= sComment||' group_rates Inserts: '||x;         
--
--        EXECUTE IMMEDIATE 'ALTER TABLE rdm_group_d LOGGING';
--        EXECUTE IMMEDIATE 'ALTER TABLE group_rates LOGGING';                 

        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END GROUP_LOAD;                

   PROCEDURE GROUP_HIST_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'GROUP_HIST_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF; 

        DELETE FROM rdm_group_hist_f
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        COMMIT;
         
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_group_hist_f NOLOGGING';
        
        INSERT /*+ append */  INTO rdm_group_hist_f
        (
            group_id,
            company_code,
            voyage,
            group_code,
            rpt_year,
            lower_beds_offered,
            lower_beds_contracted,
            lower_beds_forecasted,
            lower_beds_booked,
            lower_beds_offered_7,
            lower_beds_offered_364,
            lower_beds_contracted_364,
            lower_beds_forecasted_364,
            lower_beds_booked_364,
            lower_beds_offered_371,
            lower_beds_contracted_371,
            lower_beds_forecasted_371,
            lower_beds_booked_371,
            lower_beds_offered_728,
            lower_beds_contracted_728,
            lower_beds_forecasted_728,
            lower_beds_booked_728,
            lower_beds_offered_735,
            lower_beds_contracted_735,
            lower_beds_forecasted_735,
            lower_beds_booked_735,
            lower_beds_offered_fri
        )
        WITH tdate AS (SELECT eob_data_date, prior_friday FROM dw_system_parms@tstdwh1_halw_dwh)
             SELECT                                             /*+ parallel(gma,8) */
                   gma.grm_voyage || gma.grm_group_code AS GROUP_ID,
                    gma.company_code AS COMPANY_CODE,
                    gma.grm_voyage AS VOYAGE,
                    gma.grm_group_code AS GROUP_CODE,
                    vm.rpt_year AS RPT_YEAR,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date
                                  AND gma.expiration_date > eob_data_date
                             THEN
                                gma.grm_total_offered
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_OFFERED,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date
                                  AND gma.expiration_date > eob_data_date
                             THEN
                                gma.grm_total_contracted
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_CONTRACTED,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date
                                  AND gma.expiration_date > eob_data_date
                             THEN
                                gma.grm_total_forecast
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_FORECASTED,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date
                                  AND gma.expiration_date > eob_data_date
                             THEN
                                gma.grm_total_booked
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_BOOKED,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 7
                                  AND gma.expiration_date > eob_data_date - 7
                             THEN
                                gma.grm_total_offered
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_OFFERED_7,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 364
                                  AND gma.expiration_date > eob_data_date - 364
                             THEN
                                gma.grm_total_offered
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_OFFERED_364,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 364
                                  AND gma.expiration_date > eob_data_date - 364
                             THEN
                                gma.grm_total_contracted
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_CONTRACTED_364,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 364
                                  AND gma.expiration_date > eob_data_date - 364
                             THEN
                                gma.grm_total_forecast
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_FORECASTED_364,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 364
                                  AND gma.expiration_date > eob_data_date - 364
                             THEN
                                gma.grm_total_booked
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_BOOKED_364,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 371
                                  AND gma.expiration_date > eob_data_date - 371
                             THEN
                                gma.grm_total_offered
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_OFFERED_371,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 371
                                  AND gma.expiration_date > eob_data_date - 371
                             THEN
                                gma.grm_total_contracted
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_CONTRACTED_371,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 371
                                  AND gma.expiration_date > eob_data_date - 371
                             THEN
                                gma.grm_total_forecast
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_FORECASTED_371,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 371
                                  AND gma.expiration_date > eob_data_date - 371
                             THEN
                                gma.grm_total_booked
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_BOOKED_371,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 728
                                  AND gma.expiration_date > eob_data_date - 728
                             THEN
                                gma.grm_total_offered
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_OFFERED_728,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 728
                                  AND gma.expiration_date > eob_data_date - 728
                             THEN
                                gma.grm_total_contracted
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_CONTRACTED_728,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 728
                                  AND gma.expiration_date > eob_data_date - 728
                             THEN
                                gma.grm_total_forecast
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_FORECASTED_728,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 728
                                  AND gma.expiration_date > eob_data_date - 728
                             THEN
                                gma.grm_total_booked
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_BOOKED_728,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 735
                                  AND gma.expiration_date > eob_data_date - 735
                             THEN
                                gma.grm_total_offered
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_OFFERED_735,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 735
                                  AND gma.expiration_date > eob_data_date - 735
                             THEN
                                gma.grm_total_contracted
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_CONTRACTED_735,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 735
                                  AND gma.expiration_date > eob_data_date - 735
                             THEN
                                gma.grm_total_forecast
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_FORECASTED_735,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= eob_data_date - 735
                                  AND gma.expiration_date > eob_data_date - 735
                             THEN
                                gma.grm_total_booked
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_BOOKED_735,
                    NVL (
                       SUM (
                          CASE
                             WHEN gma.effective_date <= prior_friday
                                  AND gma.expiration_date > prior_friday
                             THEN
                                gma.grm_total_offered
                             ELSE
                                0
                          END),
                       0)
                       LOWER_BEDS_OFFERED_FRI
               FROM group_mstr_audit@tstdwh1_halw_dwh gma, 
                         voyage_mstr@tstdwh1_halw_dwh vm, 
                         tdate
              WHERE gma.grm_voyage = vm.vyd_voyage
                    AND gma.company_code = vm.company_code
                    AND vm.rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL)
                    AND (gma.grm_status_of_group IS NULL
                         OR gma.grm_status_of_group = 'A')
                    AND ( (gma.effective_date <= eob_data_date
                           AND gma.expiration_date > eob_data_date)
                         OR (gma.effective_date <= eob_data_date - 7
                             AND gma.expiration_date > eob_data_date - 7)
                         OR (gma.effective_date <= eob_data_date - 364
                             AND gma.expiration_date > eob_data_date - 364)
                         OR (gma.effective_date <= eob_data_date - 371
                             AND gma.expiration_date > eob_data_date - 371)
                         OR (gma.effective_date <= eob_data_date - 728
                             AND gma.expiration_date > eob_data_date - 728)
                         OR (gma.effective_date <= eob_data_date - 735
                             AND gma.expiration_date > eob_data_date - 735)
                         OR (gma.effective_date <= prior_friday
                             AND gma.expiration_date > prior_friday))
        --      AND GMA.GRM_VOYAGE = 'A226'
           GROUP BY gma.grm_voyage, gma.grm_group_code, gma.company_code, vm.rpt_year;
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_group_hist_f Inserts: '||x;  
    
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_group_hist_f LOGGING';
        
        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'DM_TKR'
         ,TabName        => 'RDM_GROUP_HIST_F'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;        
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;      
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END GROUP_HIST_LOAD; 
  
   PROCEDURE VOYAGE_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'VOYAGE_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;      

        DELETE FROM rdm_voyage_d
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        COMMIT;    

        EXECUTE IMMEDIATE 'ALTER TABLE rdm_voyage_d NOLOGGING';

        INSERT /*+ append */  INTO rdm_voyage_d
        (
        voyage,
        voyage_physical,
        company_code,
        ship_code,
        ship_name,
        voyage_status,
        voyage_type,
        voyage_cancel_date,
        sail_date,
        return_date,
        sea_days,
        from_port,
        to_port,
        product_code,
        trade,
        trade_description,
        subtrade,
        subtrade_description,
        season,
        season_description,
        rpt_qtr,
        rpt_year,
        sail_year,
        fiscal_sail_date,
        voyage_capacity,
        physical_voyage_capacity,
        bedsavailable,
        lower_beds_xld_fcst,
        groups_expected,
        cabin_version,
        final_paymt_days,
        nbr_ports,
        fare_tbl_cd,
        nbr_ports_revised
        )
        WITH t1
                AS (  SELECT DISTINCT vyd_voyage AS voyage, 
                                                    SUM (flagged) AS nbr_ports,
                                                    SUM (flagged_revised) AS nbr_ports_revised                                                    
                        FROM (SELECT DISTINCT
                                     vyd_voyage,
                                     vyd_port_date,
                                     CASE
                                        WHEN     VYD_BOARD_TIME IS NULL
                                             AND vyd_sail_time IS NULL
                                             AND vyd_arrv_time IS NULL
                                             AND vyd_port_nbr NOT IN ('2', '3', '4')
                                        THEN
                                           0
                                        WHEN port_category_flag = 'T'
                                        THEN
                                           0
                                        WHEN     port_category_flag = 'B'
                                             AND port_category_flag_derived = 'B'
                                        THEN
                                           0
                                        WHEN port_category_flag = 'O'
                                        THEN
                                           1
                                        WHEN vyd_port_time_code = 'SC'
                                        THEN
                                           0
                                        WHEN vyd_port_time_code = 'CF'
                                        THEN
                                           0
                                        WHEN vyd_port_nmn BETWEEN '000' AND '999'
                                        THEN
                                           0
                                        ELSE
                                           1
                                     END
                                        AS flagged,
                            case when vyd_board_time is null and vyd_sail_time is null and vyd_arrv_time is null  then 0 
                                    when port_category_flag = 'T' then 0 
                                    when port_category_flag = 'B' AND port_category_flag_derived = 'B' then 0 
                                    when vyd_port_time_code like 'C%' then 0 
                                    else 1 
                                    end as flagged_revised                                        
                                FROM voyage_itinerary@tstdwh1_halw_dwh, port_name@tstdwh1_halw_dwh p
                               WHERE vyd_port_nmn = p.port_code)
                    GROUP BY vyd_voyage
                    ORDER BY vyd_voyage)
           SELECT vm.vyd_voyage AS voyage,
                  vm.physical_voyage_nbr AS voyage_physical,
                  vm.company_code,
                  vm.vyd_ship_code AS ship_code,
                  snm.ship_name,
                  vm.vyd_voyage_status AS voyage_status,
                  vm.vyd_voyage_type AS voyage_type,
                  vm.voyage_cancel_date AS voyage_cancel_date,
                  vm.vyd_sail_date AS sail_date,
                  vm.vyd_return_date AS return_date,
                  vm.vyd_basic_sea_days AS sea_days,
                  vm.vyd_basic_from_port AS from_port,
                  vm.vyd_basic_to_port AS to_port,
                  vm.vyd_product_code AS product_code,
                  vm.vyd_trade AS trade,
                  tc.trade_description AS trade_description,
                  vm.vyd_subtrade AS subtrade,
                  sc.subtrade_description AS subtrade_description,
                  vm.vyd_season AS season,
                  sac.season_desc AS season_description,
                  vm.rpt_qtr,
                  vm.rpt_year,
                  vm.sail_year,
                  vm.fiscal_sail_date,
                  vm.voyage_capacity,
                  vm.physical_voyage_capacity,
                  vm.bedsavailable,
                  --          NVL(ym1.CANCELLATIONFCSTLB,0) AS lower_beds_xld_fcst,
                  0 AS lower_beds_xld_fcst,
                  --          NVL(ym2.GRPFUTEXP,0) AS groups_expected,
                  0 AS groups_expected,
                  vm.vyd_cabin_version AS cabin_version,
                  NVL (vrs_fp_nbr_days, 0) AS final_paymt_days,
                  t1.nbr_ports,
                  vf.vrs_fare_tbl_id AS fare_tbl_cd,
                  t1.nbr_ports_revised                  
             FROM voyage_mstr@tstdwh1_halw_dwh vm,
                  trade_code@tstdwh1_halw_dwh tc,
                  subtrade_code@tstdwh1_halw_dwh sc,
                  season_code@tstdwh1_halw_dwh sac,
                  ship_nme_mstr@tstdwh1_halw_dwh snm,
                  --          viewdwoverbooking@prdyms1.hq.halw.com ym1,
                  --          dwh_owner.ymscat ym2,
                  voyage_route_sector@tstdwh1_halw_dwh vs,
                  voyage_fare@tstdwh1_halw_dwh vf,
                  t1
            WHERE     vm.vyd_trade = tc.trade_code
                  AND vm.company_code = tc.company_code
                  AND vm.company_code = sc.company_code
                  AND vm.company_code = sac.company_code
                  AND vm.company_code = vf.company_code(+)
                  AND vm.vyd_voyage = vf.vrs_voyage(+)
                  AND vf.vrs_seq_nbr(+) = 1
                  AND vm.vyd_subtrade = sc.subtrade_code
                  AND vm.vyd_season = sac.season_code
                  AND vm.vyd_ship_code = snm.ship_key_code
                  AND vm.vyd_voyage = vs.vrs_voyage(+)
                  AND vm.company_code = vs.company_code(+)
                  AND vs.vrs_seq_nbr(+) = 1
                  AND vm.vyd_voyage = t1.voyage(+)
                  --          AND vm.vyd_voyage = ym1.voyage(+)
                  --          AND vm.vyd_voyage = ym2.voyage(+)
                  AND vm.vyd_ship_code <> 'ZZ'
                  AND vm.vyd_voyage_status IN ('A', 'H', 'C')
                  AND vm.rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_voyage_d Inserts: '||x; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_voyage_d LOGGING';
    
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END VOYAGE_LOAD;
  
--   PROCEDURE VOYAGE_LOAD_DAILY IS 
--        cProcedure_NM  CONSTANT  Varchar2(30) := 'VOYAGE_LOAD_DAILY';
--        BatchID Number := 0;
--         
--  BEGIN
--         /* Log program start */ 
--        IF  DM_INT.COMMON_JOBS.LOG_JOB (
--                    io_Batch_ID => BatchID,    
--                    in_Schema_Name => cSchema_NM,  
--                    in_Package_Name => cPackage_NM,
--                    in_Program_Name => cProcedure_NM, 
--                    in_Prog_Status => 'I',  
--                    in_Comments => 'Job in Progress', 
--                    in_Error_Message => 'NA'
--                    ) != 0 THEN                  
--            RAISE eCALL_FAILED;      
--        END IF;
--
--/******* temporary ***************/
--        DELETE FROM rdm_voyage_d_daily
--        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
--        COMMIT;    
--
--        EXECUTE IMMEDIATE 'ALTER TABLE rdm_voyage_d_daily NOLOGGING';
--
--        INSERT /*+ append */  INTO rdm_voyage_d_daily
--        SELECT * FROM dm_tkr.rdm_voyage_d_tst@pricing_audit
--        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
--        
--        x:=SQL%ROWCOUNT ;        
--        sComment  := 'rdm_voyage_d_daily Inserts: '||x; 
--        
--        EXECUTE IMMEDIATE 'ALTER TABLE rdm_voyage_d_daily LOGGING';
--        
----        EXECUTE IMMEDIATE 'TRUNCATE TABLE new_rdm_ship_config_d'; 
----        
----        INSERT /*+ append */  INTO rdm_ship_config_d 
----        ( company_code,
----          voyage,
----          category,
----          ship_code,
----          meta,
----          sub_meta,
----          nbr_of_cabins,
----          meta_revised,
----          sub_meta_revised,
----          meta_sort,
----          category_sort)
----        select COMPANY_CODE,
----               VOYAGE       ,
----               CATEGORY    ,
----               SHIP_CODE    ,
----               META          ,
----               SUB_META       ,
----               NBR_OF_CABINS   ,
----               META_REVISED   ,
----               SUB_META_REVISED   ,
----               meta_sort,
----               category_sort
----          from dm_tkr.v_ship_category@pricing_audit
----         where ship_code <> 'LP';
--   
--   COMMIT;
--
--/******* temporary ***************/
--           
--        /* Record Completion in Log */
--        IF  DM_INT.COMMON_JOBS.LOG_JOB (
--                    io_Batch_ID => BatchID,   
--                    in_Prog_Status => 'C',  
--                    in_Comments => sComment,  
--                    in_Error_Message => 'NA' 
--                    ) != 0 THEN
--            RAISE eCALL_FAILED;            
--        END IF;      
--  
--        /* Record Completion in Control File */
--        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
--                    in_Batch_ID => BatchID,                       
--                    in_Schema_Name => cSchema_NM,  
--                    in_Package_Name => cPackage_NM, 
--                    in_Program_Name => cProcedure_NM,   
--                    in_Load_Type => 'D',         
--                    in_Prog_Status => 'C', 
--                    in_Start_Dt_Parm => dBeg_Date,             
--                    in_End_Dt_Parm => dEnd_Date 
--                    ) != 0 THEN
--            RAISE eCALL_FAILED;            
--        END IF; 
--        
--  EXCEPTION  
--    WHEN eCALL_FAILED THEN
--            ROLLBACK ;
--            eErrMsg :=  'User Defined - Error in called sub-program';                  
--            
--            IF BatchID = 0 THEN                                                                           
--                    x:=-1 ;
--                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
--                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
--                                io_Batch_ID => x ,   
--                                in_Prog_Status => 'X',  
--                                in_Comments => 'Job Failed logged to placeholder  ', 
--                                in_Error_Message => eErrMsg          
--                                );
--                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
--                            in_Batch_ID => BatchID ,                       
--                            in_Schema_Name => cSchema_NM ,  
--                            in_Package_Name => cPackage_NM, 
--                            in_Program_Name => cProcedure_NM,   
--                            in_Load_Type => 'D' ,         
--                            in_Prog_Status => 'X' , 
--                            in_Start_Dt_Parm => dBeg_Date ,      
--                            in_End_Dt_Parm => dEnd_Date   
--                            );
--            ELSE
--                    /* Record error w/ the assigned batch ID */ 
--                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
--                            io_Batch_ID => BatchID,   
--                            in_Prog_Status => 'F',  
--                            in_Comments => 'Job Failed',  
--                            in_Error_Message => eErrMsg 
--                            );
--                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
--                            in_Batch_ID => BatchID,  
--                            in_Schema_Name => cSchema_NM,  
--                            in_Package_Name => cPackage_NM, 
--                            in_Program_Name => cProcedure_NM, 
--                            in_Load_Type => 'D',         
--                            in_Prog_Status  => 'F', 
--                            in_Start_Dt_Parm => dBeg_Date,  
--                            in_End_Dt_Parm => dEnd_Date  
--                            );
--            END IF ;
--            COMMIT;
--            RAISE eCALL_FAILED;     
--             
--      WHEN OTHERS THEN             
--            /* Error trap and logging w/ generic handler */
--            eErrMsg :=  SQLERRM;         
--            dbms_output.put_line(eErrMSG);                                   
--            ROLLBACK;
--                        
--            /* Record error w/ the assigned batch ID */
--            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
--                    io_Batch_ID => BatchID,   
--                    in_Prog_Status => 'F',  
--                    in_Comments => 'Job Failed',  
--                    in_Error_Message => eErrMsg 
--                    );  
--            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
--                    in_Batch_ID => BatchID,  
--                    in_Schema_Name => cSchema_NM,  
--                    in_Package_Name => cPackage_NM, 
--                    in_Program_Name => cProcedure_NM, 
--                    in_Load_Type => 'D',         
--                    in_Prog_Status => 'F' , 
--                    in_Start_Dt_Parm => dBeg_Date,  
--                    in_End_Dt_Parm => dEnd_Date              
--                    );
--            COMMIT; 
--            RAISE;          
--  END VOYAGE_LOAD_DAILY;  

   PROCEDURE VOYAGE_FEES_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'VOYAGE_FEES_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;

        DELETE FROM rdm_voyage_fees_d
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        COMMIT;    

        EXECUTE IMMEDIATE 'ALTER TABLE rdm_voyage_fees_d NOLOGGING';                 

        INSERT /*+ append */  INTO rdm_voyage_fees_d
        (
              voyage,
              company_code,
              rpt_year,
              currency_code,
              ncf,
              taxes
        )
        WITH t1 as (
        SELECT
                    vm.vyd_voyage as voyage,
                    vm.company_code,
                    vm.rpt_year, 
                    fm.fare_curr_code as currency_code,
                    fm.fare_ncf_balcony_dol as ncf,
                    NVL(vc.total_polar_curr,0) as taxes,
                    fm.fare_eff_date,
                    ROW_NUMBER () OVER (PARTITION BY vm.vyd_voyage, vm.company_code, fm.fare_curr_code ORDER BY  fm.fare_eff_date DESC) R1 
           FROM
                    voyage_mstr@stgdwh1_halw_dwh vm,
                    voyage_fare@stgdwh1_halw_dwh vf,
                    fare_mstr@stgdwh1_halw_dwh fm,
                    voyage_fees_by_currency@stgdwh1_halw_dwh vc
        WHERE
                    vm.vyd_voyage = vf.vrs_voyage
             AND vm.company_code = vf.company_code
             AND vf.vrs_fare_tbl_id = fm.fare_code
             AND fm.company_code = vf.company_code
             AND fm.fare_pgm_year = vm.sail_year
             AND fm.fare_curr_code=vc.POLAR_CURRENCY_CODE
             AND vm.vyd_voyage = vc.voyage(+) 
             AND vm.company_code = vc.company_code(+)
             AND vm.vyd_ship_code <> 'ZZ'
--             AND vm.rpt_year >= '2008'
--             AND vm.vyd_voyage = 'D310N'
--             AND rpt_year >= (SELECT rpt_year FROM rdm_date_d)
             AND vm.rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL)
)
        SELECT 
                    voyage, 
                    company_code, 
                    rpt_year, 
                    currency_code, 
                    ncf, 
                    taxes
           FROM t1
        WHERE 
                    R1 = 1
        ORDER BY company_code, voyage, currency_code;     
        
        x:=SQL%ROWCOUNT;
        sComment:= ' rdm_voyage_fees_d Inserts: '||x ;                
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_voyage_fees_d LOGGING';
           
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END VOYAGE_FEES_LOAD; 

   PROCEDURE SHIP_CONFIG_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'SHIP_CONFIG_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;

        EXECUTE IMMEDIATE 'ALTER TABLE rdm_ship_config_d  NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_ship_config_d';  
        
       INSERT /*+ append */  INTO rdm_ship_config_d 
        ( company_code,
          voyage,
          category,
          ship_code,
          meta,
          sub_meta,
          nbr_of_cabins,
          meta_revised,
          sub_meta_revised,
          meta_sort,
          category_sort)
        select COMPANY_CODE,
               VOYAGE       ,
               CATEGORY    ,
               SHIP_CODE    ,
               META          ,
               SUB_META       ,
               NBR_OF_CABINS   ,
               META_REVISED   ,
               SUB_META_REVISED   ,
               meta_sort,
               category_sort
          from dwh_owner.v_ship_category@stgdwh1_halw_dwh
         where ship_code <> 'LP';
                    
        x:=SQL%ROWCOUNT;
        sComment:= ' rdm_ship_config_d  Inserts: '||x ; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_ship_config_d LOGGING'; 
           
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END SHIP_CONFIG_LOAD;    
   
   PROCEDURE VOYAGE_ITIN_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'VOYAGE_ITIN_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;       

        DELETE FROM rdm_voyage_itinerary_d
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        COMMIT;   
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_voyage_itinerary_d NOLOGGING';        
          
        INSERT INTO rdm_voyage_itinerary_d
        (
            company_code,
            rpt_year,
            voyage,
            ship_code,
            sail_date,
            sequence_nbr,
            port_code,
            port_name,
            port_country_code,
            port_country_name,
            port_nbr,
            port_date,
            port_day_of_week,
            boarding_time,
            sail_time,
            arrival_time,
            port_flag1,
            port_flag2,
            port_flag3,
            cross_date_line_flag        
        )
        SELECT vi.company_code,
                    vm.rpt_year,
                    vi.vyd_voyage AS voyage,
                    vm.vyd_ship_code AS ship_code,
                    vm.vyd_sail_date AS sail_date,
                    vi.vyd_sequence_nbr AS sequence_nbr,
                    vi.vyd_port_nmn AS port_code,
                    CASE WHEN vi.vyd_port_nmn = 'XXX' THEN 'AT SEA' ELSE port_name END
                       AS port_name,
                    pn.port_country AS port_country_code,
                    cc.cct_country_name AS port_country_name,
                    vi.vyd_port_nbr AS port_nbr,
                    vi.vyd_port_date AS port_date,
                    vi.vyd_port_day_of_week AS port_day_of_week,
                    vi.vyd_board_time AS boarding_time,
                    vi.vyd_sail_time AS sail_time,
                    vi.vyd_arrv_time AS arrival_time,
                    vi.vyd_port_flag1 AS port_flag1,
                    vi.vyd_port_flag2 AS port_flag2,
                    vi.vyd_port_flag3 AS port_flag3,
                    vi.vyd_cross_date_line AS cross_date_line_flag
               FROM voyage_itinerary@tstdwh1_halw_dwh vi,
                    voyage_mstr@tstdwh1_halw_dwh vm,
                    port_name@tstdwh1_halw_dwh pn,
                    country_code@tstdwh1_halw_dwh cc
              WHERE     vi.vyd_voyage = vm.vyd_voyage
                    AND vi.vyd_port_nmn = pn.port_code
                    AND pn.port_country = cc.cct_country_code(+)
                    AND vi.company_code = vm.company_code
                    AND vi.company_code = pn.company_code
                    AND pn.company_code = cc.company_code(+)
                    AND vm.vyd_ship_code <> 'ZZ'
                   AND vm.vyd_voyage_status IN ('A', 'H', 'C')
                   AND vm.rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL)           
                  --and vm.rpt_year >= '2012'
           ORDER BY vi.company_code, vm.rpt_year, vi.vyd_voyage, vi.vyd_sequence_nbr;
        
        x:=SQL%ROWCOUNT;        
        sComment  := 'rdm_voyage_itinerary_d Inserts: '||x; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_voyage_itinerary_d LOGGING';  
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
            
  END VOYAGE_ITIN_LOAD;

   PROCEDURE FIN_VOYAGE_PRO_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'FIN_VOYAGE_PRO_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;       

        EXECUTE IMMEDIATE 'ALTER TABLE fin_voyage_proration NOLOGGING';   
        EXECUTE IMMEDIATE 'TRUNCATE TABLE fin_voyage_proration';   
               
        INSERT /*+ append */ INTO fin_voyage_proration
        (   
            company_code,                
            voyage_physical,
            voyage,              
            voyage_proration,    
            inventory_proration 
         )
        with t1 as (
        SELECT vm.company_code,
                 vm.rpt_year,
                 vm.vyd_trade as trade,
                 vm.vyd_subtrade as subtrade,
                 (CASE
                     WHEN an.ax_voyage IS NULL THEN vm.physical_voyage_nbr
                     ELSE an.ax_to_voyage
                  END)
                    AS voyage_physical,
                 (CASE WHEN an.ax_voyage IS NULL THEN vm.vyd_voyage ELSE an.ax_voyage END)
                    AS voyage, 
                 vm.vyd_voyage_status as voyage_status, 
                 vm.vyd_sail_date as sail_date,
                 vm.vyd_return_date as return_date,       
                 CASE
                    WHEN an.ax_voyage_days = 0
                    THEN
                       0
                    ELSE
                       (CASE
                           WHEN an.ax_voyage IS NULL THEN 1
                           ELSE (an.ax_prorate_days / an.ax_voyage_days)
                        END)
                 END
                    AS voyage_proration
            FROM voyage_mstr@tstdwh1_halw_dwh vm, abr_newxref@tstdwh1_halw_dwh an
           WHERE   vm.vyd_voyage = an.ax_voyage(+)
                 AND vm.vyd_voyage_status in ('A','H','C')
                 --Fiscal year is hardcoded instead of using sysdate because some Fall/Dec physical voyages have segments assigned to different fiscal years. i.e.T155, T155A to 2011. T155B, T155C, T155D to 2012.
                 AND vm.rpt_year >= '2007'
        ORDER BY vm.company_code,
                 vm.rpt_year,
                 voyage
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
                        CASE
                            WHEN bp.b_p_passenger_id = '999' THEN bp.passenger_count
                            WHEN bp.b_p_use IN ('S', 'D') THEN 1
                            WHEN bp.b_p_use IN ('I', 'X') THEN 2
                            ELSE 0
                        END
                else 0
            end
            ) as lower_bed_count
        from
            t3,
            bkng_mstr@tstdwh1_halw_dwh bm,
            voyage_mstr@tstdwh1_halw_dwh vm,
            bkng_psngr@tstdwh1_halw_dwh bp
        where
            t3.voyage_physical = vm.physical_voyage_nbr
            and bm.company_code = vm.company_code
            and bm.company_code = bp.company_code
            and bm.b_voyage = vm.vyd_voyage
            and bm.b_bkng_nbr = bp.b_bkng_nbr
            and bp.b_p_passenger_status = 'A'
            and t3.part_return_date is not null
        group by
            t3.voyage_physical,
            t3.part_sail_date,
            t3.part_return_date,
            vm.vyd_sail_date,
            vm.vyd_return_date
        order by
            t3.part_sail_date,
            sail_date
        ),
        t5 as (
        select
            t4.voyage_physical,
            t4.part_sail_date,
            t4.part_return_date,
            sum(t4.lower_bed_count) as lower_bed_count
        from
            t4
        group by
            t4.voyage_physical,
            t4.part_sail_date,
            t4.part_return_date
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
            t1.company_code,
            t1.voyage_physical,
            t1.voyage,
            t1.voyage_proration,
            case
                when t1.voyage_status = 'H' then 1
                when t1.voyage = t1.voyage_physical then 1
                WHEN t1.trade = 'L' AND t1.voyage <> t1.voyage_physical THEN 0
                when t1.sail_date < t7.part_return_date and t1.return_date > t7.part_sail_date then 1
                else 0
            end as inventory_proration
        from
            t1,
            t7
        where
            t1.voyage_physical = t7.voyage_physical(+)
        order by
            t1.company_code,
            t1.rpt_year,
            t1.voyage;

        x:=SQL%ROWCOUNT;        
        sComment  := 'fin_voyage_proration Inserts: '||x; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE fin_voyage_proration LOGGING';  
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
            
  END FIN_VOYAGE_PRO_LOAD;
        
   PROCEDURE CABIN_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'CABIN_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;  
  
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_cabin_d';      

        INSERT /*+ append */  INTO rdm_cabin_d
        (
            ship_code,
            version,
            meta,
            category,
            cabin,
            deck,
            position,
            side,
            insideout,
            connecting,
            total_beds_avail,
            lower_beds_avail,
            beds_note,
            cabin_description,
            cabin_notes,
            cabin_sq_ft,
            view_obstruct        
        )
        WITH a
                AS (  SELECT ship_2_key_code, company_code, MAX (ship_2_key_version) AS version_ship
                        FROM ushp_config_ship2@tstdwh1_halw_dwh
                    GROUP BY ship_2_key_code, company_code
                    ORDER BY ship_2_key_code)
           SELECT ushp.ship_2_key_code AS ship_code,
                  ushp.ship_2_key_version AS version,
                  scc.ship_catg_meta AS meta,
                  ship_2_category_xtrnl AS category,
                  ship_2_cabin_xtrnl AS cabin,
                  ship_2_deck AS deck,
                  ship_2_fore_aft_mid AS position,
                  ship_2_port_star AS side,
                  ship_2_inside_outside AS insideout,
                  ship_2_connecting_cabin_1 AS connecting,
                  ship_2_total_berths AS total_beds_avail,
                  ship_2_lower_berths AS lower_beds_avail,
                  ship_2_berth_notes AS beds_note,
                  ship_2_cabin_description AS cabin_description,
                  ship_2_cabin_notes_1 AS cabin_notes,
                  TO_NUMBER (ship_2_dim_cabin_square_f) AS CABIN_SQ_FT,
                  ship_2_view_obstruction AS view_obstruct
             FROM ushp_config_ship2@tstdwh1_halw_dwh ushp, 
                       a, 
                       ship_conf_catg@tstdwh1_halw_dwh scc
            WHERE     a.ship_2_key_code = ushp.ship_2_key_code
                  AND a.version_ship = ushp.ship_2_key_version
                  AND a.company_code = ushp.company_code            
                  AND a.company_code = scc.company_code           
                  AND a.ship_2_key_code = scc.ship_key_code
                  AND a.version_ship = scc.ship_version
                  AND ship_2_category_xtrnl = scc.ship_category_xtrnl;
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_cabin_d Inserts: '||x; 

        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_cabin_full_d';      

        INSERT /*+ append */  INTO rdm_cabin_full_d
        (
            ship_version,
            ship_code,
            version,
            meta,
            category,
            cabin,
            deck,
            position,
            side,
            insideout,
            connecting,
            total_beds_avail,
            lower_beds_avail,
            beds_note,
            cabin_description,
            cabin_notes,
            cabin_sq_ft,
            view_obstruct        
        )
        SELECT   scc.ship_version,
                      ushp.ship_2_key_code AS ship_code,
                      ushp.ship_2_key_version AS version,
                      scc.ship_catg_meta AS meta,
                      ship_2_category_xtrnl AS category,
                      ship_2_cabin_xtrnl AS cabin,
                      ship_2_deck AS deck,
                      ship_2_fore_aft_mid AS position,
                      ship_2_port_star AS side,
                      ship_2_inside_outside AS insideout,
                      ship_2_connecting_cabin_1 AS connecting,
                      ship_2_total_berths AS total_beds_avail,
                      ship_2_lower_berths AS lower_beds_avail,
                      ship_2_berth_notes AS beds_note,
                      ship_2_cabin_description AS cabin_description,
                      ship_2_cabin_notes_1 AS cabin_notes,
                      TO_NUMBER (ship_2_dim_cabin_square_f) AS CABIN_SQ_FT,
                      ship_2_view_obstruction AS view_obstruct
             FROM 
                       ushp_config_ship2@tstdwh1_halw_dwh ushp, 
                       ship_conf_catg@tstdwh1_halw_dwh scc
            WHERE   
                         ushp.ship_2_key_code = scc.ship_key_code
                  AND scc.ship_version = ushp.ship_2_key_version
                  AND ushp.ship_2_category_xtrnl = scc.ship_category_xtrnl
                  AND ushp.company_code = scc.company_code
                  AND ushp.ship_2_key_code <> 'ZZ';
        
        x:=SQL%ROWCOUNT ;        
        sComment:= sComment||' rdm_cabin_full_d Inserts: '||x ;       
        
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT; 
            RAISE;          
  END CABIN_LOAD;
   PROCEDURE CONSOLIDATOR_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'CONSOLIDATOR_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;

        DELETE FROM rdm_consolidator_d
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        COMMIT;   
         
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_consolidator_d NOLOGGING';

        INSERT /*+ append */  INTO rdm_consolidator_d
        SELECT * FROM dm_tkr.v_rdm_consolidator_d_dev@tstdwh1_halw_dwh;
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_consolidator_d Inserts: '||x     ;   
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_consolidator_d LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END CONSOLIDATOR_LOAD;  

   PROCEDURE RESOURCE_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'RESOURCE_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;  

        EXECUTE IMMEDIATE 'ALTER TABLE rsc_resource_d NOLOGGING';

        EXECUTE IMMEDIATE 'TRUNCATE TABLE rsc_resource_d';        
         
        INSERT INTO rsc_resource_d
        WITH TDAILY as (
        SELECT DISTINCT res_key, company_code FROM resource_daily_fare_table@tstdwh1_halw_dwh
        )
        SELECT
            rm.res_key,
            rm.company_code,
            res_inventory_unit_code as inventory_unit_code,
            res_controlled_item_code as controlled_item_code,
            res_on_request_days_val as on_request_days_val,
            res_product_code product_code,
            res_sub_resource_nbr as sub_resource_nbr,
            res_ct_co_flag as ct_co_flag,
            res_name,
            res_description,
            res_short_name,
            res_currency_code as currency_code,
            res_sub_package_code as sub_package_code,
            res_voucher_print_code as voucher_print_code,
            res_itinerary_print_code as itinerary_print_code,
            SUBSTR(rm.RES_KEY,1,1) res_YEAR, 
            SUBSTR(rm.RES_KEY,2,1) TRADE, 
            SUBSTR(rm.RES_KEY,3,1) res_TYPE,
            case when tdaily.res_key is null then 'N' else 'Y' end as build_flag
        FROM
            resource_mstr@tstdwh1_halw_dwh rm,
            tdaily
        WHERE
            rm.res_key = tdaily.res_key(+)
            and rm.company_code = tdaily.company_code(+);
            
        x:=SQL%ROWCOUNT;        
        sComment  := 'rsc_resource_d Inserts: '||x;  
        
        EXECUTE IMMEDIATE 'ALTER TABLE rsc_resource_d LOGGING';
        
        EXECUTE IMMEDIATE 'ALTER TABLE rsc_resource_hist_f NOLOGGING';
    
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rsc_resource_hist_f';         
         
        INSERT INTO rsc_resource_hist_f
        SELECT * FROM dm_tkr.rdm_resource_hist_f_tst@tstdwh1_halw_dwh
        WHERE fiscal_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
                
        x:=SQL%ROWCOUNT; 
        sComment:= sComment||' rsc_resource_hist_f Inserts: '||x ;  
        
        EXECUTE IMMEDIATE 'ALTER TABLE rsc_resource_hist_f LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END RESOURCE_LOAD;  

   PROCEDURE ABR_VIEW_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'ABR_VIEW_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE ab_abrview NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ab_abrview';

        INSERT INTO ab_abrview
        SELECT a.* 
        FROM ab_abrview@tstdwh1_halw_dwh a,
                 rdm_date_d d
        WHERE sch_week = eob_data_date_prior_friday;
         
        x:=SQL%ROWCOUNT ;        
        sComment  := 'ab_abrview Inserts: '||x ;   
        
        EXECUTE IMMEDIATE 'ALTER TABLE ab_abrview LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;  
            RAISE;         
  END ABR_VIEW_LOAD;   

   PROCEDURE PORT_GUESTS_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'PORT_GUESTS_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;   
        
        EXECUTE IMMEDIATE 'ALTER TABLE rsc_port_guests_f NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rsc_port_guests_f';        

        INSERT INTO rsc_port_guests_f
        WITH t1 AS (  
        SELECT d.rpt_year,a.sch_sail_date,s.sch_qtr_return_date,s.sch_qtr,s.sch_qtr_split_ratio,a.sch_polar_voyage,v.to_port,v.from_port,
                    SUM (CASE WHEN final_pax = 0 THEN a.sch_forecast_pax ELSE final_pax END) AS pax_forecast,
                    SUM (CASE WHEN final_pax = 0 THEN guests ELSE final_pax END) AS guests
                     FROM ab_abrview a,
                              rdm_voyage_d v,
                              rdm_date_d d,
                              sailing_schedule s
                    WHERE a.sch_week = eob_data_date_prior_friday
                          AND a.sch_polar_voyage = v.voyage_physical
                          AND a.sch_polar_voyage = s.sch_polar_voyage
                 GROUP BY d.rpt_year,a.sch_polar_voyage,v.to_port,v.from_port,a.sch_sail_date,s.sch_qtr_return_date,s.sch_qtr_split_ratio,s.sch_qtr
        ),
             t2 AS (
             SELECT rpt_year,sch_polar_voyage,sch_sail_date,sch_qtr_return_date,sch_qtr,sch_qtr_split_ratio,to_port AS port,pax_forecast,guests
                   FROM t1
                 UNION
                 SELECT rpt_year,sch_polar_voyage,sch_sail_date,sch_qtr_return_date,sch_qtr,sch_qtr_split_ratio,from_port,pax_forecast,guests
                   FROM t1
        )
          SELECT port,rpt_year,sch_polar_voyage,sch_sail_date,sch_qtr_return_date,
                      CASE WHEN TO_NUMBER (TO_CHAR (sch_qtr_return_date, 'mm')) = 12 THEN 1
                               ELSE TO_NUMBER (TO_CHAR (sch_qtr_return_date, 'mm')) + 1
                      END AS month_nbr,
                      TO_CHAR (sch_qtr_return_date, 'MON') AS month_desc,
                      sch_qtr,
                      sch_qtr_split_ratio,
                      SUM (pax_forecast * sch_qtr_split_ratio) AS pax_forecast,
                      SUM (guests * sch_qtr_split_ratio) AS guests
            FROM t2
        GROUP BY port,rpt_year,sch_polar_voyage,sch_sail_date,sch_qtr_return_date,sch_qtr,sch_qtr_split_ratio
        ORDER BY port,rpt_year,sch_sail_date,sch_qtr,sch_qtr_split_ratio;
         
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rsc_port_guests_f Inserts: '||x ;  
       
         EXECUTE IMMEDIATE 'ALTER TABLE rsc_port_guests_f LOGGING';
                    
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;      
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;  
            RAISE;         
  END PORT_GUESTS_LOAD; 
   PROCEDURE AGENT_LOAD IS  
        cProcedure_NM  CONSTANT  Varchar2(30) := 'AGENT_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_agent_d NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_agent_d';

        INSERT /*+ append */  INTO rdm_agent_d
        (
        agency_id,
        company_code,
        agency_nbr,
        agency_name,
        address1,
        address2,
        address3,
        city,
        state,
        zip,
        country_name,
        telephone,
        fax,
        email,
        agency_contact,
        agency_status,
        withhold_commission,
        classification,
        type_sales_program,
        adhoc_pricing,
        type_reporting,
        commission_program,
        current_tier_lvl,
        assoc_id,
        association_name,
        super_assoc,
        assoc_year,
        national_account_flag,
        consolidator_flag,
        region,
        region_description,
        agent_region_district,
        bdm,
        bdr,
        brc_class_all,
        brc_class_alaska,
        brc_class_canal,
        brc_class_carib,
        brc_class_europe,
        brc_class_long,
        brc_class_volume,
        brc_class_voyage,
        addl_bkng_cntry_cd1,
        addl_bkng_cntry_cd2,
        addl_bkng_cntry_cd3,
        addl_bkng_cntry_cd4,
        addl_bkng_cntry_cd5,
        arc_nbr,
        agency_nbr_legacy,
        country_code,
        super_assoc_name,
        agency_create_date,
        finance_type,
        pymt_method_code,
        onesource_status,
        pseudo_nbr,
        active_agency_flg,
        centurion_flg,
        interline_flg,
        ppd_flg,
        manager_name,
        owner_name,
        auto_comm_recall_flg,
        auto_comm_refund_flg,
        comm_status,
        default_group_cd,
        agency_left_earmark,
        pay_penalty_cd,
        agency_right_earmark,
        intl_fax_phone_flg
        )
        WITH t1
        AS (  SELECT company_code,
                     asn_year,
                     asn_assoc_id AS super_id,
                     asn_association_name AS super_assoc_name
                FROM assoc_mstr@tstdwh1_halw_dwh
               WHERE SUBSTR (asn_assoc_id, 3, 1) IS NULL
            ORDER BY company_code,
                     asn_year,
                     super_id,
                     super_assoc_name)
     SELECT am.agy1_agency_id || aop.agy3_year || am.company_code AS agency_id,
            am.company_code,
            am.agy1_agency_id AS agency_nbr,
            am.agy1_name AS agency_name,
            am.agy1_address_1 AS address1,
            am.agy1_address_2 AS address2,
            am.agy1_address_3 AS address3,
            am.agy1_city AS city,
            am.agy1_state AS state,
            am.agy1_zip AS zip,
            am.agy1_country_name AS country_name,
            am.agy1_telephone AS telephone,
            am.agy1_fax_telephone_intl AS fax,
            ao.agy2_email_address AS email,
            am.agy1_contact AS agency_contact,
            ao.agy2_agency_status AS agency_status,
            ao.agy2_withhold_commission AS withhold_commission,
            aop.agy3_classification AS classification,
            aop.agy3_type_sales_program AS type_sales_program,
            aop.agy3_type_adhoc_pricing AS adhoc_pricing,
            aop.agy3_type_reporting AS type_reporting,
            aop.agy3_commission_program AS commission_program,
            aop.agy3_current_tier_lvl AS current_tier_lvl,
            aop.agy3_assoc_id AS assoc_id,
            asm.asn_association_name AS association_name,
            SUBSTR (aop.agy3_assoc_id, 1, 2) AS super_assoc,
            aop.agy3_year AS assoc_year,
            CASE
               WHEN (   (SUBSTR (aop.agy3_assoc_id, 1, 2) IN
                            ('00',
                             '01',
                             '04',
                             '05',
                             '06',
                             '16',
                             '18',
                             '30',
                             '38',
                             '39',
                             '45',
                             '49',
                             '51',
                             '55',
                             '56',
                             '60',
                             '75',
                             '91',
                             '98'))
                     OR (aop.agy3_assoc_id IN
                            ('CN040',
                             'CN088',
                             'CN094',
                             'CN300',
                             'CN388',
                             'CN414',
                             'CN454',
                             'CN469',
                             'CN487',
                             'CN501',
                             'CN522',
                             'CN550'))
                     OR (am.agy1_agency_id IN
                            ('00154097',
                             '00020004',
                             '00129003',
                             '05890205',
                             '41601383',
                             '22696785',
                             '00025181',
                             '00025949',
                             '2728687',
                             '00006375',
                             '00167808',
                             '52695646',
                             '50932490',
                             '00002353',
                             '00122968',
                             '00138865',
                             '00112270')))
               THEN
                  'Y'
               ELSE
                  'N'
            END
               AS National_Account_Flag,
            ' ' AS Consolidator_Flag,
            ao.agy2_region AS region,
            dr.dsm_region_description AS region_description,
            ao.agent_region_district AS agent_region_district,
            dm.dsm_name AS bdm,
            dm.telesales_rep AS bdr,
            ao.brc_class_all,
            ao.brc_class_alaska,
            ao.brc_class_canal,
            ao.brc_class_carib,
            ao.brc_class_europe,
            ao.brc_class_long,
            ao.brc_class_volume,
            ao.brc_class_voyage,
            ao.agy2_addl_bkng_cntry_cd1 AS addl_bkng_cntry_cd1,
            ao.agy2_addl_bkng_cntry_cd2 AS addl_bkng_cntry_cd2,
            ao.agy2_addl_bkng_cntry_cd3 AS addl_bkng_cntry_cd3,
            ao.agy2_addl_bkng_cntry_cd4 AS addl_bkng_cntry_cd4,
            ao.agy2_addl_bkng_cntry_cd5 AS addl_bkng_cntry_cd5,
            am.agy1_arc_number AS arc_nbr,
            ao.agy2_orig_agency_company_id AS agency_nbr_legacy,
            am.agy1_country_code AS country_code,
            t1.super_assoc_name,
            am.agy1_create_date AS agency_create_date,
            ao.agy2_finance_type AS finance_type,
            ao.agy2_pmt_method_status AS pymt_method_code,
            am.agy1_onesource_status AS onesource_status,
            ao.agy2_pseudo_nbr AS pseudo_nbr,
            (CASE
                WHEN ao.agy2_agency_status NOT IN ('I', 'T', 'W') THEN 'Y'
                WHEN ao.agy2_agency_status IS NULL THEN 'Y'
                ELSE 'N'
             END)
               AS active_agency_flg,
            (CASE
                WHEN aop.agy3_type_sales_program IN ('CE', 'PC') THEN 'Y'
                ELSE 'N'
             END)
               AS centurion_flg,
            (CASE WHEN aop.agy3_type_sales_program = 'IL' THEN 'Y' ELSE 'N' END)
               AS interline_flg,
            (CASE
                WHEN aop.agy3_type_sales_program IN ('PP', 'PC') THEN 'Y'
                ELSE 'N'
             END)
               AS ppd_flg,
            am.AGY1_MANAGER_NAME AS manager_name,
            am.AGY1_OWNER_NAME AS owner_name,
            ao.agy2_autocomm_recall_flg AS auto_comm_recall_flg,
            ao.agy2_autocomm_refund_flg AS auto_comm_refund_flg,
            ao.agy2_commission_status AS comm_status,
            ao.agy2_default_group AS default_group_cd,
            ao.agy2_left_ear AS agency_left_earmark,
            ao.agy2_pay_penalty AS pay_penalty_cd,
            ao.agy2_right_ear AS agency_right_earmark,
            am.agy1_fax_phone_intl_flag AS intl_fax_phone_flg
       FROM agent_mstr@tstdwh1_halw_dwh am,
            agent_oc@tstdwh1_halw_dwh ao,
            agent_oc_product@tstdwh1_halw_dwh aop,
            assoc_mstr@tstdwh1_halw_dwh asm,
            dsm_region@tstdwh1_halw_dwh dr,
            dsm_mstr@tstdwh1_halw_dwh dm,
            t1
      WHERE     am.agy1_agency_id = ao.agy2_agency_id
            AND am.agy1_agency_base_code = ao.agy2_agency_base_code
            AND am.company_code = ao.company_code
            AND am.company_code = aop.company_code
            AND am.company_code = asm.company_code
            AND am.company_code = dm.company_code
            AND ao.agy2_agency_id = aop.agy3_agency_id
            AND ao.agy2_agency_base_code = aop.agy3_agency_base_code
            AND ao.agy2_operating_company = aop.agy3_operating_company
            AND aop.agy3_operating_company = asm.asn_operating_company
            AND aop.agy3_year = asm.asn_year
            AND aop.agy3_assoc_id = asm.asn_assoc_id
            AND ao.agy2_region = dr.dsm_region_code
            AND ao.agent_region_district = dm.dsm_code
            AND aop.company_code = t1.company_code(+)
            AND aop.agy3_year = t1.asn_year(+)
            AND SUBSTR (aop.agy3_assoc_id, 1, 2) = t1.super_id(+)
   --and t1.super_id = '00'
   ORDER BY agency_id;
         
        x:=SQL%ROWCOUNT;        
        sComment  := 'rdm_agent_d Inserts: '||x;   
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_agent_d LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT; 
            RAISE;          
  END AGENT_LOAD;  
   PROCEDURE BOOKING_STG_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'BOOKING_STG_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;  
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_booking_ds NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_booking_ds';

        INSERT /*+ append */ INTO rdm_booking_ds 
        SELECT * FROM dm_tkr.v_rdm_booking_d_dev@tstdwh1_halw_dwh;
--        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
         
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_booking_ds Inserts: '||x;  
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_booking_ds LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;  
            RAISE;         
  END BOOKING_STG_LOAD;
   PROCEDURE FACT_STG_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'FACT_STG_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_trans_fs NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_trans_fs';

        INSERT/*+ append */  INTO rdm_trans_fs
        SELECT * FROM dm_tkr.v_rdm_trans_f_dev@tstdwh1_halw_dwh;       
--        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
         
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_trans_fs Inserts: '||x;   
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_trans_fs LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT; 
            RAISE;           
  END FACT_STG_LOAD; 

   PROCEDURE BPA_STG_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'BPA_STG_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_bpa_fs NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_bpa_fs';

        INSERT /*+ append */  INTO rdm_bpa_fs
        SELECT * FROM dm_tkr.v_rdm_bpa_f_dev@tstdwh1_halw_dwh;
--        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
         
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_bpa_fs Inserts: '||x;     
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_bpa_fs LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END BPA_STG_LOAD; 

   PROCEDURE CHURN_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'CHURN_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;    
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_churn_fs NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_churn_fs';

        INSERT /*+ append */ INTO rdm_churn_fs 
        SELECT * FROM dm_tkr.v_rdm_churn_f_dev@tstdwh1_halw_dwh;
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_churn_fs LOGGING';

        EXECUTE IMMEDIATE 'ALTER TABLE rdm_churn_f NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_churn_f';
        
        INSERT /*+ append */ INTO rdm_churn_f
        WITH t2
        AS (SELECT company_code,
                   b_voyage,
                   b_bkng_nbr,
                   b_p_passenger_id
                                 FROM rdm_churn_fs
             WHERE (effective_date <= rptdate AND expiration_date > rptdate)
            MINUS
            SELECT company_code,
                   b_voyage,
                   b_bkng_nbr,
                   b_p_passenger_id
              FROM rdm_churn_fs
             WHERE (effective_date <= rptdate - 7
                    AND expiration_date > rptdate - 7)),
        new_churn
        AS (SELECT DISTINCT
                   t1.company_code,
                   t1.b_voyage,
                   t1.b_bkng_nbr,
                   t1.b_p_passenger_id,
                   CASE
                      WHEN (t1.effective_date <= t1.rptdate
                            AND t1.expiration_date > t1.rptdate)
                      THEN
                         (CASE
                             WHEN t1.b_p_passenger_id = '999' THEN t1.passenger_count
                             WHEN t1.b_p_use IN ('S', 'D') THEN 1
                             WHEN t1.b_p_use IN ('X', 'I') THEN 2
                             ELSE 0
                          END)
                      ELSE
                         0
                   END
                      AS new_berths,
                   CASE
                      WHEN (t1.effective_date <= t1.rptdate
                            AND t1.expiration_date > t1.rptdate)
                      THEN
                         t1.passenger_count
                      ELSE
                         0
                   END
                      AS new_pax,
                   CASE
                      WHEN (t1.effective_date <= t1.rptdate
                            AND t1.expiration_date > t1.rptdate)
                      THEN
                         t1.net_ticket_revenue
                      ELSE
                         0
                   END
                      AS new_ntr
              FROM rdm_churn_fs t1, t2
             WHERE     t1.company_code = t2.company_code
                   AND t1.b_voyage = t2.b_voyage
                   AND t1.b_bkng_nbr = t2.b_bkng_nbr
                   AND t1.b_p_passenger_id = t2.b_p_passenger_id),
        /* Cancelled Churn */
        t3
        AS (SELECT company_code,
                   b_voyage,
                   b_bkng_nbr,
                   b_p_passenger_id 
              FROM rdm_churn_fs
             WHERE (effective_date <= rptdate - 7
                    AND expiration_date > rptdate - 7)
            MINUS
            SELECT company_code,
                   b_voyage,
                   b_bkng_nbr,
                   b_p_passenger_id
              FROM rdm_churn_fs
             WHERE (effective_date <= rptdate AND expiration_date > rptdate)),
        cancelled_churn
        AS (SELECT DISTINCT
                   t1.company_code,
                   t1.b_voyage,
                   t1.b_bkng_nbr,
                   t1.b_p_passenger_id,
                   CASE
                      WHEN (t1.effective_date <= t1.rptdate - 7
                            AND t1.expiration_date > t1.rptdate - 7)
                      THEN
                         (CASE
                             WHEN t1.b_p_passenger_id = '999' THEN t1.passenger_count
                             WHEN t1.b_p_use IN ('S', 'D') THEN 1
                             WHEN t1.b_p_use IN ('X', 'I') THEN 2
                             ELSE 0
                          END)
                      ELSE
                         0
                   END
                      AS cancelled_berths,
                   CASE
                      WHEN (t1.effective_date <= t1.rptdate - 7
                            AND t1.expiration_date > t1.rptdate - 7)
                      THEN
                         t1.passenger_count
                      ELSE
                         0
                   END
                      AS cancelled_pax,
                   CASE
                      WHEN (t1.effective_date <= t1.rptdate - 7
                            AND t1.expiration_date > t1.rptdate - 7)
                      THEN
                         t1.net_ticket_revenue
                      ELSE
                         0
                   END
                      AS cancelled_ntr
              FROM rdm_churn_fs t1, t3
             WHERE     t1.company_code = t3.company_code
                   AND t1.b_voyage = t3.b_voyage
                   AND t1.b_bkng_nbr = t3.b_bkng_nbr
                   AND t1.b_p_passenger_id = t3.b_p_passenger_id
                   ),
        /* Changed Churn */
        t4
        AS (SELECT DISTINCT
                   company_code,
                   b_voyage,
                   b_bkng_nbr,
                   b_p_passenger_id,
                                      CASE
                      WHEN b_p_passenger_id = '999' THEN passenger_count
                      WHEN b_p_use IN ('S', 'D') THEN 1
                      WHEN b_p_use IN ('X', 'I') THEN 2
                      ELSE 0
                   END
                      AS cw_berths,
                   passenger_count cw_pax,
                   net_ticket_revenue cw_ntr
              FROM rdm_churn_fs
             WHERE (effective_date <= rptdate AND expiration_date > rptdate)),
        t5
        AS (SELECT DISTINCT
                   company_code,
                   b_voyage,
                   b_bkng_nbr,
                   b_p_passenger_id,
                   CASE
                      WHEN b_p_passenger_id = '999' THEN passenger_count
                      WHEN b_p_use IN ('S', 'D') THEN 1
                      WHEN b_p_use IN ('X', 'I') THEN 2
                      ELSE 0
                   END
                      AS pw_berths,
                   passenger_count pw_pax,
                   net_ticket_revenue pw_ntr
              FROM rdm_churn_fs
             WHERE (effective_date <= rptdate - 7
                    AND expiration_date > rptdate - 7)),
        changed_churn
        AS (SELECT t4.company_code,
                   t4.b_voyage,
                   t4.b_bkng_nbr,
                   t4.b_p_passenger_id,
                   (t4.cw_berths - t5.pw_berths) changed_berths,
                   (t4.cw_pax - t5.pw_pax) changed_pax,
                   (t4.cw_ntr - t5.pw_ntr) changed_ntr
              FROM t4, t5
             WHERE     t4.company_code = t5.company_code
                   AND t4.b_voyage = t5.b_voyage
                   AND t4.b_bkng_nbr = t5.b_bkng_nbr
                   AND t4.b_p_passenger_id = t5.b_p_passenger_id
                   AND ( ( (t4.cw_berths - t5.pw_berths) <> 0)
                        OR ( (t4.cw_ntr - t5.pw_ntr) <> 0))),
        master_file
        AS (SELECT DISTINCT b_bkng_nbr,
                            b_p_passenger_id,
                            company_code,
                            b_voyage,
                            rpt_year,
                            vyd_basic_sea_days,
                            rptdate                  
              FROM rdm_churn_fs)
   SELECT master_file.b_bkng_nbr || master_file.b_p_passenger_id AS BKNG_ID,
          master_file.RPTDATE,
          master_file.rpt_year AS RPT_YEAR,
          master_file.b_voyage AS VOYAGE,
          master_file.vyd_basic_sea_days AS SEA_DAYS,
          NVL(new_churn.new_berths,0) AS NEW_LOWER_BEDS,
          NVL(new_churn.new_pax,0) AS NEW_GUESTS,
          NVL(new_churn.new_ntr,0) AS NEW_NTR,
          NVL((cancelled_churn.cancelled_berths * -1),0) AS CANCELLED_LOWER_BEDS,
          NVL((cancelled_churn.cancelled_pax * -1),0) AS CANCELLED_GUESTS,
          NVL((cancelled_churn.cancelled_ntr * -1),0) AS CANCELLED_NTR,
          NVL(changed_churn.changed_berths,0) AS CHANGED_LOWER_BEDS,
          NVL(changed_churn.changed_pax,0) AS CHANGED_GUESTS,
          NVL(changed_churn.changed_ntr,0) AS CHANGED_NTR
     FROM master_file,
          new_churn,
          cancelled_churn,
          changed_churn
    WHERE     master_file.company_code = new_churn.company_code(+)
          AND master_file.b_voyage = new_churn.b_voyage(+)
          AND master_file.b_bkng_nbr = new_churn.b_bkng_nbr(+)
          AND master_file.b_p_passenger_id = new_churn.b_p_passenger_id(+)
          AND master_file.company_code = cancelled_churn.company_code(+)
          AND master_file.b_voyage = cancelled_churn.b_voyage(+)
          AND master_file.b_bkng_nbr = cancelled_churn.b_bkng_nbr(+)
          AND master_file.b_p_passenger_id =
                 cancelled_churn.b_p_passenger_id(+)
          AND master_file.company_code = changed_churn.company_code(+)
          AND master_file.b_voyage = changed_churn.b_voyage(+)
          AND master_file.b_bkng_nbr = changed_churn.b_bkng_nbr(+)
          AND master_file.b_p_passenger_id =
                 changed_churn.b_p_passenger_id(+)
          AND (   
               (new_pax IS NOT NULL OR new_pax <> 0) 
               OR
               (new_ntr IS NOT NULL OR new_ntr <> 0)
               OR 
               (cancelled_pax IS NOT NULL OR cancelled_pax <> 0) 
               OR 
               (cancelled_ntr IS NOT NULL OR cancelled_ntr <> 0)
               OR
               (changed_pax IS NOT NULL OR changed_pax <> 0)  
               OR
               (changed_ntr IS NOT NULL OR changed_ntr <> 0)
              );        
         
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_churn_f Inserts: '||x; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_churn_f LOGGING';

        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END CHURN_LOAD;

   PROCEDURE BOOKING1_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'BOOKING1_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_booking_d NOLOGGING';

        EXECUTE IMMEDIATE 'ALTER TABLE rdm_booking_d TRUNCATE PARTITION bkng_d_2012';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_booking_d TRUNCATE PARTITION bkng_d_2013';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_booking_d TRUNCATE PARTITION bkng_d_max';     

        EXECUTE IMMEDIATE 'ALTER INDEX pk_booking_d rebuild';

        BEGIN
            EXECUTE IMMEDIATE 'drop index DM_TKR.BKNG_D_I1';
            EXECUTE IMMEDIATE 'drop index DM_TKR.BKNG_D_I2';
            EXECUTE IMMEDIATE 'drop index DM_TKR.BKNG_D_I3';
            EXECUTE IMMEDIATE 'drop index DM_TKR.BKNG_D_I4';
            EXECUTE IMMEDIATE 'drop index DM_TKR.BKNG_D_I5';

        EXCEPTION 
            WHEN eNO_INDEX THEN NULL; 
        END;

        INSERT /*+ append */  INTO rdm_booking_d
        SELECT * FROM rdm_booking_ds;
--        WHERE rowid IN (SELECT MAX(rowid) FROM rdm_booking_ds GROUP BY bkng_id);
         
        x:=SQL%ROWCOUNT;        
        sComment  := 'rdm_booking_d Inserts: '||x; 
        
EXECUTE IMMEDIATE '
        CREATE INDEX BKNG_D_I1 ON RDM_BOOKING_D
        (RPT_YEAR)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION BKNG_D_I1_MIN
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I1_2008
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I1_2009
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I1_2010
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I1_2011
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I1_2012
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I1_2013
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I1_MAX
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';

        EXECUTE IMMEDIATE '
        CREATE INDEX BKNG_D_I3 ON RDM_BOOKING_D
        (BKNG_ID)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION BKNG_D_I3_A
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I3_B
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I3_C
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I3_D
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I3_E
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I3_F
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I3_G
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I3_H
            NOLOGGING
            NOCOMPRESS 
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';

        EXECUTE IMMEDIATE '
        CREATE BITMAP INDEX BKNG_D_I4 ON RDM_BOOKING_D
        (COMPANY_CODE)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION BKNG_D_I4_A
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I4_B
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I4_C
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I4_D
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I4_E
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I4_F
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I4_G
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I4_H
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';

        EXECUTE IMMEDIATE '
        CREATE BITMAP INDEX BKNG_D_I5 ON RDM_BOOKING_D
        (BKNG_STATUS)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION BKNG_D_I5_A
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I5_B
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I5_C
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I5_D
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I5_E
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I5_F
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I5_G
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I5_H
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';

        EXECUTE IMMEDIATE '
        CREATE BITMAP INDEX BKNG_D_I2 ON RDM_BOOKING_D
        (PAX_STATUS)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION BKNG_D_I2_A
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I2_B
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I2_C
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I2_D
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I2_E
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I2_F
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I2_G
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BKNG_D_I2_H
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';
      
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_booking_d LOGGING';
        
        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'DM_TKR'
         ,TabName        => 'RDM_BOOKING_D'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;      
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END BOOKING1_LOAD;  

   PROCEDURE FACT_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'FACT_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;    
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_trans_f NOLOGGING';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_trans_f TRUNCATE PARTITION tran_f_2012';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_trans_f TRUNCATE PARTITION tran_f_2013';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_trans_f TRUNCATE PARTITION tran_f_max';
        
        EXECUTE IMMEDIATE 'ALTER INDEX pk_trans_f rebuild';

        BEGIN
            EXECUTE IMMEDIATE 'drop index DM_TKR.TRANS_F_I1';
            EXECUTE IMMEDIATE 'drop index DM_TKR.TRAN_F_I2';
            EXECUTE IMMEDIATE 'drop index DM_TKR.TRAN_F_I4';

        EXCEPTION 
            WHEN eNO_INDEX THEN NULL; 
        END;

        INSERT /*+ append */  INTO rdm_trans_f
        SELECT * FROM rdm_trans_fs;
--        WHERE rowid IN (SELECT MAX(rowid) FROM rdm_trans_fs GROUP BY bkng_id);
         
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_trans_f Inserts: '||x;  
        
        EXECUTE IMMEDIATE '
        CREATE BITMAP INDEX TRANS_F_I1 ON RDM_TRANS_F
        (RPT_YEAR)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION TRAN_F_I1_MIN
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRAN_F_I1_2008
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRAN_F_I1_2009
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRAN_F_I1_2010
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRAN_F_I1_2011
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRAN_F_I1_2012
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRAN_F_I1_2013
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRAN_F_I1_MAX
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';

        EXECUTE IMMEDIATE '
        CREATE BITMAP INDEX TRAN_F_I2 ON RDM_TRANS_F
        (AGENCY_ID)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION TRANS_F_I2_A
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I2_B
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I2_C
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I2_D
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I2_E
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I2_F
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I2_G
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I2_H
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';

        EXECUTE IMMEDIATE '
        CREATE BITMAP INDEX TRAN_F_I4 ON RDM_TRANS_F
        (VOYAGE)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION TRANS_F_I3_A
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I3_B
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I3_C
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I3_D
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I3_E
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I3_F
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I3_G
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION TRANS_F_I3_H
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';

        EXECUTE IMMEDIATE 'ALTER TABLE rdm_trans_f LOGGING';
        
        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'DM_TKR'
         ,TabName        => 'RDM_TRANS_F'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;           
                  
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END FACT_LOAD; 
  
   PROCEDURE BPA_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'BPA_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;  
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_bpa_f NOLOGGING'; 
        --Still truncating partition year 2012 because of point-in-time measures     
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_bpa_f TRUNCATE PARTITION bpa_f_2012';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_bpa_f TRUNCATE PARTITION bpa_f_2013';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_bpa_f TRUNCATE PARTITION bpa_f_max';            

        EXECUTE IMMEDIATE 'ALTER INDEX pk_bpa_f rebuild';
        
        BEGIN
            EXECUTE IMMEDIATE 'drop index DM_TKR.BPA_F_I1';
            EXECUTE IMMEDIATE 'drop index DM_TKR.BPA_F_I2';
            EXECUTE IMMEDIATE 'drop index DM_TKR.BPA_F_I3';

        EXCEPTION 
            WHEN eNO_INDEX THEN NULL; 
        END;

        INSERT /*+ append */  INTO rdm_bpa_f
        SELECT * FROM rdm_bpa_fs;
--        WHERE rowid IN (SELECT MAX(rowid) FROM rdm_bpa_fs GROUP BY bkng_id);
         
        x:=SQL%ROWCOUNT;        
        sComment  := 'rdm_bpa_f Inserts: '||x; 
        
        EXECUTE IMMEDIATE '
        CREATE BITMAP INDEX BPA_F_I1 ON RDM_BPA_F
        (RPT_YEAR)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION BPA_F_I1_MIN
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I1_2008
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I1_2009
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I1_2010
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I1_2011
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I1_2012
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I1_2013
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I1_MAX
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          1M
                        NEXT             1M
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';

        EXECUTE IMMEDIATE '
        CREATE BITMAP INDEX BPA_F_I2 ON RDM_BPA_F
        (BKNG_ID)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION BPA_F_I2_A
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I2_B
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I2_C
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I2_D
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I2_E
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I2_F
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I2_G
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I2_H
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';

        EXECUTE IMMEDIATE '
        CREATE BITMAP INDEX BPA_F_I3 ON RDM_BPA_F
        (VOYAGE)
          TABLESPACE TKR_INDX
          PCTFREE    10
          INITRANS   2
          MAXTRANS   255
        NOLOGGING
        LOCAL (  
          PARTITION BPA_F_I3_A
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I3_B
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I3_C
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I3_D
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I3_E
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I3_F
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I3_G
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        INITIAL          64K
                        NEXT             1M
                        MINEXTENTS       1
                        MAXEXTENTS       UNLIMITED
                        BUFFER_POOL      DEFAULT
                       ),  
          PARTITION BPA_F_I3_H
            NOLOGGING
            TABLESPACE TKR_INDX
            PCTFREE    10
            INITRANS   2
            MAXTRANS   255
            STORAGE    (
                        BUFFER_POOL      DEFAULT
                       )
        )
        NOPARALLEL';
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_bpa_f LOGGING';
        
        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'DM_TKR'
         ,TabName        => 'RDM_BPA_F'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;

        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END BPA_LOAD;
  
   PROCEDURE BOOKING2_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'BOOKING2_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;    
        
        DELETE FROM rdm_book_d 
        WHERE bkng_nbr IN (SELECT DISTINCT bkng_nbr FROM rdm_booking_d WHERE rpt_year >= (SELECT TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY')) FROM DUAL));             
        COMMIT;

        EXECUTE IMMEDIATE 'ALTER TABLE rdm_book_d NOLOGGING';
         
        INSERT /*+ append */  INTO rdm_book_d
        (
          bkng_nbr,                  
          intl_bkng_ind,             
          domestic_intl_bkng_flag,   
          balance_due_amt_native,    
          deposit_amt1_native,       
          deposit_amt2_native,       
          received_amt_native,       
          received_amt_usd_blended,
          net_net_amt_native,
          penalty_amt_native
        )
        WITH t1 as (
        SELECT b.bkng_nbr,
        CASE WHEN SUM(b.non_intl_pty) > 0 THEN 'D'
        WHEN SUM(b.no_home_country) = COUNT(no_home_country) THEN 'D'
        ELSE 'I' END AS intl_bkng_ind,
        'N' AS domestic_intl_bkng_flag
        FROM rdm_trans_f t,
                rdm_booking_d b,
                rdm_agent_d a
        WHERE t.bkng_id = b.bkng_id
        AND t.agency_id = a.agency_id    
        AND b.company_code = a.company_code
        AND a.classification = '8'
        AND t.rpt_year >= (SELECT TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY')) FROM DUAL)
--AND t.rpt_year >= '2008'
        GROUP By bkng_nbr
        UNION        
        SELECT b.bkng_nbr,
        CASE WHEN SUM(b.non_intl_pty) > 0 THEN 'D'
        WHEN SUM(b.no_home_country) = COUNT(no_home_country) THEN 'D'
        ELSE 'I' END AS intl_bkng_ind,
        CASE WHEN SUM(b.non_intl_pty) > 0 THEN 'N'
        WHEN SUM(b.no_home_country) = COUNT(no_home_country) THEN 'N'
        ELSE 'Y' END AS domestic_intl_bkng_flag
        FROM rdm_trans_f t,
                rdm_booking_d b,
                rdm_agent_d a
        WHERE t.bkng_id = b.bkng_id
        AND t.agency_id = a.agency_id    
        AND b.company_code = a.company_code
        AND (a.classification <> '8' or a.classification IS NULL)
        AND t.rpt_year >= (SELECT TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY')) FROM DUAL)
--        AND t.rpt_year >= '2008'
        GROUP BY bkng_nbr
        )
        SELECT
            t1.bkng_nbr,
            t1.intl_bkng_ind,
            t1.domestic_intl_bkng_flag,
            p.balance_due_amt_native,
            p.deposit_amt1_native,
            p.deposit_amt2_native,
            p.received_amt_native,
            p.received_amt_usd_blended,
            p.net_net_amt_native,
            p.penalty_amt_native
        FROM dm_tkr.v_rdm_book_pymt_dev@tstdwh1_halw_dwh p, 
                t1
        WHERE p.bkng_nbr = t1.bkng_nbr;
         
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_book_d Inserts: '||x;     
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_book_d LOGGING';
             
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE; 
            
  END BOOKING2_LOAD;
   PROCEDURE DLY_NTR_NXN_LOAD IS
   /* REV_BKNG_ACT_LOAD is dependent upon this load */
        cProcedure_NM  CONSTANT  Varchar2(30) := 'DLY_NTR_NXN_LOAD';
        BatchID Number := 0;

  BEGIN
         /* Log program start */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Prog_Status => 'I',
                    in_Comments => 'Job in Progress',
                    in_Error_Message => 'NA'
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

        EXECUTE IMMEDIATE 'ALTER TABLE RDM_DLY_NTR_NEW_XLD_NET_FS NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE RDM_DLY_NTR_NEW_XLD_NET_FS';

        INSERT /*+ append */  INTO rdm_dly_ntr_new_xld_net_fs
        SELECT * FROM dm_tkr.v_rdm_dly_ntr_new_xld_net_fdev@tstdwh1_dm_tkr;

        EXECUTE IMMEDIATE 'ALTER TABLE rdm_dly_ntr_new_xld_net_fs LOGGING';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_dly_ntr_new_xld_net_f NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_dly_ntr_new_xld_net_f';

        EXECUTE IMMEDIATE 'ALTER INDEX idx_dly_ntr_nxn_bkng_id UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_dly_ntr_nxn_voyage UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_dly_ntr_nxn_year UNUSABLE';

        INSERT /*+ append */ INTO rdm_dly_ntr_new_xld_net_f
        WITH t1 as (
        /* Daily and Weekly Net Activity */
        SELECT
                    bkng_id,
                    company_code,
                    rpt_year,
                    voyage,
                    sea_days,
                    /* Beds Daily Net*/
                    beds_net_prior_fri_accum,
                    (beds_net_prior_fri_accum - beds_net_prior_thu_accum) as beds_net_prior_fri,
                    (beds_net_sat_accum - beds_net_prior_fri_accum) as beds_net_sat,
                    (beds_net_sun_accum - beds_net_sat_accum) as beds_net_sun,
                    (beds_net_mon_accum - beds_net_sun_accum) as beds_net_mon,
                    (beds_net_tue_accum - beds_net_mon_accum) as beds_net_tue,
                    (beds_net_wed_accum - beds_net_tue_accum) as beds_net_wed,
                    (beds_net_thu_accum - beds_net_wed_accum) as beds_net_thu,
                    (beds_net_fri_accum - beds_net_thu_accum) as beds_net_fri,
                    (beds_net_ytd - beds_net_prior_fri_accum) as beds_net_wtd,
                    (beds_net_today_accum - beds_net_yesterday_accum) as beds_net_daily,
                    beds_net_ytd,
                    beds_net_prior_ytd,
                    beds_rtce_net_ytd,
                    /* NTR Daily Net */
                    ntr_net_prior_fri_accum,
                    (ntr_net_prior_fri_accum - ntr_net_prior_thu_accum) as ntr_net_prior_fri,
                    (ntr_net_sat_accum - ntr_net_prior_fri_accum) as ntr_net_sat,
                    (ntr_net_sun_accum - ntr_net_sat_accum) as ntr_net_sun,
                    (ntr_net_mon_accum - ntr_net_sun_accum) as ntr_net_mon,
                    (ntr_net_tue_accum - ntr_net_mon_accum) as ntr_net_tue,
                    (ntr_net_wed_accum - ntr_net_tue_accum) as ntr_net_wed,
                    (ntr_net_thu_accum - ntr_net_wed_accum) as ntr_net_thu,
                    (ntr_net_fri_accum - ntr_net_thu_accum) as ntr_net_fri,
                    (ntr_net_ytd - ntr_net_prior_fri_accum) as ntr_net_wtd,
                    (ntr_net_today_accum - ntr_net_yesterday_accum) as ntr_net_daily,
                    ntr_net_ytd,
                    ntr_net_prior_ytd,
                    /* PCDs Daily Net */
                    pcd_net_prior_fri_accum,
                    (pcd_net_prior_fri_accum - pcd_net_prior_thu_accum) as pcd_net_prior_fri,
                    (pcd_net_sat_accum - pcd_net_prior_fri_accum) as pcd_net_sat,
                    (pcd_net_sun_accum - pcd_net_sat_accum) as pcd_net_sun,
                    (pcd_net_mon_accum - pcd_net_sun_accum) as pcd_net_mon,
                    (pcd_net_tue_accum - pcd_net_mon_accum) as pcd_net_tue,
                    (pcd_net_wed_accum - pcd_net_tue_accum) as pcd_net_wed,
                    (pcd_net_thu_accum - pcd_net_wed_accum) as pcd_net_thu,
                    (pcd_net_fri_accum - pcd_net_thu_accum) as pcd_net_fri,
                    (pcd_net_ytd - pcd_net_prior_fri_accum) as pcd_net_wtd,
                    (pcd_net_today_accum - pcd_net_yesterday_accum) as pcd_net_daily,
                    pcd_net_ytd,
                    pcd_net_prior_ytd,
                    /* Guests Daily Net */
                    guests_net_prior_fri_accum,
                    (guests_net_prior_fri_accum - guests_net_prior_thu_accum) as guests_net_prior_fri,
                    (guests_net_sat_accum - guests_net_prior_fri_accum) as guests_net_sat,
                    (guests_net_sun_accum - guests_net_sat_accum) as guests_net_sun,
                    (guests_net_mon_accum - guests_net_sun_accum) as guests_net_mon,
                    (guests_net_tue_accum - guests_net_mon_accum) as guests_net_tue,
                    (guests_net_wed_accum - guests_net_tue_accum) as guests_net_wed,
                    (guests_net_thu_accum - guests_net_wed_accum) as guests_net_thu,
                    (guests_net_fri_accum - guests_net_thu_accum) as guests_net_fri,
                    (guests_net_ytd - guests_net_prior_fri_accum) as guests_net_wtd,
                    (guests_net_today_accum - guests_net_yesterday_accum) as guests_net_daily,
                    guests_net_ytd,
                    guests_net_prior_ytd,
                    /* New fields since version 1 */
                    (beds_net_stlw - beds_net_prior_prior_fri) as beds_net_wtd_stlw,
                    (ntr_net_stlw - ntr_net_prior_prior_fri) as ntr_net_wtd_stlw,
                    (pcd_net_stlw - pcd_net_prior_prior_fri) as pcd_net_wtd_stlw,
                    (guests_net_stlw - guests_net_prior_prior_fri) as guests_net_wtd_stlw,
                    (beds_net_prior_ytd - beds_net_prior_fri_stly) as beds_net_wtd_stly,
                    (ntr_net_prior_ytd - ntr_net_prior_fri_stly) as ntr_net_wtd_stly,
                    (pcd_net_prior_ytd - pcd_net_prior_fri_stly) as pcd_net_wtd_stly,
                    (guests_net_prior_ytd - guests_net_prior_fri_stly) as guests_net_wtd_stly,
                    single_guests,
                    extra_guests,
                    beds_net_stlw,
                    beds_net_prior_prior_fri,
                    ntr_net_stlw,
                    ntr_net_prior_prior_fri,
                    pcd_net_stlw,
                    pcd_net_prior_prior_fri,
                    guests_net_stlw,
                    guests_net_prior_prior_fri,
                    /* LW */
                    --Beds
                    (beds_net_prior_sat_accum - beds_net_prior_prior_fri) as beds_net_sat_lw,
                    (beds_net_prior_sun_accum - beds_net_prior_sat_accum) as beds_net_sun_lw,
                    (beds_net_prior_mon_accum - beds_net_prior_sun_accum) as beds_net_mon_lw,
                    (beds_net_prior_tue_accum - beds_net_prior_mon_accum) as beds_net_tue_lw,
                    (beds_net_prior_wed_accum - beds_net_prior_tue_accum) as beds_net_wed_lw,
                    (beds_net_prior_thu_accum - beds_net_prior_wed_accum) as beds_net_thu_lw,
                    (beds_net_prior_fri_accum - beds_net_prior_thu_accum) as beds_net_fri_lw,
                    --NTR
                    (ntr_net_prior_sat_accum - ntr_net_prior_prior_fri) as ntr_net_sat_lw,
                    (ntr_net_prior_sun_accum - ntr_net_prior_sat_accum) as ntr_net_sun_lw,
                    (ntr_net_prior_mon_accum - ntr_net_prior_sun_accum) as ntr_net_mon_lw,
                    (ntr_net_prior_tue_accum - ntr_net_prior_mon_accum) as ntr_net_tue_lw,
                    (ntr_net_prior_wed_accum - ntr_net_prior_tue_accum) as ntr_net_wed_lw,
                    (ntr_net_prior_thu_accum - ntr_net_prior_wed_accum) as ntr_net_thu_lw,
                    (ntr_net_prior_fri_accum - ntr_net_prior_thu_accum) as ntr_net_fri_lw,
                    --PCD
                    (pcd_net_prior_sat_accum - pcd_net_prior_prior_fri) as pcd_net_sat_lw,
                    (pcd_net_prior_sun_accum - pcd_net_prior_sat_accum) as pcd_net_sun_lw,
                    (pcd_net_prior_mon_accum - pcd_net_prior_sun_accum) as pcd_net_mon_lw,
                    (pcd_net_prior_tue_accum - pcd_net_prior_mon_accum) as pcd_net_tue_lw,
                    (pcd_net_prior_wed_accum - pcd_net_prior_tue_accum) as pcd_net_wed_lw,
                    (pcd_net_prior_thu_accum - pcd_net_prior_wed_accum) as pcd_net_thu_lw,
                    (pcd_net_prior_fri_accum - pcd_net_prior_thu_accum) as pcd_net_fri_lw,
                    --Guests
                    (guests_net_prior_sat_accum - guests_net_prior_prior_fri) as guests_net_sat_lw,
                    (guests_net_prior_sun_accum - guests_net_prior_sat_accum) as guests_net_sun_lw,
                    (guests_net_prior_mon_accum - guests_net_prior_sun_accum) as guests_net_mon_lw,
                    (guests_net_prior_tue_accum - guests_net_prior_mon_accum) as guests_net_tue_lw,
                    (guests_net_prior_wed_accum - guests_net_prior_tue_accum) as guests_net_wed_lw,
                    (guests_net_prior_thu_accum - guests_net_prior_wed_accum) as guests_net_thu_lw,
                    (guests_net_prior_fri_accum - guests_net_prior_thu_accum) as guests_net_fri_lw                    
        FROM rdm_dly_ntr_new_xld_net_fs
        ),
       t2 AS (
        /* New, Xld and Changed Activity */
        SELECT
                    bkng_id,
                    company_code,
                    rpt_year,
                    voyage,
                    sea_days,
                    /* Beds Net*/
                    beds_net_prior_fri_accum,
                    beds_net_prior_fri,
                    beds_net_sat,
                    beds_net_sun,
                    beds_net_mon,
                    beds_net_tue,
                    beds_net_wed,
                    beds_net_thu,
                    beds_net_fri,
                    beds_net_wtd,
                    beds_net_daily,
                    beds_net_ytd,
                    beds_net_prior_ytd,
                    beds_rtce_net_ytd,
                    /* NTR Net*/
                    ntr_net_prior_fri_accum,
                    ntr_net_prior_fri,
                    ntr_net_sat,
                    ntr_net_sun,
                    ntr_net_mon,
                    ntr_net_tue,
                    ntr_net_wed,
                    ntr_net_thu,
                    ntr_net_fri,
                    ntr_net_wtd,
                    ntr_net_daily,
                    ntr_net_ytd,
                    ntr_net_prior_ytd,
                    /* PCDs Net */
                    pcd_net_prior_fri_accum,
                    pcd_net_prior_fri,
                    pcd_net_sat,
                    pcd_net_sun,
                    pcd_net_mon,
                    pcd_net_tue,
                    pcd_net_wed,
                    pcd_net_thu,
                    pcd_net_fri,
                    pcd_net_wtd,
                    pcd_net_daily,
                    pcd_net_ytd,
                    pcd_net_prior_ytd,
                    /* Guests Net */
                    guests_net_prior_fri_accum,
                    guests_net_prior_fri,
                    guests_net_sat,
                    guests_net_sun,
                    guests_net_mon,
                    guests_net_tue,
                    guests_net_wed,
                    guests_net_thu,
                    guests_net_fri,
                    guests_net_wtd,
                    guests_net_daily,
                    guests_net_ytd,
                    guests_net_prior_ytd,
                    /* Beds New */
                    case when guests_net_prior_fri > 0 and beds_net_prior_fri > 0 then beds_net_prior_fri else 0 end as beds_new_prior_fri,
                    case when guests_net_sat > 0 and beds_net_sat > 0 then beds_net_sat else 0 end as beds_new_sat,
                    case when guests_net_sun > 0 and beds_net_sun > 0 then beds_net_sun else 0 end as beds_new_sun,
                    case when guests_net_mon > 0 and beds_net_mon > 0 then beds_net_mon else 0 end as beds_new_mon,
                    case when guests_net_tue > 0 and beds_net_tue > 0 then beds_net_tue else 0 end as beds_new_tue,
                    case when guests_net_wed > 0 and beds_net_wed > 0 then beds_net_wed else 0 end as beds_new_wed,
                    case when guests_net_thu > 0 and beds_net_thu > 0 then beds_net_thu else 0 end as beds_new_thu,
                    case when guests_net_fri > 0 and beds_net_fri > 0 then beds_net_fri else 0 end as beds_new_fri,
                    /* Beds Xlds */
                    case when guests_net_prior_fri < 0 and beds_net_prior_fri < 0 then beds_net_prior_fri else 0 end as beds_xld_prior_fri,
                    case when guests_net_sat < 0 and beds_net_sat < 0 then beds_net_sat else 0 end as beds_xld_sat,
                    case when guests_net_sun < 0 and beds_net_sun < 0 then beds_net_sun else 0 end as beds_xld_sun,
                    case when guests_net_mon < 0 and beds_net_mon < 0 then beds_net_mon else 0 end as beds_xld_mon,
                    case when guests_net_tue < 0 and beds_net_tue < 0 then beds_net_tue else 0 end as beds_xld_tue,
                    case when guests_net_wed < 0 and beds_net_wed < 0 then beds_net_wed else 0 end as beds_xld_wed,
                    case when guests_net_thu < 0 and beds_net_thu < 0 then beds_net_thu else 0 end as beds_xld_thu,
                    case when guests_net_fri < 0 and beds_net_fri < 0 then beds_net_fri else 0 end as beds_xld_fri,
                    /* Beds Changed*/
                    case when guests_net_prior_fri = 0 and beds_net_prior_fri <> 0 then beds_net_prior_fri else 0 end as beds_changed_prior_fri,
                    case when guests_net_sat = 0 and beds_net_sat <> 0 then beds_net_sat else 0 end as beds_changed_sat,
                    case when guests_net_sun = 0 and beds_net_sun <> 0 then beds_net_sun else 0 end as beds_changed_sun,
                    case when guests_net_mon = 0 and beds_net_mon <> 0 then beds_net_mon else 0 end as beds_changed_mon,
                    case when guests_net_tue = 0 and beds_net_tue <> 0 then beds_net_tue else 0 end as beds_changed_tue,
                    case when guests_net_wed = 0 and beds_net_wed <> 0 then beds_net_wed else 0 end as beds_changed_wed,
                    case when guests_net_thu = 0 and beds_net_thu <> 0 then beds_net_thu else 0 end as beds_changed_thu,
                    case when guests_net_fri = 0 and beds_net_fri <> 0 then beds_net_fri else 0 end as beds_changed_fri,
                    /* NTR New*/
                    case when guests_net_prior_fri > 0 and ntr_net_prior_fri <> 0 then ntr_net_prior_fri else 0 end as ntr_new_prior_fri,
                    case when guests_net_sat > 0 and ntr_net_sat <> 0 then ntr_net_sat else 0 end as ntr_new_sat,
                    case when guests_net_sun > 0 and ntr_net_sun <> 0 then ntr_net_sun else 0 end as ntr_new_sun,
                    case when guests_net_mon > 0 and ntr_net_mon <> 0 then ntr_net_mon else 0 end as ntr_new_mon,
                    case when guests_net_tue > 0 and ntr_net_tue <> 0 then ntr_net_tue else 0 end as ntr_new_tue,
                    case when guests_net_wed > 0 and ntr_net_wed <> 0 then ntr_net_wed else 0 end as ntr_new_wed,
                    case when guests_net_thu > 0 and ntr_net_thu <> 0 then ntr_net_thu else 0 end as ntr_new_thu,
                    case when guests_net_fri > 0 and ntr_net_fri <> 0 then ntr_net_fri else 0 end as ntr_new_fri,
                    /* NTR Xld*/
                    case when guests_net_prior_fri < 0 and ntr_net_prior_fri <> 0 then ntr_net_prior_fri else 0 end as ntr_xld_prior_fri,
                    case when guests_net_sat < 0 and ntr_net_sat <> 0 then ntr_net_sat else 0 end as ntr_xld_sat,
                    case when guests_net_sun < 0 and ntr_net_sun <> 0 then ntr_net_sun else 0 end as ntr_xld_sun,
                    case when guests_net_mon < 0 and ntr_net_mon <> 0 then ntr_net_mon else 0 end as ntr_xld_mon,
                    case when guests_net_tue < 0 and ntr_net_tue <> 0 then ntr_net_tue else 0 end as ntr_xld_tue,
                    case when guests_net_wed < 0 and ntr_net_wed <> 0 then ntr_net_wed else 0 end as ntr_xld_wed,
                    case when guests_net_thu < 0 and ntr_net_thu <> 0 then ntr_net_thu else 0 end as ntr_xld_thu,
                    case when guests_net_fri < 0 and ntr_net_fri <> 0 then ntr_net_fri else 0 end as ntr_xld_fri,
                    /* NTR Changed*/
                    case when guests_net_prior_fri = 0 and ntr_net_prior_fri <> 0 then ntr_net_prior_fri else 0 end as ntr_changed_prior_fri,
                    case when guests_net_sat = 0 and ntr_net_sat <> 0 then ntr_net_sat else 0 end as ntr_changed_sat,
                    case when guests_net_sun = 0 and ntr_net_sun <> 0 then ntr_net_sun else 0 end as ntr_changed_sun,
                    case when guests_net_mon = 0 and ntr_net_mon <> 0 then ntr_net_mon else 0 end as ntr_changed_mon,
                    case when guests_net_tue = 0 and ntr_net_tue <> 0 then ntr_net_tue else 0 end as ntr_changed_tue,
                    case when guests_net_wed = 0 and ntr_net_wed <> 0 then ntr_net_wed else 0 end as ntr_changed_wed,
                    case when guests_net_thu = 0 and ntr_net_thu <> 0 then ntr_net_thu else 0 end as ntr_changed_thu,
                    case when guests_net_fri = 0 and ntr_net_fri <> 0 then ntr_net_fri else 0 end as ntr_changed_fri,
                    /* PCDs New */
                    case when guests_net_prior_fri > 0 and pcd_net_prior_fri > 0 then pcd_net_prior_fri else 0 end as pcd_new_prior_fri,
                    case when guests_net_sat > 0 and pcd_net_sat > 0 then pcd_net_sat else 0 end as pcd_new_sat,
                    case when guests_net_sun > 0 and pcd_net_sun > 0 then pcd_net_sun else 0 end as pcd_new_sun,
                    case when guests_net_mon > 0 and pcd_net_mon > 0 then pcd_net_mon else 0 end as pcd_new_mon,
                    case when guests_net_tue > 0 and pcd_net_tue > 0 then pcd_net_tue else 0 end as pcd_new_tue,
                    case when guests_net_wed > 0 and pcd_net_wed > 0 then pcd_net_wed else 0 end as pcd_new_wed,
                    case when guests_net_thu > 0 and pcd_net_thu > 0 then pcd_net_thu else 0 end as pcd_new_thu,
                    case when guests_net_fri > 0 and pcd_net_fri > 0 then pcd_net_fri else 0 end as pcd_new_fri,
                    /* PCDs Xld */
                    case when guests_net_prior_fri < 0 and pcd_net_prior_fri < 0 then pcd_net_prior_fri else 0 end as pcd_xld_prior_fri,
                    case when guests_net_sat < 0 and pcd_net_sat < 0 then pcd_net_sat else 0 end as pcd_xld_sat,
                    case when guests_net_sun < 0 and pcd_net_sun < 0 then pcd_net_sun else 0 end as pcd_xld_sun,
                    case when guests_net_mon < 0 and pcd_net_mon < 0 then pcd_net_mon else 0 end as pcd_xld_mon,
                    case when guests_net_tue < 0 and pcd_net_tue < 0 then pcd_net_tue else 0 end as pcd_xld_tue,
                    case when guests_net_wed < 0 and pcd_net_wed < 0 then pcd_net_wed else 0 end as pcd_xld_wed,
                    case when guests_net_thu < 0 and pcd_net_thu < 0 then pcd_net_thu else 0 end as pcd_xld_thu,
                    case when guests_net_fri < 0 and pcd_net_fri < 0 then pcd_net_fri else 0 end as pcd_xld_fri,
                    /* PCDs Changed */
                    case when guests_net_prior_fri = 0 and pcd_net_prior_fri <> 0 then pcd_net_prior_fri else 0 end as pcd_changed_prior_fri,
                    case when guests_net_sat = 0 and pcd_net_sat <> 0 then pcd_net_sat else 0 end as pcd_changed_sat,
                    case when guests_net_sun = 0 and pcd_net_sun <> 0 then pcd_net_sun else 0 end as pcd_changed_sun,
                    case when guests_net_mon = 0 and pcd_net_mon <> 0 then pcd_net_mon else 0 end as pcd_changed_mon,
                    case when guests_net_tue = 0 and pcd_net_tue <> 0 then pcd_net_tue else 0 end as pcd_changed_tue,
                    case when guests_net_wed = 0 and pcd_net_wed <> 0 then pcd_net_wed else 0 end as pcd_changed_wed,
                    case when guests_net_thu = 0 and pcd_net_thu <> 0 then pcd_net_thu else 0 end as pcd_changed_thu,
                    case when guests_net_fri = 0 and pcd_net_fri <> 0 then pcd_net_fri else 0 end as pcd_changed_fri,
                    /* Guests New */
                    case when guests_net_prior_fri > 0 then guests_net_prior_fri else 0 end as guests_new_prior_fri,
                    case when guests_net_sat > 0 then guests_net_sat else 0 end as guests_new_sat,
                    case when guests_net_sun > 0 then guests_net_sun else 0 end as guests_new_sun,
                    case when guests_net_mon > 0 then guests_net_mon else 0 end as guests_new_mon,
                    case when guests_net_tue > 0 then guests_net_tue else 0 end as guests_new_tue,
                    case when guests_net_wed > 0 then guests_net_wed else 0 end as guests_new_wed,
                    case when guests_net_thu > 0 then guests_net_thu else 0 end as guests_new_thu,
                    case when guests_net_fri > 0 then guests_net_fri else 0 end as guests_new_fri,
                    /* Guests Xld */
                    case when guests_net_prior_fri < 0 then guests_net_prior_fri else 0 end as guests_xld_prior_fri,
                    case when guests_net_sat < 0 then guests_net_sat else 0 end as guests_xld_sat,
                    case when guests_net_sun < 0 then guests_net_sun else 0 end as guests_xld_sun,
                    case when guests_net_mon < 0 then guests_net_mon else 0 end as guests_xld_mon,
                    case when guests_net_tue < 0 then guests_net_tue else 0 end as guests_xld_tue,
                    case when guests_net_wed < 0 then guests_net_wed else 0 end as guests_xld_wed,
                    case when guests_net_thu < 0 then guests_net_thu else 0 end as guests_xld_thu,
                    case when guests_net_fri < 0 then guests_net_fri else 0 end as guests_xld_fri,
                    /* New fields since version 1 */
                    beds_net_wtd_stlw,
                    ntr_net_wtd_stlw,
                    pcd_net_wtd_stlw,
                    guests_net_wtd_stlw,
                    beds_net_wtd_stly,
                    ntr_net_wtd_stly,
                    pcd_net_wtd_stly,
                    guests_net_wtd_stly,
                    single_guests,
                    extra_guests,
                    /* Beds New LW*/
                    case when guests_net_sat_lw > 0 and beds_net_sat_lw > 0 then beds_net_sat_lw else 0 end as beds_new_sat_lw,
                    case when guests_net_sun_lw > 0 and beds_net_sun_lw > 0 then beds_net_sun_lw else 0 end as beds_new_sun_lw,
                    case when guests_net_mon_lw > 0 and beds_net_mon_lw > 0 then beds_net_mon_lw else 0 end as beds_new_mon_lw,
                    case when guests_net_tue_lw > 0 and beds_net_tue_lw > 0 then beds_net_tue_lw else 0 end as beds_new_tue_lw,
                    case when guests_net_wed_lw > 0 and beds_net_wed_lw > 0 then beds_net_wed_lw else 0 end as beds_new_wed_lw,
                    case when guests_net_thu_lw > 0 and beds_net_thu_lw > 0 then beds_net_thu_lw else 0 end as beds_new_thu_lw,
                    case when guests_net_fri_lw > 0 and beds_net_fri_lw > 0 then beds_net_fri_lw else 0 end as beds_new_fri_lw,
                    /* Beds Xlds LW*/
                    case when guests_net_sat_lw < 0 and beds_net_sat_lw < 0 then beds_net_sat_lw else 0 end as beds_xld_sat_lw,
                    case when guests_net_sun_lw < 0 and beds_net_sun_lw < 0 then beds_net_sun_lw else 0 end as beds_xld_sun_lw,
                    case when guests_net_mon_lw < 0 and beds_net_mon_lw < 0 then beds_net_mon_lw else 0 end as beds_xld_mon_lw,
                    case when guests_net_tue_lw < 0 and beds_net_tue_lw < 0 then beds_net_tue_lw else 0 end as beds_xld_tue_lw,
                    case when guests_net_wed_lw < 0 and beds_net_wed_lw < 0 then beds_net_wed_lw else 0 end as beds_xld_wed_lw,
                    case when guests_net_thu_lw < 0 and beds_net_thu_lw < 0 then beds_net_thu_lw else 0 end as beds_xld_thu_lw,
                    case when guests_net_fri_lw < 0 and beds_net_fri_lw < 0 then beds_net_fri_lw else 0 end as beds_xld_fri_lw,
                    /* Beds Changed LW*/
                    case when guests_net_sat_lw = 0 and beds_net_sat_lw <> 0 then beds_net_sat_lw else 0 end as beds_changed_sat_lw,
                    case when guests_net_sun_lw = 0 and beds_net_sun_lw <> 0 then beds_net_sun_lw else 0 end as beds_changed_sun_lw,
                    case when guests_net_mon_lw = 0 and beds_net_mon_lw <> 0 then beds_net_mon_lw else 0 end as beds_changed_mon_lw,
                    case when guests_net_tue_lw = 0 and beds_net_tue_lw <> 0 then beds_net_tue_lw else 0 end as beds_changed_tue_lw,
                    case when guests_net_wed_lw = 0 and beds_net_wed_lw <> 0 then beds_net_wed_lw else 0 end as beds_changed_wed_lw,
                    case when guests_net_thu_lw = 0 and beds_net_thu_lw <> 0 then beds_net_thu_lw else 0 end as beds_changed_thu_lw,
                    case when guests_net_fri_lw = 0 and beds_net_fri_lw <> 0 then beds_net_fri_lw else 0 end as beds_changed_fri_lw,
                    /* NTR New LW*/
                    case when guests_net_sat_lw > 0 and ntr_net_sat_lw <> 0 then ntr_net_sat_lw else 0 end as ntr_new_sat_lw,
                    case when guests_net_sun_lw > 0 and ntr_net_sun_lw <> 0 then ntr_net_sun_lw else 0 end as ntr_new_sun_lw,
                    case when guests_net_mon_lw > 0 and ntr_net_mon_lw <> 0 then ntr_net_mon_lw else 0 end as ntr_new_mon_lw,
                    case when guests_net_tue_lw > 0 and ntr_net_tue_lw <> 0 then ntr_net_tue_lw else 0 end as ntr_new_tue_lw,
                    case when guests_net_wed_lw > 0 and ntr_net_wed_lw <> 0 then ntr_net_wed_lw else 0 end as ntr_new_wed_lw,
                    case when guests_net_thu_lw > 0 and ntr_net_thu_lw <> 0 then ntr_net_thu_lw else 0 end as ntr_new_thu_lw,
                    case when guests_net_fri_lw > 0 and ntr_net_fri_lw <> 0 then ntr_net_fri_lw else 0 end as ntr_new_fri_lw,
                    /* NTR Xld LW*/
                    case when guests_net_sat_lw < 0 and ntr_net_sat_lw <> 0 then ntr_net_sat_lw else 0 end as ntr_xld_sat_lw,
                    case when guests_net_sun_lw < 0 and ntr_net_sun_lw <> 0 then ntr_net_sun_lw else 0 end as ntr_xld_sun_lw,
                    case when guests_net_mon_lw < 0 and ntr_net_mon_lw <> 0 then ntr_net_mon_lw else 0 end as ntr_xld_mon_lw,
                    case when guests_net_tue_lw < 0 and ntr_net_tue_lw <> 0 then ntr_net_tue_lw else 0 end as ntr_xld_tue_lw,
                    case when guests_net_wed_lw < 0 and ntr_net_wed_lw <> 0 then ntr_net_wed_lw else 0 end as ntr_xld_wed_lw,
                    case when guests_net_thu_lw < 0 and ntr_net_thu_lw <> 0 then ntr_net_thu_lw else 0 end as ntr_xld_thu_lw,
                    case when guests_net_fri_lw < 0 and ntr_net_fri_lw <> 0 then ntr_net_fri_lw else 0 end as ntr_xld_fri_lw,
                    /* NTR Changed LW */
                    case when guests_net_sat_lw = 0 and ntr_net_sat_lw <> 0 then ntr_net_sat_lw else 0 end as ntr_changed_sat_lw,
                    case when guests_net_sun_lw = 0 and ntr_net_sun_lw <> 0 then ntr_net_sun_lw else 0 end as ntr_changed_sun_lw,
                    case when guests_net_mon_lw = 0 and ntr_net_mon_lw <> 0 then ntr_net_mon_lw else 0 end as ntr_changed_mon_lw,
                    case when guests_net_tue_lw = 0 and ntr_net_tue_lw <> 0 then ntr_net_tue_lw else 0 end as ntr_changed_tue_lw,
                    case when guests_net_wed_lw = 0 and ntr_net_wed_lw <> 0 then ntr_net_wed_lw else 0 end as ntr_changed_wed_lw,
                    case when guests_net_thu_lw = 0 and ntr_net_thu_lw <> 0 then ntr_net_thu_lw else 0 end as ntr_changed_thu_lw,
                    case when guests_net_fri_lw = 0 and ntr_net_fri_lw <> 0 then ntr_net_fri_lw else 0 end as ntr_changed_fri_lw,
                     /* PCD New LW*/
                    case when guests_net_sat_lw > 0 and pcd_net_sat_lw > 0 then pcd_net_sat_lw else 0 end as pcd_new_sat_lw,
                    case when guests_net_sun_lw > 0 and pcd_net_sun_lw > 0 then pcd_net_sun_lw else 0 end as pcd_new_sun_lw,
                    case when guests_net_mon_lw > 0 and pcd_net_mon_lw > 0 then pcd_net_mon_lw else 0 end as pcd_new_mon_lw,
                    case when guests_net_tue_lw > 0 and pcd_net_tue_lw > 0 then pcd_net_tue_lw else 0 end as pcd_new_tue_lw,
                    case when guests_net_wed_lw > 0 and pcd_net_wed_lw > 0 then pcd_net_wed_lw else 0 end as pcd_new_wed_lw,
                    case when guests_net_thu_lw > 0 and pcd_net_thu_lw > 0 then pcd_net_thu_lw else 0 end as pcd_new_thu_lw,
                    case when guests_net_fri_lw > 0 and pcd_net_fri_lw > 0 then pcd_net_fri_lw else 0 end as pcd_new_fri_lw,
                    /* PCD Xlds LW*/
                    case when guests_net_sat_lw < 0 and pcd_net_sat_lw < 0 then pcd_net_sat_lw else 0 end as pcd_xld_sat_lw,
                    case when guests_net_sun_lw < 0 and pcd_net_sun_lw < 0 then pcd_net_sun_lw else 0 end as pcd_xld_sun_lw,
                    case when guests_net_mon_lw < 0 and pcd_net_mon_lw < 0 then pcd_net_mon_lw else 0 end as pcd_xld_mon_lw,
                    case when guests_net_tue_lw < 0 and pcd_net_tue_lw < 0 then pcd_net_tue_lw else 0 end as pcd_xld_tue_lw,
                    case when guests_net_wed_lw < 0 and pcd_net_wed_lw < 0 then pcd_net_wed_lw else 0 end as pcd_xld_wed_lw,
                    case when guests_net_thu_lw < 0 and pcd_net_thu_lw < 0 then pcd_net_thu_lw else 0 end as pcd_xld_thu_lw,
                    case when guests_net_fri_lw < 0 and pcd_net_fri_lw < 0 then pcd_net_fri_lw else 0 end as pcd_xld_fri_lw,
                  /* PCDs Changed LW */
                    case when guests_net_sat_lw = 0 and pcd_net_sat_lw <> 0 then pcd_net_sat_lw else 0 end as pcd_changed_sat_lw,
                    case when guests_net_sun_lw = 0 and pcd_net_sun_lw <> 0 then pcd_net_sun_lw else 0 end as pcd_changed_sun_lw,
                    case when guests_net_mon_lw = 0 and pcd_net_mon_lw <> 0 then pcd_net_mon_lw else 0 end as pcd_changed_mon_lw,
                    case when guests_net_tue_lw = 0 and pcd_net_tue_lw <> 0 then pcd_net_tue_lw else 0 end as pcd_changed_tue_lw,
                    case when guests_net_wed_lw = 0 and pcd_net_wed_lw <> 0 then pcd_net_wed_lw else 0 end as pcd_changed_wed_lw,
                    case when guests_net_thu_lw = 0 and pcd_net_thu_lw <> 0 then pcd_net_thu_lw else 0 end as pcd_changed_thu_lw,
                    case when guests_net_fri_lw = 0 and pcd_net_fri_lw <> 0 then pcd_net_fri_lw else 0 end as pcd_changed_fri_lw,
                    /* Guests New LW */
                    case when guests_net_sat_lw > 0 then guests_net_sat_lw else 0 end as guests_new_sat_lw,
                    case when guests_net_sun_lw > 0 then guests_net_sun_lw else 0 end as guests_new_sun_lw,
                    case when guests_net_mon_lw > 0 then guests_net_mon_lw else 0 end as guests_new_mon_lw,
                    case when guests_net_tue_lw > 0 then guests_net_tue_lw else 0 end as guests_new_tue_lw,
                    case when guests_net_wed_lw > 0 then guests_net_wed_lw else 0 end as guests_new_wed_lw,
                    case when guests_net_thu_lw > 0 then guests_net_thu_lw else 0 end as guests_new_thu_lw,
                    case when guests_net_fri_lw > 0 then guests_net_fri_lw else 0 end as guests_new_fri_lw,
                    /* Guests Xld LW */
                    case when guests_net_sat_lw < 0 then guests_net_sat_lw else 0 end as guests_xld_sat_lw,
                    case when guests_net_sun_lw < 0 then guests_net_sun_lw else 0 end as guests_xld_sun_lw,
                    case when guests_net_mon_lw < 0 then guests_net_mon_lw else 0 end as guests_xld_mon_lw,
                    case when guests_net_tue_lw < 0 then guests_net_tue_lw else 0 end as guests_xld_tue_lw,
                    case when guests_net_wed_lw < 0 then guests_net_wed_lw else 0 end as guests_xld_wed_lw,
                    case when guests_net_thu_lw < 0 then guests_net_thu_lw else 0 end as guests_xld_thu_lw,
                    case when guests_net_fri_lw < 0 then guests_net_fri_lw else 0 end as guests_xld_fri_lw            
        FROM t1
      )
      SELECT 
                    bkng_id,
                    company_code,
                    rpt_year,
                    voyage,
                    sea_days,
                    beds_net_prior_fri_accum,
                    beds_net_prior_fri,
                    beds_net_sat,
                    beds_net_sun,
                    beds_net_mon,
                    beds_net_tue,
                    beds_net_wed,
                    beds_net_thu,
                    beds_net_fri,
                    beds_net_wtd,
                    beds_net_daily,
                    beds_net_ytd,
                    beds_net_prior_ytd,
                    beds_rtce_net_ytd,
                    ntr_net_prior_fri_accum,
                    ntr_net_prior_fri,
                    ntr_net_sat,
                    ntr_net_sun,
                    ntr_net_mon,
                    ntr_net_tue,
                    ntr_net_wed,
                    ntr_net_thu,
                    ntr_net_fri,
                    ntr_net_wtd,
                    ntr_net_daily,
                    ntr_net_ytd,
                    ntr_net_prior_ytd,
                    pcd_net_prior_fri_accum,
                    pcd_net_prior_fri,
                    pcd_net_sat,
                    pcd_net_sun,
                    pcd_net_mon,
                    pcd_net_tue,
                    pcd_net_wed,
                    pcd_net_thu,
                    pcd_net_fri,
                    pcd_net_wtd,
                    pcd_net_daily,
                    pcd_net_ytd,
                    pcd_net_prior_ytd,
                    guests_net_prior_fri_accum,
                    guests_net_prior_fri,
                    guests_net_sat,
                    guests_net_sun,
                    guests_net_mon,
                    guests_net_tue,
                    guests_net_wed,
                    guests_net_thu,
                    guests_net_fri,
                    guests_net_wtd,
                    guests_net_daily,
                    guests_net_ytd,
                    guests_net_prior_ytd,
                    beds_new_prior_fri,
                    beds_new_sat,
                    beds_new_sun,
                    beds_new_mon,
                    beds_new_tue,
                    beds_new_wed,
                    beds_new_thu,
                    beds_new_fri,
                    beds_xld_prior_fri,
                    beds_xld_sat,
                    beds_xld_sun,
                    beds_xld_mon,
                    beds_xld_tue,
                    beds_xld_wed,
                    beds_xld_thu,
                    beds_xld_fri,
                    beds_changed_prior_fri,
                    beds_changed_sat,
                    beds_changed_sun,
                    beds_changed_mon,
                    beds_changed_tue,
                    beds_changed_wed,
                    beds_changed_thu,
                    beds_changed_fri,
                    ntr_new_prior_fri,
                    ntr_new_sat,
                    ntr_new_sun,
                    ntr_new_mon,
                    ntr_new_tue,
                    ntr_new_wed,
                    ntr_new_thu,
                    ntr_new_fri,
                    ntr_xld_prior_fri,
                    ntr_xld_sat,
                    ntr_xld_sun,
                    ntr_xld_mon,
                    ntr_xld_tue,
                    ntr_xld_wed,
                    ntr_xld_thu,
                    ntr_xld_fri,
                    ntr_changed_prior_fri,
                    ntr_changed_sat,
                    ntr_changed_sun,
                    ntr_changed_mon,
                    ntr_changed_tue,
                    ntr_changed_wed,
                    ntr_changed_thu,
                    ntr_changed_fri,
                    pcd_new_prior_fri,
                    pcd_new_sat,
                    pcd_new_sun,
                    pcd_new_mon,
                    pcd_new_tue,
                    pcd_new_wed,
                    pcd_new_thu,
                    pcd_new_fri,
                    pcd_xld_prior_fri,
                    pcd_xld_sat,
                    pcd_xld_sun,
                    pcd_xld_mon,
                    pcd_xld_tue,
                    pcd_xld_wed,
                    pcd_xld_thu,
                    pcd_xld_fri,
                    pcd_changed_prior_fri,
                    pcd_changed_sat,
                    pcd_changed_sun,
                    pcd_changed_mon,
                    pcd_changed_tue,
                    pcd_changed_wed,
                    pcd_changed_thu,
                    pcd_changed_fri,
                    guests_new_prior_fri,
                    guests_new_sat,
                    guests_new_sun,
                    guests_new_mon,
                    guests_new_tue,
                    guests_new_wed,
                    guests_new_thu,
                    guests_new_fri,
                    guests_xld_prior_fri,
                    guests_xld_sat,
                    guests_xld_sun,
                    guests_xld_mon,
                    guests_xld_tue,
                    guests_xld_wed,
                    guests_xld_thu,
                    guests_xld_fri,
                    beds_net_wtd_stlw,
                    ntr_net_wtd_stlw,
                    pcd_net_wtd_stlw,
                    guests_net_wtd_stlw,
                    beds_net_wtd_stly,
                    ntr_net_wtd_stly,
                    pcd_net_wtd_stly,
                    guests_net_wtd_stly,
                    single_guests,
                    extra_guests,
                    beds_new_sat_lw,
                    beds_new_sun_lw,
                    beds_new_mon_lw,
                    beds_new_tue_lw,
                    beds_new_wed_lw,
                    beds_new_thu_lw,
                    beds_new_fri_lw,
                    beds_xld_sat_lw,
                    beds_xld_sun_lw,
                    beds_xld_mon_lw,
                    beds_xld_tue_lw,
                    beds_xld_wed_lw,
                    beds_xld_thu_lw,
                    beds_xld_fri_lw,
                    beds_changed_sat_lw,
                    beds_changed_sun_lw,
                    beds_changed_mon_lw,
                    beds_changed_tue_lw,
                    beds_changed_wed_lw,
                    beds_changed_thu_lw,
                    beds_changed_fri_lw,
                    ntr_new_sat_lw,
                    ntr_new_sun_lw,
                    ntr_new_mon_lw,
                    ntr_new_tue_lw,
                    ntr_new_wed_lw,
                    ntr_new_thu_lw,
                    ntr_new_fri_lw,
                    ntr_xld_sat_lw,
                    ntr_xld_sun_lw,
                    ntr_xld_mon_lw,
                    ntr_xld_tue_lw,
                    ntr_xld_wed_lw,
                    ntr_xld_thu_lw,
                    ntr_xld_fri_lw,
                    ntr_changed_sat_lw,
                    ntr_changed_sun_lw,
                    ntr_changed_mon_lw,
                    ntr_changed_tue_lw,
                    ntr_changed_wed_lw,
                    ntr_changed_thu_lw,
                    ntr_changed_fri_lw,
                    pcd_new_sat_lw,
                    pcd_new_sun_lw,
                    pcd_new_mon_lw,
                    pcd_new_tue_lw,
                    pcd_new_wed_lw,
                    pcd_new_thu_lw,
                    pcd_new_fri_lw,
                    pcd_xld_sat_lw,
                    pcd_xld_sun_lw,
                    pcd_xld_mon_lw,
                    pcd_xld_tue_lw,
                    pcd_xld_wed_lw,
                    pcd_xld_thu_lw,
                    pcd_xld_fri_lw,
                    pcd_changed_sat_lw,
                    pcd_changed_sun_lw,
                    pcd_changed_mon_lw,
                    pcd_changed_tue_lw,
                    pcd_changed_wed_lw,
                    pcd_changed_thu_lw,
                    pcd_changed_fri_lw,
                    guests_new_sat_lw,
                    guests_new_sun_lw,
                    guests_new_mon_lw,
                    guests_new_tue_lw,
                    guests_new_wed_lw,
                    guests_new_thu_lw,
                    guests_new_fri_lw,
                    guests_xld_sat_lw,
                    guests_xld_sun_lw,
                    guests_xld_mon_lw,
                    guests_xld_tue_lw,
                    guests_xld_wed_lw,
                    guests_xld_thu_lw,
                    guests_xld_fri_lw,      
                    /* WTD STLW New, Xld */
                    (beds_new_sat_lw + beds_new_sun_lw + beds_new_mon_lw + beds_new_tue_lw + beds_new_wed_lw + beds_new_thu_lw + beds_new_fri_lw) as beds_new_wtd_stlw,
                    (beds_xld_sat_lw + beds_xld_sun_lw + beds_xld_mon_lw + beds_xld_tue_lw + beds_xld_wed_lw + beds_xld_thu_lw + beds_xld_fri_lw) as beds_xld_wtd_stlw,
                    (beds_changed_sat_lw + beds_changed_sun_lw + beds_changed_mon_lw + beds_changed_tue_lw + beds_changed_wed_lw + beds_changed_thu_lw + beds_changed_fri_lw) as beds_changed_wtd_stlw,
                    (ntr_new_sat_lw + ntr_new_sun_lw + ntr_new_mon_lw + ntr_new_tue_lw + ntr_new_wed_lw + ntr_new_thu_lw + ntr_new_fri_lw) as ntr_new_wtd_stlw,
                    (ntr_xld_sat_lw + ntr_xld_sun_lw + ntr_xld_mon_lw + ntr_xld_tue_lw + ntr_xld_wed_lw + ntr_xld_thu_lw + ntr_xld_fri_lw) as ntr_xld_wtd_stlw,
                    (ntr_changed_sat_lw + ntr_changed_sun_lw + ntr_changed_mon_lw + ntr_changed_tue_lw + ntr_changed_wed_lw + ntr_changed_thu_lw + ntr_changed_fri_lw) as ntr_changed_wtd_stlw,
                    (pcd_new_sat_lw + pcd_new_sun_lw + pcd_new_mon_lw + pcd_new_tue_lw + pcd_new_wed_lw + pcd_new_thu_lw + pcd_new_fri_lw) as pcd_new_wtd_stlw,
                    (pcd_xld_sat_lw + pcd_xld_sun_lw + pcd_xld_mon_lw + pcd_xld_tue_lw + pcd_xld_wed_lw + pcd_xld_thu_lw + pcd_xld_fri_lw) as pcd_xld_wtd_stlw,
                    (pcd_changed_sat_lw + pcd_changed_sun_lw + pcd_changed_mon_lw + pcd_changed_tue_lw + pcd_changed_wed_lw + pcd_changed_thu_lw + pcd_changed_fri_lw) as pcd_changed_wtd_stlw,
                    (guests_new_sat_lw + guests_new_sun_lw + guests_new_mon_lw + guests_new_tue_lw + guests_new_wed_lw + guests_new_thu_lw + guests_new_fri_lw) as guests_new_wtd_stlw,
                    (guests_xld_sat_lw + guests_xld_sun_lw + guests_xld_mon_lw + guests_xld_tue_lw + guests_xld_wed_lw + guests_xld_thu_lw + guests_xld_fri_lw) as guests_xld_wtd_stlw,
                    /* Daily New, Xld, Changed */ 
            CASE
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '7' THEN beds_new_sat
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '1' THEN beds_new_sun
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '2' THEN beds_new_mon
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '3' THEN beds_new_tue
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '4' THEN beds_new_wed
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '5' THEN beds_new_thu
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '6' THEN beds_new_fri
               ELSE 0
            END
            AS beds_new_daily, 
            CASE
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '7' THEN beds_xld_sat
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '1' THEN beds_xld_sun
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '2' THEN beds_xld_mon
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '3' THEN beds_xld_tue
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '4' THEN beds_xld_wed
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '5' THEN beds_xld_thu
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '6' THEN beds_xld_fri
               ELSE 0
            END
            AS beds_xld_daily,    
            CASE
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '7' THEN beds_changed_sat
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '1' THEN beds_changed_sun
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '2' THEN beds_changed_mon
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '3' THEN beds_changed_tue
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '4' THEN beds_changed_wed
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '5' THEN beds_changed_thu
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '6' THEN beds_changed_fri
               ELSE 0
            END
            AS beds_changed_daily,

            CASE
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '7' THEN ntr_new_sat
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '1' THEN ntr_new_sun
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '2' THEN ntr_new_mon
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '3' THEN ntr_new_tue
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '4' THEN ntr_new_wed
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '5' THEN ntr_new_thu
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '6' THEN ntr_new_fri
               ELSE 0
            END
            AS ntr_new_daily, 
            CASE
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '7' THEN ntr_xld_sat
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '1' THEN ntr_xld_sun
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '2' THEN ntr_xld_mon
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '3' THEN ntr_xld_tue
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '4' THEN ntr_xld_wed
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '5' THEN ntr_xld_thu
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '6' THEN ntr_xld_fri
               ELSE 0
            END
            AS ntr_xld_daily,    
            CASE
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '7' THEN ntr_changed_sat
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '1' THEN ntr_changed_sun
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '2' THEN ntr_changed_mon
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '3' THEN ntr_changed_tue
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '4' THEN ntr_changed_wed
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '5' THEN ntr_changed_thu
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '6' THEN ntr_changed_fri
               ELSE 0
            END
            AS ntr_changed_daily,
            
            CASE
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '7' THEN pcd_new_sat
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '1' THEN pcd_new_sun
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '2' THEN pcd_new_mon
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '3' THEN pcd_new_tue
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '4' THEN pcd_new_wed
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '5' THEN pcd_new_thu
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '6' THEN pcd_new_fri
               ELSE 0
            END
            AS pcd_new_daily, 
            CASE
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '7' THEN pcd_xld_sat
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '1' THEN pcd_xld_sun
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '2' THEN pcd_xld_mon
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '3' THEN pcd_xld_tue
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '4' THEN pcd_xld_wed
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '5' THEN pcd_xld_thu
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '6' THEN pcd_xld_fri
               ELSE 0
            END
            AS pcd_xld_daily,    
            CASE
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '7' THEN pcd_changed_sat
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '1' THEN pcd_changed_sun
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '2' THEN pcd_changed_mon
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '3' THEN pcd_changed_tue
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '4' THEN pcd_changed_wed
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '5' THEN pcd_changed_thu
               WHEN TO_CHAR (dt.eob_data_date, 'd') = '6' THEN pcd_changed_fri
               ELSE 0
            END
            AS pcd_changed_daily                    
          FROM t2, dw_system_parms@tstdwh1_halw_dwh dt;

        x:=SQL%ROWCOUNT;
        sComment  := 'rdm_dly_ntr_new_xld_net_f Inserts: '||x;

        EXECUTE IMMEDIATE 'ALTER INDEX idx_dly_ntr_nxn_bkng_id REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_dly_ntr_nxn_voyage REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_dly_ntr_nxn_year REBUILD';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_dly_ntr_new_xld_net_f LOGGING';

        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Prog_Status => 'C',
                    in_Comments => sComment,
                    in_Error_Message => 'NA'
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Load_Type => 'D',
                    in_Prog_Status => 'C',
                    in_Start_Dt_Parm => dBeg_Date,
                    in_End_Dt_Parm => dEnd_Date
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

  EXCEPTION
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';

            IF BatchID = 0 THEN
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,
                                in_Prog_Status => 'X',
                                in_Comments => 'Job Failed logged to placeholder  ',
                                in_Error_Message => eErrMsg
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,
                            in_Schema_Name => cSchema_NM ,
                            in_Package_Name => cPackage_NM,
                            in_Program_Name => cProcedure_NM,
                            in_Load_Type => 'D' ,
                            in_Prog_Status => 'X' ,
                            in_Start_Dt_Parm => dBeg_Date ,
                            in_End_Dt_Parm => dEnd_Date
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,
                            in_Prog_Status => 'F',
                            in_Comments => 'Job Failed',
                            in_Error_Message => eErrMsg
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,
                            in_Schema_Name => cSchema_NM,
                            in_Package_Name => cPackage_NM,
                            in_Program_Name => cProcedure_NM,
                            in_Load_Type => 'D',
                            in_Prog_Status  => 'F',
                            in_Start_Dt_Parm => dBeg_Date,
                            in_End_Dt_Parm => dEnd_Date
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;

      WHEN OTHERS THEN
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;
            dbms_output.put_line(eErrMSG);
            ROLLBACK;

            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Prog_Status => 'F',
                    in_Comments => 'Job Failed',
                    in_Error_Message => eErrMsg
                    );
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Load_Type => 'D',
                    in_Prog_Status => 'F' ,
                    in_Start_Dt_Parm => dBeg_Date,
                    in_End_Dt_Parm => dEnd_Date
                    );
            COMMIT;
            RAISE;
  END DLY_NTR_NXN_LOAD;
  
   PROCEDURE WTD_NTR_ADJ_LOAD IS 
   /* REV_BKNG_ACT_LOAD is dependent upon this load */
        cProcedure_NM  CONSTANT  Varchar2(30) := 'WTD_NTR_ADJ_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;   
               
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_wtd_ntr_adj_fs NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_wtd_ntr_adj_fs';

        INSERT /*+ append */ INTO rdm_wtd_ntr_adj_fs
        SELECT * FROM dm_tkr.v_rdm_wtd_ntr_adj_f_dev@tstdwh1_halw_dwh;

        EXECUTE IMMEDIATE 'ALTER TABLE rdm_wtd_ntr_adj_fs LOGGING';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_wtd_ntr_adj_f NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_wtd_ntr_adj_f'; 

        EXECUTE IMMEDIATE 'ALTER INDEX idx_wtd_ntr_adj_bkng_id UNUSABLE';   
        EXECUTE IMMEDIATE 'ALTER INDEX idx_wtd_ntr_adj_voyage UNUSABLE'; 
        EXECUTE IMMEDIATE 'ALTER INDEX idx_wtd_ntr_adj_year UNUSABLE';

            INSERT /*+ append */ INTO rdm_wtd_ntr_adj_f
            WITH LW AS (
            SELECT
            company_code,
            b_voyage,
            b_bkng_nbr,
            b_p_passenger_id,
            rpt_year,
            vyd_basic_sea_days,
            /* Flags */
            b_p_use AS b_p_use_lw,
            blended_conversion_rate AS blended_conversion_rate_lw,
            b_category AS b_category_lw,
            /* Measures */
            net_ticket_revenue AS net_ticket_revenue_lw,
            hca_rev AS hca_rev_lw,
            air_cost AS air_cost_lw,
            commission AS commission_lw, 
            rev_adj_proj_tc AS rev_adj_proj_tc_lw,
            gap_cocktail AS gap_cocktail_lw,
            b_p_onboard_credit_amt AS b_p_onboard_credit_amt_lw,
            b_p_port_charges AS b_p_port_charges_lw,
            tax_margin AS tax_margin_lw  
            FROM   rdm_wtd_ntr_adj_fs
            WHERE (effective_date <= prior_friday AND expiration_date > prior_friday)
            ),
            CW as (
            SELECT
            t1.company_code,
            t1.b_voyage,
            t1.b_bkng_nbr,
            t1.b_p_passenger_id,
            t1.rpt_year,
            t1.vyd_basic_sea_days,
            /* Flags */
            t1.b_p_use AS b_p_use_cw,
            t1.blended_conversion_rate AS blended_conversion_rate_cw,
            t1.b_category AS b_category_cw,
            adj.retro,
            t1.b_p_conc_cd,
            /* Measures */
            t1.net_ticket_revenue AS net_ticket_revenue_cw,
            t1.hca_rev AS hca_rev_cw,
            t1.air_cost AS air_cost_cw,
            t1.commission AS commission_cw,
            t1.rev_adj_proj_tc AS rev_adj_proj_tc_cw,
            t1.gap_cocktail AS gap_cocktail_cw,
            t1.b_p_onboard_credit_amt AS b_p_onboard_credit_amt_cw,
            t1.b_p_port_charges AS b_p_port_charges_cw,
            t1.tax_margin AS tax_margin_cw   
            FROM   rdm_wtd_ntr_adj_fs t1, 
                        dm_tkr.v_bkng_psngr_man_adj_full_dev@tstdwh1_halw_dwh adj
            WHERE t1.b_voyage = adj.b_voyage(+)
            AND t1.b_bkng_nbr = adj.b_bkng_nbr(+)
            AND t1.b_p_passenger_id = adj.b_p_passenger_id(+)
            AND adj.b_man_data_date(+) BETWEEN t1.prior_friday and t1.prior_friday+7
            AND adj.retro(+) <> 0
            /* Company code needs to be added to bkng_psngr_man_adj_full */
            --AND RDM_WTD_NTR_ADJ_F_stg_data.company_code = adj.company_code(+)  
            AND (t1.effective_date <= t1.prior_friday+7 AND t1.expiration_date > t1.prior_friday+7)
            ),
            BOTH_WKS as (
            SELECT 
                        cw.b_bkng_nbr || cw.b_p_passenger_id AS bkng_id,
                        cw.company_code,
                        cw.rpt_year,
                        cw.b_voyage AS voyage,
                        cw.vyd_basic_sea_days AS sea_days,
                        /* Flags */
                        CASE WHEN cw.b_p_conc_cd IN ('TC', 'UC', 'TL', 'UF', 'HB') OR (lw.rev_adj_proj_tc_lw <> cw.rev_adj_proj_tc_cw) THEN 'Y' ELSE 'N' END 
                            AS tc_flag,
                        CASE WHEN lw.blended_conversion_rate_lw <> cw.blended_conversion_rate_cw THEN 'Y' ELSE 'N' END 
                            AS currency_rate_flag,
                        CASE WHEN cw.retro <> 0 THEN 'Y' ELSE 'N' END 
                            AS retro_flag,
                        CASE WHEN cw.b_p_passenger_id = '999' THEN 'Y' ELSE 'N' END 
                            AS charter_flag,
                        CASE WHEN lw.b_category_lw <> cw.b_category_cw THEN 'Y' ELSE 'N' END 
                            AS category_flag,
                        CASE WHEN lw.b_p_use_lw <> cw.b_p_use_cw THEN 'Y' ELSE 'N' END 
                            AS pax_type_flag,
                        CASE WHEN cw.b_p_conc_cd = 'PU' THEN 'Y' ELSE 'N' END 
                            AS spu_flag,
                        /* Measures */
                        lw.net_ticket_revenue_lw,
                        cw.net_ticket_revenue_cw,
                        lw.hca_rev_lw,
                        cw.hca_rev_cw,
                        lw.air_cost_lw,
                        cw.air_cost_cw,
                        lw.commission_lw,
                        cw.commission_cw,
                        lw.gap_cocktail_lw,
                        cw.gap_cocktail_cw,
                        lw.b_p_onboard_credit_amt_lw,
                        cw.b_p_onboard_credit_amt_cw,
                        lw.b_p_port_charges_lw,
                        cw.b_p_port_charges_cw,
                        lw.tax_margin_lw,
                        cw.tax_margin_cw            
            FROM LW, CW
            WHERE lw.b_bkng_nbr= cw.b_bkng_nbr
            AND lw.b_p_passenger_id = cw.b_p_passenger_id
            AND lw.b_voyage = cw.b_voyage
            AND lw.company_code = cw.company_code            
            ),
            OUTPUT as (
            SELECT
                        bkng_id,
                        company_code,
                        rpt_year,
                        voyage,
                        sea_days,
                        pax_type_flag,
                        category_flag,
                        currency_rate_flag,
                        tc_flag,
                        charter_flag,
                        retro_flag,
                       /* All */
                       (net_ticket_revenue_cw - net_ticket_revenue_lw) 
                          AS all_amt,
                       CASE WHEN (net_ticket_revenue_cw - net_ticket_revenue_lw) <> 0 THEN 1 ELSE 0 END
                          AS ALL_GUESTS,
                       /* Retro */
                       CASE WHEN retro_flag = 'Y' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' 
                                THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END 
                          AS retro_amt,       
                       CASE WHEN (CASE WHEN retro_flag = 'Y' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N'
                                 THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END 
                          AS RETRO_GUESTS,  
                       /* Air Margin */
                       CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' AND (air_cost_cw - air_cost_lw) = 0 
                                THEN (hca_rev_cw - hca_rev_lw) ELSE 0 END 
                          AS air_margin_amt,
                       CASE WHEN (CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' AND (air_cost_cw - air_cost_lw) = 0 
                                 THEN (hca_rev_cw - hca_rev_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END
                          AS AIR_MARGIN_GUESTS, 
                       (air_cost_cw - air_cost_lw)
                          AS air_cost_amt,
                       (hca_rev_cw - hca_rev_lw)
                          AS hca_rev_amt,                              
                       /* Air Cost Adj */                    
                       CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' AND (air_cost_cw - air_cost_lw) <> 0 
                                THEN ( (hca_rev_cw - hca_rev_lw) + (air_cost_cw - air_cost_lw) ) ELSE 0 END
                          AS air_cost_adj_amt,                     
                       CASE WHEN (CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' AND (air_cost_cw - air_cost_lw) <> 0 
                                 THEN ( (hca_rev_cw - hca_rev_lw) + (air_cost_cw - air_cost_lw) ) ELSE 0 END) <> 0 THEN 1 ELSE 0 END 
                          AS AIR_COST_ADJ_GUESTS,           
                       /* Commission */
                       CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                THEN (commission_cw - commission_lw) ELSE 0 END 
                          AS commission_amt, 
                       CASE WHEN (CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                 THEN (commission_cw - commission_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END 
                          AS COMMISSION_GUESTS,  
                       /* TC */
                       CASE WHEN tc_flag = 'Y' AND currency_rate_flag = 'N' AND charter_flag = 'N' 
                                THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END
                          AS tc_amt,
                       CASE WHEN (CASE WHEN tc_flag = 'Y' AND currency_rate_flag = 'N' AND charter_flag = 'N'  
                                 THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END
                          AS TC_GUESTS,  
                       /* Onboard Credit */     
                       CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                THEN (b_p_onboard_credit_amt_cw - b_p_onboard_credit_amt_lw) ELSE 0 END
                          AS onboard_credit_amt,
                       CASE WHEN (CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                 THEN (b_p_onboard_credit_amt_cw - b_p_onboard_credit_amt_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END
                          AS ONBOARD_CREDIT_GUESTS,   
                       /* Port Charges */       
                       CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                THEN (b_p_port_charges_cw - b_p_port_charges_lw) ELSE 0 END 
                          AS port_charges_amt,
                       CASE WHEN (CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                 THEN (b_p_port_charges_cw - b_p_port_charges_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END
                          AS PORT_CHARGES_GUESTS,  
                       /* Tax Margin */  
                        CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                 THEN (tax_margin_cw - tax_margin_lw) ELSE 0 END
                          AS tax_margin_amt,         
                       CASE WHEN (CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                 THEN (tax_margin_cw - tax_margin_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END
                          AS TAX_MARGIN_GUESTS,  
                       /* GAP/Cocktail */
                       CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                THEN (gap_cocktail_cw - gap_cocktail_lw) ELSE 0 END
                          AS gap_cocktail_amt,
                       CASE WHEN (CASE WHEN pax_type_flag = 'N' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                 THEN (gap_cocktail_cw - gap_cocktail_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END
                          AS GAP_COCKTAIL_GUESTS,   
                       /* Currency */
                       CASE WHEN currency_rate_flag = 'Y' AND charter_flag = 'N' 
                                THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END
                          AS currency_amt,
                       CASE WHEN (CASE WHEN currency_rate_flag = 'Y' AND charter_flag = 'N'  
                                 THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END 
                          AS CURRENCY_GUESTS,  
                       /* Charter */
                       CASE WHEN charter_flag = 'Y' 
                                THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END
                          AS charter_amt,                    
                       CASE WHEN (CASE WHEN charter_flag = 'Y'  
                                 THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END
                          AS CHARTER_GUESTS,  
                       /* SPU */
                       CASE WHEN spu_flag = 'Y' AND category_flag = 'Y' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END
                          AS spu_amt, 
                       CASE WHEN (CASE WHEN spu_flag = 'Y' AND category_flag = 'Y' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N'  
                                 THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END
                          AS SPU_GUESTS, 
                       /* Category */
                       CASE WHEN category_flag = 'Y' AND spu_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END
                          AS category_amt,
                       CASE WHEN (CASE WHEN category_flag = 'Y' AND spu_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N'  
                                 THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END
                          AS CATEGORY_GUESTS, 
                       /* Pax Type */
                       CASE WHEN pax_type_flag = 'Y' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END
                          AS GUEST_TYPE_AMT,
                       CASE WHEN (CASE WHEN pax_type_flag = 'Y' AND category_flag = 'N' AND currency_rate_flag = 'N' AND tc_flag = 'N' AND charter_flag = 'N' AND retro_flag = 'N' 
                                 THEN (net_ticket_revenue_cw - net_ticket_revenue_lw) ELSE 0 END) <> 0 THEN 1 ELSE 0 END 
                          AS GUEST_TYPE_GUESTS
            FROM BOTH_WKS
            )
            SELECT
                        DISTINCT
                        bkng_id,
                        company_code,
                        rpt_year,
                        voyage,
                        sea_days,
                        all_amt,
                        all_guests,
                        retro_amt,
                        retro_guests,
                        air_margin_amt,
                        air_margin_guests,
                        air_cost_adj_amt,
                        air_cost_adj_guests,
                        commission_amt,
                        commission_guests,
                        tc_amt,
                        tc_guests,
                        (onboard_credit_amt * -1) onboard_credit_amt,
                        onboard_credit_guests,
                        port_charges_amt,
                        port_charges_guests,
                        tax_margin_amt,
                        tax_margin_guests,
                        gap_cocktail_amt,
                        gap_cocktail_guests,
                        currency_amt,
                        currency_guests,
                        charter_amt,
                        charter_guests,
                        spu_amt,
                        spu_guests,
                        category_amt,
                        category_guests,
                        guest_type_amt,
                        guest_type_guests,
                        /* Retro amt needs to be added to Others calculation */
                        /* Others */
                        (all_amt - air_margin_amt - air_cost_adj_amt - commission_amt - tc_amt - (onboard_credit_amt * -1) - 
                        port_charges_amt - tax_margin_amt - gap_cocktail_amt - currency_amt - charter_amt - spu_amt - category_amt - GUEST_TYPE_AMT) 
                            AS others_amt,
                        CASE WHEN (ROUND(all_amt,2) - ROUND(air_margin_amt,2) - ROUND(air_cost_adj_amt,2) - ROUND(commission_amt,2) - ROUND(tc_amt,2) - ROUND((onboard_credit_amt * -1),2) - 
                        ROUND(port_charges_amt,2) - ROUND(tax_margin_amt,2) - ROUND(gap_cocktail_amt,2) - ROUND(currency_amt,2) - ROUND(charter_amt,2) - ROUND(spu_amt,2) - ROUND(category_amt,2) - ROUND(GUEST_TYPE_AMT,2)) <> 0 THEN 1 ELSE 0 END
                            AS OTHERS_GUESTS              
            FROM output
            WHERE all_amt <> 0
            ORDER BY company_code, rpt_year, voyage, bkng_id;
        
                x:=SQL%ROWCOUNT; 
                sComment  := 'rdm_wtd_ntr_adj_f Inserts: '||x;  

        EXECUTE IMMEDIATE 'ALTER INDEX idx_wtd_ntr_adj_bkng_id REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_wtd_ntr_adj_voyage REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_wtd_ntr_adj_year REBUILD';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_wtd_ntr_adj_f LOGGING';

        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END WTD_NTR_ADJ_LOAD; 
   PROCEDURE REV_BKNG_ACT_LOAD IS
   /* This load is dependent upon DLY_NTR_NXN_LOAD and WTD_NTR_ADJ_LOAD */
        cProcedure_NM  CONSTANT  Varchar2(30) := 'REV_BKNG_ACT_LOAD';
        BatchID Number := 0;

  BEGIN
         /* Log program start */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Prog_Status => 'I',
                    in_Comments => 'Job in Progress',
                    in_Error_Message => 'NA'
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

        EXECUTE IMMEDIATE 'ALTER TABLE rev_bkng_activity NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rev_bkng_activity';

        EXECUTE IMMEDIATE 'ALTER INDEX idx_rev_bkng_activity_voyage UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_rev_bkng_activity_year UNUSABLE';

        INSERT /*+ append */ INTO rev_bkng_activity
        WITH bkng_data_detail as (
        SELECT /*+ parallel (t,4) parallel (r9,4) */
            v.company_code,
            v.rpt_year,
            v.rpt_qtr,
            v.season,
            v.season_description,
            v.trade,
            v.trade_description,
            v.subtrade,
            v.subtrade_description,
            v.ship_code || ' - ' || v.ship_name as ship,
            v.sail_date,
            v.from_port,
            v.to_port,
            b.tours_indicator as ct_flag,
            v.voyage,
            v.sea_days as days,
            CASE WHEN b.pax_id = '999'
                       THEN 'Y'
                       ELSE (CASE WHEN v.voyage_status = 'H' THEN 'Y' ELSE 'N' END)
            END AS charter,
            v.voyage_status,
            v.voyage_type,                
            b.apo_meta,
            b.meta,
            b.promo_sob,
            b.prom_description,
--            b.bkng_source,
            CASE WHEN b.pax_id = '999'
                       THEN 'CHARTER'
                       ELSE b.bkng_source
            END AS bkng_source,
            b.bkng_type,
            b.fare_type,
            b.channel_type,
            b.bkng_channel,
            case when a.country_name in ('GREAT BRITAIN','UNITED KINGDOM') then 'GREAT BRITAIN' else a.country_name end as agency_country,
            b.source_second,
            a.agency_nbr,
            a.agency_name,
            r9.beds_new_prior_fri,
            r9.beds_new_sat,
            r9.beds_new_sun,
            r9.beds_new_mon,
            r9.beds_new_tue,
            r9.beds_new_wed,
            r9.beds_new_thu,
            r9.beds_new_fri,
            r9.beds_xld_prior_fri,
            r9.beds_xld_sat,
            r9.beds_xld_sun,
            r9.beds_xld_mon,
            r9.beds_xld_tue,
            r9.beds_xld_wed,
            r9.beds_xld_thu,
            r9.beds_xld_fri,
            r9.beds_changed_prior_fri,
            r9.beds_changed_sat,
            r9.beds_changed_sun,
            r9.beds_changed_mon,
            r9.beds_changed_tue,
            r9.beds_changed_wed,
            r9.beds_changed_thu,
            r9.beds_changed_fri,
            r9.beds_net_prior_fri_accum,
            r9.beds_net_prior_fri,
            r9.beds_net_sat,
            r9.beds_net_sun,
            r9.beds_net_mon,
            r9.beds_net_tue,
            r9.beds_net_wed,
            r9.beds_net_thu,
            r9.beds_net_fri,
            r9.beds_net_wtd,
            r9.beds_net_daily,
            r9.ntr_new_prior_fri,
            r9.ntr_new_sat,
            r9.ntr_new_sun,
            r9.ntr_new_mon,
            r9.ntr_new_tue,
            r9.ntr_new_wed,
            r9.ntr_new_thu,
            r9.ntr_new_fri,
            r9.ntr_xld_prior_fri,
            r9.ntr_xld_sat,
            r9.ntr_xld_sun,
            r9.ntr_xld_mon,
            r9.ntr_xld_tue,
            r9.ntr_xld_wed,
            r9.ntr_xld_thu,
            r9.ntr_xld_fri,
            r9.ntr_changed_prior_fri,
            r9.ntr_changed_sat,
            r9.ntr_changed_sun,
            r9.ntr_changed_mon,
            r9.ntr_changed_tue,
            r9.ntr_changed_wed,
            r9.ntr_changed_thu,
            r9.ntr_changed_fri,
            r9.ntr_net_prior_fri_accum,
            r9.ntr_net_prior_fri,
            r9.ntr_net_sat,
            r9.ntr_net_sun,
            r9.ntr_net_mon,
            r9.ntr_net_tue,
            r9.ntr_net_wed,
            r9.ntr_net_thu,
            r9.ntr_net_fri,
            r9.ntr_net_wtd,
            r9.ntr_net_daily,
            r9.guests_net_ytd,
            r9.beds_net_ytd,
            r9.ntr_net_ytd,
            r9.pcd_net_ytd,
            r9.beds_rtce_net_ytd,
            r9.guests_net_prior_ytd,
            r9.beds_net_prior_ytd,
            r9.ntr_net_prior_ytd,
            r9.pcd_net_prior_ytd,
            r9.pcd_new_prior_fri,
            r9.pcd_new_sat,
            r9.pcd_new_sun,
            r9.pcd_new_mon,
            r9.pcd_new_tue,
            r9.pcd_new_wed,
            r9.pcd_new_thu,
            r9.pcd_new_fri,
            r9.pcd_xld_prior_fri,
            r9.pcd_xld_sat,
            r9.pcd_xld_sun,
            r9.pcd_xld_mon,
            r9.pcd_xld_tue,
            r9.pcd_xld_wed,
            r9.pcd_xld_thu,
            r9.pcd_xld_fri,
            r9.pcd_changed_prior_fri,
            r9.pcd_changed_sat,
            r9.pcd_changed_sun,
            r9.pcd_changed_mon,
            r9.pcd_changed_tue,
            r9.pcd_changed_wed,
            r9.pcd_changed_thu,
            r9.pcd_changed_fri,
            r9.pcd_net_prior_fri_accum,
            r9.pcd_net_prior_fri,
            r9.pcd_net_sat,
            r9.pcd_net_sun,
            r9.pcd_net_mon,
            r9.pcd_net_tue,
            r9.pcd_net_wed,
            r9.pcd_net_thu,
            r9.pcd_net_fri,
            r9.pcd_net_wtd,
            r9.pcd_net_daily,
            r9.guests_new_prior_fri,
            r9.guests_new_sat,
            r9.guests_new_sun,
            r9.guests_new_mon,
            r9.guests_new_tue,
            r9.guests_new_wed,
            r9.guests_new_thu,
            r9.guests_new_fri,
            r9.guests_xld_prior_fri,
            r9.guests_xld_sat,
            r9.guests_xld_sun,
            r9.guests_xld_mon,
            r9.guests_xld_tue,
            r9.guests_xld_wed,
            r9.guests_xld_thu,
            r9.guests_xld_fri,
            r9.guests_net_prior_fri_accum,
            r9.guests_net_prior_fri,
            r9.guests_net_sat,
            r9.guests_net_sun,
            r9.guests_net_mon,
            r9.guests_net_tue,
            r9.guests_net_wed,
            r9.guests_net_thu,
            r9.guests_net_fri,
            r9.guests_net_wtd,
            r9.guests_net_daily,
            r11.all_amt,
            r11.all_guests,
            r11.retro_amt,
            r11.retro_guests,
            r11.air_margin_amt,
            r11.air_margin_guests,
            r11.air_cost_adj_amt,
            r11.air_cost_adj_guests,
            r11.commission_amt,
            r11.commission_guests,
            r11.tc_amt,
            r11.tc_guests,
            r11.onboard_credit_amt,
            r11.onboard_credit_guests,
            r11.port_charges_amt,
            r11.port_charges_guests,
            r11.tax_margin_amt,
            r11.tax_margin_guests,
            r11.gap_cocktail_amt,
            r11.gap_cocktail_guests,
            r11.currency_amt,
            r11.currency_guests,
            r11.charter_amt,
            r11.charter_guests,
            r11.spu_amt,
            r11.spu_guests,
            r11.category_amt,
            r11.category_guests,
            r11.guest_type_amt,
            r11.guest_type_guests,
            r11.others_amt,
            r11.others_guests,
            r9.beds_net_wtd_stlw,
            r9.ntr_net_wtd_stlw,
            r9.pcd_net_wtd_stlw,
            r9.guests_net_wtd_stlw,
            r9.beds_net_wtd_stly,
            r9.ntr_net_wtd_stly,
            r9.pcd_net_wtd_stly,
            r9.guests_net_wtd_stly,
            r9.single_guests,
            r9.extra_guests,
            r9.beds_new_sat_lw,
            r9.beds_new_sun_lw,
            r9.beds_new_mon_lw,
            r9.beds_new_tue_lw,
            r9.beds_new_wed_lw,
            r9.beds_new_thu_lw,
            r9.beds_new_fri_lw,
            r9.beds_xld_sat_lw,
            r9.beds_xld_sun_lw,
            r9.beds_xld_mon_lw,
            r9.beds_xld_tue_lw,
            r9.beds_xld_wed_lw,
            r9.beds_xld_thu_lw,
            r9.beds_xld_fri_lw,
            r9.beds_changed_sat_lw,
            r9.beds_changed_sun_lw,
            r9.beds_changed_mon_lw,
            r9.beds_changed_tue_lw,
            r9.beds_changed_wed_lw,
            r9.beds_changed_thu_lw,
            r9.beds_changed_fri_lw,
            r9.ntr_new_sat_lw,
            r9.ntr_new_sun_lw,
            r9.ntr_new_mon_lw,
            r9.ntr_new_tue_lw,
            r9.ntr_new_wed_lw,
            r9.ntr_new_thu_lw,
            r9.ntr_new_fri_lw,
            r9.ntr_xld_sat_lw,
            r9.ntr_xld_sun_lw,
            r9.ntr_xld_mon_lw,
            r9.ntr_xld_tue_lw,
            r9.ntr_xld_wed_lw,
            r9.ntr_xld_thu_lw,
            r9.ntr_xld_fri_lw,
            r9.ntr_changed_sat_lw,
            r9.ntr_changed_sun_lw,
            r9.ntr_changed_mon_lw,
            r9.ntr_changed_tue_lw,
            r9.ntr_changed_wed_lw,
            r9.ntr_changed_thu_lw,
            r9.ntr_changed_fri_lw,
            r9.pcd_new_sat_lw,
            r9.pcd_new_sun_lw,
            r9.pcd_new_mon_lw,
            r9.pcd_new_tue_lw,
            r9.pcd_new_wed_lw,
            r9.pcd_new_thu_lw,
            r9.pcd_new_fri_lw,
            r9.pcd_xld_sat_lw,
            r9.pcd_xld_sun_lw,
            r9.pcd_xld_mon_lw,
            r9.pcd_xld_tue_lw,
            r9.pcd_xld_wed_lw,
            r9.pcd_xld_thu_lw,
            r9.pcd_xld_fri_lw,
            r9.pcd_changed_sat_lw,
            r9.pcd_changed_sun_lw,
            r9.pcd_changed_mon_lw,
            r9.pcd_changed_tue_lw,
            r9.pcd_changed_wed_lw,
            r9.pcd_changed_thu_lw,
            r9.pcd_changed_fri_lw,
            r9.guests_new_sat_lw,
            r9.guests_new_sun_lw,
            r9.guests_new_mon_lw,
            r9.guests_new_tue_lw,
            r9.guests_new_wed_lw,
            r9.guests_new_thu_lw,
            r9.guests_new_fri_lw,
            r9.guests_xld_sat_lw,
            r9.guests_xld_sun_lw,
            r9.guests_xld_mon_lw,
            r9.guests_xld_tue_lw,
            r9.guests_xld_wed_lw,
            r9.guests_xld_thu_lw,
            r9.guests_xld_fri_lw,
            r9.beds_new_wtd_stlw,
            r9.beds_xld_wtd_stlw,
            r9.beds_changed_wtd_stlw,
            r9.ntr_new_wtd_stlw,
            r9.ntr_xld_wtd_stlw,
            r9.ntr_changed_wtd_stlw,
            r9.pcd_new_wtd_stlw,
            r9.pcd_xld_wtd_stlw,
            r9.pcd_changed_wtd_stlw,
            r9.guests_new_wtd_stlw,
            r9.guests_xld_wtd_stlw,
            r9.beds_new_daily,
            r9.beds_xld_daily,
            r9.beds_changed_daily,
            r9.ntr_new_daily,
            r9.ntr_xld_daily,
            r9.ntr_changed_daily,
            r9.pcd_new_daily,
            r9.pcd_xld_daily,
            r9.pcd_changed_daily,
            b.category
        FROM 
                rdm_trans_f t,
                rdm_booking_d b,
                rdm_voyage_d v,
                rdm_agent_d a,
                rdm_dly_ntr_new_xld_net_f r9,
                rdm_wtd_ntr_adj_f r11,
                rdm_date_d dt
           WHERE t.bkng_id = b.bkng_id
               AND t.agency_id = a.agency_id
               AND b.company_code = a.company_code
               AND t.voyage = v.voyage
               AND b.company_code = v.company_code
               AND t.bkng_id = r9.bkng_id
               AND b.company_code = r9.company_code
               AND t.bkng_id = r11.bkng_id(+)
               AND t.rpt_year >= dt.rpt_year-1
--               AND t.voyage = 'D482'
--               AND t.voyage = 'V240A'
        ),
        bkng_data_summary AS (
        SELECT
        t1.company_code,
        t1.rpt_year,
        t1.rpt_qtr,
        t1.season,
        t1.season_description,
        t1.trade,
        t1.trade_description,
        t1.subtrade,
        t1.subtrade_description,
        t1.ship,
        t1.sail_date,
        t1.from_port,
        t1.to_port,
        t1.ct_flag,
        t1.voyage,
        t1.days,
        t1.charter,
        t1.voyage_status,
        t1.voyage_type,
        t1.apo_meta,
        t1.meta,
        t1.promo_sob,
        t1.prom_description,
        t1.bkng_source,
        t1.bkng_type,
        t1.fare_type,
        t1.channel_type,
        t1.bkng_channel,
        t1.agency_country,
        t1.source_second,
        t1.agency_nbr,
        t1.agency_name,
        --Beds New
            NVL(SUM (beds_new_prior_fri),0) as beds_new_prior_fri,
            NVL(SUM (beds_new_sat),0) as beds_new_sat,
            NVL(SUM (beds_new_sun),0) as beds_new_sun,
            NVL(SUM (beds_new_mon),0) as beds_new_mon,
            NVL(SUM (beds_new_tue),0) as beds_new_tue,
            NVL(SUM (beds_new_wed),0) as beds_new_wed,
            NVL(SUM (beds_new_thu),0) as beds_new_thu,
            NVL(SUM (beds_new_fri),0) as beds_new_fri,
        --Beds Xld
            NVL(SUM (beds_xld_prior_fri),0) as beds_xld_prior_fri,
            NVL(SUM (beds_xld_sat),0) as beds_xld_sat,
            NVL(SUM (beds_xld_sun),0) as beds_xld_sun,
            NVL(SUM (beds_xld_mon),0) as beds_xld_mon,
            NVL(SUM (beds_xld_tue),0) as beds_xld_tue,
            NVL(SUM (beds_xld_wed),0) as beds_xld_wed,
            NVL(SUM (beds_xld_thu),0) as beds_xld_thu,
            NVL(SUM (beds_xld_fri),0) as beds_xld_fri,
        --Beds Changed
            NVL(SUM (beds_changed_prior_fri),0) as beds_changed_prior_fri,
            NVL(SUM (beds_changed_sat),0) as beds_changed_sat,
            NVL(SUM (beds_changed_sun),0) as beds_changed_sun,
            NVL(SUM (beds_changed_mon),0) as beds_changed_mon,
            NVL(SUM (beds_changed_tue),0) as beds_changed_tue,
            NVL(SUM (beds_changed_wed),0) as beds_changed_wed,
            NVL(SUM (beds_changed_thu),0) as beds_changed_thu,
            NVL(SUM (beds_changed_fri),0) as beds_changed_fri,
        --Beds Net
            NVL(SUM (beds_net_prior_fri_accum),0) as beds_net_prior_fri_accum,
            NVL(SUM (beds_net_prior_fri),0) as beds_net_prior_fri,
            NVL(SUM (beds_net_sat),0) as beds_net_sat,
            NVL(SUM (beds_net_sun),0) as beds_net_sun,
            NVL(SUM (beds_net_mon),0) as beds_net_mon,
            NVL(SUM (beds_net_tue),0) as beds_net_tue,
            NVL(SUM (beds_net_wed),0) as beds_net_wed,
            NVL(SUM (beds_net_thu),0) as beds_net_thu,
            NVL(SUM (beds_net_fri),0) as beds_net_fri,
            NVL(SUM (beds_net_wtd),0) as beds_net_wtd,
            NVL(SUM (beds_net_daily),0) as beds_net_daily,
        --NTR New
            NVL(ROUND(SUM(ntr_new_prior_fri),2),0) as ntr_new_prior_fri,
            NVL(ROUND(SUM(ntr_new_sat),2),0) as ntr_new_sat,
            NVL(ROUND(SUM(ntr_new_sun),2),0) as ntr_new_sun,
            NVL(ROUND(SUM(ntr_new_mon),2),0) as ntr_new_mon,
            NVL(ROUND(SUM(ntr_new_tue),2),0) as ntr_new_tue,
            NVL(ROUND(SUM(ntr_new_wed),2),0) as ntr_new_wed,
            NVL(ROUND(SUM(ntr_new_thu),2),0) as ntr_new_thu,
            NVL(ROUND(SUM(ntr_new_fri),2),0) as ntr_new_fri,
        --NTR Xld
            NVL(ROUND(SUM(ntr_xld_prior_fri),2),0) as ntr_xld_prior_fri,
            NVL(ROUND(SUM(ntr_xld_sat),2),0) as ntr_xld_sat,
            NVL(ROUND(SUM(ntr_xld_sun),2),0) as ntr_xld_sun,
            NVL(ROUND(SUM(ntr_xld_mon),2),0) as ntr_xld_mon,
            NVL(ROUND(SUM(ntr_xld_tue),2),0) as ntr_xld_tue,
            NVL(ROUND(SUM(ntr_xld_wed),2),0) as ntr_xld_wed,
            NVL(ROUND(SUM(ntr_xld_thu),2),0) as ntr_xld_thu,
            NVL(ROUND(SUM(ntr_xld_fri),2),0) as ntr_xld_fri,
        --NTR Changed
            NVL(ROUND(SUM(ntr_changed_prior_fri),2),0) as ntr_changed_prior_fri,
            NVL(ROUND(SUM(ntr_changed_sat),2),0) as ntr_changed_sat,
            NVL(ROUND(SUM(ntr_changed_sun),2),0) as ntr_changed_sun,
            NVL(ROUND(SUM(ntr_changed_mon),2),0) as ntr_changed_mon,
            NVL(ROUND(SUM(ntr_changed_tue),2),0) as ntr_changed_tue,
            NVL(ROUND(SUM(ntr_changed_wed),2),0) as ntr_changed_wed,
            NVL(ROUND(SUM(ntr_changed_thu),2),0) as ntr_changed_thu,
            NVL(ROUND(SUM(ntr_changed_fri),2),0) as ntr_changed_fri,
        --NTR Net
            NVL(ROUND(SUM(ntr_net_prior_fri_accum),2),0) as ntr_net_prior_fri_accum,
            NVL(ROUND(SUM(ntr_net_prior_fri),2),0) as ntr_net_prior_fri,
            NVL(ROUND(SUM(ntr_net_sat),2),0) as ntr_net_sat,
            NVL(ROUND(SUM(ntr_net_sun),2),0) as ntr_net_sun,
            NVL(ROUND(SUM(ntr_net_mon),2),0) as ntr_net_mon,
            NVL(ROUND(SUM(ntr_net_tue),2),0) as ntr_net_tue,
            NVL(ROUND(SUM(ntr_net_wed),2),0) as ntr_net_wed,
            NVL(ROUND(SUM(ntr_net_thu),2),0) as ntr_net_thu,
            NVL(ROUND(SUM(ntr_net_fri),2),0) as ntr_net_fri,
            NVL(ROUND(SUM(ntr_net_wtd),2),0) as ntr_net_wtd,
            NVL(ROUND(SUM(ntr_net_daily),2),0) as ntr_net_daily,
        --YTD
            NVL(SUM (guests_net_ytd),0) as guests_net_ytd,
            NVL(SUM (beds_net_ytd),0) as beds_net_ytd,
            NVL(ROUND(SUM(ntr_net_ytd),2),0) as ntr_net_ytd,
            NVL(SUM(pcd_net_ytd),0) as pcd_net_ytd,
            NVL(SUM(beds_rtce_net_ytd),0) as beds_rtce_net_ytd,
         --STLY
            NVL(SUM (guests_net_prior_ytd),0) as guests_net_prior_ytd,
            NVL(SUM (beds_net_prior_ytd),0) as beds_net_prior_ytd,
            NVL(ROUND(SUM(ntr_net_prior_ytd),2),0) as ntr_net_prior_ytd,
            NVL(SUM(pcd_net_prior_ytd),0) as pcd_net_prior_ytd,
        --PCD New
            NVL(SUM(pcd_new_prior_fri),0) as pcd_new_prior_fri,
            NVL(SUM(pcd_new_sat),0) as pcd_new_sat,
            NVL(SUM(pcd_new_sun),0) as pcd_new_sun,
            NVL(SUM(pcd_new_mon),0) as pcd_new_mon,
            NVL(SUM(pcd_new_tue),0) as pcd_new_tue,
            NVL(SUM(pcd_new_wed),0) as pcd_new_wed,
            NVL(SUM(pcd_new_thu),0) as pcd_new_thu,
            NVL(SUM(pcd_new_fri),0) as pcd_new_fri,
        --PCD Xld
            NVL(SUM(pcd_xld_prior_fri),0) as pcd_xld_prior_fri,
            NVL(SUM(pcd_xld_sat),0) as pcd_xld_sat,
            NVL(SUM(pcd_xld_sun),0) as pcd_xld_sun,
            NVL(SUM(pcd_xld_mon),0) as pcd_xld_mon,
            NVL(SUM(pcd_xld_tue),0) as pcd_xld_tue,
            NVL(SUM(pcd_xld_wed),0) as pcd_xld_wed,
            NVL(SUM(pcd_xld_thu),0) as pcd_xld_thu,
            NVL(SUM(pcd_xld_fri),0) as pcd_xld_fri,
        --PCD Changed
            NVL(SUM(pcd_changed_prior_fri),0) as pcd_changed_prior_fri,
            NVL(SUM(pcd_changed_sat),0) as pcd_changed_sat,
            NVL(SUM(pcd_changed_sun),0) as pcd_changed_sun,
            NVL(SUM(pcd_changed_mon),0) as pcd_changed_mon,
            NVL(SUM(pcd_changed_tue),0) as pcd_changed_tue,
            NVL(SUM(pcd_changed_wed),0) as pcd_changed_wed,
            NVL(SUM(pcd_changed_thu),0) as pcd_changed_thu,
            NVL(SUM(pcd_changed_fri),0) as pcd_changed_fri,
        --PCD Net
            NVL(SUM(pcd_net_prior_fri_accum),0) as pcd_net_prior_fri_accum,
            NVL(SUM(pcd_net_prior_fri),0) as pcd_net_prior_fri,
            NVL(SUM(pcd_net_sat),0) as pcd_net_sat,
            NVL(SUM(pcd_net_sun),0) as pcd_net_sun,
            NVL(SUM(pcd_net_mon),0) as pcd_net_mon,
            NVL(SUM(pcd_net_tue),0) as pcd_net_tue,
            NVL(SUM(pcd_net_wed),0) as pcd_net_wed,
            NVL(SUM(pcd_net_thu),0) as pcd_net_thu,
            NVL(SUM(pcd_net_fri),0) as pcd_net_fri,
            NVL(SUM(pcd_net_wtd),0) as pcd_net_wtd,
            NVL(SUM(pcd_net_daily),0) as pcd_net_daily,
        --Guests New
            NVL(SUM (guests_new_prior_fri),0) as guests_new_prior_fri,
            NVL(SUM (guests_new_sat),0) as guests_new_sat,
            NVL(SUM (guests_new_sun),0) as guests_new_sun,
            NVL(SUM (guests_new_mon),0) as guests_new_mon,
            NVL(SUM (guests_new_tue),0) as guests_new_tue,
            NVL(SUM (guests_new_wed),0) as guests_new_wed,
            NVL(SUM (guests_new_thu),0) as guests_new_thu,
            NVL(SUM (guests_new_fri),0) as guests_new_fri,
        --Guests Xld
            NVL(SUM (guests_xld_prior_fri),0) as guests_xld_prior_fri,
            NVL(SUM (guests_xld_sat),0) as guests_xld_sat,
            NVL(SUM (guests_xld_sun),0) as guests_xld_sun,
            NVL(SUM (guests_xld_mon),0) as guests_xld_mon,
            NVL(SUM (guests_xld_tue),0) as guests_xld_tue,
            NVL(SUM (guests_xld_wed),0) as guests_xld_wed,
            NVL(SUM (guests_xld_thu),0) as guests_xld_thu,
            NVL(SUM (guests_xld_fri),0) as guests_xld_fri,
        --Guests Net
            NVL(SUM (guests_net_prior_fri_accum),0) as guests_net_prior_fri_accum,
            NVL(SUM (guests_net_prior_fri),0) as guests_net_prior_fri,
            NVL(SUM (guests_net_sat),0) as guests_net_sat,
            NVL(SUM (guests_net_sun),0) as guests_net_sun,
            NVL(SUM (guests_net_mon),0) as guests_net_mon,
            NVL(SUM (guests_net_tue),0) as guests_net_tue,
            NVL(SUM (guests_net_wed),0) as guests_net_wed,
            NVL(SUM (guests_net_thu),0) as guests_net_thu,
            NVL(SUM (guests_net_fri),0) as guests_net_fri,
            NVL(SUM (guests_net_wtd),0) as guests_net_wtd,
            NVL(SUM (guests_net_daily),0) as guests_net_daily,
        --RDM_WTD_NTR_ADJ_F
                 NVL(SUM (all_amt),0) all_amt,
                 NVL(SUM (all_guests),0) all_guests,
                 NVL(SUM (retro_amt),0) retro_amt,
                 NVL(SUM (retro_guests),0) retro_guests,
                 NVL(SUM (air_margin_amt),0) air_margin_amt,
                 NVL(SUM (air_margin_guests),0) air_margin_guests,
                 NVL(SUM (air_cost_adj_amt),0) air_cost_adj_amt,
                 NVL(SUM (air_cost_adj_guests),0) air_cost_adj_guests,
                 NVL(SUM (commission_amt),0) commission_amt,
                 NVL(SUM (commission_guests),0) commission_guests,
                 NVL(SUM (tc_amt),0) tc_amt,
                 NVL(SUM (tc_guests),0) tc_guests,
                 NVL(SUM (onboard_credit_amt),0) onboard_credit_amt,
                 NVL(SUM (onboard_credit_guests),0) onboard_credit_guests,
                 NVL(SUM (port_charges_amt),0) port_charges_amt,
                 NVL(SUM (port_charges_guests),0) port_charges_guests,
                 NVL(SUM (tax_margin_amt),0) tax_margin_amt,
                 NVL(SUM (tax_margin_guests),0) tax_margin_guests,
                 NVL(SUM (gap_cocktail_amt),0) gap_cocktail_amt,
                 NVL(SUM (gap_cocktail_guests),0) gap_cocktail_guests,
                 NVL(SUM (currency_amt),0) currency_amt,
                 NVL(SUM (currency_guests),0) currency_guests,
                 NVL(SUM (charter_amt),0) charter_amt,
                 NVL(SUM (charter_guests),0) charter_guests,
                 NVL(SUM (spu_amt),0) spu_amt,
                 NVL(SUM (spu_guests),0) spu_guests,
                 NVL(SUM (category_amt),0) category_amt,
                 NVL(SUM (category_guests),0) category_guests,
                 NVL(SUM (guest_type_amt),0) guest_type_amt,
                 NVL(SUM (guest_type_guests),0) guest_type_guests,
                 NVL(SUM (others_amt),0) others_amt,
                 NVL(SUM (others_guests),0) others_guests,
        --New fields since version 1
                 NVL(SUM(beds_net_wtd_stlw),0) as beds_net_wtd_stlw,
                 NVL(SUM(ntr_net_wtd_stlw),0) as ntr_net_wtd_stlw,
                 NVL(SUM(pcd_net_wtd_stlw),0) as pcd_net_wtd_stlw,
                 NVL(SUM(guests_net_wtd_stlw),0) as guests_net_wtd_stlw,
                 NVL(SUM(beds_net_wtd_stly),0) as beds_net_wtd_stly,
                 NVL(SUM(ntr_net_wtd_stly),0) as ntr_net_wtd_stly,
                 NVL(SUM(pcd_net_wtd_stly),0) as pcd_net_wtd_stly,
                 NVL(SUM(guests_net_wtd_stly),0) as guests_net_wtd_stly,
                 NVL(SUM(single_guests),0) as single_guests,
                 NVL(SUM(extra_guests),0) as extra_guests,
        --Beds New, Xld, Changed LW
                 NVL(SUM(beds_new_sat_lw),0) as beds_new_sat_lw,
                 NVL(SUM(beds_new_sun_lw),0) as beds_new_sun_lw,
                 NVL(SUM(beds_new_mon_lw),0) as beds_new_mon_lw,
                 NVL(SUM(beds_new_tue_lw),0) as beds_new_tue_lw,
                 NVL(SUM(beds_new_wed_lw),0) as beds_new_wed_lw,
                 NVL(SUM(beds_new_thu_lw),0) as beds_new_thu_lw,
                 NVL(SUM(beds_new_fri_lw),0) as beds_new_fri_lw,
                 NVL(SUM(beds_xld_sat_lw),0) as beds_xld_sat_lw,
                 NVL(SUM(beds_xld_sun_lw),0) as beds_xld_sun_lw,
                 NVL(SUM(beds_xld_mon_lw),0) as beds_xld_mon_lw,
                 NVL(SUM(beds_xld_tue_lw),0) as beds_xld_tue_lw,
                 NVL(SUM(beds_xld_wed_lw),0) as beds_xld_wed_lw,
                 NVL(SUM(beds_xld_thu_lw),0) as beds_xld_thu_lw,
                 NVL(SUM(beds_xld_fri_lw),0) as beds_xld_fri_lw,
                 NVL(SUM (beds_changed_sat_lw),0) as beds_changed_sat_lw,
                 NVL(SUM (beds_changed_sun_lw),0) as beds_changed_sun_lw,
                 NVL(SUM (beds_changed_mon_lw),0) as beds_changed_mon_lw,
                 NVL(SUM (beds_changed_tue_lw),0) as beds_changed_tue_lw,
                 NVL(SUM (beds_changed_wed_lw),0) as beds_changed_wed_lw,
                 NVL(SUM (beds_changed_thu_lw),0) as beds_changed_thu_lw,
                 NVL(SUM (beds_changed_fri_lw),0) as beds_changed_fri_lw,
        --NTR New, Xld, Changed LW
                 NVL(SUM(ntr_new_sat_lw),0) as ntr_new_sat_lw,
                 NVL(SUM(ntr_new_sun_lw),0) as ntr_new_sun_lw,
                 NVL(SUM(ntr_new_mon_lw),0) as ntr_new_mon_lw,
                 NVL(SUM(ntr_new_tue_lw),0) as ntr_new_tue_lw,
                 NVL(SUM(ntr_new_wed_lw),0) as ntr_new_wed_lw,
                 NVL(SUM(ntr_new_thu_lw),0) as ntr_new_thu_lw,
                 NVL(SUM(ntr_new_fri_lw),0) as ntr_new_fri_lw,
                 NVL(SUM(ntr_xld_sat_lw),0) as ntr_xld_sat_lw,
                 NVL(SUM(ntr_xld_sun_lw),0) as ntr_xld_sun_lw,
                 NVL(SUM(ntr_xld_mon_lw),0) as ntr_xld_mon_lw,
                 NVL(SUM(ntr_xld_tue_lw),0) as ntr_xld_tue_lw,
                 NVL(SUM(ntr_xld_wed_lw),0) as ntr_xld_wed_lw,
                 NVL(SUM(ntr_xld_thu_lw),0) as ntr_xld_thu_lw,
                 NVL(SUM(ntr_xld_fri_lw),0) as ntr_xld_fri_lw,
                 NVL(SUM (ntr_changed_sat_lw),0) as ntr_changed_sat_lw,
                 NVL(SUM (ntr_changed_sun_lw),0) as ntr_changed_sun_lw,
                 NVL(SUM (ntr_changed_mon_lw),0) as ntr_changed_mon_lw,
                 NVL(SUM (ntr_changed_tue_lw),0) as ntr_changed_tue_lw,
                 NVL(SUM (ntr_changed_wed_lw),0) as ntr_changed_wed_lw,
                 NVL(SUM (ntr_changed_thu_lw),0) as ntr_changed_thu_lw,
                 NVL(SUM (ntr_changed_fri_lw),0) as ntr_changed_fri_lw,
        --PCDs New, Xld, Changed LW
                 NVL(SUM(pcd_new_sat_lw),0) as pcd_new_sat_lw,
                 NVL(SUM(pcd_new_sun_lw),0) as pcd_new_sun_lw,
                 NVL(SUM(pcd_new_mon_lw),0) as pcd_new_mon_lw,
                 NVL(SUM(pcd_new_tue_lw),0) as pcd_new_tue_lw,
                 NVL(SUM(pcd_new_wed_lw),0) as pcd_new_wed_lw,
                 NVL(SUM(pcd_new_thu_lw),0) as pcd_new_thu_lw,
                 NVL(SUM(pcd_new_fri_lw),0) as pcd_new_fri_lw,
                 NVL(SUM(pcd_xld_sat_lw),0) as pcd_xld_sat_lw,
                 NVL(SUM(pcd_xld_sun_lw),0) as pcd_xld_sun_lw,
                 NVL(SUM(pcd_xld_mon_lw),0) as pcd_xld_mon_lw,
                 NVL(SUM(pcd_xld_tue_lw),0) as pcd_xld_tue_lw,
                 NVL(SUM(pcd_xld_wed_lw),0) as pcd_xld_wed_lw,
                 NVL(SUM(pcd_xld_thu_lw),0) as pcd_xld_thu_lw,
                 NVL(SUM(pcd_xld_fri_lw),0) as pcd_xld_fri_lw,
                 NVL(SUM(pcd_changed_sat_lw),0) as pcd_changed_sat_lw,
                 NVL(SUM(pcd_changed_sun_lw),0) as pcd_changed_sun_lw,
                 NVL(SUM(pcd_changed_mon_lw),0) as pcd_changed_mon_lw,
                 NVL(SUM(pcd_changed_tue_lw),0) as pcd_changed_tue_lw,
                 NVL(SUM(pcd_changed_wed_lw),0) as pcd_changed_wed_lw,
                 NVL(SUM(pcd_changed_thu_lw),0) as pcd_changed_thu_lw,
                 NVL(SUM(pcd_changed_fri_lw),0) as pcd_changed_fri_lw,
        --PCDs New, Xld LW
                 NVL(SUM(guests_new_sat_lw),0) as guests_new_sat_lw,
                 NVL(SUM(guests_new_sun_lw),0) as guests_new_sun_lw,
                 NVL(SUM(guests_new_mon_lw),0) as guests_new_mon_lw,
                 NVL(SUM(guests_new_tue_lw),0) as guests_new_tue_lw,
                 NVL(SUM(guests_new_wed_lw),0) as guests_new_wed_lw,
                 NVL(SUM(guests_new_thu_lw),0) as guests_new_thu_lw,
                 NVL(SUM(guests_new_fri_lw),0) as guests_new_fri_lw,
                 NVL(SUM(guests_xld_sat_lw),0) as guests_xld_sat_lw,
                 NVL(SUM(guests_xld_sun_lw),0) as guests_xld_sun_lw,
                 NVL(SUM(guests_xld_mon_lw),0) as guests_xld_mon_lw,
                 NVL(SUM(guests_xld_tue_lw),0) as guests_xld_tue_lw,
                 NVL(SUM(guests_xld_wed_lw),0) as guests_xld_wed_lw,
                 NVL(SUM(guests_xld_thu_lw),0) as guests_xld_thu_lw,
                 NVL(SUM(guests_xld_fri_lw),0) as guests_xld_fri_lw,
        --WTD LW
                 NVL(SUM(beds_new_wtd_stlw),0) as beds_new_wtd_stlw,
                 NVL(SUM(beds_xld_wtd_stlw),0) as beds_xld_wtd_stlw,
                 NVL(SUM(beds_changed_wtd_stlw),0) as beds_changed_wtd_stlw,
                 NVL(SUM(ntr_new_wtd_stlw),0) as ntr_new_wtd_stlw,
                 NVL(SUM(ntr_xld_wtd_stlw),0) as ntr_xld_wtd_stlw,
                 NVL(SUM(ntr_changed_wtd_stlw),0) as ntr_changed_wtd_stlw,
                 NVL(SUM(pcd_new_wtd_stlw),0) as pcd_new_wtd_stlw,
                 NVL(SUM(pcd_xld_wtd_stlw),0) as pcd_xld_wtd_stlw,
                 NVL(SUM(pcd_changed_wtd_stlw),0) as pcd_changed_wtd_stlw,
                 NVL(SUM(guests_new_wtd_stlw),0) as guests_new_wtd_stlw,
                 NVL(SUM(guests_xld_wtd_stlw),0) as guests_xld_wtd_stlw,
        --Daily New, Xld, Changed
                 NVL(SUM(beds_new_daily),0) as beds_new_daily,
                 NVL(SUM(beds_xld_daily),0) as beds_xld_daily,        
                 NVL(SUM(beds_changed_daily),0) as beds_changed_daily,  
                 NVL(SUM(ntr_new_daily),0) as ntr_new_daily,
                 NVL(SUM(ntr_xld_daily),0) as ntr_xld_daily,        
                 NVL(SUM(ntr_changed_daily),0) as ntr_changed_daily,   
                 NVL(SUM(pcd_new_daily),0) as pcd_new_daily,
                 NVL(SUM(pcd_xld_daily),0) as pcd_xld_daily,        
                 NVL(SUM(pcd_changed_daily),0) as pcd_changed_daily,
                 s.meta_revised,
                 s.meta_sort                   
        FROM bkng_data_detail t1, rdm_ship_config_d s
        WHERE t1.company_code = s.company_code(+)
               AND t1.voyage = s.voyage(+)
               AND t1.category = s.category(+)
        GROUP BY t1.company_code, t1.rpt_year, t1.rpt_qtr, t1.season, t1.season_description, t1.trade,
                        t1.trade_description, t1.subtrade, t1.subtrade_description, t1.ship, t1.sail_date,
                        t1.from_port, t1.to_port, t1.ct_flag, t1.voyage, t1.days, t1.charter, t1.voyage_status,
                        t1.voyage_type, t1.apo_meta, t1.meta, t1.promo_sob, t1.prom_description,
                        t1.bkng_source, t1.bkng_type, t1.fare_type, t1.channel_type, t1.bkng_channel,
                        t1.agency_country, t1.source_second, t1.agency_nbr, t1.agency_name, s.meta_revised, s.meta_sort
               )
               SELECT 
                    v.company_code,
                    v.rpt_year,
                    v.rpt_qtr,
                    v.season,
                    v.season_description,
                    v.trade,
                    v.trade_description,
                    v.subtrade,
                    v.subtrade_description,
                    v.ship_code || ' - ' || v.ship_name as ship,
                    v.sail_date,
                    v.from_port,
                    v.to_port,
                    b.ct_flag,
                    v.voyage,
                    v.sea_days as days,
                    NVL(b.charter,(CASE WHEN v.voyage_status = 'H' THEN 'Y' ELSE 'N' END)) AS charter,
                    v.voyage_status,
                    v.voyage_type,  
                    b.apo_meta,
                    b.meta,
                    b.promo_sob,
                    b.prom_description,
                    b.bkng_source,
                    b.bkng_type,
                    b.fare_type,
                    b.channel_type,
                    b.bkng_channel,
                    b.agency_country,
                    b.source_second,
                    b.agency_nbr,
                    b.agency_name,
                    NVL(b.beds_new_prior_fri,0) AS beds_new_prior_fri,
                    NVL(b.beds_new_sat,0) AS beds_new_sat,
                    NVL(b.beds_new_sun,0) AS beds_new_sun,
                    NVL(b.beds_new_mon,0) AS beds_new_mon,
                    NVL(b.beds_new_tue,0) AS beds_new_tue,
                    NVL(b.beds_new_wed,0) AS beds_new_wed,
                    NVL(b.beds_new_thu,0) AS beds_new_thu,
                    NVL(b.beds_new_fri,0) AS beds_new_fri,
                    NVL(b.beds_xld_prior_fri,0) AS beds_xld_prior_fri,
                    NVL(b.beds_xld_sat,0) AS beds_xld_sat,
                    NVL(b.beds_xld_sun,0) AS beds_xld_sun,
                    NVL(b.beds_xld_mon,0) AS beds_xld_mon,
                    NVL(b.beds_xld_tue,0) AS beds_xld_tue,
                    NVL(b.beds_xld_wed,0) AS beds_xld_wed,
                    NVL(b.beds_xld_thu,0) AS beds_xld_thu,
                    NVL(b.beds_xld_fri,0) AS beds_xld_fri,
                    NVL(b.beds_changed_prior_fri,0) AS beds_changed_prior_fri,
                    NVL(b.beds_changed_sat,0) AS beds_changed_sat,
                    NVL(b.beds_changed_sun,0) AS beds_changed_sun,
                    NVL(b.beds_changed_mon,0) AS beds_changed_mon,
                    NVL(b.beds_changed_tue,0) AS beds_changed_tue,
                    NVL(b.beds_changed_wed,0) AS beds_changed_wed,
                    NVL(b.beds_changed_thu,0) AS beds_changed_thu,
                    NVL(b.beds_changed_fri,0) AS beds_changed_fri,
                    NVL(b.beds_net_prior_fri_accum,0) AS beds_net_prior_fri_accum,
                    NVL(b.beds_net_prior_fri,0) AS beds_net_prior_fri,
                    NVL(b.beds_net_sat,0) AS beds_net_sat,
                    NVL(b.beds_net_sun,0) AS beds_net_sun,
                    NVL(b.beds_net_mon,0) AS beds_net_mon,
                    NVL(b.beds_net_tue,0) AS beds_net_tue,
                    NVL(b.beds_net_wed,0) AS beds_net_wed,
                    NVL(b.beds_net_thu,0) AS beds_net_thu,
                    NVL(b.beds_net_fri,0) AS beds_net_fri,
                    NVL(b.beds_net_wtd,0) AS beds_net_wtd,
                    NVL(b.beds_net_daily,0) AS beds_net_daily,
                    NVL(b.ntr_new_prior_fri,0) AS ntr_new_prior_fri,
                    NVL(b.ntr_new_sat,0) AS ntr_new_sat,
                    NVL(b.ntr_new_sun,0) AS ntr_new_sun,
                    NVL(b.ntr_new_mon,0) AS ntr_new_mon,
                    NVL(b.ntr_new_tue,0) AS ntr_new_tue,
                    NVL(b.ntr_new_wed,0) AS ntr_new_wed,
                    NVL(b.ntr_new_thu,0) AS ntr_new_thu,
                    NVL(b.ntr_new_fri,0) AS ntr_new_fri,
                    NVL(b.ntr_xld_prior_fri,0) AS ntr_xld_prior_fri,
                    NVL(b.ntr_xld_sat,0) AS ntr_xld_sat,
                    NVL(b.ntr_xld_sun,0) AS ntr_xld_sun,
                    NVL(b.ntr_xld_mon,0) AS ntr_xld_mon,
                    NVL(b.ntr_xld_tue,0) AS ntr_xld_tue,
                    NVL(b.ntr_xld_wed,0) AS ntr_xld_wed,
                    NVL(b.ntr_xld_thu,0) AS ntr_xld_thu,
                    NVL(b.ntr_xld_fri,0) AS ntr_xld_fri,
                    NVL(b.ntr_changed_prior_fri,0) AS ntr_changed_prior_fri,
                    NVL(b.ntr_changed_sat,0) AS ntr_changed_sat,
                    NVL(b.ntr_changed_sun,0) AS ntr_changed_sun,
                    NVL(b.ntr_changed_mon,0) AS ntr_changed_mon,
                    NVL(b.ntr_changed_tue,0) AS ntr_changed_tue,
                    NVL(b.ntr_changed_wed,0) AS ntr_changed_wed,
                    NVL(b.ntr_changed_thu,0) AS ntr_changed_thu,
                    NVL(b.ntr_changed_fri,0) AS ntr_changed_fri,
                    NVL(b.ntr_net_prior_fri_accum,0) AS ntr_net_prior_fri_accum,
                    NVL(b.ntr_net_prior_fri,0) AS ntr_net_prior_fri,
                    NVL(b.ntr_net_sat,0) AS ntr_net_sat,
                    NVL(b.ntr_net_sun,0) AS ntr_net_sun,
                    NVL(b.ntr_net_mon,0) AS ntr_net_mon,
                    NVL(b.ntr_net_tue,0) AS ntr_net_tue,
                    NVL(b.ntr_net_wed,0) AS ntr_net_wed,
                    NVL(b.ntr_net_thu,0) AS ntr_net_thu,
                    NVL(b.ntr_net_fri,0) AS ntr_net_fri,
                    NVL(b.ntr_net_wtd,0) AS ntr_net_wtd,
                    NVL(b.ntr_net_daily,0) AS ntr_net_daily,
                    NVL(b.guests_net_ytd,0) AS guests_net_ytd,
                    NVL(b.beds_net_ytd,0) AS beds_net_ytd,
                    NVL(b.ntr_net_ytd,0) AS ntr_net_ytd,
                    NVL(b.pcd_net_ytd,0) AS pcd_net_ytd,
                    NVL(b.beds_rtce_net_ytd,0) AS beds_rtce_net_ytd,
                    NVL(b.guests_net_prior_ytd,0) AS guests_net_prior_ytd,
                    NVL(b.beds_net_prior_ytd,0) AS beds_net_prior_ytd,
                    NVL(b.ntr_net_prior_ytd,0) AS ntr_net_prior_ytd,
                    NVL(b.pcd_net_prior_ytd,0) AS pcd_net_prior_ytd,
                    NVL(b.pcd_new_prior_fri,0) AS pcd_new_prior_fri,
                    NVL(b.pcd_new_sat,0) AS pcd_new_sat,
                    NVL(b.pcd_new_sun,0) AS pcd_new_sun,
                    NVL(b.pcd_new_mon,0) AS pcd_new_mon,
                    NVL(b.pcd_new_tue,0) AS pcd_new_tue,
                    NVL(b.pcd_new_wed,0) AS pcd_new_wed,
                    NVL(b.pcd_new_thu,0) AS pcd_new_thu,
                    NVL(b.pcd_new_fri,0) AS pcd_new_fri,
                    NVL(b.pcd_xld_prior_fri,0) AS pcd_xld_prior_fri,
                    NVL(b.pcd_xld_sat,0) AS pcd_xld_sat,
                    NVL(b.pcd_xld_sun,0) AS pcd_xld_sun,
                    NVL(b.pcd_xld_mon,0) AS pcd_xld_mon,
                    NVL(b.pcd_xld_tue,0) AS pcd_xld_tue,
                    NVL(b.pcd_xld_wed,0) AS pcd_xld_wed,
                    NVL(b.pcd_xld_thu,0) AS pcd_xld_thu,
                    NVL(b.pcd_xld_fri,0) AS pcd_xld_fri,
                    NVL(b.pcd_changed_prior_fri,0) AS pcd_changed_prior_fri,
                    NVL(b.pcd_changed_sat,0) AS pcd_changed_sat,
                    NVL(b.pcd_changed_sun,0) AS pcd_changed_sun,
                    NVL(b.pcd_changed_mon,0) AS pcd_changed_mon,
                    NVL(b.pcd_changed_tue,0) AS pcd_changed_tue,
                    NVL(b.pcd_changed_wed,0) AS pcd_changed_wed,
                    NVL(b.pcd_changed_thu,0) AS pcd_changed_thu,
                    NVL(b.pcd_changed_fri,0) AS pcd_changed_fri,
                    NVL(b.pcd_net_prior_fri_accum,0) AS pcd_net_prior_fri_accum,
                    NVL(b.pcd_net_prior_fri,0) AS pcd_net_prior_fri,
                    NVL(b.pcd_net_sat,0) AS pcd_net_sat,
                    NVL(b.pcd_net_sun,0) AS pcd_net_sun,
                    NVL(b.pcd_net_mon,0) AS pcd_net_mon,
                    NVL(b.pcd_net_tue,0) AS pcd_net_tue,
                    NVL(b.pcd_net_wed,0) AS pcd_net_wed,
                    NVL(b.pcd_net_thu,0) AS pcd_net_thu,
                    NVL(b.pcd_net_fri,0) AS pcd_net_fri,
                    NVL(b.pcd_net_wtd,0) AS pcd_net_wtd,
                    NVL(b.pcd_net_daily,0) AS pcd_net_daily,
                    NVL(b.guests_new_prior_fri,0) AS guests_new_prior_fri,
                    NVL(b.guests_new_sat,0) AS guests_new_sat,
                    NVL(b.guests_new_sun,0) AS guests_new_sun,
                    NVL(b.guests_new_mon,0) AS guests_new_mon,
                    NVL(b.guests_new_tue,0) AS guests_new_tue,
                    NVL(b.guests_new_wed,0) AS guests_new_wed,
                    NVL(b.guests_new_thu,0) AS guests_new_thu,
                    NVL(b.guests_new_fri,0) AS guests_new_fri,
                    NVL(b.guests_xld_prior_fri,0) AS guests_xld_prior_fri,
                    NVL(b.guests_xld_sat,0) AS guests_xld_sat,
                    NVL(b.guests_xld_sun,0) AS guests_xld_sun,
                    NVL(b.guests_xld_mon,0) AS guests_xld_mon,
                    NVL(b.guests_xld_tue,0) AS guests_xld_tue,
                    NVL(b.guests_xld_wed,0) AS guests_xld_wed,
                    NVL(b.guests_xld_thu,0) AS guests_xld_thu,
                    NVL(b.guests_xld_fri,0) AS guests_xld_fri,
                    NVL(b.guests_net_prior_fri_accum,0) AS guests_net_prior_fri_accum,
                    NVL(b.guests_net_prior_fri,0) AS guests_net_prior_fri,
                    NVL(b.guests_net_sat,0) AS guests_net_sat,
                    NVL(b.guests_net_sun,0) AS guests_net_sun,
                    NVL(b.guests_net_mon,0) AS guests_net_mon,
                    NVL(b.guests_net_tue,0) AS guests_net_tue,
                    NVL(b.guests_net_wed,0) AS guests_net_wed,
                    NVL(b.guests_net_thu,0) AS guests_net_thu,
                    NVL(b.guests_net_fri,0) AS guests_net_fri,
                    NVL(b.guests_net_wtd,0) AS guests_net_wtd,
                    NVL(b.guests_net_daily,0) AS guests_net_daily,
                    NVL(b.all_amt,0) AS all_amt,
                    NVL(b.all_guests,0) AS all_guests,
                    NVL(b.retro_amt,0) AS retro_amt,
                    NVL(b.retro_guests,0) AS retro_guests,
                    NVL(b.air_margin_amt,0) AS air_margin_amt,
                    NVL(b.air_margin_guests,0) AS air_margin_guests,
                    NVL(b.air_cost_adj_amt,0) AS air_cost_adj_amt,
                    NVL(b.air_cost_adj_guests,0) AS air_cost_adj_guests,
                    NVL(b.commission_amt,0) AS commission_amt,
                    NVL(b.commission_guests,0) AS commission_guests,
                    NVL(b.tc_amt,0) AS tc_amt,
                    NVL(b.tc_guests,0) AS tc_guests,
                    NVL(b.onboard_credit_amt,0) AS onboard_credit_amt,
                    NVL(b.onboard_credit_guests,0) AS onboard_credit_guests,
                    NVL(b.port_charges_amt,0) AS port_charges_amt,
                    NVL(b.port_charges_guests,0) AS port_charges_guests,
                    NVL(b.tax_margin_amt,0) AS tax_margin_amt,
                    NVL(b.tax_margin_guests,0) AS tax_margin_guests,
                    NVL(b.gap_cocktail_amt,0) AS gap_cocktail_amt,
                    NVL(b.gap_cocktail_guests,0) AS gap_cocktail_guests,
                    NVL(b.currency_amt,0) AS currency_amt,
                    NVL(b.currency_guests,0) AS currency_guests,
                    NVL(b.charter_amt,0) AS charter_amt,
                    NVL(b.charter_guests,0) AS charter_guests,
                    NVL(b.spu_amt,0) AS spu_amt,
                    NVL(b.spu_guests,0) AS spu_guests,
                    NVL(b.category_amt,0) AS category_amt,
                    NVL(b.category_guests,0) AS category_guests,
                    NVL(b.guest_type_amt,0) AS guest_type_amt,
                    NVL(b.guest_type_guests,0) AS guest_type_guests,
                    NVL(b.others_amt,0) AS others_amt,
                    NVL(b.others_guests,0) AS others_guests,
                    NVL(b.beds_net_wtd_stlw,0) AS beds_net_wtd_stlw,
                    NVL(b.ntr_net_wtd_stlw,0) AS ntr_net_wtd_stlw,
                    NVL(b.pcd_net_wtd_stlw,0) AS pcd_net_wtd_stlw,
                    NVL(b.guests_net_wtd_stlw,0) AS guests_net_wtd_stlw,
                    NVL(b.beds_net_wtd_stly,0) AS beds_net_wtd_stly,
                    NVL(b.ntr_net_wtd_stly,0) AS ntr_net_wtd_stly,
                    NVL(b.pcd_net_wtd_stly,0) AS pcd_net_wtd_stly,
                    NVL(b.guests_net_wtd_stly,0) AS guests_net_wtd_stly,
                    NVL(b.single_guests,0) AS single_guests,
                    NVL(b.extra_guests,0) AS extra_guests,
                    NVL(b.beds_new_sat_lw,0) AS beds_new_sat_lw,
                    NVL(b.beds_new_sun_lw,0) AS beds_new_sun_lw,
                    NVL(b.beds_new_mon_lw,0) AS beds_new_mon_lw,
                    NVL(b.beds_new_tue_lw,0) AS beds_new_tue_lw,
                    NVL(b.beds_new_wed_lw,0) AS beds_new_wed_lw,
                    NVL(b.beds_new_thu_lw,0) AS beds_new_thu_lw,
                    NVL(b.beds_new_fri_lw,0) AS beds_new_fri_lw,
                    NVL(b.beds_xld_sat_lw,0) AS beds_xld_sat_lw,
                    NVL(b.beds_xld_sun_lw,0) AS beds_xld_sun_lw,
                    NVL(b.beds_xld_mon_lw,0) AS beds_xld_mon_lw,
                    NVL(b.beds_xld_tue_lw,0) AS beds_xld_tue_lw,
                    NVL(b.beds_xld_wed_lw,0) AS beds_xld_wed_lw,
                    NVL(b.beds_xld_thu_lw,0) AS beds_xld_thu_lw,
                    NVL(b.beds_xld_fri_lw,0) AS beds_xld_fri_lw,
                    NVL(b.beds_changed_sat_lw,0) AS beds_changed_sat_lw,
                    NVL(b.beds_changed_sun_lw,0) AS beds_changed_sun_lw,
                    NVL(b.beds_changed_mon_lw,0) AS beds_changed_mon_lw,
                    NVL(b.beds_changed_tue_lw,0) AS beds_changed_tue_lw,
                    NVL(b.beds_changed_wed_lw,0) AS beds_changed_wed_lw,
                    NVL(b.beds_changed_thu_lw,0) AS beds_changed_thu_lw,
                    NVL(b.beds_changed_fri_lw,0) AS beds_changed_fri_lw,
                    NVL(b.ntr_new_sat_lw,0) AS ntr_new_sat_lw,
                    NVL(b.ntr_new_sun_lw,0) AS ntr_new_sun_lw,
                    NVL(b.ntr_new_mon_lw,0) AS ntr_new_mon_lw,
                    NVL(b.ntr_new_tue_lw,0) AS ntr_new_tue_lw,
                    NVL(b.ntr_new_wed_lw,0) AS ntr_new_wed_lw,
                    NVL(b.ntr_new_thu_lw,0) AS ntr_new_thu_lw,
                    NVL(b.ntr_new_fri_lw,0) AS ntr_new_fri_lw,
                    NVL(b.ntr_xld_sat_lw,0) AS ntr_xld_sat_lw,
                    NVL(b.ntr_xld_sun_lw,0) AS ntr_xld_sun_lw,
                    NVL(b.ntr_xld_mon_lw,0) AS ntr_xld_mon_lw,
                    NVL(b.ntr_xld_tue_lw,0) AS ntr_xld_tue_lw,
                    NVL(b.ntr_xld_wed_lw,0) AS ntr_xld_wed_lw,
                    NVL(b.ntr_xld_thu_lw,0) AS ntr_xld_thu_lw,
                    NVL(b.ntr_xld_fri_lw,0) AS ntr_xld_fri_lw,
                    NVL(b.ntr_changed_sat_lw,0) AS ntr_changed_sat_lw,
                    NVL(b.ntr_changed_sun_lw,0) AS ntr_changed_sun_lw,
                    NVL(b.ntr_changed_mon_lw,0) AS ntr_changed_mon_lw,
                    NVL(b.ntr_changed_tue_lw,0) AS ntr_changed_tue_lw,
                    NVL(b.ntr_changed_wed_lw,0) AS ntr_changed_wed_lw,
                    NVL(b.ntr_changed_thu_lw,0) AS ntr_changed_thu_lw,
                    NVL(b.ntr_changed_fri_lw,0) AS ntr_changed_fri_lw,
                    NVL(b.pcd_new_sat_lw,0) AS pcd_new_sat_lw,
                    NVL(b.pcd_new_sun_lw,0) AS pcd_new_sun_lw,
                    NVL(b.pcd_new_mon_lw,0) AS pcd_new_mon_lw,
                    NVL(b.pcd_new_tue_lw,0) AS pcd_new_tue_lw,
                    NVL(b.pcd_new_wed_lw,0) AS pcd_new_wed_lw,
                    NVL(b.pcd_new_thu_lw,0) AS pcd_new_thu_lw,
                    NVL(b.pcd_new_fri_lw,0) AS pcd_new_fri_lw,
                    NVL(b.pcd_xld_sat_lw,0) AS pcd_xld_sat_lw,
                    NVL(b.pcd_xld_sun_lw,0) AS pcd_xld_sun_lw,
                    NVL(b.pcd_xld_mon_lw,0) AS pcd_xld_mon_lw,
                    NVL(b.pcd_xld_tue_lw,0) AS pcd_xld_tue_lw,
                    NVL(b.pcd_xld_wed_lw,0) AS pcd_xld_wed_lw,
                    NVL(b.pcd_xld_thu_lw,0) AS pcd_xld_thu_lw,
                    NVL(b.pcd_xld_fri_lw,0) AS pcd_xld_fri_lw,
                    NVL(b.pcd_changed_sat_lw,0) AS pcd_changed_sat_lw,
                    NVL(b.pcd_changed_sun_lw,0) AS pcd_changed_sun_lw,
                    NVL(b.pcd_changed_mon_lw,0) AS pcd_changed_mon_lw,
                    NVL(b.pcd_changed_tue_lw,0) AS pcd_changed_tue_lw,
                    NVL(b.pcd_changed_wed_lw,0) AS pcd_changed_wed_lw,
                    NVL(b.pcd_changed_thu_lw,0) AS pcd_changed_thu_lw,
                    NVL(b.pcd_changed_fri_lw,0) AS pcd_changed_fri_lw,
                    NVL(b.guests_new_sat_lw,0) AS guests_new_sat_lw,
                    NVL(b.guests_new_sun_lw,0) AS guests_new_sun_lw,
                    NVL(b.guests_new_mon_lw,0) AS guests_new_mon_lw,
                    NVL(b.guests_new_tue_lw,0) AS guests_new_tue_lw,
                    NVL(b.guests_new_wed_lw,0) AS guests_new_wed_lw,
                    NVL(b.guests_new_thu_lw,0) AS guests_new_thu_lw,
                    NVL(b.guests_new_fri_lw,0) AS guests_new_fri_lw,
                    NVL(b.guests_xld_sat_lw,0) AS guests_xld_sat_lw,
                    NVL(b.guests_xld_sun_lw,0) AS guests_xld_sun_lw,
                    NVL(b.guests_xld_mon_lw,0) AS guests_xld_mon_lw,
                    NVL(b.guests_xld_tue_lw,0) AS guests_xld_tue_lw,
                    NVL(b.guests_xld_wed_lw,0) AS guests_xld_wed_lw,
                    NVL(b.guests_xld_thu_lw,0) AS guests_xld_thu_lw,
                    NVL(b.guests_xld_fri_lw,0) AS guests_xld_fri_lw,
                    NVL(b.beds_new_wtd_stlw,0) AS beds_new_wtd_stlw,
                    NVL(b.beds_xld_wtd_stlw,0) AS beds_xld_wtd_stlw,
                    NVL(b.beds_changed_wtd_stlw,0) AS beds_changed_wtd_stlw,
                    NVL(b.ntr_new_wtd_stlw,0) AS ntr_new_wtd_stlw,
                    NVL(b.ntr_xld_wtd_stlw,0) AS ntr_xld_wtd_stlw,
                    NVL(b.ntr_changed_wtd_stlw,0) AS ntr_changed_wtd_stlw,
                    NVL(b.pcd_new_wtd_stlw,0) AS pcd_new_wtd_stlw,
                    NVL(b.pcd_xld_wtd_stlw,0) AS pcd_xld_wtd_stlw,
                    NVL(b.pcd_changed_wtd_stlw,0) AS pcd_changed_wtd_stlw,
                    NVL(b.guests_new_wtd_stlw,0) AS guests_new_wtd_stlw,
                    NVL(b.guests_xld_wtd_stlw,0) AS guests_xld_wtd_stlw,
                    NVL(b.beds_new_daily,0) AS beds_new_daily,
                    NVL(b.beds_xld_daily,0) AS beds_xld_daily,
                    NVL(b.beds_changed_daily,0) AS beds_changed_daily,
                    NVL(b.ntr_new_daily,0) AS ntr_new_daily,
                    NVL(b.ntr_xld_daily,0) AS ntr_xld_daily,
                    NVL(b.ntr_changed_daily,0) AS ntr_changed_daily,
                    NVL(b.pcd_new_daily,0) AS pcd_new_daily,
                    NVL(b.pcd_xld_daily,0) AS pcd_xld_daily,
                    NVL(b.pcd_changed_daily,0) AS pcd_changed_daily,
                    b.meta_revised,
                    b.meta_sort
        FROM
                 rdm_voyage_d v,
                 bkng_data_summary b,
                 rdm_date_d dt
        WHERE v.voyage = b.voyage(+)
        AND v.company_code = b.company_code(+)
        --AND v.voyage = 'V240A'
        AND v.rpt_year >= TO_NUMBER(dt.rpt_year)-1;        

        x:=SQL%ROWCOUNT;
        sComment  := 'rev_bkng_activity Inserts: '||x;

        EXECUTE IMMEDIATE 'ALTER INDEX idx_rev_bkng_activity_voyage REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_rev_bkng_activity_year REBUILD';
        EXECUTE IMMEDIATE 'ALTER TABLE rev_bkng_activity LOGGING';

        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Prog_Status => 'C',
                    in_Comments => sComment,
                    in_Error_Message => 'NA'
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Load_Type => 'D',
                    in_Prog_Status => 'C',
                    in_Start_Dt_Parm => dBeg_Date,
                    in_End_Dt_Parm => dEnd_Date
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

  EXCEPTION
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';

            IF BatchID = 0 THEN
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,
                                in_Prog_Status => 'X',
                                in_Comments => 'Job Failed logged to placeholder  ',
                                in_Error_Message => eErrMsg
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,
                            in_Schema_Name => cSchema_NM ,
                            in_Package_Name => cPackage_NM,
                            in_Program_Name => cProcedure_NM,
                            in_Load_Type => 'D' ,
                            in_Prog_Status => 'X' ,
                            in_Start_Dt_Parm => dBeg_Date ,
                            in_End_Dt_Parm => dEnd_Date
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,
                            in_Prog_Status => 'F',
                            in_Comments => 'Job Failed',
                            in_Error_Message => eErrMsg
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,
                            in_Schema_Name => cSchema_NM,
                            in_Package_Name => cPackage_NM,
                            in_Program_Name => cProcedure_NM,
                            in_Load_Type => 'D',
                            in_Prog_Status  => 'F',
                            in_Start_Dt_Parm => dBeg_Date,
                            in_End_Dt_Parm => dEnd_Date
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;

      WHEN OTHERS THEN
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;
            dbms_output.put_line(eErrMSG);
            ROLLBACK;

            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Prog_Status => 'F',
                    in_Comments => 'Job Failed',
                    in_Error_Message => eErrMsg
                    );
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Load_Type => 'D',
                    in_Prog_Status => 'F' ,
                    in_Start_Dt_Parm => dBeg_Date,
                    in_End_Dt_Parm => dEnd_Date
                    );
            COMMIT;
            RAISE;
  END REV_BKNG_ACT_LOAD;  

   PROCEDURE PREPAID_SPA_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'PREPAID_SPA_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
        EXECUTE IMMEDIATE 'ALTER TABLE OBR_PREPAIDSPA_FS NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE OBR_PREPAIDSPA_FS'; 

       INSERT /*+ append */  INTO obr_prepaidspa_fs
select
            substr(g.code_co, 1, 1) as company_code,
            g.voyage,
            g.ship,
            trunc(g.date_voy_sail) as sail_date,
            g.nbr_bkg AS BKNG_NBR,
            g.nbr_pty AS PAX_ID,
            g.nbr_bkg || 0 || g.nbr_pty as bkng_id,
            gs.status AS SERVICE_STATUS,
            trunc(gs.date_created) as SERVICE_CREATED,
            NVL(trunc(gs.cancelled_date),'31-DEC-1900') as SERVICE_CANCELLED_KEY,
            trunc(gs.cancelled_date) as SERVICE_CANCELLED,            
            case when ss.category_name like 'Acupun%' then 1 else 0 end as ACUPUNCTURE_COUNT,
            case when ss.category_name like 'Acupun%' then gs.paymentreceived else 0 end as acupuncture_rev,
            case when ss.category_name like 'Couples%' then 1 else 0 end as COUPLES_TREATMENTS_COUNT,
            case when ss.category_name like 'Couples%' then gs.paymentreceived else 0 end as couples_treatments_rev,
            case when ss.category_name like 'Detox%' then 1 else 0 end as BODY_THERAPIES_COUNT,
            case when ss.category_name like 'Detox%' then gs.paymentreceived else 0 end as body_therapies_rev,
            case when ss.category_name like 'Facial%' then 1 else 0 end as FACIAL_TREATMENTS_COUNT,
            case when ss.category_name like 'Facial%' then gs.paymentreceived else 0 end as facial_treatments_rev,
            case when ss.category_name like 'Fitness%' then 1 else 0 end as FITNESS_PROGRAM_COUNT,
            case when ss.category_name like 'Fitness%' then gs.paymentreceived else 0 end as fitness_program_rev,
            case when ss.category_name like 'Hair%' then 1 else 0 end as HAIR_SERVICES_COUNT,
            case when ss.category_name like 'Hair%' then gs.paymentreceived else 0 end as hair_services_rev,
            case when ss.category_name like 'Massage%' then 1 else 0 end as MASSAGE_COUNT,
            case when ss.category_name like 'Massage%' then gs.paymentreceived else 0 end as massage_rev,
            case when ss.category_name like 'Men%' then 1 else 0 end as MENS_SERVICES_COUNT,
            case when ss.category_name like 'Men%' then gs.paymentreceived else 0 end as mens_services_rev,
            case when ss.category_name like 'Nail%' then 1 else 0 end as NAIL_SERVICES_COUNT,
            case when ss.category_name like 'Nail%' then gs.paymentreceived else 0 end as nail_services_rev,
            case when ss.category_name like 'Relax%' then 1 else 0 end as RELAXATION_COUNT,
            case when ss.category_name like 'Relax%' then gs.paymentreceived else 0 end as relaxation_rev,
            case when ss.category_name like 'Tooth%' then 1 else 0 end as TOOTH_WHITENING_COUNT,
            case when ss.category_name like 'Tooth%' then gs.paymentreceived else 0 end as tooth_whitening_rev,
            case when ss.category_name like 'Treat%' then 1 else 0 end as TREATMENTS_COUNT,
            case when ss.category_name like 'Treat%' then gs.paymentreceived else 0 end as treatments_rev,
            case when ss.category_name like 'Wax%' then 1 else 0 end as WAXING_COUNT,
            case when ss.category_name like 'Wax%' then gs.paymentreceived else 0 end as waxing_rev,
            case when ss.category_name like 'YSPA%' then 1 else 0 end as YOUTH_SPA_COUNT,
            case when ss.category_name like 'YSPA%' then gs.paymentreceived else 0 end as YOUTH_SPA_REV,
            case when (ss.category_name not like 'Acupun%' and 
                                ss.category_name not like 'Couples%' and 
                                ss.category_name not like 'Detox%' and 
                                ss.category_name not like 'Facial%' and 
                                ss.category_name not like 'Fitness%' and 
                                ss.category_name not like 'Hair%' and 
                                ss.category_name not like 'Massage%' and 
                                ss.category_name not like 'Men%' and 
                                ss.category_name not like 'Nail%' and 
                                ss.category_name not like 'Relax%' and 
                                ss.category_name not like 'Tooth%' and 
                                ss.category_name not like 'Treat%' and 
                                ss.category_name not like 'Wax%' and 
                                ss.category_name not like 'YSPA%')
                    then 1 else 0
            end as OTHER_COUNT,
            case when (ss.category_name not like 'Acupun%' and 
                                ss.category_name not like 'Couples%' and 
                                ss.category_name not like 'Detox%' and 
                                ss.category_name not like 'Facial%' and 
                                ss.category_name not like 'Fitness%' and 
                                ss.category_name not like 'Hair%' and 
                                ss.category_name not like 'Massage%' and 
                                ss.category_name not like 'Men%' and 
                                ss.category_name not like 'Nail%' and 
                                ss.category_name not like 'Relax%' and 
                                ss.category_name not like 'Tooth%' and 
                                ss.category_name not like 'Treat%' and 
                                ss.category_name not like 'Wax%' and 
                                ss.category_name not like 'YSPA%')
                    then gs.paymentreceived else 0
            end as other_rev,
            case when ss.category_name is not null then 1 else 0 end as TOTAL_SERVICE_COUNT,       
            case when ss.category_name is not null then gs.paymentreceived else 0 end as TOTAL_SERVICE_REV,
            gs.service AS SERVICE_ID,
            gs.servicename AS SERVICE_NAME,
            gs.servicedate AS SERVICE_DATE,
            gs.servicetime AS SERVICE_TIME,
            p.id_agent as agent_id
        from guest@prdweb_hal_webfocus g, 
             guest_spa@prdweb_hal_webfocus gs, 
             spa_service@prdweb_hal_webfocus ss,
             purchase@prdweb_hal_webfocus p
        where g.id_guest = gs.id_guest
        and g.ship = ss.ship_code
        and gs.service = ss.product_code
        and gs.id_purchase = p.id_purchase
        and g.date_voy_sail >= sysdate;

        EXECUTE IMMEDIATE 'ALTER TABLE obr_prepaidspa_fs LOGGING';    
        EXECUTE IMMEDIATE 'ALTER TABLE obr_prepaidspa_f NOLOGGING';   
        EXECUTE IMMEDIATE 'ALTER INDEX idx_spa_bkng_id UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_spa_company UNUSABLE';       
        EXECUTE IMMEDIATE 'ALTER INDEX idx_spa_svc_created UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_spa_svc_cancelled UNUSABLE';
             
        DELETE FROM obr_prepaidspa_f
        WHERE sail_date >= sysdate;       
        
        INSERT /*+ append */  INTO obr_prepaidspa_f
        SELECT * FROM obr_prepaidspa_fs
        WHERE rowid IN (SELECT max(rowid) from obr_prepaidspa_fs 
        GROUP BY bkng_id, service_id, service_created, service_cancelled, service_date, service_time, service_status, company_code);
    
        x:=SQL%ROWCOUNT;        
        sComment  := 'obr_prepaidspa_f Inserts: '||x;
        
        EXECUTE IMMEDIATE 'ALTER INDEX idx_spa_bkng_id REBUILD';       
        EXECUTE IMMEDIATE 'ALTER INDEX idx_spa_company REBUILD';      
        EXECUTE IMMEDIATE 'ALTER INDEX idx_spa_svc_created REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_spa_svc_cancelled REBUILD'; 
        EXECUTE IMMEDIATE 'ALTER TABLE obr_prepaidspa_f LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END PREPAID_SPA_LOAD;

   PROCEDURE PREPAID_SHX_LOAD IS 
    -- Mod HH 10/04/2012
        cProcedure_NM  CONSTANT  Varchar2(30) := 'PREPAID_SHX_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
        EXECUTE IMMEDIATE 'ALTER TABLE obr_prepaidshx_fs NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE obr_prepaidshx_fs';            
        
        INSERT /*+ append */  INTO obr_prepaidshx_fs
        with t1 as (
        select
        company_code,
        port_code,
        port_name
        from port_name@tstdwh1_halw_dwh
        )
        , t2 as (
        select
            company_code,
            rpt_year,
            vyd_voyage
        from voyage_mstr@tstdwh1_halw_dwh vm
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL)
        )
        select
               substr(g.code_co, 1, 1) as company_code,
               t2.rpt_year,
               g.voyage,
               g.ship as ship_code,
               trunc(g.date_voy_sail) as sail_date,
               g.nbr_bkg as bkng_nbr,
               g.nbr_pty as pax_id,
               g.nbr_bkg || 0 || g.nbr_pty as bkng_id,
               st.code_port as port_code,
               t1.port_name,
               gshx.code_status as service_status,
               trunc(gshx.date_booked) as service_created,
               NVL(trunc(gshx.date_xld), '31-DEC-1900') as service_cancelled_key,
               trunc(gshx.date_xld) as service_cancelled,
               trunc(gshx.date_svc) as service_date,
               gshx.id_tour as shorex_id,
               st.name_tour_title as shorex_name,
               gshx.weight,
               gshx.code_revenue_center as revenue_center_code,
               gshx.code_sale_type as sales_type_code,
               1 as shorex_count,
               gshx.amt_price as shorex_rev,
               g.name_last,
               g.name_first,
               g.id_prod as voyage_tdef,
               trunc(g.date_dprt_prod) as voyage_tdef_start_date,
               st.nbr_tour as excursion_tour_nbr,
               p.id_agent as agent_id,
               st.code_dept_acct as acct_dept_code,
               g.name_title,
               g.cabin,
               g.group_id as group_code,
               g.age,
               g.gender,
               gshx.id_purchase as purchase_id,
               gshx.id_block_mgmt as block_mgt_id,
               st.nbr_duration as shorex_duration,
               TO_CHAR(std.time_dprt,'HH:MI AM') as shorex_depart_time,
               substr(replace(g.id_prod,'-','  '),-6,6) as tdef ,
               confirm.id_purchase,
               gshx.amt_paid,
               gshx.code_promo,
               gshx.amt_promo_discount                               
        from 
            guest@prdweb_hal_webfocus g, 
            guest_shx@prdweb_hal_webfocus gshx, 
            shx_tour@prdweb_hal_webfocus st,
            purchase@prdweb_hal_webfocus p,
            shx_block_management@prdweb_hal_webfocus sbm,
            shx_tour_departure@prdweb_hal_webfocus std,
            t1, t2 ,
            (
                select 
                      id_purchase,  text_confirmation_nbr   
                from 
                        confirmation@prdweb_hal_webfocus  
                where 
                    rowid in                                        
                    (
                    select 
                        max(rowid) 
                    from 
                        confirmation@prdweb_hal_webfocus  
                    group by
                        id_purchase
                        )
                )  confirm            
        where        
          p.id_purchase = confirm.id_purchase (+)   
        and g.id_guest = gshx.id_guest 
        and substr(g.code_co, 1, 1) = substr(st.code_co, 1, 1)
        and substr(st.code_co, 1, 1) = t1.company_code(+)
        and substr(g.code_co, 1, 1) = t2.company_code
        and gshx.id_tour = st.id_tour
        and st.code_port = t1.port_code(+)
        and g.voyage = t2.vyd_voyage
        and gshx.id_purchase = p.id_purchase
        and gshx.id_block_mgmt = sbm.id_block_mgmt
        and gshx.date_svc = sbm.date_svc
        and sbm.id_tour_dprt = std.id_tour_dprt
        and g.date_voy_sail >= sysdate;
        
        EXECUTE IMMEDIATE 'ALTER TABLE obr_prepaidshx_fs LOGGING';
        
        EXECUTE IMMEDIATE 'ALTER TABLE obr_prepaidshx_f NOLOGGING';   
                
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_bkng_id UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_company UNUSABLE';              
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_svc_created UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_svc_cancelled UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_rpt_year UNUSABLE';     
        
        DELETE from obr_prepaidshx_f
        WHERE sail_date >= sysdate;
        
        INSERT /*+ append */  INTO obr_prepaidshx_f
        SELECT * FROM obr_prepaidshx_fs
        WHERE rowid IN (SELECT MAX(rowid) from obr_prepaidshx_fs 
        GROUP BY bkng_id, port_code, shorex_id, service_created, service_cancelled, service_date, service_status, company_code);
        
        x:=SQL%ROWCOUNT;        
        sComment  := 'obr_prepaidshx_f Inserts: '||x;
        
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_bkng_id REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_company REBUILD';              
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_svc_created REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_svc_cancelled REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_shx_rpt_year REBUILD'; 

         EXECUTE IMMEDIATE 'ALTER TABLE obr_prepaidshx_f LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;      
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT; 
            RAISE;           
  END PREPAID_SHX_LOAD;
   PROCEDURE PREPAID_GIFT_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'PREPAID_GIFT_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
        EXECUTE IMMEDIATE 'ALTER TABLE obr_prepaidgift_f NOLOGGING';       
                
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_company UNUSABLE';              
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_rpt_year UNUSABLE';  
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_created UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_xld UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_status UNUSABLE';
        
        DELETE from obr_prepaidgift_f
        WHERE sail_date >= sysdate;
        
        INSERT /*+ append */  INTO obr_prepaidgift_f
        with t1 as (
        select
            company_code,
            rpt_year,
            voyage
        from rdm_voyage_d
        where rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL)
         )
        select
          substr(g.code_co, 1, 1) as company_code,
          t1.rpt_year,
          g.voyage,
          g.ship as ship_code,
          trunc(g.date_voy_sail) as sail_date,
          g.nbr_bkg as bkng_nbr,
          g.nbr_pty as guest_id,
          g.nbr_bkg || 0 || g.nbr_pty as bkng_id,
          gg.code_status as gift_status,
          trunc(gg.date_booked) as gift_created,
          trunc(gg.date_xld) as gift_xld,
          trunc(gg.date_delivery_gift) as gift_delivery_date,
          gg.code_delivery_location as gift_delivery_location,
          gg.id_guest_gift as guest_gift_id,
          gg.id_purchase as purchase_id,
          gg.id_gift_item as gift_item_nbr,
          gi.name_gift as gift_name,
          1 as gift_count,
          gg.amt_price as gift_price,
          gg.amt_discount as gift_discount,
          gg.amt_tax as gift_tax,
          gg.amt_fees as gift_fees,
          gg.amt_total as gift_rev,
          gg.amt_paid,
          g.name_title,
          g.name_last,
          g.name_first,
          g.cabin,
          g.group_id as group_code,
          g.id_past_guest as loyalty_id,
          g.age,
          g.gender,
          g.id_prod as voyage_tdef,
          trunc(g.date_dprt_prod) as voyage_tdef_start_date,
          substr(replace(g.id_prod,'-','  '),-6,6) as tdef,
          g.product_duration,
          gg.code_sale_type as sales_type_code,
          gg.id_gift_block_mgmt as gift_block_mgt_id,
          gg.code_promo,
          gg.code_revenue_center as revenue_center_code,
          p.id_agent as agent_id,
          gi.code_item,
          gi.flag_pinnacle_grill as dining_flag,
          gi.id_gift_category as gift_category_id,
          gc.name_category as gift_category_name
     from guest@prdweb_hal_webfocus g,
            guest_gift@prdweb_hal_webfocus gg,
            purchase@prdweb_hal_webfocus p,
            gift_item@prdweb_hal_webfocus gi,
            gift_category@prdweb_hal_webfocus gc,
            t1
     where g.id_guest = gg.id_guest
     and gg.id_purchase = p.id_purchase
     and gg.id_gift_item = gi.id_gift_item
     and g.voyage = t1.voyage
     and substr(g.code_co, 1, 1) = substr(gi.code_co, 1, 1)
     and substr(g.code_co, 1, 1) = substr(p.code_co, 1, 1)
     and substr(g.code_co, 1, 1) = t1.company_code
     and substr(gi.code_co, 1, 1) = substr(gc.code_co, 1, 1)
     and gi.id_gift_category = gc.id_gift_category
     and g.date_voy_sail >= sysdate;
                   
        x:=SQL%ROWCOUNT;        
        sComment  := 'obr_prepaidgift_f Inserts: '||x;
        
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_company REBUILD';              
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_rpt_year REBUILD';  
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_created REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_xld REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX idx_gift_status REBUILD';

        EXECUTE IMMEDIATE 'ALTER TABLE obr_prepaidgift_f LOGGING';
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;  
            RAISE;         
  END PREPAID_GIFT_LOAD;

   PROCEDURE BKNG_SERVICE_ITEMS_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'BKNG_SERVICE_ITEMS_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
        DELETE FROM rdm_bkng_service_items_d
        WHERE rpt_year >= (SELECT TO_CHAR (SYSDATE, 'YYYY') FROM DUAL);            
        COMMIT;
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_bkng_service_items_d NOLOGGING';
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_bkng_id UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_co UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_yr UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_nbr UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_type UNUSABLE';
        COMMIT;

        INSERT /*+ append */ INTO rdm_bkng_service_items_d
        (
          bkng_id,
          company_code,
          rpt_year,
          bkng_nbr,
          guest_id,
          sequence_nbr,
          item_nbr,
          item_status,
          item_desc,
          item_type,
          item_remarks_1,
          item_remarks_2,
          item_add_dt,
          redemption_id,
          from_promo_cd,
          gl_acct_dept_cd,
          item_cost_amt,
          load_dt,
          item_xld_dt,
          item_from_cd,
          minimum_order_amt,
          item_quantity, 
          voyage,
          item_currency_cd,
          redemption_id_flag,
          item_price_amt,
          item_delivery_dt,
          item_message_text_1,
          item_message_text_2,
          item_sent_to,
          item_sent_from          
        )
        SELECT
                   bkng_id,
                   company_code,
                   rpt_year,
                   bkng_nbr,
                   guest_id,
                   sequence_nbr,
                   item_nbr,
                   item_status,
                   item_desc,
                   item_type,
                   item_remarks_1,
                   item_remarks_2,
                   item_add_dt,
                   redemption_id,
                   from_promo_cd,
                   gl_acct_dept_cd,
                   item_cost_amt,
                   load_dt,
                   item_xld_dt,
                   item_from_cd,
                   minimum_order_amt,
                   item_quantity,
                   voyage,
                   item_currency_cd,
                   redemption_id_flag,
                   item_price_amt,
                   item_delivery_dt,
                   item_message_text_1,
                   item_message_text_2,
                   item_sent_to,
                   item_sent_from                      
          FROM dm_tkr.v_rdm_bkng_service_items_d_dev@tstdwh1_halw_dwh
        WHERE rpt_year >= (SELECT TO_CHAR (SYSDATE, 'YYYY') FROM DUAL);     
        
        x:=SQL%ROWCOUNT;        
        sComment  := 'rdm_bkng_service_items_d Inserts: '||x;
        
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_bkng_id REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_co REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_yr REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_nbr REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX bkng_svc_itm_type REBUILD';
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_bkng_service_items_d NOLOGGING';     
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;      
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT; 
            RAISE;            
  END BKNG_SERVICE_ITEMS_LOAD; 

   PROCEDURE PROMO_PRICING_LOAD  IS 
      /****************************************************************
       NAME: Promo_Pricing_Load
       
       PURPOSE: 
            Load  rows for  INV_PROMO_PRICING_F. 
            Input: Audit tables
             
       RETURN:N/A
       
       PARAMETERS:         
            None        
                                      
       REVISIONS:
       Ver     Date        Author  Description
       ------  ----------  ------  ---------------------------------------------
       1.0     10/15/2012  Venky    initial set up
       ************************************************************/

        
        cProgram_NM  CONSTANT  Varchar2(30) := 'PROMO_PRICING_LOAD';
        BatchID Number := 0;
        x Number ;   
        Beg_Dt Date := SYSDATE;
        End_Dt Date := SYSDATE;
        d_EOB_Data_Date            Date;                            

     BEGIN
     
     
      -- Log program start
        IF  DM_INT.COMMON_JOBS.LOG_JOB 
                (   io_Batch_ID  => BatchID, in_Schema_Name  =>  cSchema_NM,  in_Package_Name   => cPackage_NM,
                    in_Program_Name  =>cProgram_NM, in_Prog_Status => 'I',  in_Comments  => 'Job in Progress', in_Error_Message   => 'NA' ) != 0                                         
        THEN                  
            RAISE eCALL_FAILED ;
            
        END IF;

      sComment:= TO_CHAR(SYSDATE, 'HH24:MI:SS');

      --Get system data date
      BEGIN 
         SELECT eob_data_date 
           INTO d_EOB_Data_Date 
           FROM dw_system_parms@stgdwh1_halw_dwh;

         EXCEPTION WHEN NO_DATA_FOUND THEN 
            d_EOB_Data_Date := NULL;           
      END;
       
      IF d_EOB_Data_Date IS NULL THEN
         eErrMsg :=  'No EOB Data Date found in dw_system_parms' ;        
         RAISE eINCONSISTENCY;
      END IF;

        
        EXECUTE IMMEDIATE 'TRUNCATE TABLE inv_promo_pricing_f'; 
        EXECUTE IMMEDIATE 'ALTER TABLE inv_promo_pricing_f NOLOGGING';
        BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE inv_promo_pricing_f DROP PRIMARY KEY CASCADE';
        exception
        when others then null;
        end;
        BEGIN
        EXECUTE IMMEDIATE 'DROP INDEX PK_inv_promo_pricing_f';
        exception
        when others then null;
        end;
        BEGIN
        EXECUTE IMMEDIATE 'DROP INDEX UK_INV_PROMO_PRICING_F';
        exception
        when others then null;
        end;
        BEGIN
        EXECUTE IMMEDIATE 'DROP INDEX IDX_INV_PROMO_EFF_EXP_DATES';
        exception
        when others then null;
        end;

         EXECUTE IMMEDIATE 'TRUNCATE TABLE inv_promo_pricing_f'; 
        -- Insert New 
        x:=0;

        INSERT /*+ append */ INTO inv_promo_pricing_f (
               batch_id,
               load_dt,
               curr_rec_ind,
               rec_effective_date,
               rec_expiration_date,
               -----Data Cols \/  ---
               eob_data_date,
               id,
               Company_Code,
               Voyage,
               Promo_Code,
               Category,
               tour_id,
               Currency_Code,
               Base_Currency,
               Effective_Date,
               Expiration_Date,
               Promo_Desc,
               Promo_Status,
               Promo_Rate_Single,
               Promo_Rate_Double,
               Promo_Rate_Third,
               Promo_Rate_Fourth,
               Promo_Rate_Up_Child,
               Original_Effective_Date,
               Original_Expiration_Date,
               Promo_Effective_Date,
               Tours_Eligible_Ind,
               Sail_Year,
               Trade,
               Subtrade,
               Ship_Code,
               From_Port,
               To_Port,
               Voyage_Status,
               Sail_Date,
               Sea_Days,
               Season,
               land_NCF,
               Fee,
               Standard_Tax,
               Exclusive_Tax,
               Up_Adult_Tax,
               Up_Child_Tax,
               Category_Closed_Status)
 SELECT /*+ PARALLEL */ 
        BatchID,                  
        SYSDATE,          
        'Y',
        trunc(d_eob_data_date),    
        to_date('12/31/9999', 'MM/DD/YYYY') ,  -- EOD               
        ---- data cols \/ ----
        d_eob_data_date,
        ROWNUM,
        Company_Code,
        Voyage,
        Promo_Code,
        Category,
        Tour_Id,
        Currency_Code,
        Base_Currency,
        Effective_date,
        Expiration_Date,
        Promo_Desc,
        Promo_Status,
        Promo_Rate_Single,
        Promo_Rate_Double,
        Promo_Rate_Third,
        Promo_Rate_Fourth,
        Promo_Rate_Up_Child,
        Original_Effective_Date,
        Original_Expiration_Date,
        promo_effective_date,
        Tours_Eligible_Ind,
        sail_year,
        Trade,
        Subtrade,
        Ship_Code,
        From_Port,
        To_Port,
        Voyage_Status,
        Sail_Date,
        Sea_Days,
        Season,
        land_ncf,
        fee,
        Standard_Tax,
        Exclusive_Tax,
        Up_Adult_Tax,
        Up_Child_Tax,
        category_closed_status
   FROM dwh_owner.v_inv_promo_pricing_f@stgdwh1_halw_dwh;
                                                                    
        x:=SQL%ROWCOUNT ;                   
        sComment:= sComment||' First Insert '||x||TO_CHAR(SYSDATE, ' HH24:MI:SS');

commit;

        -- Set the category closed status to NULL for records without audit
        INSERT /*+ append */ INTO inv_promo_pricing_f (
               batch_id,
               load_dt,
               curr_rec_ind,
               rec_effective_date,
               rec_expiration_date,
               -----Data Cols \/  ---
               eob_data_date,
               id,
               Company_Code,
               Voyage,
               Promo_Code,
               Category,
               tour_id,
               Currency_Code,
               Base_Currency,
               Effective_Date,
               Expiration_Date,
               Promo_Desc,
               Promo_Status,
               Promo_Rate_Single,
               Promo_Rate_Double,
               Promo_Rate_Third,
               Promo_Rate_Fourth,
               Promo_Rate_Up_Child,
               Original_Effective_Date,
               Original_Expiration_Date,
               Promo_Effective_Date,
               Tours_Eligible_Ind,
               Sail_Year,
               Trade,
               Subtrade,
               Ship_Code,
               From_Port,
               To_Port,
               Voyage_Status,
               Sail_Date,
               Sea_Days,
               Season,
               land_NCF,
               Fee,
               Standard_Tax,
               Exclusive_Tax,
               Up_Adult_Tax,
               Up_Child_Tax,
               Category_Closed_Status)
 SELECT /*+ PARALLEL */
        BatchID,                  
        SYSDATE,          
        'Y',
        trunc(d_eob_data_date),    
        to_date('12/31/9999', 'MM/DD/YYYY') ,  -- EOD               
        ---- data cols \/ ----
        d_eob_data_date,
        x+ROWNUM,
        p.Company_Code,
        p.Voyage,
        p.Promo_Code,
        p.Category,
        p.Tour_Id,
        p.Currency_Code,
        p.Base_Currency,
        p.promo_effective_date Effective_date,
        p.Effective_date Expiration_Date,
        p.Promo_Desc,
        p.Promo_Status,
        p.Promo_Rate_Single,
        p.Promo_Rate_Double,
        p.Promo_Rate_Third,
        p.Promo_Rate_Fourth,
        p.Promo_Rate_Up_Child,
        p.Original_Effective_Date,
        p.Original_Expiration_Date,
        NULL promo_effective_date,
        p.Tours_Eligible_Ind,
        p.sail_year,
        p.Trade,
        p.Subtrade,
        p.Ship_Code,
        p.From_Port,
        p.To_Port,
        p.Voyage_Status,
        p.Sail_Date,
        p.Sea_Days,
        p.Season,
        p.land_ncf,
        p.fee,
        p.Standard_Tax,
        p.Exclusive_Tax,
        p.Up_Adult_Tax,
        p.Up_Child_Tax,
        NULL category_closed_status
   FROM inv_promo_pricing_f p
  WHERE p.promo_effective_date IS NOT NULL;

        sComment:= sComment||' No Audit Inserts '||SQL%ROWCOUNT||TO_CHAR(SYSDATE, ' HH24:MI:SS');
COMMIT;

  x:=SQL%ROWCOUNT ;   

EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX PK_INV_PROMO_PRICING_F ON INV_PROMO_PRICING_F(ID)';

EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX UK_INV_PROMO_PRICING_F ON INV_PROMO_PRICING_F(COMPANY_CODE, VOYAGE, PROMO_CODE, CATEGORY, TOUR_ID, CURRENCY_CODE, BASE_CURRENCY, EFFECTIVE_DATE, REC_EFFECTIVE_DATE)';

EXECUTE IMMEDIATE 'CREATE INDEX IDX_INV_PROMO_EFF_EXP_DATES ON INV_PROMO_PRICING_F(EFFECTIVE_DATE, EXPIRATION_DATE)';

EXECUTE IMMEDIATE 'ALTER TABLE INV_PROMO_PRICING_F ADD (CONSTRAINT PK_INV_PROMO_PRICING_F  PRIMARY KEY  (ID)  USING INDEX PK_INV_PROMO_PRICING_F ENABLE VALIDATE)';

 sComment:= sComment||' Enabled PK '||TO_CHAR(SYSDATE, 'HH24:MI:SS');

MERGE INTO inv_promo_pricing_f f
USING ( WITH promo AS(
           SELECT /*+ PARALLEL */Voyage,
                  Base_Currency,
                  Currency_Code,
                  Promo_Code,
                  id,
                  CASE WHEN LAG(Promo_Code) OVER (PARTITION BY Company_Code, Voyage, promo_code, Category, Tour_Id, Currency_Code, Base_Currency
                  ORDER BY Company_Code, Voyage, promo_code, Category, Tour_Id, Currency_Code, Base_Currency, Effective_date, Expiration_date) IS NOT NULL THEN effective_date
                  ELSE TO_DATE('01/01/1900', 'MM/DD/YYYY')
                  END effective_date,
                  CASE WHEN LEAD(Promo_Code) OVER (PARTITION BY Company_Code, Voyage, promo_code, Category, Tour_Id, Currency_Code, Base_Currency
                  ORDER BY Company_Code, Voyage, promo_code, Category, Tour_Id, Currency_Code, Base_Currency, Effective_date, Expiration_date) IS NOT NULL THEN expiration_date
                  ELSE TO_DATE('12/31/9999', 'MM/DD/YYYY') END expiration_date,
                  category,
                  Company_Code
             FROM inv_promo_pricing_f)
             SELECT /*+ PARALLEL */
                  p.company_code, 
                  p.voyage,
                  p.promo_code,
                  p.category,
                  p.id,
                  SUM(CASE WHEN bp.b_p_passenger_id = '999' THEN bp.passenger_count
                           WHEN bp.b_p_use IN ('S', 'D') THEN 1
                           WHEN bp.b_p_use IN ('I', 'X') THEN 2
                           ELSE 0
                  END) lower_bed_count,
                  SUM(CASE WHEN bp.b_p_use = '*' AND bp.b_p_age > 0 AND bp.b_p_age < 19 THEN 1 ELSE 0 END) child_bed_count,
                  SUM(CASE WHEN bp.b_p_use = '*' AND (NVL(bp.b_p_age, 0) = 0 OR NVL(bp.b_p_age, 0) > 18) THEN 1 ELSE 0 END) extra_bed_count
             FROM bkng_psngr@stgdwh1_halw_dwh bp,
                  promo p
            WHERE bp.b_p_passenger_status = 'A'
              AND p.voyage = bp.b_voyage
              AND p.company_code = bp.company_code
              AND p.promo_code = bp.b_p_promo_sob
              AND p.category = bp.b_category
              AND p.effective_date <= bp.open_date
              AND p.expiration_date > bp.open_date
         GROUP BY p.company_code,
                  p.voyage,
                  p.promo_code,
                  p.category,
                  p.id) s
          ON (f.id = s.id)
          WHEN MATCHED THEN
            UPDATE SET f.Category_Lower_Bed_Count = s.lower_bed_count,
                       f.Category_Extra_Bed_Count = s.extra_bed_count,
                       f.Category_Child_Bed_Count = s.child_bed_count;

 sComment:= sComment||' Category Inserts '||SQL%ROWCOUNT||TO_CHAR(SYSDATE, ' HH24:MI:SS');


MERGE INTO inv_promo_pricing_f f
USING ( WITH promo AS(
           SELECT /*+ PARALLEL */Voyage,
                  Base_Currency,
                  Currency_Code,
                  Promo_Code,
                  id,
                  CASE WHEN LAG(Promo_Code) OVER (PARTITION BY Company_Code, Voyage, promo_code, Category, Tour_Id, Currency_Code, Base_Currency
                  ORDER BY Company_Code, Voyage, promo_code, Category, Tour_Id, Currency_Code, Base_Currency, Effective_date, Expiration_date) IS NOT NULL THEN effective_date
                  ELSE TO_DATE('01/01/1900', 'MM/DD/YYYY')
                  END effective_date,
                  CASE WHEN LEAD(Promo_Code) OVER (PARTITION BY Company_Code, Voyage, promo_code, Category, Tour_Id, Currency_Code, Base_Currency
                  ORDER BY Company_Code, Voyage, promo_code, Category, Tour_Id, Currency_Code, Base_Currency, Effective_date, Expiration_date) IS NOT NULL THEN expiration_date
                  ELSE TO_DATE('12/31/9999', 'MM/DD/YYYY') END expiration_date,
                  category,
                  Company_Code
             FROM inv_promo_pricing_f)
             SELECT /*+ PARALLEL */
                  p.company_code, 
                  p.voyage,
                  p.promo_code,
                  p.category,
                  p.id,
                  SUM(CASE WHEN bp.b_p_passenger_id = '999' THEN bp.passenger_count
                           WHEN bp.b_p_use IN ('S', 'D') THEN 1
                           WHEN bp.b_p_use IN ('I', 'X') THEN 2
                           ELSE 0
                  END) lower_bed_count,
                  SUM(CASE WHEN bp.b_p_use = '*' AND bp.b_p_age > 0 AND bp.b_p_age < 19 THEN 1 ELSE 0 END) child_bed_count,
                  SUM(CASE WHEN bp.b_p_use = '*' AND (NVL(bp.b_p_age, 0) = 0 OR NVL(bp.b_p_age, 0) > 18) THEN 1 ELSE 0 END) extra_bed_count
             FROM bkng_psngr@stgdwh1_halw_dwh bp,
                  promo p
            WHERE bp.b_p_passenger_status = 'A'
              AND p.voyage = bp.b_voyage
              AND p.company_code = bp.company_code
              AND p.promo_code = bp.b_p_promo_sob
              AND p.category = NVL(bp.b_p_apo, bp.b_category)
              AND p.effective_date <= bp.open_date
              AND p.expiration_date > bp.open_date
         GROUP BY p.company_code,
                  p.voyage,
                  p.promo_code,
                  p.category,
                  p.id) s
          ON (f.id = s.id)
          WHEN MATCHED THEN
            UPDATE SET f.APO_Lower_Bed_Count = s.lower_bed_count,
                       f.APO_Extra_Bed_Count = s.extra_bed_count,
                       f.APO_Child_Bed_Count = s.child_bed_count;

 sComment:= sComment||' APO Inserts '||SQL%ROWCOUNT||TO_CHAR(SYSDATE, ' HH24:MI:SS');

        END_DT := SYSDATE ;  -- set end date to sysdate , not crit for polar 
        
     -- Record Completion in Log    
        IF  DM_INT.COMMON_JOBS.LOG_JOB 
                (   io_Batch_ID  => BatchID,   in_Prog_Status => 'C',  in_Comments  => sComment,  in_Error_Message   => 'NA' ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
                             
        -- Record Completion in Control File
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID, in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
                    in_Start_DT_Parm  => Beg_DT, in_End_DT_Parm =>  End_Dt 
                ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
        
        
     EXCEPTION 
        WHEN eINCONSISTENCY THEN              
            ROLLBACK ;
             
             -- Record Error
            x:=  DM_INT.COMMON_JOBS.LOG_JOB  
                    (   io_Batch_ID  => BatchID,   in_Prog_Status => 'F',  in_Comments  => 'Job Failed'  ,  in_Error_Message   => eErrMsg ) ;
            
            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID,  in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D', in_Prog_Status  => 'F' , 
                    in_Start_DT_Parm  => Beg_DT ,             in_End_DT_Parm =>  End_Dt 
                )  ;
            RAISE eINCONSISTENCY;

        WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program' ;                  
                                  
            x:=-1 ;
            -- Record Error   against    palceholder w/  batch of -1   since batch not recorded 
            x:=  DM_INT.COMMON_JOBS.LOG_JOB 
                    ( io_Batch_ID  => x, in_Prog_Status => 'X', in_Comments  => 'Job Failed logged to placeholder  ' , in_Error_Message   => eErrMsg   ) ;

            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID ,                       in_Schema_Name => cSchema_NM ,  in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM  ,   in_Load_Type  => 'D' ,         in_Prog_Status  => 'X' , 
                    in_Start_DT_Parm  => Beg_DT ,             in_End_DT_Parm =>  End_Dt                 )  ;
          RAISE;

              
        WHEN OTHERS THEN
         dbms_output.put_line('Other error') ; 
        -- error trap and logging   Add a generic handler 
            eErrMsg :=  SQLERRM;                                              
            ROLLBACK ;
            
            --  record error w/ the assigned batch ID
            x:=  DM_INT.COMMON_JOBS.LOG_JOB 
                    (   io_Batch_ID  => BatchID,   in_Prog_Status => 'F',  in_Comments  => 'Job Failed',  in_Error_Message   => eErrMsg ) ;
          
            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID,  in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D',  in_Prog_Status  => 'F' , 
                    in_Start_DT_Parm  => Beg_DT ,  in_End_DT_Parm =>  End_Dt                  )  ;
           RAISE;                       

  END PROMO_PRICING_LOAD; 

   PROCEDURE REV_AK_BALANCER1_LOAD IS
        cProcedure_NM  CONSTANT  Varchar2(30) := 'REV_AK_BALANCER1_LOAD';
        BatchID Number := 0;

  BEGIN

         /* Log program start */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Prog_Status => 'I',
                    in_Comments => 'Job in Progress',
                    in_Error_Message => 'NA'
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

        EXECUTE IMMEDIATE 'ALTER TABLE rev_ak_balancer_tour_data NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rev_ak_balancer_tour_data';

        INSERT /*+ append */ INTO rev_ak_balancer_tour_data
        (
            bkng_id,
            company_code,
            voyage,
            rpt_year,
            std_land_cost,
            basic_margin,
            tour_margin,
            cpp_guest_count,
            cpp_tour_usd_blended,
            cpp_tour_comm_usd_blended,
            berkely_tour_pymt_amt,
            cpp_margin,
            ship_cost_blended,
            ship_contribution_blended,
            tour_margin_rev,
            tour_margin_rev_7,
            std_land_cost_364,
            basic_margin_364,
            tour_margin_364,
            cpp_guest_count_364,
            cpp_tour_usd_blended_364,
            cpp_tour_comm_usd_blended_364,
            berkely_tour_pymt_amt_364,
            cpp_margin_364,
            ship_cost_blended_364,
            ship_contribution_blended_364,
            tour_margin_rev_364,
            tour_margin_rev_371,
            std_land_cost_728,
            basic_margin_728,
            tour_margin_728,
            cpp_guest_count_728,
            cpp_tour_usd_blended_728,
            cpp_tour_comm_usd_blended_728,
            berkely_tour_pymt_amt_728,
            cpp_margin_728,
            ship_cost_blended_728,
            ship_contribution_blended_728,
            tour_margin_rev_728,
            tour_margin_rev_735,
            guest_count,
            guest_count_7,
            guest_count_364,
            guest_count_371,
            guest_count_728,
            guest_count_735,
            lower_bed_count,
            lower_bed_count_7,
            lower_bed_count_364,
            lower_bed_count_371,
            lower_bed_count_728,
            lower_bed_count_735,
            ntr_fin,
            ntr_fin_7,
            ntr_fin_364,
            ntr_fin_371,
            ntr_fin_728,
            ntr_fin_735,
            tour_margin_rev_fri,
            guest_count_fri,
            lower_bed_count_fri,
            ntr_fin_fri,
            ship_contribution_blended_fri,
            ship_contribution_blended_371,
            ship_contribution_blended_735
        )
             SELECT
                        t.bkng_id,
                        t.company_code,
                        t.voyage,
                        t.rpt_year,
        --Current
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.std_land_cost_converted ELSE 0 END),0) AS std_land_cost,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.basic_margin ELSE 0 END),0) AS basic_margin,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.tour_margin ELSE 0 END),0) AS tour_margin,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.cpp_guest_count ELSE 0 END),0) AS cpp_guest_count,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.cpp_tour_usd_blended ELSE 0 END),0) AS cpp_tour_usd_blended,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0) AS cpp_tour_comm_usd_blended,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.berkely_tour_pymt_amt ELSE 0 END),0) AS berkely_tour_pymt_amt,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.cpp_margin ELSE 0 END),0) AS cpp_margin,
        --Ship Costs
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN (t.net_cruise_fare * t.blended_conversion_rate) - (t.b_p_port_charges * t.blended_conversion_rate) ELSE 0 END),0) AS ship_cost_blended,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN (t.net_cruise_fare * t.blended_conversion_rate) + (t.b_p_port_charges * t.blended_conversion_rate) ELSE 0 END),0) AS ship_contribution_blended,
        --Tour Margin Rev Current Week
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.tour_margin ELSE 0 END),0)
                          +
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.cpp_tour_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.berkely_tour_pymt_amt ELSE 0 END),0)
                        ) ) AS tour_margin_rev,
        --Tour Margin Rev Last Week
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-7 AND t.expiration_date > d.eob_data_date-7 THEN t.tour_margin ELSE 0 END),0)
                          +
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-7 AND t.expiration_date > d.eob_data_date-7 THEN t.cpp_tour_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-7 AND t.expiration_date > d.eob_data_date-7 THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-7 AND t.expiration_date > d.eob_data_date-7 THEN t.berkely_tour_pymt_amt ELSE 0 END),0)
                       ) ) AS tour_margin_rev_7,
        --STLY
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.std_land_cost_converted ELSE 0 END),0) AS std_land_cost_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.basic_margin ELSE 0 END),0) AS basic_margin_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.tour_margin ELSE 0 END),0) AS tour_margin_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.cpp_guest_count ELSE 0 END),0) AS cpp_guest_count_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.cpp_tour_usd_blended ELSE 0 END),0) AS cpp_tour_usd_blended_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0) AS cpp_tour_comm_usd_blended_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.berkely_tour_pymt_amt ELSE 0 END),0) AS berkely_tour_pymt_amt_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.cpp_margin ELSE 0 END),0) AS cpp_margin_364,
        --Ship Costs STLY
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN (t.net_cruise_fare * t.blended_conversion_rate) - (t.b_p_port_charges * t.blended_conversion_rate) ELSE 0 END),0) AS ship_cost_blended_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN (t.net_cruise_fare * t.blended_conversion_rate) + (t.b_p_port_charges * t.blended_conversion_rate) ELSE 0 END),0) AS ship_contribution_blended_364,
        --Tour Margin rev Current Week STLY
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.tour_margin ELSE 0 END),0)
                          +
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.cpp_tour_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.berkely_tour_pymt_amt ELSE 0 END),0)
                        ) ) AS tour_margin_rev_364,
        --Tour Margin Rev Last Week STLY
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-371 AND t.expiration_date > d.eob_data_date-371 THEN t.tour_margin ELSE 0 END),0)
                          +
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-371 AND t.expiration_date > d.eob_data_date-371 THEN t.cpp_tour_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-371 AND t.expiration_date > d.eob_data_date-371 THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-371 AND t.expiration_date > d.eob_data_date-371 THEN t.berkely_tour_pymt_amt ELSE 0 END),0)
                       ) ) AS tour_margin_rev_371,
        --STLY less 1 year
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.std_land_cost_converted ELSE 0 END),0) AS std_land_cost_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.basic_margin ELSE 0 END),0) AS basic_margin_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.tour_margin ELSE 0 END),0) AS tour_margin_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.cpp_guest_count ELSE 0 END),0) AS cpp_guest_count_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.cpp_tour_usd_blended ELSE 0 END),0) AS cpp_tour_usd_blended_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0) AS cpp_tour_comm_usd_blended_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.berkely_tour_pymt_amt ELSE 0 END),0) AS berkely_tour_pymt_amt_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.cpp_margin ELSE 0 END),0) AS cpp_margin_728,
        --Ship Costs STLY less 1 year
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN (t.net_cruise_fare * t.blended_conversion_rate) - (t.b_p_port_charges * t.blended_conversion_rate) ELSE 0 END),0) AS ship_cost_blended_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN (t.net_cruise_fare * t.blended_conversion_rate) + (t.b_p_port_charges * t.blended_conversion_rate) ELSE 0 END),0) AS ship_contribution_blended_728,
        --Tour Margin rev Current Week STLY less 1 year
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.tour_margin ELSE 0 END),0)
                          +
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.cpp_tour_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.berkely_tour_pymt_amt ELSE 0 END),0)
                        ) ) AS tour_margin_rev_728,
        --Tour Margin rev Last Week STLY less 1 year
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-735 AND t.expiration_date > d.eob_data_date-735 THEN t.tour_margin ELSE 0 END),0)
                          +
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-735 AND t.expiration_date > d.eob_data_date-735 THEN t.cpp_tour_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-735 AND t.expiration_date > d.eob_data_date-735 THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-735 AND t.expiration_date > d.eob_data_date-735 THEN t.berkely_tour_pymt_amt ELSE 0 END),0)
                       ) ) AS tour_margin_rev_735,
        --Guest Counts
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN 1 ELSE 0 END),0) AS guest_count,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-7 AND t.expiration_date > d.eob_data_date-7 THEN 1 ELSE 0 END),0) AS guest_count_7,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN 1 ELSE 0 END),0) AS guest_count_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-371 AND t.expiration_date > d.eob_data_date-371 THEN 1 ELSE 0 END),0) AS guest_count_371,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN 1 ELSE 0 END),0) AS guest_count_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-735 AND t.expiration_date > d.eob_data_date-735 THEN 1 ELSE 0 END),0) AS guest_count_735,
        --Lower Bed Counts
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date
                                THEN (CASE WHEN t.cabin_use IN ('S', 'D') THEN 1 WHEN t.cabin_use IN ('X', 'I') THEN 2 ELSE 0 END) ELSE 0 END),0) AS lower_bed_count,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-7 AND t.expiration_date > d.eob_data_date-7
                                THEN (CASE WHEN t.cabin_use IN ('S', 'D') THEN 1 WHEN t.cabin_use IN ('X', 'I') THEN 2 ELSE 0 END) ELSE 0 END),0) AS lower_bed_count_7,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364
                                THEN (CASE WHEN t.cabin_use IN ('S', 'D') THEN 1 WHEN t.cabin_use IN ('X', 'I') THEN 2 ELSE 0 END) ELSE 0 END),0) AS lower_bed_count_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-371 AND t.expiration_date > d.eob_data_date-371
                                THEN (CASE WHEN t.cabin_use IN ('S', 'D') THEN 1 WHEN t.cabin_use IN ('X', 'I') THEN 2 ELSE 0 END) ELSE 0 END),0) AS lower_bed_count_371,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728
                                THEN (CASE WHEN t.cabin_use IN ('S', 'D') THEN 1 WHEN t.cabin_use IN ('X', 'I') THEN 2 ELSE 0 END) ELSE 0 END),0) AS lower_bed_count_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-735 AND t.expiration_date > d.eob_data_date-735
                                THEN (CASE WHEN t.cabin_use IN ('S', 'D') THEN 1 WHEN t.cabin_use IN ('X', 'I') THEN 2 ELSE 0 END) ELSE 0 END),0) AS lower_bed_count_735,
        --NTR Finance
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date AND t.expiration_date > d.eob_data_date THEN t.ntr_fin_usd ELSE 0 END),0) AS ntr_fin,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-7 AND t.expiration_date > d.eob_data_date-7 THEN t.ntr_fin_usd ELSE 0 END),0) AS ntr_fin_7,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-364 AND t.expiration_date > d.eob_data_date-364 THEN t.ntr_fin_usd ELSE 0 END),0) AS ntr_fin_364,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-371 AND t.expiration_date > d.eob_data_date-371 THEN t.ntr_fin_usd ELSE 0 END),0) AS ntr_fin_371,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-728 AND t.expiration_date > d.eob_data_date-728 THEN t.ntr_fin_usd ELSE 0 END),0) AS ntr_fin_728,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-735 AND t.expiration_date > d.eob_data_date-735 THEN t.ntr_fin_usd ELSE 0 END),0) AS ntr_fin_735,
        --Tour Margin Rev Frozen Friday
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date_prior_friday AND t.expiration_date > d.eob_data_date_prior_friday THEN t.tour_margin ELSE 0 END),0)
                          +
                        ( NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date_prior_friday AND t.expiration_date > d.eob_data_date_prior_friday THEN t.cpp_tour_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date_prior_friday AND t.expiration_date > d.eob_data_date_prior_friday THEN t.cpp_tour_comm_usd_blended ELSE 0 END),0)
                           -
                          NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date_prior_friday AND t.expiration_date > d.eob_data_date_prior_friday THEN t.berkely_tour_pymt_amt ELSE 0 END),0)
                       ) ) AS tour_margin_rev_fri,
        --Guest Counts Frozen Friday
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date_prior_friday AND t.expiration_date > d.eob_data_date_prior_friday THEN 1 ELSE 0 END),0) AS guest_count_fri,
        --Lower Bed Counts Frozen Friday
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date_prior_friday AND t.expiration_date > d.eob_data_date_prior_friday
                                THEN (CASE WHEN t.cabin_use IN ('S', 'D') THEN 1 WHEN t.cabin_use IN ('X', 'I') THEN 2 ELSE 0 END) ELSE 0 END),0) AS lower_bed_count_fri,
        --NTR Finance Frozen Friday
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date_prior_friday AND t.expiration_date > d.eob_data_date_prior_friday THEN t.ntr_fin_usd ELSE 0 END),0) AS ntr_fin_fri,
        --Ship Contribution Blended Frozen Friday, 371 and 735
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date_prior_friday AND t.expiration_date > d.eob_data_date_prior_friday THEN (t.net_cruise_fare * t.blended_conversion_rate) + (t.b_p_port_charges * t.blended_conversion_rate) ELSE 0 END),0) AS ship_contribution_blended_fri,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-371 AND t.expiration_date > d.eob_data_date-371 THEN (t.net_cruise_fare * t.blended_conversion_rate) + (t.b_p_port_charges * t.blended_conversion_rate) ELSE 0 END),0) AS ship_contribution_blended_371,
                        NVL(SUM(CASE WHEN t.effective_date <= d.eob_data_date-735 AND t.expiration_date > d.eob_data_date-735 THEN (t.net_cruise_fare * t.blended_conversion_rate) + (t.b_p_port_charges * t.blended_conversion_rate) ELSE 0 END),0) AS ship_contribution_blended_735
               FROM
                         dm_tkr.fin_tour_margin_audit@prdbi1_dm_tkr_ro t,
--                         dm_tkr.fin_tour_margin_audit@stgbi2_dm_tkr t,
--                         fin_tour_margin_audit t,                         
                         (SELECT eob_data_date, prior_friday as eob_data_date_prior_friday from dw_system_parms@tstdwh1_halw_dwh) d
             WHERE
                        t.guest_status = 'A'
                AND  t.rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY')-1 FROM DUAL)             
             --  AND  t.rpt_year >= '2012'
                AND (
                        (t.effective_date <= d.eob_data_date and t.expiration_date > d.eob_data_date)
                        OR
                        (t.effective_date <= d.eob_data_date-7 and t.expiration_date > d.eob_data_date-7)
                        OR
                        (t.effective_date <= d.eob_data_date-364 and t.expiration_date > d.eob_data_date-364)
                        OR
                        (t.effective_date <= d.eob_data_date-371 and t.expiration_date > d.eob_data_date-371)
                        OR
                        (t.effective_date <= d.eob_data_date-728 and t.expiration_date > d.eob_data_date-728)
                        OR
                        (t.effective_date <= d.eob_data_date-735 and t.expiration_date > d.eob_data_date-735)
                        OR
                        (t.effective_date <= d.eob_data_date_prior_friday and t.expiration_date > d.eob_data_date_prior_friday)
                        )
        GROUP BY t.bkng_id, t.company_code, t.voyage, t.rpt_year;

        x:=SQL%ROWCOUNT;
        sComment  := 'rev_ak_balancer_tour_data Inserts: '||x;

        COMMIT;
        
        EXECUTE IMMEDIATE 'ALTER TABLE rev_ak_balancer_tour_data LOGGING';

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'DM_TKR'
         ,TabName        => 'REV_AK_BALANCER_TOUR_DATA'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;

        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Prog_Status => 'C',
                    in_Comments => sComment,
                    in_Error_Message => 'NA'
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Load_Type => 'D',
                    in_Prog_Status => 'C',
                    in_Start_Dt_Parm => dBeg_Date,
                    in_End_Dt_Parm => dEnd_Date
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

  EXCEPTION
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';

            IF BatchID = 0 THEN
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,
                                in_Prog_Status => 'X',
                                in_Comments => 'Job Failed logged to placeholder  ',
                                in_Error_Message => eErrMsg
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,
                            in_Schema_Name => cSchema_NM ,
                            in_Package_Name => cPackage_NM,
                            in_Program_Name => cProcedure_NM,
                            in_Load_Type => 'D' ,
                            in_Prog_Status => 'X' ,
                            in_Start_Dt_Parm => dBeg_Date ,
                            in_End_Dt_Parm => dEnd_Date
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,
                            in_Prog_Status => 'F',
                            in_Comments => 'Job Failed',
                            in_Error_Message => eErrMsg
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,
                            in_Schema_Name => cSchema_NM,
                            in_Package_Name => cPackage_NM,
                            in_Program_Name => cProcedure_NM,
                            in_Load_Type => 'D',
                            in_Prog_Status  => 'F',
                            in_Start_Dt_Parm => dBeg_Date,
                            in_End_Dt_Parm => dEnd_Date
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;

      WHEN OTHERS THEN
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;
            dbms_output.put_line(eErrMSG);
            ROLLBACK;

            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Prog_Status => 'F',
                    in_Comments => 'Job Failed',
                    in_Error_Message => eErrMsg
                    );
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Load_Type => 'D',
                    in_Prog_Status => 'F' ,
                    in_Start_Dt_Parm => dBeg_Date,
                    in_End_Dt_Parm => dEnd_Date
                    );
            COMMIT;
            RAISE;
  END REV_AK_BALANCER1_LOAD;

   PROCEDURE REV_AK_BALANCER2_LOAD IS
        cProcedure_NM  CONSTANT  Varchar2(30) := 'REV_AK_BALANCER2_LOAD';
        BatchID Number := 0;

  BEGIN
         /* Log program start */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Prog_Status => 'I',
                    in_Comments => 'Job in Progress',
                    in_Error_Message => 'NA'
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

        EXECUTE IMMEDIATE 'ALTER TABLE rev_ak_balancer_churn NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rev_ak_balancer_churn';

        INSERT /*+ append */ INTO rev_ak_balancer_churn
          (
            bkng_id,
            company_code,
            voyage,
            rpt_year,
            tour_margin_rev_new,
            tour_margin_rev_364_new,
            tour_margin_rev_728_new,
            tour_margin_rev_xld,
            tour_margin_rev_364_xld,
            tour_margin_rev_728_xld,
            tour_margin_rev_chg,
            tour_margin_rev_364_chg,
            tour_margin_rev_728_chg,
            guest_count_new,
            guest_count_364_new,
            guest_count_728_new,
            guest_count_xld,
            guest_count_364_xld,
            guest_count_728_xld,
            guest_count_chg,
            guest_count_364_chg,
            guest_count_728_chg,
            lower_bed_count_new,
            lower_bed_count_364_new,
            lower_bed_count_728_new,
            lower_bed_count_xld,
            lower_bed_count_364_xld,
            lower_bed_count_728_xld,
            lower_bed_count_chg,
            lower_bed_count_364_chg,
            lower_bed_count_728_chg,
            ntr_fin_new,
            ntr_fin_364_new,
            ntr_fin_728_new,
            ntr_fin_xld,
            ntr_fin_364_xld,
            ntr_fin_728_xld,
            ntr_fin_chg,
            ntr_fin_364_chg,
            ntr_fin_728_chg,
            ship_contribution_blended_new,
            ship_contrib_blended_364_new,
            ship_contrib_blended_728_new,
            ship_contribution_blended_xld,
            ship_contrib_blended_364_xld,
            ship_contrib_blended_728_xld,
            ship_contribution_blended_chg,
            ship_contrib_blended_364_chg,
            ship_contrib_blended_728_chg
          )
        WITH t1 as (
        SELECT
                    bkng_id,
                    company_code,
                    voyage,
                    rpt_year,
                    (tour_margin_rev - tour_margin_rev_fri) as tour_margin_rev,
                    (tour_margin_rev_364 - tour_margin_rev_371) as tour_margin_rev_364,
                    (tour_margin_rev_728 - tour_margin_rev_735) as tour_margin_rev_728,
                    (guest_count - guest_count_fri) as guest_count,
                    (guest_count_364 - guest_count_371) as guest_count_364,
                    (guest_count_728 - guest_count_735) as guest_count_728,
                    (lower_bed_count - lower_bed_count_fri) as lower_bed_count,
                    (lower_bed_count_364 - lower_bed_count_371) as lower_bed_count_364,
                    (lower_bed_count_728 - lower_bed_count_735) as lower_bed_count_728,
                    (ntr_fin - ntr_fin_fri) as ntr_fin,
                    (ntr_fin_364 - ntr_fin_371) as ntr_fin_364,
                    (ntr_fin_728 - ntr_fin_735) as ntr_fin_728,
                    (ship_contribution_blended - ship_contribution_blended_fri) as ship_contribution_blended,
                    (ship_contribution_blended_364 - ship_contribution_blended_371) as ship_contribution_blended_364,
                    (ship_contribution_blended_728 - ship_contribution_blended_735) as ship_contribution_blended_728
        FROM rev_ak_balancer_tour_data
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY')-1 FROM DUAL)  
--        WHERE rpt_year >= '2012'
        ),
        t2 as (
        SELECT
                    bkng_id,
                    company_code,
                    voyage,
                    rpt_year,
                    /* Tour Margin Rev New, Xld, Chg */
                    case when guest_count > 0 then tour_margin_rev else 0 end as tour_margin_rev_new,
                    case when guest_count_364 > 0 then tour_margin_rev_364 else 0 end as tour_margin_rev_364_new,
                    case when guest_count_728 > 0 then tour_margin_rev_728 else 0 end as tour_margin_rev_728_new,
                    case when guest_count < 0 then tour_margin_rev else 0 end as tour_margin_rev_xld,
                    case when guest_count_364 < 0 then tour_margin_rev_364 else 0 end as tour_margin_rev_364_xld,
                    case when guest_count_728 < 0 then tour_margin_rev_728 else 0 end as tour_margin_rev_728_xld,
                    case when guest_count = 0 and tour_margin_rev <> 0 then tour_margin_rev else 0 end as tour_margin_rev_chg,
                    case when guest_count_364 = 0 and tour_margin_rev_364 <> 0 then tour_margin_rev_364 else 0 end as tour_margin_rev_364_chg,
                    case when guest_count_728 = 0 and tour_margin_rev_728 <> 0 then tour_margin_rev_728 else 0 end as tour_margin_rev_728_chg,
                    /* Guest Count New, Xld, Chg */
                    case when guest_count > 0 then guest_count else 0 end as guest_count_new,
                    case when guest_count_364 > 0 then guest_count_364 else 0 end as guest_count_364_new,
                    case when guest_count_728 > 0 then guest_count_728 else 0 end as guest_count_728_new,
                    case when guest_count < 0 then guest_count else 0 end as guest_count_xld,
                    case when guest_count_364 < 0 then guest_count_364 else 0 end as guest_count_364_xld,
                    case when guest_count_728 < 0 then guest_count_728 else 0 end as guest_count_728_xld,
                    /* Note: Guest Change Not Possible*/
                    0 as guest_count_chg,
                    0 as guest_count_364_chg,
                    0 as guest_count_728_chg,
                    /* Lower Bed Count New, Xld, Chg */
                    case when guest_count > 0 and lower_bed_count > 0 then lower_bed_count else 0 end as lower_bed_count_new,
                    case when guest_count_364 > 0 and lower_bed_count_364 > 0 then lower_bed_count_364 else 0 end as lower_bed_count_364_new,
                    case when guest_count_728 > 0 and lower_bed_count_728 > 0 then lower_bed_count_728 else 0 end as lower_bed_count_728_new,
                    case when guest_count < 0 and lower_bed_count < 0 then lower_bed_count else 0 end as lower_bed_count_xld,
                    case when guest_count_364 < 0 and lower_bed_count_364 < 0 then lower_bed_count_364 else 0 end as lower_bed_count_364_xld,
                    case when guest_count_728 < 0 and lower_bed_count_728 < 0 then lower_bed_count_728 else 0 end as lower_bed_count_728_xld,
                    case when guest_count = 0 and lower_bed_count <> 0 then lower_bed_count else 0 end as lower_bed_count_chg,
                    case when guest_count_364 = 0 and lower_bed_count_364 <> 0 then lower_bed_count_364 else 0 end as lower_bed_count_364_chg,
                    case when guest_count_728 = 0 and lower_bed_count_728 <> 0 then lower_bed_count_728 else 0 end as lower_bed_count_728_chg,
                    /* NTR Finance New, Xld, Chg */
                    case when guest_count > 0 then ntr_fin else 0 end as ntr_fin_new,
                    case when guest_count_364 > 0 then ntr_fin_364 else 0 end as ntr_fin_364_new,
                    case when guest_count_728 > 0 then ntr_fin_728 else 0 end as ntr_fin_728_new,
                    case when guest_count < 0 then ntr_fin else 0 end as ntr_fin_xld,
                    case when guest_count_364 < 0 then ntr_fin_364 else 0 end as ntr_fin_364_xld,
                    case when guest_count_728 < 0 then ntr_fin_728 else 0 end as ntr_fin_728_xld,
                    case when guest_count = 0 and ntr_fin <> 0 then ntr_fin else 0 end as ntr_fin_chg,
                    case when guest_count_364 = 0 and ntr_fin_364 <> 0 then ntr_fin_364 else 0 end as ntr_fin_364_chg,
                    case when guest_count_728 = 0 and ntr_fin_728 <> 0 then ntr_fin_728 else 0 end as ntr_fin_728_chg,
                    /* Ship Contribution Blended New, Xld, Chg */
                    case when guest_count > 0 then ship_contribution_blended else 0 end as ship_contribution_blended_new,
                    case when guest_count_364 > 0 then ship_contribution_blended_364 else 0 end as ship_contrib_blended_364_new,
                    case when guest_count_728 > 0 then ship_contribution_blended_728 else 0 end as ship_contrib_blended_728_new,
                    case when guest_count < 0 then ship_contribution_blended else 0 end as ship_contribution_blended_xld,
                    case when guest_count_364 < 0 then ship_contribution_blended_364 else 0 end as ship_contrib_blended_364_xld,
                    case when guest_count_728 < 0 then ship_contribution_blended_728 else 0 end as ship_contrib_blended_728_xld,
                    case when guest_count = 0 and ship_contribution_blended <> 0 then ship_contribution_blended else 0 end as ship_contribution_blended_chg,
                    case when guest_count_364 = 0 and ship_contribution_blended_364 <> 0 then ship_contribution_blended_364 else 0 end as ship_contrib_blended_364_chg,
                    case when guest_count_728 = 0 and ship_contribution_blended_728 <> 0 then ship_contribution_blended_728 else 0 end as ship_contrib_blended_728_chg
        FROM t1
        )
        SELECT
                    bkng_id,
                    company_code,
                    voyage,
                    rpt_year,
                    tour_margin_rev_new,
                    tour_margin_rev_364_new,
                    tour_margin_rev_728_new,
                    tour_margin_rev_xld,
                    tour_margin_rev_364_xld,
                    tour_margin_rev_728_xld,
                    tour_margin_rev_chg,
                    tour_margin_rev_364_chg,
                    tour_margin_rev_728_chg,
                    guest_count_new,
                    guest_count_364_new,
                    guest_count_728_new,
                    guest_count_xld,
                    guest_count_364_xld,
                    guest_count_728_xld,
                    guest_count_chg,
                    guest_count_364_chg,
                    guest_count_728_chg,
                    lower_bed_count_new,
                    lower_bed_count_364_new,
                    lower_bed_count_728_new,
                    lower_bed_count_xld,
                    lower_bed_count_364_xld,
                    lower_bed_count_728_xld,
                    lower_bed_count_chg,
                    lower_bed_count_364_chg,
                    lower_bed_count_728_chg,
                    ntr_fin_new,
                    ntr_fin_364_new,
                    ntr_fin_728_new,
                    ntr_fin_xld,
                    ntr_fin_364_xld,
                    ntr_fin_728_xld,
                    ntr_fin_chg,
                    ntr_fin_364_chg,
                    ntr_fin_728_chg,
                    ship_contribution_blended_new,
                    ship_contrib_blended_364_new,
                    ship_contrib_blended_728_new,
                    ship_contribution_blended_xld,
                    ship_contrib_blended_364_xld,
                    ship_contrib_blended_728_xld,
                    ship_contribution_blended_chg,
                    ship_contrib_blended_364_chg,
                    ship_contrib_blended_728_chg
        FROM t2
      WHERE (
                    (tour_margin_rev_new <> 0) OR (tour_margin_rev_364_new <> 0) OR (tour_margin_rev_728_new <> 0)
                        OR
                    (tour_margin_rev_xld <> 0) OR (tour_margin_rev_364_xld <> 0) OR (tour_margin_rev_728_xld <> 0)
                        OR
                    (tour_margin_rev_chg <> 0) OR (tour_margin_rev_364_chg <> 0) OR (tour_margin_rev_728_chg <> 0)
                        OR
                    (guest_count_new <> 0) OR (guest_count_364_new <> 0) OR  (guest_count_728_new <> 0)
                        OR
                    (guest_count_xld <> 0) OR (guest_count_364_xld <> 0) OR (guest_count_728_xld <> 0)
                        OR
                    (guest_count_chg <> 0) OR (guest_count_364_chg <> 0) OR (guest_count_728_chg <> 0)
                        OR
                    (lower_bed_count_new <> 0) OR (lower_bed_count_364_new <> 0) OR  (lower_bed_count_728_new <> 0)
                        OR
                    (lower_bed_count_xld <> 0) OR (lower_bed_count_364_xld <> 0) OR (lower_bed_count_728_xld <> 0)
                        OR
                    (lower_bed_count_chg <> 0) OR (lower_bed_count_364_chg <> 0) OR (lower_bed_count_728_chg <> 0)
                        OR
                    (ntr_fin_new <> 0) OR (ntr_fin_364_new <> 0) OR (ntr_fin_728_new <> 0)
                        OR
                    (ntr_fin_xld <> 0) OR (ntr_fin_364_xld <> 0) OR (ntr_fin_728_xld <> 0)
                        OR
                    (ntr_fin_chg <> 0) OR (ntr_fin_364_chg <> 0) OR (ntr_fin_728_chg <> 0)
               );

        x:=SQL%ROWCOUNT;
        sComment  := 'rev_ak_balancer_churn Inserts: '||x;

        EXECUTE IMMEDIATE 'ALTER TABLE rev_ak_balancer_churn LOGGING';

        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'DM_TKR'
         ,TabName        => 'REV_AK_BALANCER_CHURN'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;

        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Prog_Status => 'C',
                    in_Comments => sComment,
                    in_Error_Message => 'NA'
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Load_Type => 'D',
                    in_Prog_Status => 'C',
                    in_Start_Dt_Parm => dBeg_Date,
                    in_End_Dt_Parm => dEnd_Date
                    ) != 0 THEN
            RAISE eCALL_FAILED;
        END IF;

  EXCEPTION
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';

            IF BatchID = 0 THEN
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,
                                in_Prog_Status => 'X',
                                in_Comments => 'Job Failed logged to placeholder  ',
                                in_Error_Message => eErrMsg
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,
                            in_Schema_Name => cSchema_NM ,
                            in_Package_Name => cPackage_NM,
                            in_Program_Name => cProcedure_NM,
                            in_Load_Type => 'D' ,
                            in_Prog_Status => 'X' ,
                            in_Start_Dt_Parm => dBeg_Date ,
                            in_End_Dt_Parm => dEnd_Date
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,
                            in_Prog_Status => 'F',
                            in_Comments => 'Job Failed',
                            in_Error_Message => eErrMsg
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,
                            in_Schema_Name => cSchema_NM,
                            in_Package_Name => cPackage_NM,
                            in_Program_Name => cProcedure_NM,
                            in_Load_Type => 'D',
                            in_Prog_Status  => 'F',
                            in_Start_Dt_Parm => dBeg_Date,
                            in_End_Dt_Parm => dEnd_Date
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;

      WHEN OTHERS THEN
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;
            dbms_output.put_line(eErrMSG);
            ROLLBACK;

            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,
                    in_Prog_Status => 'F',
                    in_Comments => 'Job Failed',
                    in_Error_Message => eErrMsg
                    );
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,
                    in_Schema_Name => cSchema_NM,
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM,
                    in_Load_Type => 'D',
                    in_Prog_Status => 'F' ,
                    in_Start_Dt_Parm => dBeg_Date,
                    in_End_Dt_Parm => dEnd_Date
                    );
            COMMIT;
            RAISE;
  END REV_AK_BALANCER2_LOAD; 

   PROCEDURE FCD_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'FCD_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE rdm_fcd_d ';              

        INSERT INTO rdm_fcd_d 
        (
          company_code          ,
          fcd_id                ,
          fcd_open_date         ,
          fcd_change_date       ,
          fcd_effective_date    ,
          book_by_shutoff_date  ,
          sail_by_shutoff_date  ,
          voyage_original       ,
          name_first            ,
          name_last             ,
          bkng_nbr_original     ,
          bkng_agency           ,
          credit_type           ,
          source_code           ,
          fcd_status            ,
          pax_id_original       ,
          fcd_open_userid       ,
          fcd_open_user_name    ,
          fcd_customer_id       ,
          fcd_desc              ,
          fcd_currency_code     ,
          credit_amt_suite      ,
          credit_amt_verandah   ,
          credit_amt_outside    ,
          credit_amt_inside     ,
          deposit_amt_suite     ,
          deposit_amt_verandah  ,
          deposit_amt_outside   ,
          deposit_amt_inside    ,
          fcd_unused_amt        ,
          fcd_received_amt      
        )
        SELECT 
            f.company_code,
            f.fcc_id as fcd_id,
            f.fcc_open_date as fcd_open_date,
            f.fcc_change_date as fcd_change_date,
            f.fcc_effective_date as fcd_effective_date,
            f.fcc_shutoff_date_book_by as book_by_shutoff_date,
            f.fcc_shutoff_date_sail_by as sail_by_shutoff_date,
            f.fcc_orig_voyage as voyage_original,
            f.fcc_name_first as name_first,
            f.fcc_name_last as name_last,
            f.fcc_orig_bkg_nbr as bkng_nbr_original,
            f.fcc_bkg_agent as bkng_agency,
            f.fcc_credit_type as credit_type,
            f.fcc_source_code as source_code,
            f.fcc_status as fcd_status,
            f.fcc_orig_pax_id as pax_id_original,
            f.fcc_open_userid as fcd_open_userid,
            u.user_name as fcd_open_user_name,
            f.fcc_cust_id as fcd_customer_id,
            f.fcc_description as fcd_desc,  
            f.fcc_currency_code as fcd_currency_code, 
            f.fcc_amount_suite as credit_amt_suite,       
            f.fcc_amount_balc as credit_amt_verandah,         
            f.fcc_amount_out as credit_amt_outside,          
            f.fcc_amount_in as credit_amt_inside,     
            f.fcc_dep_amount_suite as deposit_amt_suite,       
            f.fcc_dep_amount_balc as deposit_amt_verandah,         
            f.fcc_dep_amount_out as deposit_amt_outside,          
            f.fcc_dep_amount_in as deposit_amt_inside,   
            f.fcc_amount_unused as fcd_unused_amt,       
            f.fcc_amount_received as fcd_received_amt
        FROM fcc_mstr@tstdwh1_halw_dwh f, 
                user_details@tstdwh1_halw_dwh u
        WHERE f.company_code = u.company_code(+)
        AND f.fcc_open_userid = u.userid_polar(+);
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'rdm_fcd_d Inserts: '||x;
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
                       
  END FCD_LOAD; 

   PROCEDURE GP_MANUAL_ADJ_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'GP_MANUAL_ADJ_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF; 

        DELETE FROM gp_manual_adj 
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        COMMIT;
         
        EXECUTE IMMEDIATE 'ALTER TABLE gp_manual_adj NOLOGGING';
                      
        INSERT /*+ append */ INTO gp_manual_adj
       (
          company_code,
          bkng_id,
          bkng_nbr,
          voyage,
          guest_id,
          manual_adj_account,
          manual_adj_code,
          sequence_nbr,
          manual_adj_amt,
          change_date,
          change_remarks,
          change_userid,
          open_date,
          open_userid,
          manual_adj_percent,
          rpt_year,
          change_user_name,
          open_user_name
        )  
       SELECT 
            bpm.company_code,
            bpm.b_bkng_nbr || bpm.b_p_passenger_id as bkng_id,
            bpm.b_bkng_nbr as bkng_nbr,
            bpm.b_voyage as voyage,
            bpm.b_p_passenger_id as guest_id,
            bpm.b_man_account as manual_adj_account,
            bpm.b_man_adj_code_dtl1 as manual_adj_code,
            bpm.b_man_seq_no as sequence_nbr,
            bpm.b_man_amount as manual_adj_amt,
            bpm.b_man_change_date as change_date,
            bpm.b_man_change_remarks as change_remarks,
            bpm.b_man_change_userid as change_userid,
            bpm.b_man_open_date as open_date,
            bpm.b_man_open_userid as open_userid,
            bpm.b_man_pct as manual_adj_percent,
            vm.rpt_year,
            ud1.user_name AS change_user_name,
            ud2.user_name AS open_user_name
        FROM bkng_psngr_man_adj@tstdwh1_halw_dwh bpm,
                voyage_mstr@tstdwh1_halw_dwh vm,
                user_details@tstdwh1_halw_dwh ud1,
                user_details@tstdwh1_halw_dwh ud2
        WHERE bpm.company_code = vm.company_code
             AND bpm.b_voyage = vm.vyd_voyage 
             AND bpm.b_man_change_userid = ud1.userid_polar(+)
             AND bpm.company_code = ud1.company_code(+) 
             AND bpm.b_man_open_userid = ud2.userid_polar(+)
             AND bpm.company_code = ud2.company_code(+)                  
             AND bpm.b_man_account not in ('UFCAB','SPUHA')
             AND vm.rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'gp_manual_adj  Inserts: '||x;  
     
        EXECUTE IMMEDIATE 'ALTER TABLE gp_manual_adj LOGGING';            

        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
                       
  END GP_MANUAL_ADJ_LOAD;
  
     PROCEDURE SLS_RPTG_FY_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'SLS_RPTG_FY_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE sls_rptg_fy  ';              

        INSERT INTO sls_rptg_fy  
        (
          company_code             ,
          agency_name              ,
          agency_nbr               ,
          agency_name_nbr          ,
          arc_nbr                  ,
          country_code             ,
          agency_country           ,
          agency_status            ,
          classification           ,
          type_sales_program       ,
          region_description       ,
          agent_region_district    ,
          bdm                      ,
          bdr                      ,
          association_name         ,
          assoc_id                 ,
          super_assoc              ,
          super_assoc_nbr_name     ,
          assoc_year               ,
          season_description       ,
          product_type             ,
          trade                    ,
          trade_description        ,
          tours_indicator          ,
          ship                     ,
          voyage                   ,
          bkng_source              ,
          bkng_type                ,
          channel_type             ,
          bkng_channel             ,
          source_second            ,
          domestic_intl_bkng_flag  ,
          agency_id                ,
          bkng_id                  ,
          bkng_cancel_code         ,
          super_assoc_name         ,
          voyage_status            ,
          national_account_flag    ,
          fy_guests_curr_ytd       ,
          fy_guests_prior_stly     ,
          fy_guests_curr_gain      ,
          fy_guests_curr_stlw      ,
          fy_guests_curr_gain_wk   ,
          fy_guests_next_ytd       ,
          fy_guests_curr_stly      ,
          fy_guests_next_gain      ,
          fy_guests_next_stlw      ,
          fy_guests_next_gain_wk   ,
          fy_ntr_sls_prior2_ye     ,
          fy_ntr_sls_prior_ye      ,
          fy_ntr_sls_prior_stly    ,
          fy_ntr_sls_curr_ytd      ,
          fy_ntr_sls_next_ytd      
        )
        WITH t1
                   AS (SELECT
                             v.company_code,
                              a.agency_name,
                              a.agency_nbr,
                              a.agency_name || ' - ' || a.agency_nbr
                                 AS agency_name_nbr,
                              a.arc_nbr,
                              a.country_code,
                              CASE
                                 WHEN a.country_name IN
                                         ('GREAT BRITAIN', 'UNITED KINGDOM')
                                 THEN
                                    'GREAT BRITAIN'
                                 ELSE
                                    a.country_name
                              END
                                 AS agency_country,
                              a.agency_status,
                              a.classification,
                              a.type_sales_program,
                              a.region_description,
                              a.agent_region_district,
                              a.bdm,
                              a.bdr,
                              a.association_name,
                              a.assoc_id,
                              a.super_assoc,
                              CASE
                                 WHEN a.super_assoc_name IS NULL
                                 THEN
                                    a.super_assoc_name
                                 ELSE
                                    a.super_assoc || ' - ' || a.super_assoc_name
                              END
                                 AS super_assoc_nbr_name,
                              a.assoc_year,
                              v.season_description,
                              b.product_type,
                              v.trade,
                              v.trade_description,
                              b.tours_indicator,
                              v.ship_code || ' - ' || v.ship_name AS ship,
                              v.voyage,
                              b.bkng_source,
                              b.bkng_type,
                              b.channel_type,
                              b.bkng_channel,
                              b.source_second,
                              bk.domestic_intl_bkng_flag,
                              a.agency_id,
                              b.bkng_id,
                              b.bkng_cancel_code,
                              a.super_assoc_name,
                              v.voyage_status,
                              a.national_account_flag,
                              CASE
                                 WHEN v.rpt_year = dt.rpt_year THEN bpa.guests_total
                                 ELSE 0
                              END
                                 AS fy_guests_curr_ytd,
                              CASE
                                 WHEN v.rpt_year = (dt.rpt_year - 1)
                                 THEN
                                    bpa.guests_364
                                 ELSE
                                    0
                              END
                                 AS fy_guests_prior_stly,
                              CASE
                                 WHEN v.rpt_year = dt.rpt_year THEN bpa.guests_7
                                 ELSE 0
                              END
                                 AS fy_guests_curr_stlw,
                              CASE
                                 WHEN v.rpt_year = (dt.rpt_year + 1)
                                 THEN
                                    bpa.guests_total
                                 ELSE
                                    0
                              END
                                 AS fy_guests_next_ytd,
                              CASE
                                 WHEN v.rpt_year = dt.rpt_year THEN bpa.guests_364
                                 ELSE 0
                              END
                                 AS fy_guests_curr_stly,
                              CASE
                                 WHEN v.rpt_year = (dt.rpt_year + 1)
                                 THEN
                                    bpa.guests_7
                                 ELSE
                                    0
                              END
                                 AS fy_guests_next_stlw,
                              CASE
                                 WHEN v.rpt_year = (dt.rpt_year - 2)
                                 THEN
                                    bpa.ntr_sls_total
                                 ELSE
                                    0
                              END
                                 AS fy_ntr_sls_prior2_ye,
                              CASE
                                 WHEN v.rpt_year = (dt.rpt_year - 1)
                                 THEN
                                    bpa.ntr_sls_total
                                 ELSE
                                    0
                              END
                                 AS fy_ntr_sls_prior_ye,
                              CASE
                                 WHEN v.rpt_year = (dt.rpt_year - 1)
                                 THEN
                                    bpa.ntr_sls_364
                                 ELSE
                                    0
                              END
                                 AS fy_ntr_sls_prior_stly,
                              CASE
                                 WHEN v.rpt_year = dt.rpt_year THEN bpa.ntr_sls_total
                                 ELSE 0
                              END
                                 AS fy_ntr_sls_curr_ytd,
                              CASE
                                 WHEN v.rpt_year = (dt.rpt_year + 1)
                                 THEN
                                    bpa.ntr_sls_total
                                 ELSE
                                    0
                              END
                                 AS fy_ntr_sls_next_ytd
                         FROM rdm_trans_f t,
                              rdm_voyage_d v,
                              rdm_booking_d b,
                              rdm_agent_d a,
                              rdm_bpa_f bpa,
                              rdm_book_d bk,
                              rdm_date_d dt
                        WHERE     t.bkng_id = b.bkng_id
                              AND t.agency_id = a.agency_id
                              AND t.voyage = v.voyage
                              AND b.bkng_id = bpa.bkng_id
                              AND b.bkng_nbr = bk.bkng_nbr
                              AND v.company_code = b.company_code
                              AND v.company_code = a.company_code
                              AND v.company_code = bpa.company_code
                              --AND A.AGENCY_STATUS NOT IN ('I', 'W', 'T')
                              --AND a.assoc_year = dt.sail_year
                              --AND (b.bkng_cancel_code <> 'W'
                              --OR b.bkng_cancel_code IS NULL)
                              AND (   v.rpt_year = (dt.rpt_year - 2)
                                   OR v.rpt_year = (dt.rpt_year - 1)
                                   OR v.rpt_year = dt.rpt_year
                                   OR v.rpt_year = (dt.rpt_year + 1))
                              AND v.voyage_status IN ('A', 'H'))
           SELECT t1.company_code,
                  t1.agency_name,
                  t1.agency_nbr,
                  t1.agency_name_nbr,
                  t1.arc_nbr,
                  t1.country_code,
                  t1.agency_country,
                  t1.agency_status,
                  t1.classification,
                  t1.type_sales_program,
                  t1.region_description,
                  t1.agent_region_district,
                  t1.bdm,
                  t1.bdr,
                  t1.association_name,
                  t1.assoc_id,
                  t1.super_assoc,
                  t1.super_assoc_nbr_name,
                  t1.assoc_year,
                  t1.season_description,
                  t1.product_type,
                  t1.trade,
                  t1.trade_description,
                  t1.tours_indicator,
                  t1.ship,
                  t1.voyage,
                  t1.bkng_source,
                  t1.bkng_type,
                  t1.channel_type,
                  t1.bkng_channel,
                  t1.source_second,
                  t1.domestic_intl_bkng_flag,
                  t1.agency_id,
                  t1.bkng_id,
                  t1.bkng_cancel_code,
                  t1.super_assoc_name,
                  t1.voyage_status,
                  t1.national_account_flag,
                  t1.fy_guests_curr_ytd,
                  t1.fy_guests_prior_stly,
                  t1.fy_guests_curr_ytd - t1.fy_guests_prior_stly
                     AS fy_guests_curr_gain,
                  t1.fy_guests_curr_stlw,
                  t1.fy_guests_curr_ytd - t1.fy_guests_curr_stlw
                     AS fy_guests_curr_gain_wk,
                  t1.fy_guests_next_ytd,
                  t1.fy_guests_curr_stly,
                  t1.fy_guests_next_ytd - t1.fy_guests_curr_stly
                     AS fy_guests_next_gain,
                  t1.fy_guests_next_stlw,
                  t1.fy_guests_next_ytd - t1.fy_guests_next_stlw
                     AS fy_guests_next_gain_wk,
                  t1.fy_ntr_sls_prior2_ye,
                  t1.fy_ntr_sls_prior_ye,
                  t1.fy_ntr_sls_prior_stly,
                  t1.fy_ntr_sls_curr_ytd,
                  t1.fy_ntr_sls_next_ytd
             FROM t1;
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'sls_rptg_fy  Inserts: '||x;
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;             
  END SLS_RPTG_FY_LOAD;   
  
PROCEDURE FIN_CALENDAR_PRORATION IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'FIN_CALENDAR_PRORATION';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;       

        EXECUTE IMMEDIATE 'ALTER TABLE fin_calendar_proration NOLOGGING';   
        EXECUTE IMMEDIATE 'TRUNCATE TABLE fin_calendar_proration';
        
        INSERT /*+ append */ INTO fin_calendar_proration
            (   
                   COMPANY_CODE    ,
                   VOYAGE_PHYSICAL, 
                   RPT_YEAR        , 
                   RPT_QTR         ,
                   QTR_SPLIT_RATIO ,
                   SCHEDULE_WEEK    
                
         )
        
    with t1 as (
    select
        company_code,
        sch_polar_voyage,
        sch_ship_code,
        max(sch_week) as sch_week
    from sailing_schedule@tstdwh1_halw_dwh
    where sch_fiscal_year >= '2007'
    and sch_polar_voyage not like '%?%'
    group by
        company_code,
        sch_polar_voyage,
        sch_ship_code
    order by
        company_code,
        sch_polar_voyage,
        sch_ship_code
    )
    select
        ss.company_code,
        ss.sch_polar_voyage as voyage_physical,
        --ss.sch_ship_code as ship_code,
        --ss.sch_sail_date as sail_date, 
        ss.sch_fiscal_year as rpt_year,
        ss.sch_qtr as rpt_qtr, 
        ss.sch_qtr_split_ratio as qtr_split_ratio,
        ss.sch_week as schedule_week
    from
        t1,
        sailing_schedule@tstdwh1_halw_dwh ss
    where
        t1.company_code = ss.company_code
        and  t1.sch_polar_voyage = ss.sch_polar_voyage
        and t1.sch_ship_code=ss.sch_ship_code
        and t1.sch_week=ss.sch_week
    order by
        ss.company_code,
        voyage_physical;
        
x:=SQL%ROWCOUNT;        
        sComment  := 'fin_calendar_proration Inserts: '||x; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE fin_calendar_proration LOGGING';  
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
            
  END FIN_CALENDAR_PRORATION;  
  
   PROCEDURE AIR_PSNGR_FLIGHT IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'AIR_PSNGR_FLIGHT';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;       

        EXECUTE IMMEDIATE 'ALTER TABLE AIR_PSNGR_FLIGHT NOLOGGING';   
        EXECUTE IMMEDIATE 'TRUNCATE TABLE AIR_PSNGR_FLIGHT';
       insert /*+ append */  into air_psngr_flight 
       (COMPANY_CD,        
        RPT_YEAR   ,      
        VOYAGE      ,     
        BKNG_ID      ,    
        BKNG_NBR      ,    
        GUEST_ID       ,   
        SEQUENCE_NBR    , 
        AIR_CARRIER      ,
        FLIGHT_NBR       ,
        FROM_CITY         ,
        DEPART_DT        ,
        DEPART_TIME       ,
        TO_CITY          ,
        ARRIVAL_DT       ,
        ARRIVAL_TIME      ,
        FLIGHT_DIRECTION  ,
        MANUAL_AIR_FLG)
        
        with t1 as
(
select
    af.company_code as company_cd,
    vm.rpt_year,
    af.afmst_voyage as voyage,
    af.afmst_bkng_nbr || af.afmst_passenger_id as bkng_id,
    af.afmst_bkng_nbr as bkng_nbr,
    af.afmst_passenger_id as guest_id,
    af.afmst_seq_nbr as sequence_nbr,
    nvl(af.afmst_air_carrier,'??') as air_carrier,
    nvl(af.afmst_flight_nbr,'0000') as flight_nbr,
    nvl(af.afmst_from_city,'???') as from_city,
    af.afmst_depart_date as depart_dt,
    af.afmst_depart_time as depart_time,
    nvl(af.afmst_to_city,'???') as to_city,
    af.afmst_arrival_date as arrival_dt,
    af.afmst_arrival_time as arrival_time,
    af.afmst_flight_direction as flight_direction,
    'N' as manual_air_flg
from air_flt_mstr@tstdwh1_halw_dwh af, voyage_mstr@tstdwh1_halw_dwh vm
where af.company_code = vm.company_code
and af.afmst_voyage = vm.vyd_voyage
and vm.rpt_year >= (SELECT TO_CHAR (SYSDATE, 'YYYY') FROM DUAL)
union all
select
    ma.company_code as company_cd,
    vm.rpt_year,
    ma.mair_voyage_nbr as voyage,
    ma.mair_booking_nbr || ma.mair_passenger_id as bkng_id,
    ma.mair_booking_nbr as bkng_nbr,
    ma.mair_passenger_id as guest_id,
    ma.mair_itin_seq_nbr as sequence_nbr,
    nvl(ma.mair_air_carrier,'??') as air_carrier,
    nvl(ma.mair_flight_nbr,'0000') as flight_nbr,
    nvl(ma.mair_from_city,'???') as from_city,
    ma.mair_depart_date as depart_dt,
    ma.mair_depart_time as depart_time,
    nvl(ma.mair_to_city,'???') as to_city,
    ma.mair_arrival_date as arrival_dt,
    ma.mair_arrival_time as arrival_time,
    ma.mair_record_type as flight_direction,
    'Y' as manual_air_flg
from manual_air@tstdwh1_halw_dwh ma, voyage_mstr@tstdwh1_halw_dwh vm
where ma.company_code = vm.company_code
and ma.mair_voyage_nbr = vm.vyd_voyage 
and vm.rpt_year >= (SELECT TO_CHAR (SYSDATE, 'YYYY') FROM DUAL)
)
select *
from t1
;   
        
x:=SQL%ROWCOUNT;        
        sComment  := 'AIR_PSNGR_FLIGHT Inserts: '||x; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE AIR_PSNGR_FLIGHT LOGGING';  
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
            
  END AIR_PSNGR_FLIGHT;  
  
    PROCEDURE RSC_PSNGR_ITIN IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'RSC_PSNGR_ITIN';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;       

        EXECUTE IMMEDIATE 'ALTER TABLE RSC_PSNGR_ITIN NOLOGGING';   
        EXECUTE IMMEDIATE 'TRUNCATE TABLE  RSC_PSNGR_ITIN';
        
  
  insert /*+ append */  into  RSC_PSNGR_ITIN 
      ( COMPANY_CD,            
      RPT_YEAR     ,        
      VOYAGE        ,        
      BKNG_ID        ,      
      BKNG_NBR        ,      
      GUEST_ID         ,    
      RSC_CD            ,   
      RSC_DESC           , 
      VENDOR_NAME,
      RSC_YEAR            ,  
      RSC_TRADE            , 
      RSC_TYPE              ,
      RSC_WITHOUT_YEAR      ,
      RSC_POS_7_CHAR_4      ,
      ALLOCATION_DT         ,
      SEQUENCE_NBR         ,
      ITIN_TYPE             ,
      TDEF                 ,
      PACKAGE_ADDED_FLG     ,
      PRE_POST_PACKAGE_XFR,
      ITIN_DURATION_VALUE,
      ITIN_DURATION_CD,
      HOTEL_OCCUPANCY_CD,
      INVENTORY_STATUS_CD,
      RSC_ADD_DT,
      RSC_WAITLIST_FLG,
      TOUR_SPLIT_CD)   
         
     select
    bpi.company_code as company_cd,
    vm.rpt_year,
    bpi.pi_voyage as voyage,
    bpi.pi_booking_nbr || bpi.pi_passenger_id as bkng_id,
    bpi.pi_booking_nbr as bkng_nbr,
    bpi.pi_passenger_id as guest_id,
    bpi.pi_resource_info as rsc_cd,
    rm.res_description as rsc_desc,
    rm.res_name as vendor_name,
    substr(bpi.pi_resource_info,1,1) as rsc_year,
    substr(bpi.pi_resource_info,2,1) as rsc_trade,
    substr(bpi.pi_resource_info,3,1) as rsc_type,
    substr(bpi.pi_resource_info,2) as rsc_without_year,
    substr(bpi.pi_resource_info,7,4) as rsc_pos_7_char_4,
    bpi.pi_allocation_date as allocation_dt,
    bpi.sequence as sequence_nbr,
    bpi.pi_itinerary_type as itin_type,
    bpi.pi_tdef_id as tdef,
    bpi.pi_package_added_flag as package_added_flg,
    bpi.pi_pre_post_pkg_xfr as pre_post_package_xfr,
    bpi.pi_duration as itin_duration_value,
    bpi.pi_duration_type as itin_duration_cd,
    bpi.pi_pax_hotel_occupancy as hotel_occupancy_cd,
    bpi.pi_pax_inventory_status as inventory_status_cd,
    bpi.pi_resource_add_date as rsc_add_dt,
    bpi.pi_resource_wlst_flag as rsc_waitlist_flg,
    bpi.pi_tour_split_code as tour_split_cd
from bkng_psngr_itinerary@tstdwh1_halw_dwh bpi, voyage_mstr@tstdwh1_halw_dwh vm, resource_mstr@tstdwh1_halw_dwh rm
where bpi.company_code = vm.company_code
and bpi.company_code = rm.company_code
and bpi.pi_voyage = vm.vyd_voyage
and bpi.pi_resource_info = rm.res_key
and vm.rpt_year >= (SELECT TO_CHAR (SYSDATE, 'YYYY') -1 FROM DUAL);

 
        
x:=SQL%ROWCOUNT;        
        sComment  := 'RSC_PSNGR_ITIN Inserts: '||x; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE RSC_PSNGR_ITIN LOGGING';  
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
            
  END RSC_PSNGR_ITIN; 
  
      PROCEDURE  RSC_HPB_ROUTE_SEQ IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := ' RSC_HPB_ROUTE_SEQ';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;       

        EXECUTE IMMEDIATE 'ALTER TABLE  RSC_HPB_ROUTE_SEQ NOLOGGING';   
        EXECUTE IMMEDIATE 'TRUNCATE TABLE   RSC_HPB_ROUTE_SEQ';
        
       insert /*+ append */  into   RSC_HPB_ROUTE_SEQ 
       ( RSC_WITHOUT_YEAR    ,
      BEFORE_AFTER_CD      ,
      PICKUP_LOCATION_CD   ,
      PICKUP_SEQUENCE_NBR  ,
      FROM_PORT            )
      
     select 
    h.hpb_resource_code as rsc_without_year,
    h.hpb_before_after as before_after_cd,
    h.hpb_pickup_location_code as pickup_location_cd,
    h.hpb_pickup_sequence as pickup_sequence_nbr,
    h.hpb_departure_port as from_port
from hpb_route_seq@tstdwh1_halw_dwh h;   
   
 
        
x:=SQL%ROWCOUNT;        
        sComment  := ' RSC_HPB_ROUTE_SEQ Inserts: '||x; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE  RSC_HPB_ROUTE_SEQ LOGGING';  
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
            
  END RSC_HPB_ROUTE_SEQ;                         
            
   PROCEDURE GIFT_ITEMS_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'GIFT_ITEMS_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE igor_gift_items'; 
                 
        INSERT /*+ append */ INTO igor_gift_items
        SELECT
                  v.company_code,
                  v.rpt_year,
                  v.sail_year,
                  v.voyage,
                  v.ship_code,
                  v.sail_date,
                  v.sea_days,
                  pg.bkng_nbr,
                  to_char(pg.guest_id, '999') as guest_id,
                  pg.bkng_nbr || 0 || pg.guest_id as bkng_id,
                  bp.b_cabin as cabin,
                  bp.b_category as category,
                  bp.b_p_forename as name_first,
                  bp.b_p_surname as name_last,
                  pg.guest_gift_id,
                  0 as gift_seq_nbr,          
                  pg.code_item as gift_item_nbr,
                  pg.gift_name,
                  pg.gift_status,
                  pg.gift_created as gift_added_date,
                  pg.gift_xld as gift_xld_date,
                  pg.gift_delivery_date,
                  pg.gift_delivery_location,
                  pg.gift_count,
                  pg.gift_price,
                  'IGOR' as source_type
             FROM obr_prepaidgift_f pg,
                    rdm_voyage_d v,
                    bkng_psngr@tstdwh1_halw_dwh bp  
             WHERE pg.voyage = v.voyage
             AND pg.company_code = v.company_code
             AND pg.company_code = bp.company_code
             AND pg.voyage = bp.b_voyage
             AND pg.bkng_nbr = bp.b_bkng_nbr 
             AND (0 || pg.guest_id) = bp.b_p_passenger_id
             AND v.rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
       COMMIT; 

        EXECUTE IMMEDIATE 'TRUNCATE TABLE polar_gift_items';              

        INSERT /*+ append */ INTO polar_gift_items
        SELECT * FROM dm_tkr.v_polar_gift_items_dev@tstdwh1_halw_dwh;        
        COMMIT;
        
        EXECUTE IMMEDIATE 'ALTER TABLE obr_gift_items_combined NOLOGGING';

        EXECUTE IMMEDIATE 'ALTER TABLE obr_gift_items_combined TRUNCATE PARTITION obr_gift_2013';
        EXECUTE IMMEDIATE 'ALTER TABLE obr_gift_items_combined TRUNCATE PARTITION obr_gift_2014';     
        EXECUTE IMMEDIATE 'ALTER TABLE obr_gift_items_combined TRUNCATE PARTITION obr_gift_2015';
        EXECUTE IMMEDIATE 'ALTER TABLE obr_gift_items_combined TRUNCATE PARTITION obr_gift_2016';
        EXECUTE IMMEDIATE 'ALTER TABLE obr_gift_items_combined TRUNCATE PARTITION obr_gift_max';

        EXECUTE IMMEDIATE 'ALTER INDEX pk_obr_gift rebuild';          
                               
        INSERT /*+ append */ INTO obr_gift_items_combined
        SELECT * FROM igor_gift_items
        UNION
        SELECT * FROM polar_gift_items;        
        
        x:=SQL%ROWCOUNT ;        
        sComment  := 'obr_gift_items_combined Inserts: '||x;
               
        EXECUTE IMMEDIATE 'ALTER TABLE obr_gift_items_combined LOGGING';                 
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;    
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END GIFT_ITEMS_LOAD;
  
  
     PROCEDURE GROUP_ALLOT_CATG_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'GROUP_ALLOT_CATG_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;       

        DELETE FROM rdm_group_allot_catg_d
        WHERE rpt_year >= (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL);
        COMMIT;   
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_group_allot_catg_d NOLOGGING';        

        INSERT /*+ append */ INTO rdm_group_allot_catg_d
        select
           ga.company_code,
           vm.rpt_year,
           ga.gra_voyage as voyage,
           ga.gra_group_code as group_code,
           ga.gra_voyage || ga.gra_group_code as group_id,
           ga.gra_category as category,
           ga.gra_beds_offered as lower_beds_offered,
           ga.gra_beds_fore as lower_beds_forecasted,
           ga.gra_beds_blkd as lower_beds_blocked,
           ga.gra_beds_contracted as lower_beds_contracted,
           ga.gra_promo_indicator as promo_indicator
    from  group_allotments@tstdwh1_halw_dwh ga,
          voyage_mstr@tstdwh1_halw_dwh vm
    where ga.gra_voyage = vm.vyd_voyage
        and ga.company_code = vm.company_code    
        and vm.rpt_year >= (SELECT TO_CHAR (SYSDATE, 'YYYY') FROM DUAL);
--      and vm.rpt_year >= '2008';
         
        x:=SQL%ROWCOUNT;        
        sComment  := 'rdm_group_allot_catg_d Inserts: '||x; 
        
        EXECUTE IMMEDIATE 'ALTER TABLE rdm_group_allot_catg_d LOGGING';  
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
            
  END GROUP_ALLOT_CATG_LOAD;  

   PROCEDURE DAILY_INVENTORY_LOAD  IS 
      /****************************************************************
       NAME: DAILY_INVENTORY_LOAD
       
       PURPOSE: 
            Load  rows for  INV_DAILY_INVENTORY_F. 
            Input: Audit tables
             
       RETURN:N/A
       
       PARAMETERS:         
            None        
                                      
       REVISIONS:
       Ver     Date        Author  Description
       ------  ----------  ------  ---------------------------------------------
       1.0     02/14/2013  Venky    initial set up
       ************************************************************/

        
        cProgram_NM  CONSTANT  Varchar2(30) := 'DAILY_INVENTORY_LOAD';
        BatchID Number := 0;
        x Number ;   
        Beg_Dt Date := SYSDATE;
        End_Dt Date := SYSDATE;
        d_EOB_Data_Date            Date;                            

     BEGIN
     
     
      -- Log program start
        IF  DM_INT.COMMON_JOBS.LOG_JOB 
                (   io_Batch_ID  => BatchID, in_Schema_Name  =>  cSchema_NM,  in_Package_Name   => cPackage_NM,
                    in_Program_Name  =>cProgram_NM, in_Prog_Status => 'I',  in_Comments  => 'Job in Progress', in_Error_Message   => 'NA' ) != 0                                         
        THEN                  
            RAISE eCALL_FAILED ;
            
        END IF;

      sComment:= TO_CHAR(SYSDATE, 'HH24:MI:SS');

      --Get system data date
      BEGIN 
         SELECT eob_data_date 
           INTO d_EOB_Data_Date 
           FROM dw_system_parms@stgdwh1_halw_dwh
          WHERE dw_system_id = 'DW';

         EXCEPTION WHEN NO_DATA_FOUND THEN 
            d_EOB_Data_Date := NULL;           
      END;
       
      IF d_EOB_Data_Date IS NULL THEN
         eErrMsg :=  'No EOB Data Date found in dw_system_parms' ;        
         RAISE eINCONSISTENCY;
      END IF;

         EXECUTE IMMEDIATE 'TRUNCATE TABLE inv_daily_inventory_f'; 
        -- Insert New 
        x:=0;

        INSERT /*+ append */ INTO inv_daily_inventory_f (
               eob_data_date,
               COMPANY_CODE,
  VOYAGE,
  VOYAGE_PHYSICAL,
  CATEGORY,
  META_REVISED,
  SAIL_YEAR,
  RPT_YEAR,
  SEASON,
  SEASON_DESCRIPTION,
  TRADE_CODE,
  TRADE_DESCRIPTION,
  SUBTRADE_CODE,
  SUBTRADE_DESCRIPTION,
  SHIP_CODE,
  SHIP_NAME,
  SHIP_VERSION,
  FROM_PORT,
  TO_PORT,
  VOYAGE_STATUS,
  SAIL_DATE,
  SEA_DAYS,
  DAYS_TO_SAIL,
  WEEKS_TO_SAIL,
  LOWER_BEDS_AVAIL,
  LOWER_BERTH_CAPACITY,
  UPPER_BERTH_CAPACITY,
  GUESTS_TOTAL,
  GUESTS_7,
  GUESTS_FRI,
  GUESTS_FRI_7,
  GUESTS_FRI_28,
  PCD_TOTAL,
  PCD_7,
  PCD_FRI,
  PCD_FRI_7,
  PCD_FRI_28,
  LOWER_BEDS_TOTAL,
  LOWER_BEDS_7,
  LOWER_BEDS_FRI,
  LOWER_BEDS_FRI_7,
  LOWER_BEDS_FRI_28,
  UPPER_BEDS_TOTAL,
  UPPER_BEDS_7,
  UPPER_BEDS_FRI,
  UPPER_BEDS_FRI_7,
  UPPER_BEDS_FRI_28,
  SINGLE_BEDS_TOTAL,
  SINGLE_BEDS_7,
  SINGLE_BEDS_FRI,
  SINGLE_BEDS_FRI_7,
  SINGLE_BEDS_FRI_28,
  NTR,
  NTR_7,
  NTR_FRI,
  NTR_FRI_7,
  NTR_FRI_28,
  OPT_TOTAL,
  GROUP_EXPECTED,
  CALC_AVAL_ACTUAL,
  VAR_SL_WEEKS,
  CRUISE_ONLY_SOLD,
  TOURS_SOLD,
  CATEGORY_CAPACITY,
  VOYAGE_PRORATION,
  INVENTORY_PRORATION,
  META_SORT,
  CATEGORY_SORT)
  WITH category_total AS 
(
select /*+ PARALLEL */ 
       company_code,
       voyage,
       category,
       Guests_Total,
       Guests_7,
       Guests_Fri,
       Guests_Fri_7,
       Guests_Fri_28,
       PCD_Total,
       PCD_7,
       PCD_Fri,
       PCD_Fri_7,
       PCD_Fri_28,
       Lower_Beds_Total,
       Lower_Beds_7,
       Lower_Beds_Fri,
       Lower_Beds_Fri_7,
       Lower_Beds_Fri_28,
       Upper_Beds_Total,
       Upper_Beds_7,
       Upper_Beds_Fri,
       Upper_Beds_Fri_7,
       Upper_Beds_Fri_28,
       Single_Beds_Total,
       Single_Beds_7,
       Single_Beds_Fri,
       Single_Beds_Fri_7,
       Single_Beds_Fri_28,
       NTR,
       NTR_7,
       NTR_Fri,
       NTR_Fri_7,
       NTR_Fri_28
  from dm_tkr.v_booking_category@stgdwh1_halw_dwh
)
select /*+ PARALLEL */ 
       d_eob_data_date,
       vc.company_code,
       vc.Voyage,
       vp.voyage_physical,
       vc.Category,
       vc.Meta_Revised,
       vc.Sail_Year,
       vc.rpt_Year,
       vc.Season,
       vc.Season_Description,
       vc.trade_code,
       vc.Trade_Description,
       vc.subtrade_code,
       vc.Subtrade_Description,
       vc.ship_code,
       vc.ship_name,
       vc.ship_version,
       vc.From_Port,
       vc.To_Port,
       vc.Voyage_Status,
       vc.Sail_Date,
       vc.Sea_Days,
       vc.Days_To_Sail,
       vc.Weeks_To_Sail,
       NULL Lower_Beds_Avail,
       vc.Lower_Berth_Capacity,
       vc.Upper_Berth_Capacity,
       ct.Guests_Total,
       ct.Guests_7,
       ct.Guests_Fri,
       ct.Guests_Fri_7,
       ct.Guests_Fri_28,
       ct.PCD_Total,
       ct.PCD_7,
       ct.PCD_Fri,
       ct.PCD_Fri_7,
       ct.PCD_Fri_28,
       ct.Lower_Beds_Total,
       ct.Lower_Beds_7,
       ct.Lower_Beds_Fri,
       ct.Lower_Beds_Fri_7,
       ct.Lower_Beds_Fri_28,
       ct.Upper_Beds_Total,
       ct.Upper_Beds_7,
       ct.Upper_Beds_Fri,
       ct.Upper_Beds_Fri_7,
       ct.Upper_Beds_Fri_28,
       ct.Single_Beds_Total,
       ct.Single_Beds_7,
       ct.Single_Beds_Fri,
       ct.Single_Beds_Fri_7,
       ct.Single_Beds_Fri_28,
       ct.NTR,
       ct.NTR_7,
       ct.NTR_Fri,
       ct.NTR_Fri_7,
       ct.NTR_Fri_28,
       vc.Opt_Total,
       vc.Group_Expected,
       vc.Calc_Aval_Actual,
       vc.var_SL_Weeks,
       vc.Cruise_Only_Sold,
       vc.Tours_Sold,
       vc.Category_Capacity,
       vp.voyage_proration,
       vp.inventory_proration,
       vc.meta_sort,
       vc.category_sort
  FROM dwh_owner.v_voyage_inventory_category@stgdwh1_halw_dwh vc,
       fin_voyage_proration vp,
       category_total ct
 WHERE vc.voyage = vp.voyage
  and vc.company_code = vp.company_code
   and vc.voyage = ct.voyage(+)
  and vc.company_code = ct.company_code(+)
  and vc.category = ct.category(+);
                                                                    
        x:=SQL%ROWCOUNT ;                   
        sComment:= sComment||' Daily Inventory Load Insert '||x||TO_CHAR(SYSDATE, ' HH24:MI:SS');

commit;

        -- Populate lower beds avail to calculate the upgrade availability
        MERGE INTO inv_daily_inventory_f f
USING ( SELECT ship_code,
       ship_version,
       Category,
       SUM(Total_Beds_Avail) Total_Beds_Avail,
       SUM(Lower_Beds_Avail) Lower_Beds_Avail
  FROM rdm_cabin_full_d
GROUP BY ship_code,
       ship_version,
       Category) s
          ON (f.ship_code = s.ship_code AND f.ship_version = s.ship_version AND f.Category = s.Category AND f.voyage_physical = f.voyage)
          WHEN MATCHED THEN
            UPDATE SET f.Lower_Beds_Avail = s.Lower_Beds_Avail;

        sComment:= sComment||' Merges '||SQL%ROWCOUNT||TO_CHAR(SYSDATE, ' HH24:MI:SS');
COMMIT;

        END_DT := SYSDATE ;  -- set end date to sysdate , not crit for polar 
        
     -- Record Completion in Log    
        IF  DM_INT.COMMON_JOBS.LOG_JOB 
                (   io_Batch_ID  => BatchID,   in_Prog_Status => 'C',  in_Comments  => sComment,  in_Error_Message   => 'NA' ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
                             
        -- Record Completion in Control File
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID, in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
                    in_Start_DT_Parm  => Beg_DT, in_End_DT_Parm =>  End_Dt 
                ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
        
        
     EXCEPTION 
        WHEN eINCONSISTENCY THEN              
            ROLLBACK ;
             
             -- Record Error
            x:=  DM_INT.COMMON_JOBS.LOG_JOB  
                    (   io_Batch_ID  => BatchID,   in_Prog_Status => 'F',  in_Comments  => 'Job Failed'  ,  in_Error_Message   => eErrMsg ) ;
            
            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID,  in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D', in_Prog_Status  => 'F' , 
                    in_Start_DT_Parm  => Beg_DT ,             in_End_DT_Parm =>  End_Dt 
                )  ;
            RAISE eINCONSISTENCY;

        WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program' ;                  
                                  
            x:=-1 ;
            -- Record Error   against    palceholder w/  batch of -1   since batch not recorded 
            x:=  DM_INT.COMMON_JOBS.LOG_JOB 
                    ( io_Batch_ID  => x, in_Prog_Status => 'X', in_Comments  => 'Job Failed logged to placeholder  ' , in_Error_Message   => eErrMsg   ) ;

            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID ,                       in_Schema_Name => cSchema_NM ,  in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM  ,   in_Load_Type  => 'D' ,         in_Prog_Status  => 'X' , 
                    in_Start_DT_Parm  => Beg_DT ,             in_End_DT_Parm =>  End_Dt                 )  ;
          RAISE;

              
        WHEN OTHERS THEN
         dbms_output.put_line('Other error') ; 
        -- error trap and logging   Add a generic handler 
            eErrMsg :=  SQLERRM;                                              
            ROLLBACK ;
            
            --  record error w/ the assigned batch ID
            x:=  DM_INT.COMMON_JOBS.LOG_JOB 
                    (   io_Batch_ID  => BatchID,   in_Prog_Status => 'F',  in_Comments  => 'Job Failed',  in_Error_Message   => eErrMsg ) ;
          
            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID,  in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D',  in_Prog_Status  => 'F' , 
                    in_Start_DT_Parm  => Beg_DT ,  in_End_DT_Parm =>  End_Dt                  )  ;
           RAISE;                       

  END DAILY_INVENTORY_LOAD; 

     PROCEDURE DAILY_INVENTORY_LOAD_FINAL  IS 
      /****************************************************************
       NAME: DAILY_INVENTORY_LOAD
       
       PURPOSE: 
            Load  rows for  INV_DAILY_INVENTORY_F. 
            Input: Audit tables
             
       RETURN:N/A
       
       PARAMETERS:         
            None        
                                      
       REVISIONS:
       Ver     Date        Author  Description
       ------  ----------  ------  ---------------------------------------------
       1.0     02/14/2013  Venky    initial set up
       ************************************************************/

        
        cProgram_NM  CONSTANT  Varchar2(30) := 'DAILY_INVENTORY_LOAD_FINAL';
        BatchID Number := 0;
        x Number ;   
        Beg_Dt Date := SYSDATE;
        End_Dt Date := SYSDATE;
        d_EOB_Data_Date            Date;                            

     BEGIN
     
     
      -- Log program start
        IF  DM_INT.COMMON_JOBS.LOG_JOB 
                (   io_Batch_ID  => BatchID, in_Schema_Name  =>  cSchema_NM,  in_Package_Name   => cPackage_NM,
                    in_Program_Name  =>cProgram_NM, in_Prog_Status => 'I',  in_Comments  => 'Job in Progress', in_Error_Message   => 'NA' ) != 0                                         
        THEN                  
            RAISE eCALL_FAILED ;
            
        END IF;

      sComment:= TO_CHAR(SYSDATE, 'HH24:MI:SS');

      --Get system data date
      BEGIN 
         SELECT eob_data_date 
           INTO d_EOB_Data_Date 
           FROM dw_system_parms@stgdwh1_halw_dwh
          WHERE dw_system_id = 'DW';

         EXCEPTION WHEN NO_DATA_FOUND THEN 
            d_EOB_Data_Date := NULL;           
      END;
       
      IF d_EOB_Data_Date IS NULL THEN
         eErrMsg :=  'No EOB Data Date found in dw_system_parms' ;        
         RAISE eINCONSISTENCY;
      END IF;

         EXECUTE IMMEDIATE 'TRUNCATE TABLE inv_currency_booking_f'; 

INSERT /*+ append */ INTO inv_currency_booking_f (
               eob_data_date,
               COMPANY_CODE,
  VOYAGE,
  CATEGORY,
  CURRENCY_CODE,
  GUESTS_TOTAL,
  GUESTS_7,
  GUESTS_FRI,
  GUESTS_FRI_7,
  GUESTS_FRI_28,
  PCD_TOTAL,
  PCD_7,
  PCD_FRI,
  PCD_FRI_7,
  PCD_FRI_28,
  LOWER_BEDS_TOTAL,
  LOWER_BEDS_7,
  LOWER_BEDS_FRI,
  LOWER_BEDS_FRI_7,
  LOWER_BEDS_FRI_28,
  UPPER_BEDS_TOTAL,
  UPPER_BEDS_7,
  UPPER_BEDS_FRI,
  UPPER_BEDS_FRI_7,
  UPPER_BEDS_FRI_28,
  SINGLE_BEDS_TOTAL,
  SINGLE_BEDS_7,
  SINGLE_BEDS_FRI,
  SINGLE_BEDS_FRI_7,
  SINGLE_BEDS_FRI_28,
  NTR,
  NTR_7,
  NTR_FRI,
  NTR_FRI_7,
  NTR_FRI_28)
select /*+ PARALLEL */ 
       d_eob_data_date,
       company_code,
       voyage,
       category,
       currency_code,
       Guests_Total,
       Guests_7,
       Guests_Fri,
       Guests_Fri_7,
       Guests_Fri_28,
       PCD_Total,
       PCD_7,
       PCD_Fri,
       PCD_Fri_7,
       PCD_Fri_28,
       Lower_Beds_Total,
       Lower_Beds_7,
       Lower_Beds_Fri,
       Lower_Beds_Fri_7,
       Lower_Beds_Fri_28,
       Upper_Beds_Total,
       Upper_Beds_7,
       Upper_Beds_Fri,
       Upper_Beds_Fri_7,
       Upper_Beds_Fri_28,
       Single_Beds_Total,
       Single_Beds_7,
       Single_Beds_Fri,
       Single_Beds_Fri_7,
       Single_Beds_Fri_28,
       NTR,
       NTR_7,
       NTR_Fri,
       NTR_Fri_7,
       NTR_Fri_28
  from dwh_owner.v_booking_category_final@stgdwh1_halw_dwh;

  x:=SQL%ROWCOUNT ;                   
        sComment:= sComment||' Booking Category Insert '||x||TO_CHAR(SYSDATE, ' HH24:MI:SS');

commit;

         EXECUTE IMMEDIATE 'TRUNCATE TABLE inv_daily_inventory_f'; 
        -- Insert New 
        x:=0;

        INSERT /*+ append */ INTO inv_daily_inventory_f (
               eob_data_date,
               COMPANY_CODE,
  VOYAGE,
  VOYAGE_PHYSICAL,
  CATEGORY,
  META_REVISED,
  SAIL_YEAR,
  RPT_YEAR,
  SEASON,
  SEASON_DESCRIPTION,
  TRADE_CODE,
  TRADE_DESCRIPTION,
  SUBTRADE_CODE,
  SUBTRADE_DESCRIPTION,
  SHIP_CODE,
  SHIP_NAME,
  SHIP_VERSION,
  FROM_PORT,
  TO_PORT,
  VOYAGE_STATUS,
  SAIL_DATE,
  SEA_DAYS,
  DAYS_TO_SAIL,
  WEEKS_TO_SAIL,
  DAYS_TO_SAIL_FRI,
  WEEKS_TO_SAIL_FRI,
  LOWER_BEDS_AVAIL,
  LOWER_BERTH_CAPACITY,
  UPPER_BERTH_CAPACITY,
  GUESTS_TOTAL,
  GUESTS_7,
  GUESTS_FRI,
  GUESTS_FRI_7,
  GUESTS_FRI_28,
  PCD_TOTAL,
  PCD_7,
  PCD_FRI,
  PCD_FRI_7,
  PCD_FRI_28,
  LOWER_BEDS_TOTAL,
  LOWER_BEDS_7,
  LOWER_BEDS_FRI,
  LOWER_BEDS_FRI_7,
  LOWER_BEDS_FRI_28,
  UPPER_BEDS_TOTAL,
  UPPER_BEDS_7,
  UPPER_BEDS_FRI,
  UPPER_BEDS_FRI_7,
  UPPER_BEDS_FRI_28,
  SINGLE_BEDS_TOTAL,
  SINGLE_BEDS_7,
  SINGLE_BEDS_FRI,
  SINGLE_BEDS_FRI_7,
  SINGLE_BEDS_FRI_28,
  NTR,
  NTR_7,
  NTR_FRI,
  NTR_FRI_7,
  NTR_FRI_28,
  OPT_TOTAL,
  GROUP_EXPECTED,
  CALC_AVAL_ACTUAL,
  VAR_SL_WEEKS,
  CRUISE_ONLY_SOLD,
  TOURS_SOLD,
  CATEGORY_CAPACITY,
  Category_Hold_Flag,
  OPEN_FOR_SALE,
  Group_Avail_Offer,
  Group_Total_Offer,
  Group_Offer_limit,
  Group_Hold_Flag,
  VOYAGE_PRORATION,
  INVENTORY_PRORATION,
  META_SORT,
  CATEGORY_SORT)
  WITH category_total AS 
(
select /*+ PARALLEL */ 
       company_code,
       voyage,
       category,
       SUM(Guests_Total) Guests_Total,
       SUM(Guests_7) Guests_7,
       SUM(Guests_Fri) Guests_Fri,
       SUM(Guests_Fri_7) Guests_Fri_7,
       SUM(Guests_Fri_28) Guests_Fri_28,
       SUM(PCD_Total) PCD_Total,
       SUM(PCD_7) PCD_7,
       SUM(PCD_Fri) PCD_Fri,
       SUM(PCD_Fri_7) PCD_Fri_7,
       SUM(PCD_Fri_28) PCD_Fri_28,
       SUM(Lower_Beds_Total) Lower_Beds_Total,
       SUM(Lower_Beds_7) Lower_Beds_7,
       SUM(Lower_Beds_Fri) Lower_Beds_Fri,
       SUM(Lower_Beds_Fri_7) Lower_Beds_Fri_7,
       SUM(Lower_Beds_Fri_28) Lower_Beds_Fri_28,
       SUM(Upper_Beds_Total) Upper_Beds_Total,
       SUM(Upper_Beds_7) Upper_Beds_7,
       SUM(Upper_Beds_Fri) Upper_Beds_Fri,
       SUM(Upper_Beds_Fri_7) Upper_Beds_Fri_7,
       SUM(Upper_Beds_Fri_28) Upper_Beds_Fri_28,
       SUM(Single_Beds_Total) Single_Beds_Total,
       SUM(Single_Beds_7) Single_Beds_7,
       SUM(Single_Beds_Fri) Single_Beds_Fri,
       SUM(Single_Beds_Fri_7) Single_Beds_Fri_7,
       SUM(Single_Beds_Fri_28) Single_Beds_Fri_28,
       SUM(NTR) NTR,
       SUM(NTR_7) NTR_7,
       SUM(NTR_Fri) NTR_Fri,
       SUM(NTR_Fri_7) NTR_Fri_7,
       SUM(NTR_Fri_28) NTR_Fri_28
  from inv_currency_booking_f
group by company_code,
       voyage,
       category
)
select /*+ PARALLEL */ 
       d_eob_data_date,
       vc.company_code,
       vc.Voyage,
       vp.voyage_physical,
       vc.Category,
       vc.Meta_Revised,
       vc.Sail_Year,
       vc.rpt_Year,
       vc.Season,
       vc.Season_Description,
       vc.trade_code,
       vc.Trade_Description,
       vc.subtrade_code,
       vc.Subtrade_Description,
       vc.ship_code,
       vc.ship_name,
       vc.ship_version,
       vc.From_Port,
       vc.To_Port,
       vc.Voyage_Status,
       vc.Sail_Date,
       vc.Sea_Days,
       vc.Days_To_Sail,
       vc.Weeks_To_Sail,
       vc.Days_To_Sail_Fri,
       vc.Weeks_To_Sail_Fri,
       NULL Lower_Beds_Avail,
       vc.Lower_Berth_Capacity,
       vc.Upper_Berth_Capacity,
       ct.Guests_Total,
       ct.Guests_7,
       ct.Guests_Fri,
       ct.Guests_Fri_7,
       ct.Guests_Fri_28,
       ct.PCD_Total,
       ct.PCD_7,
       ct.PCD_Fri,
       ct.PCD_Fri_7,
       ct.PCD_Fri_28,
       ct.Lower_Beds_Total,
       ct.Lower_Beds_7,
       ct.Lower_Beds_Fri,
       ct.Lower_Beds_Fri_7,
       ct.Lower_Beds_Fri_28,
       ct.Upper_Beds_Total,
       ct.Upper_Beds_7,
       ct.Upper_Beds_Fri,
       ct.Upper_Beds_Fri_7,
       ct.Upper_Beds_Fri_28,
       ct.Single_Beds_Total,
       ct.Single_Beds_7,
       ct.Single_Beds_Fri,
       ct.Single_Beds_Fri_7,
       ct.Single_Beds_Fri_28,
       ct.NTR,
       ct.NTR_7,
       ct.NTR_Fri,
       ct.NTR_Fri_7,
       ct.NTR_Fri_28,
       vc.Opt_Total,
       vc.Group_Expected,
       vc.Calc_Aval_Actual,
       vc.var_SL_Weeks,
       vc.Cruise_Only_Sold,
       vc.Tours_Sold,
       vc.Category_Capacity,
       vc.Category_Hold_Flag,
       vc.open_for_sale, 
       vc.Group_Avail_Offer,
       vc.Group_Total_Offer,
       vc.Group_Offer_limit,
       vc.Group_Hold_Flag,
       vp.voyage_proration,
       vp.inventory_proration,
       vc.meta_sort,
       vc.category_sort
  FROM dwh_owner.v_voyage_inventory_category@stgdwh1_halw_dwh vc,
       fin_voyage_proration vp,
       category_total ct
 WHERE vc.voyage = vp.voyage
   and vc.company_code = vp.company_code
   and vc.voyage = ct.voyage(+)
   and vc.company_code = ct.company_code(+)
   and vc.category = ct.category(+)
   and vc.voyage_status != 'H'
UNION ALL
select /*+ PARALLEL */ 
       d_eob_data_date,
       vc.company_code,
       vc.Voyage,
       vp.voyage_physical,
       'XX' Category,
       'XX' Meta_Revised,
       MAX(vc.Sail_Year),
       MAX(vc.rpt_Year),
       MAX(vc.Season),
       MAX(vc.Season_Description),
       MAX(vc.trade_code),
       MAX(vc.Trade_Description),
       MAX(vc.subtrade_code),
       MAX(vc.Subtrade_Description),
       MAX(vc.ship_code),
       MAX(vc.ship_name),
       MAX(vc.ship_version),
       MAX(vc.From_Port),
       MAX(vc.To_Port),
       MAX(vc.Voyage_Status),
       MAX(vc.Sail_Date),
       MAX(vc.Sea_Days),
       MAX(vc.Days_To_Sail),
       MAX(vc.Weeks_To_Sail),
       MAX(vc.Days_To_Sail_Fri),
       MAX(vc.Weeks_To_Sail_Fri),
       NULL Lower_Beds_Avail,
       MAX(vc.Lower_Berth_Capacity),
       MAX(vc.Upper_Berth_Capacity),
       MAX(ct.Guests_Total),
       MAX(ct.Guests_7),
       MAX(ct.Guests_Fri),
       MAX(ct.Guests_Fri_7),
       MAX(ct.Guests_Fri_28),
       MAX(ct.PCD_Total),
       MAX(ct.PCD_7),
       MAX(ct.PCD_Fri),
       MAX(ct.PCD_Fri_7),
       MAX(ct.PCD_Fri_28),
       MAX(ct.Lower_Beds_Total),
       MAX(ct.Lower_Beds_7),
       MAX(ct.Lower_Beds_Fri),
       MAX(ct.Lower_Beds_Fri_7),
       MAX(ct.Lower_Beds_Fri_28),
       MAX(ct.Upper_Beds_Total),
       MAX(ct.Upper_Beds_7),
       MAX(ct.Upper_Beds_Fri),
       MAX(ct.Upper_Beds_Fri_7),
       MAX(ct.Upper_Beds_Fri_28),
       MAX(ct.Single_Beds_Total),
       MAX(ct.Single_Beds_7),
       MAX(ct.Single_Beds_Fri),
       MAX(ct.Single_Beds_Fri_7),
       MAX(ct.Single_Beds_Fri_28),
       MAX(ct.NTR),
       MAX(ct.NTR_7),
       MAX(ct.NTR_Fri),
       MAX(ct.NTR_Fri_7),
       MAX(ct.NTR_Fri_28),
       SUM(vc.Opt_Total) Opt_Total,
       SUM(vc.Group_Expected),
       SUM(vc.Calc_Aval_Actual),
       MAX(vc.var_SL_Weeks),
       SUM(vc.Cruise_Only_Sold),
       SUM(vc.Tours_Sold),
       SUM(vc.Category_Capacity),
       NULL Category_Hold_Flag,
       NULL open_for_sale, 
       NULL Group_Avail_Offer,
       NULL Group_Total_Offer,
       NULL Group_Offer_limit,
       NULL Group_Hold_Flag,
       MAX(vp.voyage_proration),
       MAX(vp.inventory_proration),
       NULL meta_sort,
       NULL category_sort
  FROM dwh_owner.v_voyage_inventory_category@stgdwh1_halw_dwh vc,
       fin_voyage_proration vp,
       category_total ct
 WHERE vc.voyage = vp.voyage
  and vc.company_code = vp.company_code
   and vc.voyage = ct.voyage(+)
  and vc.company_code = ct.company_code(+)
 -- and vc.category = ct.category(+)
  and vc.voyage_status = 'H'
  GROUP BY  vc.company_code,
       vc.Voyage,
       vp.voyage_physical;
                                                                    
        x:=SQL%ROWCOUNT ;                   
        sComment:= sComment||' Daily Inventory Load Insert '||x||TO_CHAR(SYSDATE, ' HH24:MI:SS');

commit;

        -- Populate lower beds avail to calculate the upgrade availability
        MERGE INTO inv_daily_inventory_f f
USING ( SELECT ship_code,
       ship_version,
       Category,
       SUM(Total_Beds_Avail) Total_Beds_Avail,
       SUM(Lower_Beds_Avail) Lower_Beds_Avail
  FROM rdm_cabin_full_d
GROUP BY ship_code,
       ship_version,
       Category) s
          ON (f.ship_code = s.ship_code AND f.ship_version = s.ship_version AND f.Category = s.Category AND f.voyage_physical = f.voyage)
          WHEN MATCHED THEN
            UPDATE SET f.Lower_Beds_Avail = s.Lower_Beds_Avail;

        sComment:= sComment||' Merges '||SQL%ROWCOUNT||TO_CHAR(SYSDATE, ' HH24:MI:SS');
COMMIT;

        END_DT := SYSDATE ;  -- set end date to sysdate , not crit for polar 
        
     -- Record Completion in Log    
        IF  DM_INT.COMMON_JOBS.LOG_JOB 
                (   io_Batch_ID  => BatchID,   in_Prog_Status => 'C',  in_Comments  => sComment,  in_Error_Message   => 'NA' ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
                             
        -- Record Completion in Control File
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID, in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D', in_Prog_Status  => 'C' , 
                    in_Start_DT_Parm  => Beg_DT, in_End_DT_Parm =>  End_Dt 
                ) != 0                                         
        THEN
            RAISE eCALL_FAILED ;            
        END IF;      
        
        
     EXCEPTION 
        WHEN eINCONSISTENCY THEN              
            ROLLBACK ;
             
             -- Record Error
            x:=  DM_INT.COMMON_JOBS.LOG_JOB  
                    (   io_Batch_ID  => BatchID,   in_Prog_Status => 'F',  in_Comments  => 'Job Failed'  ,  in_Error_Message   => eErrMsg ) ;
            
            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID,  in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D', in_Prog_Status  => 'F' , 
                    in_Start_DT_Parm  => Beg_DT ,             in_End_DT_Parm =>  End_Dt 
                )  ;
            RAISE eINCONSISTENCY;

        WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program' ;                  
                                  
            x:=-1 ;
            -- Record Error   against    palceholder w/  batch of -1   since batch not recorded 
            x:=  DM_INT.COMMON_JOBS.LOG_JOB 
                    ( io_Batch_ID  => x, in_Prog_Status => 'X', in_Comments  => 'Job Failed logged to placeholder  ' , in_Error_Message   => eErrMsg   ) ;

            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID ,                       in_Schema_Name => cSchema_NM ,  in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM  ,   in_Load_Type  => 'D' ,         in_Prog_Status  => 'X' , 
                    in_Start_DT_Parm  => Beg_DT ,             in_End_DT_Parm =>  End_Dt                 )  ;
          RAISE;

              
        WHEN OTHERS THEN
         dbms_output.put_line('Other error') ; 
        -- error trap and logging   Add a generic handler 
            eErrMsg :=  SQLERRM;                                              
            ROLLBACK ;
            
            --  record error w/ the assigned batch ID
            x:=  DM_INT.COMMON_JOBS.LOG_JOB 
                    (   io_Batch_ID  => BatchID,   in_Prog_Status => 'F',  in_Comments  => 'Job Failed',  in_Error_Message   => eErrMsg ) ;
          
            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL 
                (   in_Batch_ID => BatchID,  in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM, 
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D',  in_Prog_Status  => 'F' , 
                    in_Start_DT_Parm  => Beg_DT ,  in_End_DT_Parm =>  End_Dt                  )  ;
           RAISE;                       

  END DAILY_INVENTORY_LOAD_FINAL;


    PROCEDURE DAILY_PRICING_LOAD  IS
      /****************************************************************
       NAME: DAILY_PRICING_LOAD

       PURPOSE:
            Load  rows for  INV_DAILY_PRICING_F.
            Input: Audit tables

       RETURN:N/A

       PARAMETERS:
            None

       REVISIONS:
       Ver     Date        Author  Description
       ------  ----------  ------  ---------------------------------------------
       1.0     05/13/2013  Venky    initial set up
       ************************************************************/


        cProgram_NM  CONSTANT  Varchar2(30) := 'DAILY_PRICING_LOAD';
        BatchID Number := 0;
        x Number ;
        Beg_Dt Date := SYSDATE;
        End_Dt Date := SYSDATE;
        d_EOB_Data_Date            Date;
        d_prior_friday             Date;

     BEGIN


      -- Log program start
        IF  DM_INT.COMMON_JOBS.LOG_JOB
                (   io_Batch_ID  => BatchID, in_Schema_Name  =>  cSchema_NM,  in_Package_Name   => cPackage_NM,
                    in_Program_Name  =>cProgram_NM, in_Prog_Status => 'I',  in_Comments  => 'Job in Progress', in_Error_Message   => 'NA' ) != 0
        THEN
            RAISE eCALL_FAILED ;

        END IF;

      sComment:= TO_CHAR(SYSDATE, 'HH24:MI:SS');

      --Get system data date and prior Friday date
      BEGIN
         SELECT eob_data_date,
                prior_friday
           INTO d_EOB_Data_Date,
                d_prior_friday
           FROM dw_system_parms@stgdwh1_halw_dwh
          WHERE dw_system_id = 'DW';

         EXCEPTION WHEN NO_DATA_FOUND THEN
            d_EOB_Data_Date := NULL;
            d_prior_friday := NULL;
      END;

      IF d_EOB_Data_Date IS NULL THEN
         eErrMsg :=  'No EOB Data Date found in dw_system_parms' ;
         RAISE eINCONSISTENCY;
      END IF;

      IF d_prior_friday IS NULL THEN
         eErrMsg :=  'No prior friday found in dw_system_parms' ;
         RAISE eINCONSISTENCY;
      END IF;


         EXECUTE IMMEDIATE 'TRUNCATE TABLE inv_daily_pricing_f';
        -- Insert New
        x:=0;

        INSERT /*+ append */ INTO inv_daily_pricing_f (
               eob_data_date,
  COMPANY_CODE,
  VOYAGE,
  PROMO_CODE,
  CATEGORY,
  CURRENCY_CODE,
  BASE_CURRENCY,
  META_REVISED,
  PROMO_RATE_SINGLE,
  PROMO_RATE_DOUBLE,
  PROMO_RATE_THIRD,
  PROMO_RATE_UP_CHILD,
  EFFECTIVE_DATE,
  COMMISSION_IND,
  INCLUDE_AIR_FLAG,
  PROMO_TYPE,
  PAX_TYPE,
  AGENCY_PROMO,
  CAMPAIGN_PROMO,
  BEST_BUY_ELIGIBLE,
  BASIS_UNIT,
  PROMO_DESC,
  PROMO_STATUS,
  CAMPAIGN_EFFECTIVE_DATE,
  CATEGORY_CLOSED_STATUS,
  TOURS_ELIGIBLE_IND,
  FEE,
  LAND_NCF,
  STANDARD_TAX,
  EXCLUSIVE_TAX,
  UP_ADULT_TAX,
  UP_CHILD_TAX,
  EXCHANGE_RATE,
  APO_LOWER_BED_TOTAL,
  APO_EXTRA_BED_TOTAL)
 WITH promo_pricing AS (
SELECT /*+ PARALLEL */
          p.company_code,
          p.voyage,
          p.promo_code,
          p.category,
          p.Tour_ID,
          p.currency_code,
          p.base_currency,
          MAX(p.promo_desc) promo_desc,
          RANK() OVER (PARTITION BY p.company_code, p.voyage,  p.promo_code, p.category,  p.currency_code, p.base_currency ORDER BY p.company_code, p.voyage, p.promo_code, p.category, p.Tour_ID, p.currency_code, p.base_currency) promo_rank,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.promo_rate_single END) AS promo_rate_single,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.promo_rate_double END) AS promo_rate_double,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.promo_rate_third END) AS promo_rate_third,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.promo_rate_up_child END) AS promo_rate_up_child,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.promo_status END) AS promo_status,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.category_closed_status END) AS category_closed_status,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.tours_eligible_ind END) AS tours_eligible_ind,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.fee END) AS fee,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.land_ncf END) AS land_ncf,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.standard_tax END) AS standard_tax,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.exclusive_tax END) AS exclusive_tax,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.up_adult_tax END) AS up_adult_tax,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.up_child_tax END) AS up_child_tax,
          MAX(CASE WHEN p.effective_date <= d_EOB_Data_Date  AND p.expiration_date > d_EOB_Data_Date  THEN p.effective_date END) AS effective_date,
        -- APO Lower Beds Count
        SUM(p.APO_Lower_Bed_count) AS APO_Lower_Bed_Total,
         -- APO Extra Beds Count
        SUM(p.APO_Extra_Bed_count) AS APO_Extra_Bed_Total
     FROM inv_promo_pricing_f p
     WHERE  p.effective_date + 0 <= d_EOB_Data_Date
       and p.expiration_date + 0 > d_prior_friday - 28
       AND EXISTS (SELECT 1
               FROM inv_daily_inventory_f d
               where d.company_code = p.company_code
                 AND d.voyage = p.voyage
                 AND d.category = p.category)
     GROUP BY  p.company_code,
          p.voyage,
          p.promo_code,
          p.category,
          p.Tour_ID,
          p.currency_code,
          p.base_currency
 )
 select d_EOB_Data_Date,
        pp.company_code,
        pp.voyage,
        pp.promo_code,
        pp.category,
        pp.currency_code,
        pp.base_currency,
        sc.meta_revised,
        pp.promo_rate_single,
        pp.promo_rate_double,
        pp.promo_rate_third,
        pp.promo_rate_up_child,
        pp.effective_date,
        vp.commission_ind,
        vp.include_air_flag,
        vp.promo_type,
        vp.pax_type,
        vp.agency_promo,
        vp.campaign_promo,
        vp.best_buy_eligible,
        vp.basis_unit,
        pp.promo_desc,
        pp.promo_status,
        vp.campaign_effective_date,
        pp.category_closed_status,
        pp.tours_eligible_ind,
        pp.fee,
        pp.land_ncf,
        pp.standard_tax,
        pp.exclusive_tax,
        pp.up_adult_tax,
        pp.up_child_tax,
        vp.exchange_rate,
        -- APO Lower Beds Count
        pp.APO_Lower_Bed_Total,
         -- APO Extra Beds Count
        pp.APO_Extra_Bed_Total
 from promo_pricing pp,
      dwh_owner.v_promo_details@stgdwh1_dm_tkr vp,
      dwh_owner.v_ship_category@stgdwh1_dm_tkr sc
 where promo_rank = 1
  AND pp.company_code = sc.company_code
       AND pp.voyage = sc.voyage
       AND pp.category = sc.category
  AND  pp.company_code  = vp.company_code
       AND  pp.voyage = vp.voyage
       AND  pp.promo_code = vp.promo_code
       AND  pp.currency_code = vp.currency_code
       AND  pp.base_currency = vp.base_currency;

        x:=SQL%ROWCOUNT ;
        sComment:= sComment||' Daily Pricing Load Insert '||x||TO_CHAR(SYSDATE, ' HH24:MI:SS');

commit;

       MERGE INTO inv_daily_pricing_f f
USING ( WITH min_eff_date AS
          (
SELECT /*+ PARALLEL */ company_code,
        voyage,
        promo_code,
        category,
        currency_code,
        base_currency,
        promo_rate_single,
        promo_rate_double,
        promo_rate_third,
        promo_rate_up_child,
        effective_date,
        expiration_date,
        cur_effective_date,
        promo_rank,
        MAX(actual_effective_date) OVER (PARTITION BY  company_code, voyage, DECODE (SUBSTR (promo_code, 1, 2), 'RH', 'RH', promo_code), category, currency_code, base_currency  ORDER BY company_code, voyage, DECODE (SUBSTR (promo_code, 1, 2), 'RH', 'RH', promo_code), category, currency_code,  base_currency, effective_date, expiration_date) actual_effective_date
FROM
(
SELECT /*+ PARALLEL */ p.company_code,
        p.voyage,
        p.promo_code,
        p.category,
        p.currency_code,
        p.base_currency,
        p.promo_rate_single,
        p.promo_rate_double,
        p.promo_rate_third,
        p.promo_rate_up_child,
        p.effective_date,
        p.expiration_date,
        CASE WHEN LAG(p.promo_rate_double) OVER (PARTITION BY  p.company_code, p.voyage, DECODE (SUBSTR (p.promo_code, 1, 2), 'RH', 'RH', p.promo_code), p.category, p.currency_code, p.base_currency  ORDER BY p.company_code, p.voyage, DECODE (SUBSTR (p.promo_code, 1, 2), 'RH', 'RH', p.promo_code), p.category, p.currency_code,  p.base_currency, p.effective_date, p.expiration_date) = p.promo_rate_double AND
                  LAG(p.promo_rate_third) OVER (PARTITION BY  p.company_code, p.voyage, DECODE (SUBSTR (p.promo_code, 1, 2), 'RH', 'RH', p.promo_code), p.category, p.currency_code, p.base_currency  ORDER BY p.company_code, p.voyage, DECODE (SUBSTR (p.promo_code, 1, 2), 'RH', 'RH', p.promo_code), p.category, p.currency_code,  p.base_currency, p.effective_date, p.expiration_date) = p.promo_rate_third AND
                  LAG(p.promo_rate_up_child) OVER (PARTITION BY  p.company_code, p.voyage, DECODE (SUBSTR (p.promo_code, 1, 2), 'RH', 'RH', p.promo_code), p.category, p.currency_code, p.base_currency  ORDER BY p.company_code, p.voyage, DECODE (SUBSTR (p.promo_code, 1, 2), 'RH', 'RH', p.promo_code), p.category, p.currency_code,  p.base_currency, p.effective_date, p.expiration_date) = p.promo_rate_up_child
             THEN (CASE WHEN LAG(p.expiration_date) OVER (PARTITION BY  p.company_code, p.voyage, DECODE (SUBSTR (p.promo_code, 1, 2), 'RH', 'RH', p.promo_code), p.category, p.currency_code, p.base_currency  ORDER BY p.company_code, p.voyage, DECODE (SUBSTR (p.promo_code, 1, 2), 'RH', 'RH', p.promo_code), p.category, p.currency_code,  p.base_currency, p.effective_date, p.expiration_date) = p.effective_date THEN NULL
                       ELSE p.effective_date
                  END)
             ELSE p.effective_date
        END actual_effective_date,
        RANK() OVER (PARTITION BY p.company_code, p.voyage,  p.promo_code, p.category,  p.currency_code, p.base_currency ORDER BY p.company_code, p.voyage, p.promo_code, p.category, p.Tour_ID, p.currency_code, p.base_currency) promo_rank,
       tt.effective_date cur_effective_date
  FROM inv_promo_pricing_f p,
       inv_promo_pricing_f tt
WHERE p.promo_status != 'T'
-- AND p.category_closed_status = 'Open'
                                AND  p.company_code = tt.company_code
                                AND p.voyage = tt.voyage
                                AND p.tour_id = tt.tour_id
                                AND DECODE (SUBSTR (p.promo_code, 1, 2), 'RH', 'RH', p.promo_code) = DECODE (SUBSTR (tt.promo_code, 1, 2), 'RH', 'RH', tt.promo_code)
                                AND p.category = tt.category
                                AND p.currency_code = tt.currency_code
                                AND p.base_currency = tt.base_currency
                                AND tt.effective_date <= TRUNC(SYSDATE) - 1  AND tt.expiration_date > TRUNC(SYSDATE) - 1
                                AND tt.promo_status = 'O'
                                AND tt.category_closed_status = 'Open'
)
)
select company_code,
        voyage,
        promo_code,
        category,
        currency_code,
        base_currency,
        MIN(cur_effective_date),
        MIN(actual_effective_date) actual_effective_date
    from min_eff_date
where cur_effective_date = effective_date
ANd promo_rank = 1
--and voyage = 'V356'
--and currency_code = 'USD'
----and p.promo_code = 'RH1'
--and category = 'MM'
GROUP BY company_code,
        voyage,
        promo_code,
        category,
        currency_code,
        base_currency
having  MIN(cur_effective_date) <>
        MIN(actual_effective_date) ) s
          ON (f.company_code = s.company_code
                                AND f.voyage = s.voyage
                                AND f.promo_code = s.promo_code
                                AND f.category = s.category
                                AND f.currency_code = s.currency_code
                                AND f.base_currency = s.base_currency )
          WHEN MATCHED THEN
            UPDATE SET f.effective_date = s.actual_effective_date;

        sComment:= sComment||' Merges '||SQL%ROWCOUNT||TO_CHAR(SYSDATE, ' HH24:MI:SS');
        COMMIT;

        END_DT := SYSDATE ;  -- set end date to sysdate , not crit for polar

     -- Record Completion in Log
        IF  DM_INT.COMMON_JOBS.LOG_JOB
                (   io_Batch_ID  => BatchID,   in_Prog_Status => 'C',  in_Comments  => sComment,  in_Error_Message   => 'NA' ) != 0
        THEN
            RAISE eCALL_FAILED ;
        END IF;

        -- Record Completion in Control File
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL
                (   in_Batch_ID => BatchID, in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM,
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D', in_Prog_Status  => 'C' ,
                    in_Start_DT_Parm  => Beg_DT, in_End_DT_Parm =>  End_Dt
                ) != 0
        THEN
            RAISE eCALL_FAILED ;
        END IF;


     EXCEPTION
        WHEN eINCONSISTENCY THEN
            ROLLBACK ;

             -- Record Error
            x:=  DM_INT.COMMON_JOBS.LOG_JOB
                    (   io_Batch_ID  => BatchID,   in_Prog_Status => 'F',  in_Comments  => 'Job Failed'  ,  in_Error_Message   => eErrMsg ) ;

            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL
                (   in_Batch_ID => BatchID,  in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM,
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D', in_Prog_Status  => 'F' ,
                    in_Start_DT_Parm  => Beg_DT ,             in_End_DT_Parm =>  End_Dt
                )  ;
            RAISE eINCONSISTENCY;

        WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program' ;

            x:=-1 ;
            -- Record Error   against    palceholder w/  batch of -1   since batch not recorded
            x:=  DM_INT.COMMON_JOBS.LOG_JOB
                    ( io_Batch_ID  => x, in_Prog_Status => 'X', in_Comments  => 'Job Failed logged to placeholder  ' , in_Error_Message   => eErrMsg   ) ;

            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL
                (   in_Batch_ID => BatchID ,                       in_Schema_Name => cSchema_NM ,  in_Package_Name => cPackage_NM,
                    in_Program_Name =>  cProgram_NM  ,   in_Load_Type  => 'D' ,         in_Prog_Status  => 'X' ,
                    in_Start_DT_Parm  => Beg_DT ,             in_End_DT_Parm =>  End_Dt                 )  ;
          RAISE;


        WHEN OTHERS THEN
         dbms_output.put_line('Other error') ;
        -- error trap and logging   Add a generic handler
            eErrMsg :=  SQLERRM;
            ROLLBACK ;

            --  record error w/ the assigned batch ID
            x:=  DM_INT.COMMON_JOBS.LOG_JOB
                    (   io_Batch_ID  => BatchID,   in_Prog_Status => 'F',  in_Comments  => 'Job Failed',  in_Error_Message   => eErrMsg ) ;

            x:=    DM_INT.COMMON_JOBS.LOG_PROC_CTRL
                (   in_Batch_ID => BatchID,  in_Schema_Name => cSchema_NM, in_Package_Name => cPackage_NM,
                    in_Program_Name =>  cProgram_NM, in_Load_Type  => 'D',  in_Prog_Status  => 'F' ,
                    in_Start_DT_Parm  => Beg_DT ,  in_End_DT_Parm =>  End_Dt                  )  ;
           RAISE;

  END DAILY_PRICING_LOAD;
  
   PROCEDURE SLS_BKNG_ACT_LOAD IS   
        cProcedure_NM  CONSTANT  Varchar2(30) := 'SLS_BKNG_ACT_LOAD';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
        EXECUTE IMMEDIATE 'ALTER TABLE sls_bkng_activity NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE sls_bkng_activity';          

        INSERT /*+ append */ INTO sls_bkng_activity 
WITH t3 AS (
            SELECT /*+ parallel(t,8) parallel (b,8) parallel (bpa,8)*/
                           a.agency_id,
                           a.agency_name,
                           a.agency_nbr,
                           a.bdm,
                           a.bdr,
                           a.super_assoc,
                           a.super_assoc_name,
                           a.association_name,
                           a.assoc_id,
                           a.arc_nbr,
                           a.agency_status,
                           a.classification,
                           a.type_sales_program,
                           a.agent_region_district,
                           a.agency_create_date,
                           a.address1,
                           a.city,
                           a.state,
                           a.zip,
                           a.country_code,
                           a.country_name,
                           a.telephone,
                           v.rpt_qtr,
                           v.rpt_year,
                           v.sail_year,
                           v.season_description,
                           v.trade_description,
                           v.subtrade_description,
                           a.region_description,
                           a.region,
                           v.ship_code,
                           v.ship_name,
                           v.sail_date,
                           v.from_port || '/' || v.to_port as segment, 
                           v.sea_days,
                           v.voyage_status,
                           b.nationality,
                           b.bkng_nbr || b.pax_id as bkng_id,
                           v.voyage,
                           b.bkng_nbr,
                           b.pax_id,
                           b.title,
                           b.first_name,
                           b.last_name,
                           b.suffix,
                           b.suffix || ' ' || b.last_name AS suffix_last_name,
                           b.bkng_open_date,
                           b.bkng_status,
                           b.pax_status,
                           t2.nvs_bkng_nbr AS bkng_nbr_nvs,
                           b.promo_sob,
                           b.comm_withheld,
                           b.tours_indicator,
                           b.tour_id,
                           b.group_code,
                           b.group_type,
                           b.bkng_source,
                           b.bkng_type,
                           b.channel_type,
                           b.bkng_channel,
                           b.source_second,
                           b.home_country,
                           bk.domestic_intl_bkng_flag,
                           b.product_type,
                           b.company_code AS company_code_1,
                          CASE WHEN b.company_code = 'H' THEN 'HAL' ELSE 'SBN' END
                              AS company_code_3,
                           CASE
                              WHEN b.company_code = 'H' THEN 'Holland America Line'
                              ELSE 'Seabourn'
                           END
                              AS company_name,
                           CASE
                              WHEN v.voyage_status = 'H'
                              THEN
                                 '04. Charter'
                              WHEN b.reassign_userid LIKE 'HA09%'
                              THEN
                                 '07. PCC'
                              WHEN b.reassign_userid LIKE 'SB09%'
                              THEN
                                 '07. PCC'
                              WHEN a.classification = '0'
                              THEN
                                 '09. Consumer Inbound Direct'
                              WHEN     a.classification = '1'
                                   AND a.region = '30'
                                   AND a.country_code = 'CA'
                              THEN
                                 '01. Canada'
                              WHEN a.classification = '1' AND a.region = '30'
                              THEN
                                 '02. Western (excludes Canada)'
                              WHEN a.classification = '1' AND a.region = '10'
                              THEN
                                 '03. Eastern'
                              WHEN a.classification = '4'
                              THEN
                                 '05. Incentive'
                              WHEN a.classification = '5'
                              THEN
                                 '10. Online'
                              WHEN a.classification = '8'
                              THEN
                                 '06. International (excludes Direct)'
                              WHEN a.classification = '2' OR a.classification = '3'
                              THEN
                                 '11. Series and Wholesalers'
                              ELSE
                                 '99. Other'
                           END
                              AS Sales_Type,
                           b.title || ' ' || b.first_name || ' ' || b.last_name
                              AS guest_full_name,
                           b.open_userid,
                           b.open_user_name,
                           b.reassign_userid,
                           b.reassign_user_name,
                           b.source_third,
                           CASE
                              WHEN v.sail_year = dt.sail_year THEN bpa.guests_total
                              ELSE 0
                           END
                              AS cy_guests_curr_ytd,
                           CASE
                              WHEN v.sail_year = (dt.sail_year - 1)
                              THEN
                                 bpa.guests_364
                              ELSE
                                 0
                           END
                              AS cy_guests_prior_stly,
                           CASE
                              WHEN v.sail_year = dt.sail_year THEN bpa.guests_7
                              ELSE 0
                           END
                              AS cy_guests_curr_stlw,
                           CASE
                              WHEN v.sail_year = (dt.sail_year + 1)
                              THEN
                                 bpa.guests_total
                              ELSE
                                 0
                           END
                              AS cy_guests_next_ytd,
                           CASE
                              WHEN v.sail_year = dt.sail_year THEN bpa.guests_364
                              ELSE 0
                           END
                              AS cy_guests_curr_stly,
                           CASE
                              WHEN v.sail_year = (dt.sail_year + 1) THEN bpa.guests_7
                              ELSE 0
                           END
                              AS cy_guests_next_stlw,
                           CASE
                              WHEN v.sail_year = dt.sail_year
                              THEN
                                 bpa.rev_qualified_total
                              ELSE
                                 0
                           END
                              AS cy_rev_qualified_curr_ytd,
                           CASE
                              WHEN v.sail_year = (dt.sail_year - 1)
                              THEN
                                 bpa.rev_qualified_364
                              ELSE
                                 0
                           END
                              AS cy_rev_qualified_prior_stly,
                           CASE
                              WHEN v.sail_year = dt.sail_year
                              THEN
                                 bpa.rev_qualified_7
                              ELSE
                                 0
                           END
                              AS cy_rev_qualified_curr_stlw,
                           CASE
                              WHEN v.sail_year = (dt.sail_year + 1)
                              THEN
                                 bpa.rev_qualified_total
                              ELSE
                                 0
                           END
                              AS cy_rev_qualified_next_ytd,
                           CASE
                              WHEN v.sail_year = dt.sail_year
                              THEN
                                 bpa.rev_qualified_364
                              ELSE
                                 0
                           END
                              AS cy_rev_qualified_curr_stly,
                           CASE
                              WHEN v.sail_year = (dt.sail_year + 1)
                              THEN
                                 bpa.rev_qualified_7
                              ELSE
                                 0
                           END
                              AS cy_rev_qualified_next_stlw,
                           CASE
                              WHEN v.sail_year = (dt.sail_year - 1)
                              THEN
                                 bpa.guests_total
                              ELSE
                                 0
                           END
                              AS cy_guests_prior_ye,
                           CASE
                              WHEN v.sail_year = (dt.sail_year - 1)
                              THEN
                                 bpa.rev_qualified_total
                              ELSE
                                 0
                           END
                              AS cy_rev_qualified_prior_ye,
                           CASE
                              WHEN v.sail_year = (dt.sail_year - 1)
                              THEN
                                 (bpa.guests_total * v.sea_days)
                              ELSE
                                 0
                           END
                              AS cy_pcd_prior_ye,
                           CASE
                              WHEN v.sail_year = dt.sail_year
                              THEN
                                 (bpa.guests_total * v.sea_days)
                              ELSE
                                 0
                           END
                              AS cy_pcd_curr_ytd,
                           CASE
                              WHEN v.sail_year = (dt.sail_year - 1)
                              THEN
                                 (bpa.guests_364 * v.sea_days)
                              ELSE
                                 0
                           END
                              AS cy_pcd_prior_stly,
                           CASE
                              WHEN v.sail_year = (dt.sail_year + 1)
                              THEN
                                 (bpa.guests_total * v.sea_days)
                              ELSE
                                 0
                           END
                              AS cy_pcd_next_ytd,
                           CASE
                              WHEN v.sail_year = dt.sail_year
                              THEN
                                 (bpa.guests_364 * v.sea_days)
                              ELSE
                                 0
                           END
                              AS cy_pcd_curr_stly,
                           CASE
                              WHEN v.rpt_year = dt.rpt_year THEN bpa.guests_total
                              ELSE 0
                           END
                              AS fy_guests_curr_ytd,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year - 1) THEN bpa.guests_364
                              ELSE 0
                           END
                              AS fy_guests_prior_stly,
                           CASE
                              WHEN v.rpt_year = dt.rpt_year THEN bpa.guests_7
                              ELSE 0
                           END
                              AS fy_guests_curr_stlw,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year + 1)
                              THEN
                                 bpa.guests_total
                              ELSE
                                 0
                           END
                              AS fy_guests_next_ytd,
                           CASE
                              WHEN v.rpt_year = dt.rpt_year THEN bpa.guests_364
                              ELSE 0
                           END
                              AS fy_guests_curr_stly,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year + 1) THEN bpa.guests_7
                              ELSE 0
                           END
                              AS fy_guests_next_stlw,
                           CASE
                              WHEN v.rpt_year = dt.rpt_year THEN bpa.ntr_total
                              ELSE 0
                           END
                              AS fy_ntr_fin_curr_ytd,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year - 1) THEN bpa.ntr_364
                              ELSE 0
                           END
                              AS fy_ntr_fin_prior_stly,
                           CASE
                              WHEN v.rpt_year = dt.rpt_year THEN bpa.ntr_7
                              ELSE 0
                           END
                              AS fy_ntr_fin_curr_stlw,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year + 1) THEN bpa.ntr_total
                              ELSE 0
                           END
                              AS fy_ntr_fin_next_ytd,
                           CASE
                              WHEN v.rpt_year = dt.rpt_year THEN bpa.ntr_364
                              ELSE 0
                           END
                              AS fy_ntr_fin_curr_stly,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year + 1) THEN bpa.ntr_7
                              ELSE 0
                           END
                              AS fy_ntr_fin_next_stlw,
                           --Current Year to Date
                           CASE
                              WHEN a.classification = '8' AND b.dir_flag = 'Y'
                              THEN
                                 0
                              WHEN v.rpt_year = dt.rpt_year
                              THEN
                                 bpa.ntr_sls_total - bpa.free_reduced_ntr_sls_usd
                              ELSE
                                 0
                           END
                              AS fy_ntr_sls_curr_ytd,
                           --Current Year Same Time Last Week
                           CASE
                              WHEN a.classification = '8' AND b.dir_flag = 'Y'
                              THEN
                                 0
                              WHEN v.rpt_year = dt.rpt_year
                              THEN
                                 bpa.ntr_sls_7 - bpa.free_reduced_ntr_sls_usd_7
                              ELSE
                                 0
                           END
                              AS fy_ntr_sls_curr_stlw,
                           --Prior Year Same Time Last Year
                           CASE
                              WHEN a.classification = '8' AND b.dir_flag = 'Y'
                              THEN
                                 0
                              WHEN v.rpt_year = (dt.rpt_year - 1)
                              THEN
                                 bpa.ntr_sls_364 - bpa.free_reduced_ntr_sls_usd_364
                              ELSE
                                 0
                           END
                              AS fy_ntr_sls_prior_stly,
                           --Prior Year Same Time Last Year, Last Week
                           CASE
                              WHEN a.classification = '8' AND b.dir_flag = 'Y'
                              THEN
                                 0
                              WHEN v.rpt_year = (dt.rpt_year - 1)
                              THEN
                                 bpa.ntr_sls_371 - bpa.free_reduced_ntr_sls_usd_371
                              ELSE
                                 0
                           END
                              AS fy_ntr_sls_prior_stly_lw,
                           --Next Year to Date
                           CASE
                              WHEN a.classification = '8' AND b.dir_flag = 'Y'
                              THEN
                                 0
                              WHEN v.rpt_year = (dt.rpt_year + 1)
                              THEN
                                 bpa.ntr_sls_total - bpa.free_reduced_ntr_sls_usd
                              ELSE
                                 0
                           END
                              AS fy_ntr_sls_next_ytd,
                           --Next Year Same Time Last Week
                           CASE
                              WHEN a.classification = '8' AND b.dir_flag = 'Y'
                              THEN
                                 0
                              WHEN v.rpt_year = (dt.rpt_year + 1)
                              THEN
                                 bpa.ntr_sls_7 - bpa.free_reduced_ntr_sls_usd_7
                              ELSE
                                 0
                           END
                              AS fy_ntr_sls_next_stlw,
                           --Current Year Same Time Last Year
                           CASE
                              WHEN a.classification = '8' AND b.dir_flag = 'Y'
                              THEN
                                 0
                              WHEN v.rpt_year = dt.rpt_year
                              THEN
                                 bpa.ntr_sls_364 - bpa.free_reduced_ntr_sls_usd_364
                              ELSE
                                 0
                           END
                              AS fy_ntr_sls_curr_stly,
                           --Current Year Same Time Last Year, Last Week
                           CASE
                              WHEN a.classification = '8' AND b.dir_flag = 'Y'
                              THEN
                                 0
                              WHEN v.rpt_year = dt.rpt_year
                              THEN
                                 bpa.ntr_sls_371 - bpa.free_reduced_ntr_sls_usd_371
                              ELSE
                                 0
                           END
                              AS fy_ntr_sls_curr_stly_lw,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year - 1)
                              THEN
                                 bpa.guests_total
                              ELSE
                                 0
                           END
                              AS fy_guests_prior_ye,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year - 1) THEN bpa.ntr_total
                              ELSE 0
                           END
                              AS fy_ntr_fin_prior_ye,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year - 1)
                              THEN
                                 (bpa.guests_total * v.sea_days)
                              ELSE
                                 0
                           END
                              AS fy_pcd_prior_ye,
                           CASE
                              WHEN v.rpt_year = dt.rpt_year
                              THEN
                                 (bpa.guests_total * v.sea_days)
                              ELSE
                                 0
                           END
                              AS fy_pcd_curr_ytd,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year - 1)
                              THEN
                                 (bpa.guests_364 * v.sea_days)
                              ELSE
                                 0
                           END
                              AS fy_pcd_prior_stly,
                           CASE
                              WHEN v.rpt_year = (dt.rpt_year + 1)
                              THEN
                                 (bpa.guests_total * v.sea_days)
                              ELSE
                                 0
                           END
                              AS fy_pcd_next_ytd,
                           CASE
                              WHEN v.rpt_year = dt.rpt_year
                              THEN
                                 (bpa.guests_364 * v.sea_days)
                              ELSE
                                 0
                           END
                              AS fy_pcd_curr_stly,
                           case when b.pax_status = 'A' then t.guest_count else 0 end as guest_count ,
                           case when b.pax_status = 'A' then t.guest_count_qualified else 0 end as guest_count_qualified,
                           case when b.pax_status = 'A' then t.ct_rev_usd_blended else 0 end as ct_rev_usd_blended,
                           case when b.pax_status = 'A' then t.ct_rev_usd_local else 0 end as ct_rev_usd_local,
                           case when b.pax_status = 'A' then t.ct_rev_native else 0 end as ct_rev_native,
                           case when b.pax_status = 'A' then t.ct_net_rev_usd_blended else 0 end as ct_net_rev_usd_blended,
                           case when b.pax_status = 'A' then t.ct_net_rev_usd_local else 0 end as ct_net_rev_usd_local,
                           case when b.pax_status = 'A' then t.ct_net_rev_native else 0 end as ct_net_rev_native,
                           case when b.pax_status = 'A' then t.cta_rev_usd_blended else 0 end as cta_rev_usd_blended,
                           case when b.pax_status = 'A' then t.cta_rev_usd_local else 0 end as cta_rev_usd_local,
                           case when b.pax_status = 'A' then t.cta_rev_native else 0 end as cta_rev_native,
                           case when b.pax_status = 'A' then t.cta_rev_usd_qualified else 0 end as cta_rev_usd_qualified,
                           case when b.pax_status = 'A' then t.cta_net_rev_usd_blended else 0 end as cta_net_rev_usd_blended,
                           case when b.pax_status = 'A' then t.cta_net_rev_usd_local else 0 end as cta_net_rev_usd_local,
                           case when b.pax_status = 'A' then t.cta_net_rev_native else 0 end as cta_net_rev_native,
                           case when b.pax_status = 'A' then t.ntr_sls else 0 end as ntr_sls,
                           case when b.pax_status = 'A' then t.ntr_sls_native else 0 end as ntr_sls_native,
                           case when b.pax_status = 'A' then t.npr_usd_blended else 0 end as npr_usd_blended,
                           case when b.pax_status = 'A' then t.ntr_fin else 0 end as ntr_fin,
                           case when b.pax_status = 'A' then t.ntr_fin_native else 0 end as ntr_fin_native,
                           case when b.pax_status = 'A' then t.gross_rev_usd_blended else 0 end as gross_rev_usd_blended,
                           case when b.pax_status = 'A' then t.package_rev_usd_blended else 0 end as package_rev_usd_blended,
                           case when b.pax_status = 'A' then t.nda_amount_usd_blended else 0 end as nda_amount_usd_blended,
                           b.waitlist_remarks,
                           b.currency_code,
                           a.adhoc_pricing,
                           b.bkng_cancel_date,
                           b.pax_cancel_date,
                           a.agency_contact,
                           a.fax,
                           a.email,
                           a.finance_type,
                           a.pymt_method_code,
                           v.season,
                           v.trade,
                           v.subtrade,
                           v.from_port,
                           v.to_port,
                           v.return_date,
                           v.final_paymt_days,
                           b.left_ear,
                           b.right_ear,
                           b.xld_userid,
                           b.bkng_cancel_code,
                           b.bkng_confirm_date,
                           b.bkng_contact,
                           b.category,
                           b.deposit_date,
                           b.dir_flag,
                           b.primary_agent,
                           b.apo_meta,
                           b.meta,
                           b.bkng_origin,
                           b.bkng_sub_origin,
                           b.activate_code,
                           b.agency_type,
                           b.balance_due_date,
                           b.call_back_code,
                           b.call_back_date,
                           b.dining_code,
                           b.dining_time_conf_flag,
                           b.dining_room_code,
                           b.dining_time_code,
                           b.dining_time_waitlist_code,
                           b.accrued_sea_days,
                           b.option_date,
                           CASE
                                WHEN (b.open_userid LIKE 'PA%' OR a.country_code = 'AU') THEN 'AUS'
                                WHEN (b.open_userid LIKE 'HAUKA%' OR a.country_code IN ('IE', 'UK', 'GB')) THEN 'UK'
                                ELSE 'OTHER'
                           END as office,
                           CASE
                                WHEN a.super_assoc in ('00','04','75','98') THEN 'AAA'
                                WHEN a.super_assoc in ('01','03','06') THEN 'CAA'
                                ELSE 'OTHER'
                           END as aaa_indicator,
                           g.group_id,
                           g.group_status,
                           g.group_open_date,
                           g.group_open_userid,
                           g.auto_refund_flag,
                           g.group_cancel_date,
                           g.group_currency_code,
                           g.group_name,
                           g.group_right_ear,
                           g.group_left_ear,
                           CASE
                                WHEN v.sail_year = (dt.sail_year - 1)
                                THEN  bpa.ct_net_rev_usd_local
                                ELSE 0
                           END AS cy_ct_net_rev_prior_ye,
                           CASE
                                WHEN v.sail_year = (dt.sail_year - 1)
                                THEN bpa.ct_net_rev_usd_local_364
                                ELSE  0
                           END AS cy_ct_net_rev_prior_stly,
                           CASE
                                 WHEN v.sail_year = dt.sail_year
                                 THEN bpa.ct_net_rev_usd_local
                                 ELSE  0
                           END AS cy_ct_net_rev_curr_ytd,
                           CASE
                                 WHEN v.sail_year = dt.sail_year
                                 THEN bpa.ct_net_rev_usd_local_364
                                 ELSE  0
                           END AS cy_ct_net_rev_curr_stly,
                           CASE
                                WHEN v.sail_year = dt.sail_year
                                THEN bpa.ct_net_rev_usd_local_7
                                ELSE 0
                           END AS cy_ct_net_rev_curr_stlw,
                           CASE
                                WHEN v.sail_year = (dt.sail_year + 1)
                                THEN  bpa.ct_net_rev_usd_local
                                ELSE   0
                           END AS cy_ct_net_rev_next_ytd,
                           CASE
                                WHEN v.sail_year = (dt.sail_year + 1)
                                THEN  bpa.ct_net_rev_usd_local_7
                                ELSE   0
                           END AS cy_ct_net_rev_next_stlw,
                           to_char(v.sail_date,'Q') as sail_qtr,
                           da.natl_acct_flg,
                           da.director,
--CT_NET_REV_USD_BLENDED
                            CASE
                                WHEN v.sail_year = (dt.sail_year - 1)
                                THEN  bpa.ct_net_rev_usd_blended
                                ELSE 0
                           END AS cy_ct_net_rev_bld_prior_ye,
                           CASE
                                WHEN v.sail_year = (dt.sail_year - 1)
                                THEN bpa.ct_net_rev_usd_blended_364
                                ELSE  0
                           END AS cy_ct_net_rev_bld_prior_stly,
                           CASE
                                 WHEN v.sail_year = dt.sail_year
                                 THEN bpa.ct_net_rev_usd_blended
                                 ELSE  0
                           END AS cy_ct_net_rev_bld_curr_ytd,
                           CASE
                                 WHEN v.sail_year = dt.sail_year
                                 THEN bpa.ct_net_rev_usd_blended_364
                                 ELSE  0
                           END AS cy_ct_net_rev_bld_curr_stly,
                           CASE
                                WHEN v.sail_year = dt.sail_year
                                THEN bpa.ct_net_rev_usd_blended_7
                                ELSE 0
                           END AS cy_ct_net_rev_bld_curr_stlw,
                           CASE
                                WHEN v.sail_year = (dt.sail_year + 1)
                                THEN  bpa.ct_net_rev_usd_blended
                                ELSE   0
                           END AS cy_ct_net_rev_bld_next_ytd,
                           CASE
                                WHEN v.sail_year = (dt.sail_year + 1)
                                THEN  bpa.ct_net_rev_usd_blended_7
                                ELSE   0
                           END AS cy_ct_net_rev_bld_next_stlw,
--CT_REV_USD_BLENDED                       
                           CASE
                                WHEN v.sail_year = (dt.sail_year - 1)
                                THEN  bpa.ct_rev_usd_blended
                                ELSE 0
                           END AS cy_ct_rev_bld_prior_ye,
                           CASE
                                WHEN v.sail_year = (dt.sail_year - 1)
                                THEN bpa.ct_rev_usd_blended_364
                                ELSE  0
                           END AS cy_ct_rev_bld_prior_stly,
                           CASE
                                 WHEN v.sail_year = dt.sail_year
                                 THEN bpa.ct_rev_usd_blended
                                 ELSE  0
                           END AS cy_ct_rev_bld_curr_ytd,
                           CASE
                                 WHEN v.sail_year = dt.sail_year
                                 THEN bpa.ct_rev_usd_blended_364
                                 ELSE  0
                           END AS cy_ct_rev_bld_curr_stly,
                           CASE
                                WHEN v.sail_year = dt.sail_year
                                THEN bpa.ct_rev_usd_blended_7
                                ELSE 0
                           END AS cy_ct_rev_bld_curr_stlw,
                           CASE
                                WHEN v.sail_year = (dt.sail_year + 1)
                                THEN  bpa.ct_rev_usd_blended
                                ELSE   0
                           END AS cy_ct_rev_bld_next_ytd,
                           CASE
                                WHEN v.sail_year = (dt.sail_year + 1)
                                THEN  bpa.ct_rev_usd_blended_7
                                ELSE   0
                           END AS cy_ct_rev_bld_next_stlw                           
                      FROM rdm_trans_f t,
                           rdm_voyage_d v,
                           rdm_booking_d b,
                           rdm_agent_d a,
                           rdm_bpa_f bpa,
                           rdm_book_d bk,
                           rdm_group_d g,
                           sls_nvs_xref_voy_bkng t2,
                           rdm_date_d dt,
                           sls_director_agency da
                     WHERE   t.bkng_id = b.bkng_id
                           AND t.rpt_year = v.rpt_year
                           AND t.agency_id = a.agency_id
                           AND t.voyage = v.voyage
                           AND b.bkng_id = bpa.bkng_id
                           AND b.bkng_nbr = bk.bkng_nbr
                           AND b.bkng_nbr = t2.polar_bkng_nbr(+)
                           AND b.company_code = v.company_code
                           AND b.company_code = a.company_code
                           AND b.company_code = bpa.company_code
                           and b.company_code = g.company_code(+)
                           and b.group_id = g.group_id(+)
                           and a.agency_id = da.agency_id
                           and (v.rpt_year = (dt.rpt_year - 1)
                                OR v.rpt_year = dt.rpt_year
                                OR v.rpt_year = (dt.rpt_year + 1)
                                OR v.rpt_year = (dt.rpt_year + 2))
                                )
           SELECT t3.agency_id,
                  t3.agency_name,
                  t3.agency_nbr,
                  t3.bdm,
                  t3.bdr,
                  t3.super_assoc,
                  t3.super_assoc_name,
                  t3.association_name,
                  t3.assoc_id,
                  t3.arc_nbr,
                  t3.agency_status,
                  t3.classification,
                  t3.type_sales_program,
                  t3.agent_region_district,
                  t3.agency_create_date,
                  t3.address1,
                  t3.city,
                  t3.state,
                  t3.zip,
                  t3.country_code,
                  t3.country_name,
                  t3.telephone,
                  t3.rpt_qtr,
                  t3.rpt_year,
                  t3.sail_year,
                  t3.season_description,
                  t3.trade_description,
                  t3.subtrade_description,
                  t3.region_description,
                  t3.region,
                  t3.ship_code,
                  t3.ship_name,
                  t3.sail_date,
                  t3.segment, 
                  t3.sea_days,
                  t3.voyage_status,
                  t3.nationality,
                  t3.bkng_id,
                  t3.voyage,
                  t3.bkng_nbr,
                  t3.pax_id,
                  t3.title,
                  t3.first_name,
                  t3.last_name,
                  t3.suffix,
                  t3.suffix_last_name,
                  t3.bkng_open_date,
                  t3.bkng_status,
                  t3.pax_status,
                  t3.bkng_nbr_nvs,
                  t3.promo_sob,
                  t3.comm_withheld,
                  t3.tours_indicator,
                  t3.tour_id,
                  t3.group_code,
                  t3.group_type,
                  t3.bkng_source,
                  t3.bkng_type,
                  t3.channel_type,
                  t3.bkng_channel,
                  t3.source_second,
                  t3.home_country,
                  t3.domestic_intl_bkng_flag,
                  t3.product_type,
                  t3.company_code_1,
                  t3.company_code_3,
                  t3.company_name,
                  t3.Sales_type,
                  t3.guest_full_name,
                  t3.open_userid,
                  t3.open_user_name,
                  t3.reassign_userid,
                  t3.reassign_user_name,
                  t3.source_third,
                  t3.cy_guests_curr_ytd,
                  t3.cy_guests_prior_stly,
                  t3.cy_guests_curr_ytd - t3.cy_guests_prior_stly
                     AS cy_guests_curr_gain,
                  t3.cy_guests_curr_stlw,
                  t3.cy_guests_curr_ytd - t3.cy_guests_curr_stlw
                     AS cy_guests_curr_gain_wk,
                  t3.cy_guests_next_ytd,
                  t3.cy_guests_curr_stly,
                  t3.cy_guests_next_ytd - t3.cy_guests_curr_stly
                     AS cy_guests_next_gain,
                  t3.cy_guests_next_stlw,
                  t3.cy_guests_next_ytd - t3.cy_guests_next_stlw
                     AS cy_guests_next_gain_wk,
                  t3.cy_rev_qualified_curr_ytd,
                  t3.cy_rev_qualified_prior_stly,
                  t3.cy_rev_qualified_curr_ytd - t3.cy_rev_qualified_prior_stly
                     AS cy_rev_qualified_curr_gain,
                  t3.cy_rev_qualified_curr_stlw,
                  t3.cy_rev_qualified_curr_ytd - t3.cy_rev_qualified_curr_stlw
                     AS cy_rev_qualified_curr_gain_wk,
                  t3.cy_rev_qualified_next_ytd,
                  t3.cy_rev_qualified_curr_stly,
                  t3.cy_rev_qualified_next_ytd - t3.cy_rev_qualified_curr_stly
                     AS cy_rev_qualified_next_gain,
                  t3.cy_rev_qualified_next_stlw,
                  t3.cy_rev_qualified_next_ytd - t3.cy_rev_qualified_next_stlw
                     AS cy_rev_qualified_next_gain_wk,
                  t3.cy_guests_prior_ye,
                  t3.cy_rev_qualified_prior_ye,
                  t3.cy_pcd_prior_ye,
                  t3.cy_pcd_curr_ytd,
                  t3.cy_pcd_prior_stly,
                  t3.cy_pcd_next_ytd,
                  t3.cy_pcd_curr_stly,
                  t3.fy_guests_curr_ytd,
                  t3.fy_guests_prior_stly,
                  t3.fy_guests_curr_ytd - t3.fy_guests_prior_stly
                     AS fy_guests_curr_gain,
                  t3.fy_guests_curr_stlw,
                  t3.fy_guests_curr_ytd - t3.fy_guests_curr_stlw
                     AS fy_guests_curr_gain_wk,
                  t3.fy_guests_next_ytd,
                  t3.fy_guests_curr_stly,
                  t3.fy_guests_next_ytd - t3.fy_guests_curr_stly
                     AS fy_guests_next_gain,
                  t3.fy_guests_next_stlw,
                  t3.fy_guests_next_ytd - t3.fy_guests_next_stlw
                     AS fy_guests_next_gain_wk,
                  t3.fy_ntr_fin_curr_ytd,
                  t3.fy_ntr_fin_prior_stly,
                  t3.fy_ntr_fin_curr_ytd - t3.fy_ntr_fin_prior_stly
                     AS fy_ntr_fin_curr_gain,
                  t3.fy_ntr_fin_curr_stlw,
                  t3.fy_ntr_fin_curr_ytd - t3.fy_ntr_fin_curr_stlw
                     AS fy_ntr_fin_curr_gain_wk,
                  t3.fy_ntr_fin_next_ytd,
                  t3.fy_ntr_fin_curr_stly,
                  t3.fy_ntr_fin_next_ytd - t3.fy_ntr_fin_curr_stly
                     AS fy_ntr_fin_next_gain,
                  t3.fy_ntr_fin_next_stlw,
                  t3.fy_ntr_fin_next_ytd - t3.fy_ntr_fin_next_stlw
                     AS fy_ntr_fin_next_gain_wk,
                  t3.fy_ntr_sls_curr_ytd AS fy_ntr_sls_curr_ytd,
                  t3.fy_ntr_sls_curr_ytd - t3.fy_ntr_sls_curr_stlw
                     AS fy_ntr_sls_curr_gain_wk,
                  t3.fy_ntr_sls_prior_stly AS fy_ntr_sls_prior_stly,
                  t3.fy_ntr_sls_prior_stly - t3.fy_ntr_sls_prior_stly_lw
                     AS fy_ntr_sls_prior_stly_gain_wk,
                  t3.fy_ntr_sls_curr_ytd - t3.fy_ntr_sls_prior_stly
                     AS fy_ntr_sls_priorcurr_gain,
                    (t3.fy_ntr_sls_curr_ytd - t3.fy_ntr_sls_curr_stlw)
                  - (t3.fy_ntr_sls_prior_stly - t3.fy_ntr_sls_prior_stly_lw)
                     AS fy_ntr_sls_priorcurr_gain_wk,
                  t3.fy_ntr_sls_next_ytd AS fy_ntr_sls_next_ytd,
                  t3.fy_ntr_sls_next_ytd - t3.fy_ntr_sls_next_stlw
                     AS fy_ntr_sls_next_gain_wk,
                  t3.fy_ntr_sls_curr_stly AS fy_ntr_sls_curr_stly,
                  t3.fy_ntr_sls_curr_stly - t3.fy_ntr_sls_curr_stly_lw
                     AS fy_ntr_sls_curr_stly_gain_wk,
                  t3.fy_ntr_sls_next_ytd - t3.fy_ntr_sls_curr_stly
                     AS fy_ntr_sls_currnext_gain,
                    (t3.fy_ntr_sls_next_ytd - t3.fy_ntr_sls_next_stlw)
                  - (t3.fy_ntr_sls_curr_stly - t3.fy_ntr_sls_curr_stly_lw)
                     AS fy_ntr_sls_currnext_gain_wk,
                  t3.fy_guests_prior_ye,
                  t3.fy_ntr_fin_prior_ye,
                  t3.fy_pcd_prior_ye,
                  t3.fy_pcd_curr_ytd,
                  t3.fy_pcd_prior_stly,
                  t3.fy_pcd_next_ytd,
                  t3.fy_pcd_curr_stly,
                  t3.guest_count,
                  t3.guest_count_qualified,
                  t3.ct_rev_usd_blended,
                  t3.ct_rev_usd_local,
                  t3.ct_rev_native,
                  t3.ct_net_rev_usd_blended,
                  t3.ct_net_rev_usd_local,
                  t3.ct_net_rev_native,
                  t3.cta_rev_usd_blended,
                  t3.cta_rev_usd_local,
                  t3.cta_rev_native,
                  t3.cta_rev_usd_qualified,
                  t3.cta_net_rev_usd_blended,
                  t3.cta_net_rev_usd_local,
                  t3.cta_net_rev_native,
                  t3.ntr_sls,
                  t3.ntr_sls_native,
                  t3.npr_usd_blended,
                  t3.ntr_fin,
                  t3.ntr_fin_native,
                  t3.gross_rev_usd_blended,
                  t3.package_rev_usd_blended,
                  t3.nda_amount_usd_blended,
                  t3.waitlist_remarks,
                  t3.currency_code,
                  t3.adhoc_pricing,
                  t3.bkng_cancel_date,
                  t3.pax_cancel_date,
                  t3.agency_contact,
                  t3.fax,
                  t3.email,
                  t3.finance_type,
                  t3.pymt_method_code,
                  t3.season,
                  t3.trade,
                  t3.subtrade,
                  t3.from_port,
                  t3.to_port,
                  t3.return_date,
                  t3.final_paymt_days,
                  t3.left_ear,
                  t3.right_ear,
                  t3.xld_userid,
                  t3.bkng_cancel_code,
                  t3.bkng_confirm_date,
                  t3.bkng_contact,
                  t3.category,
                  t3.deposit_date,
                  t3.dir_flag,
                  t3.primary_agent,
                  t3.apo_meta,
                  t3.meta,
                  t3.bkng_origin,
                  t3.bkng_sub_origin,
                  t3.activate_code,
                  t3.agency_type,
                  t3.balance_due_date,
                  t3.call_back_code,
                  t3.call_back_date,
                  t3.dining_code,
                  t3.dining_time_conf_flag,
                  t3.dining_room_code,
                  t3.dining_time_code,
                  t3.dining_time_waitlist_code,
                  t3.accrued_sea_days,
                  t3.option_date,
                  t3.office,
                  t3.aaa_indicator,
                  t3.group_id,
                  t3.group_status,
                  t3.group_open_date,
                  t3.group_open_userid,
                  t3.auto_refund_flag,
                  t3.group_cancel_date,
                  t3.group_currency_code,
                  t3.group_name,
                  t3.group_right_ear,
                  t3.group_left_ear,
                  t3.cy_ct_net_rev_prior_ye,
                  t3.cy_ct_net_rev_prior_stly,
                  t3.cy_ct_net_rev_curr_ytd,
                  t3.cy_ct_net_rev_curr_stly,
                  t3.cy_ct_net_rev_curr_stlw,
                  t3.cy_ct_net_rev_next_ytd,
                  t3.cy_ct_net_rev_next_stlw,
                  t3.cy_ct_net_rev_curr_ytd - t3.cy_ct_net_rev_prior_stly AS cy_ct_net_rev_curr_gain,
                  t3.cy_ct_net_rev_curr_ytd - t3.cy_ct_net_rev_curr_stlw AS cy_ct_net_rev_curr_gain_wk,
                  t3.cy_ct_net_rev_next_ytd - t3.cy_ct_net_rev_curr_stly AS cy_ct_net_rev_next_gain,
                  t3.cy_ct_net_rev_next_ytd - t3.cy_ct_net_rev_next_stlw AS cy_ct_net_rev_next_gain_wk,
                  t3.sail_qtr,
                  t3.natl_acct_flg,
                  t3.director as natl_acct_director,
                  t3.cy_ct_net_rev_bld_prior_ye,
                  t3.cy_ct_net_rev_bld_prior_stly,
                  t3.cy_ct_net_rev_bld_curr_ytd,
                  t3.cy_ct_net_rev_bld_curr_stly,
                  t3.cy_ct_net_rev_bld_curr_stlw,
                  t3.cy_ct_net_rev_bld_next_ytd,
                  t3.cy_ct_net_rev_bld_next_stlw,
                  t3.cy_ct_net_rev_bld_curr_ytd - t3.cy_ct_net_rev_bld_prior_stly AS cy_ct_net_rev_bld_curr_gain,
                  t3.cy_ct_net_rev_bld_curr_ytd - t3.cy_ct_net_rev_bld_curr_stlw AS cy_ct_net_rev_bld_curr_gain_wk,
                  t3.cy_ct_net_rev_bld_next_ytd - t3.cy_ct_net_rev_bld_curr_stly AS cy_ct_net_rev_bld_next_gain,
                  t3.cy_ct_net_rev_bld_next_ytd - t3.cy_ct_net_rev_bld_next_stlw AS cy_ct_net_rev_bld_next_gain_wk,
                  t3.cy_ct_rev_bld_prior_ye,
                  t3.cy_ct_rev_bld_prior_stly,
                  t3.cy_ct_rev_bld_curr_ytd,
                  t3.cy_ct_rev_bld_curr_stly,
                  t3.cy_ct_rev_bld_curr_stlw,
                  t3.cy_ct_rev_bld_next_ytd,
                  t3.cy_ct_rev_bld_next_stlw,
                  t3.cy_ct_rev_bld_curr_ytd - t3.cy_ct_rev_bld_prior_stly AS cy_ct_rev_bld_curr_gain,
                  t3.cy_ct_rev_bld_curr_ytd - t3.cy_ct_rev_bld_curr_stlw AS cy_ct_rev_bld_curr_gain_wk,
                  t3.cy_ct_rev_bld_next_ytd - t3.cy_ct_rev_bld_curr_stly AS cy_ct_rev_bld_next_gain,
                  t3.cy_ct_rev_bld_next_ytd - t3.cy_ct_rev_bld_next_stlw AS cy_ct_rev_bld_next_gain_wk
             FROM t3;
                     
        x:=SQL%ROWCOUNT;        
        sComment  := 'sls_bkng_activity Inserts: '||x;

        EXECUTE IMMEDIATE 'ALTER TABLE sls_bkng_activity LOGGING';
        
        BEGIN
          SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName        => 'DM_TKR'
         ,TabName        => 'SLS_BKNG_ACTIVITY'
         ,Degree            => 4
         ,Cascade           => TRUE
         );
        END;           
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;           
  END SLS_BKNG_ACT_LOAD;
  
    PROCEDURE TRN_OPS_MANIFEST_LOAD IS   
          cProcedure_NM  CONSTANT  Varchar2(30) := 'TRN_OPS_MANIFEST_LOAD';
          BatchID Number := 0;
           
    BEGIN
           /* Log program start */ 
          IF  DM_INT.COMMON_JOBS.LOG_JOB (
                      io_Batch_ID => BatchID,    
                      in_Schema_Name => cSchema_NM,  
                      in_Package_Name => cPackage_NM,
                      in_Program_Name => cProcedure_NM, 
                      in_Prog_Status => 'I',  
                      in_Comments => 'Job in Progress', 
                      in_Error_Message => 'NA'
                      ) != 0 THEN                  
              RAISE eCALL_FAILED;      
          END IF;
          
          EXECUTE IMMEDIATE 'ALTER TABLE trn_ops_manifest NOLOGGING';
          EXECUTE IMMEDIATE 'TRUNCATE TABLE trn_ops_manifest';          
  
          INSERT /*+ append */ INTO trn_ops_manifest

     WITH
----------------------------------------------------------------------------------------------------------
-- Get all voyages departing for current year and forward
----------------------------------------------------------------------------------------------------------
     qvoy AS (
   SELECT vm.company_code,
          vm.vyd_ship_code,
          vm.vyd_voyage,
          TRUNC(vm.vyd_sail_date)         AS vyd_sail_date,
          TRUNC(vm.vyd_return_date)       AS vyd_return_date,
          vm.vyd_trade,
          vm.vyd_basic_from_port,
          vm.vyd_basic_to_port,
          sn.ship_name
     FROM voyage_mstr@tstdwh1_halw_dwh vm,
          ship_nme_mstr@tstdwh1_halw_dwh sn
    WHERE vm.vyd_ship_code NOT IN('LP','ZZ')
      AND vm.rpt_year >= to_char(SYSDATE,'YYYY')
      AND vm.vyd_voyage_status IN ('A','H')
      AND vm.vyd_ship_code = sn.ship_key_code      )
 
----------------------------------------------------------------------------------------------------------
-- Get booking data and special service requirements
----------------------------------------------------------------------------------------------------------
    ,qbkg AS (
   SELECT DISTINCT
          qvoy.company_code,
          qvoy.vyd_voyage, 
          qvoy.vyd_ship_code,
          qvoy.vyd_sail_date, 
          qvoy.vyd_return_date, 
          qvoy.vyd_trade,
          bm.b_bkng_nbr                       AS bkng_nbr,
          bp.b_p_passenger_id                 AS pax_id,
          bp.b_bkng_nbr||bp.b_p_passenger_id  AS bkng_id,
          bp.b_category,
          NVL (bp.b_p_apo, bp.b_category)     AS apo_cd,
          bp.b_cabin,
          bm.b_voyage||bm.b_group_code        AS group_id,
          bm.b_booking_agent,
          c.item_nbr, 
          c.item_desc,
          c.item_remarks_1, 
          c.item_remarks_2, 
          bp.b_p_surname,
          bp.b_p_forename,
          bp.b_p_title,
          bm.b_group_code,
          bm.b_contact,
          NVL(bm.b_travel_with_id,' ')        AS b_travel_with_id,
          bm.b_status,
          bm.b_link_id,
          bm.b_link_sequence_flag,
          bm.b_dir_flag,
          bm.b_tour_id,
          TRUNC(bm.b_tour_start_date)         AS b_tour_start_date,
          TRUNC(bm.b_tour_end_date)           AS b_tour_end_date,
          bp.b_p_loyalty_pgm_code,
          bp.b_p_sex,
          bp.b_p_air_home_city,
          bp.b_p_air_flag,
          bp.b_p_air_city,
          TRUNC(pif.passport_expire_date)     AS passport_expire_date,
          NVL(pif.passport,' ')               AS passport,
          pif.pp_issued_at,
          pif.alien_resident_id,
          TRUNC(pif.birthdate)                AS birthdate,
          pif.cell_phone,
          pif.email_address,
          pif.home_address1,
          pif.home_address2,
          pif.home_city,
          pif.home_state,
          pif.home_zip,
          pif.home_phone,
          pif.home_country,
          pif.nationality,
          gm.grm_group_name,
          am.agy1_telephone,
          am.agy1_name,
          am.agy1_contact,
          oc.agy2_email_address,
          bd.b_dir_home_phone,
          bd.b_dir_email_address,
    (CASE WHEN bm.b_link_id IS NULL THEN NULL ELSE
          (CASE WHEN EXISTS (SELECT bm2.b_link_id 
                               FROM bkng_mstr@tstdwh1_halw_dwh bm2 
                              WHERE bm2.company_code =bm.company_code
                                AND bm2.ship_key_code=bm.ship_key_code 
                                AND bm2.b_link_id=bm.b_link_id
                                AND bm2.b_sail_date < bm.b_sail_date)
                THEN 'Yes' ELSE NULL 
          END)  
  END) AS intransit_voy_flag,
     CASE WHEN qvoy.vyd_ship_code IN('SR','SS','SG') AND NVL (bp.b_p_apo, bp.b_category) IN('OW','CS') and bp.b_cabin IN('1','2','3','4','5','6') THEN 'YES'           
          WHEN qvoy.vyd_ship_code IN('SR','SS','SG') AND NVL (bp.b_p_apo, bp.b_category) IS NULL AND bp.b_category IN('OW','CS') AND bp.b_cabin in('1','2','3','4','5','6') THEN 'YES'  
          WHEN qvoy.vyd_ship_code IN('SO','SJ','SQ') AND NVL (bp.b_p_apo, bp.b_category) IN('OW','SS','WG','GR') AND bp.b_cabin IN('600','601','700','701','731','743','744','800','801') THEN 'YES'
          WHEN qvoy.vyd_ship_code IN('SO','SJ','SQ') AND NVL (bp.b_p_apo, bp.b_category) IS NULL AND bp.b_category IN('OW','SS','WG') AND bp.b_cabin IN('600','601','700','701','731','743','744','800','801') THEN 'YES'
          WHEN qvoy.vyd_ship_code IN('SO','SJ','SQ') AND NVL (bp.b_p_apo, bp.b_category) IS NULL AND bp.b_category = 'GR' AND bp.b_cabin IN('600','601','700','701','731','743','744','800','801','7002','7013','7435','7446')  THEN 'YES' 
          ELSE 'NO'  END sbn_amenity_cabin,
          cc.cct_country_name
     FROM qvoy,
          bkng_mstr@tstdwh1_halw_dwh bm,
          bkng_psngr@tstdwh1_halw_dwh bp,
          bkng_psngr_pif@tstdwh1_halw_dwh pif,
          rdm_bkng_service_items_d c,
          group_mstr@tstdwh1_halw_dwh gm,
          agent_mstr@tstdwh1_halw_dwh am,
          agent_oc@tstdwh1_halw_dwh  oc,
          bkng_direct@tstdwh1_halw_dwh bd,
          country_code@tstdwh1_halw_dwh cc
    WHERE bm.company_code = qvoy.company_code
      AND bm.b_voyage = qvoy.vyd_voyage  
      AND bm.b_bkng_nbr = bp.b_bkng_nbr(+)
      AND bm.b_voyage = bp.b_voyage(+)
      AND bp.b_bkng_nbr||bp.b_p_passenger_id = c.bkng_id(+)
      AND bp.b_bkng_nbr = pif.booking_nbr(+)
      AND bp.b_p_passenger_id = pif.passenger_id(+)
      AND bm.company_code = gm.company_code(+)
      AND bm.b_voyage = gm.grm_voyage(+)
      AND bm.b_group_code = gm.grm_group_code(+)
      AND bm.company_code = am.company_code(+) 
      AND bm.b_booking_agent = am.agy1_agency_id(+)
      AND am.company_code = oc.company_code(+) 
      AND am.agy1_agency_id = oc.agy2_agency_id(+)
      AND am.agy1_agency_base_code = oc.agy2_agency_base_code(+)
      AND bm.b_bkng_nbr = bd.b_bkng_nbr(+)
      AND bm.b_voyage = bd.b_voyage(+)
      AND bm.b_status IN ('B','G')
      AND (NVL(bm.b_tours_indicator,' ') <> 'L' OR bm.b_tours_indicator IS NULL)
      AND bp.b_p_passenger_status = 'A'
      AND bp.b_p_passenger_id <>'999' 
      AND pif.nationality = cc.cct_country_code(+)  
      AND c.item_nbr(+) IN ('2501','2502','2602','2612','2613','2618','8610','8611','8612','8613','8614','8623') )
   
----------------------------------------------------------------------------------------------------------
-- Select item numbers that pertain to Wheelchair and/or Oxygen requirements
----------------------------------------------------------------------------------------------------------
    ,qwch AS (
   SELECT qbkg.bkng_nbr, qbkg.pax_id,
          (CASE WHEN qbkg.item_nbr = '2501' THEN qbkg.item_desc ELSE NULL  END) AS ssi_2501,
          (CASE WHEN qbkg.item_nbr = '2502' THEN qbkg.item_desc ELSE NULL  END) AS ssi_2502,
          (CASE WHEN qbkg.item_nbr = '2602' THEN qbkg.item_desc ELSE NULL  END) AS ssi_2602,
          (CASE WHEN qbkg.item_nbr = '2612' THEN qbkg.item_desc ELSE NULL  END) AS ssi_2612,
          (CASE WHEN qbkg.item_nbr = '2613' THEN qbkg.item_desc ELSE NULL  END) AS ssi_2613,
          (CASE WHEN qbkg.item_nbr = '2618' THEN qbkg.item_desc ELSE NULL  END) AS ssi_2618,
          (CASE WHEN qbkg.item_nbr = '8623' THEN qbkg.item_desc ELSE NULL  END) AS ssi_8623
     FROM qbkg)
    
   ,qwch2 AS (
   SELECT qwch.bkng_nbr, qwch.pax_id,
          MAX(qwch.ssi_2501) ssi_2501,    MAX(qwch.ssi_2502) ssi_2502,
          MAX(qwch.ssi_2602) ssi_2602,    MAX(qwch.ssi_2612) ssi_2612,
          MAX(qwch.ssi_2613) ssi_2613,    MAX(qwch.ssi_2618) ssi_2618,
          MAX(qwch.ssi_8623) ssi_8623
     FROM qwch
 GROUP BY qwch.bkng_nbr, qwch.pax_id)

 ,qwchoxy AS (
   SELECT qwch2.bkng_nbr, qwch2.pax_id,
          TRIM( TRIM(NVL(qwch2.ssi_2501,' '))||' '|| TRIM(NVL(qwch2.ssi_2502,' '))||' '||
                TRIM(NVL(qwch2.ssi_2602,' '))||' '|| TRIM(NVL(qwch2.ssi_2612,' '))||' '||
                TRIM(NVL(qwch2.ssi_2613,' '))||' '|| TRIM(NVL(qwch2.ssi_2618,' '))    ) AS wheelchair_o2
     FROM qwch2)
----------------------------------------------------------------------------------------------------------
-- Get all Remarks for selected item numbers
----------------------------------------------------------------------------------------------------------
    ,qrmk AS (
   SELECT qbkg.bkng_nbr, qbkg.pax_id,
          (CASE WHEN qbkg.item_nbr = '2501' THEN qbkg.item_remarks_1 ELSE NULL  END) AS ssg1_2501,
          (CASE WHEN qbkg.item_nbr = '2501' THEN qbkg.item_remarks_2 ELSE NULL  END) AS ssg2_2501,
          (CASE WHEN qbkg.item_nbr = '2502' THEN qbkg.item_remarks_1 ELSE NULL  END) AS ssg1_2502,
          (CASE WHEN qbkg.item_nbr = '2502' THEN qbkg.item_remarks_2 ELSE NULL  END) AS ssg2_2502,
          (CASE WHEN qbkg.item_nbr = '2602' THEN qbkg.item_remarks_1 ELSE NULL  END) AS ssg1_2602,
          (CASE WHEN qbkg.item_nbr = '2602' THEN qbkg.item_remarks_2 ELSE NULL  END) AS ssg2_2602,
          (CASE WHEN qbkg.item_nbr = '2612' THEN qbkg.item_remarks_1 ELSE NULL  END) AS ssg1_2612,
          (CASE WHEN qbkg.item_nbr = '2612' THEN qbkg.item_remarks_2 ELSE NULL  END) AS ssg2_2612,
          (CASE WHEN qbkg.item_nbr = '2613' THEN qbkg.item_remarks_1 ELSE NULL  END) AS ssg1_2613,
          (CASE WHEN qbkg.item_nbr = '2613' THEN qbkg.item_remarks_2 ELSE NULL  END) AS ssg2_2613,
          (CASE WHEN qbkg.item_nbr = '2618' THEN qbkg.item_remarks_1 ELSE NULL  END) AS ssg1_2618,
          (CASE WHEN qbkg.item_nbr = '2618' THEN qbkg.item_remarks_2 ELSE NULL  END) AS ssg2_2618,
          (CASE WHEN qbkg.item_nbr = '8610' THEN qbkg.item_remarks_1 ELSE NULL  END) AS ssg1_8610,
          (CASE WHEN qbkg.item_nbr = '8610' THEN qbkg.item_remarks_2 ELSE NULL  END) AS ssg2_8610,
          (CASE WHEN qbkg.item_nbr = '8611' THEN qbkg.item_desc      ELSE NULL  END) AS ssg_8611,
          (CASE WHEN qbkg.item_nbr = '8612' THEN qbkg.item_desc      ELSE NULL  END) AS ssg_8612,
          (CASE WHEN qbkg.item_nbr = '8613' THEN qbkg.item_desc      ELSE NULL  END) AS ssg_8613,
          (CASE WHEN qbkg.item_nbr = '8614' THEN qbkg.item_desc      ELSE NULL  END) AS ssg_8614,
          (CASE WHEN qbkg.item_nbr = '8623' THEN qbkg.item_remarks_1 ELSE NULL  END) AS ssg1_8623,
          (CASE WHEN qbkg.item_nbr = '8623' THEN qbkg.item_remarks_2 ELSE NULL  END) AS ssg2_8623
     FROM qbkg)
    
   ,qrmk2 AS (
   SELECT qrmk.bkng_nbr, qrmk.pax_id,
          MAX(qrmk.ssg1_2501) ssg1_2501,  MAX(qrmk.ssg2_2501) ssg2_2501,
          MAX(qrmk.ssg1_2502) ssg1_2502,  MAX(qrmk.ssg2_2502) ssg2_2502,
          MAX(qrmk.ssg1_2602) ssg1_2602,  MAX(qrmk.ssg2_2602) ssg2_2602,
          MAX(qrmk.ssg1_2612) ssg1_2612,  MAX(qrmk.ssg2_2612) ssg2_2612,
          MAX(qrmk.ssg1_2613) ssg1_2613,  MAX(qrmk.ssg2_2613) ssg2_2613,
          MAX(qrmk.ssg1_2618) ssg1_2618,  MAX(qrmk.ssg2_2618) ssg2_2618,
          MAX(qrmk.ssg1_8610) ssg1_8610,  MAX(qrmk.ssg2_8610) ssg2_8610,
          MAX(qrmk.ssg1_8623) ssg1_8623,  MAX(qrmk.ssg2_8623) ssg2_8623,
          MAX(qrmk.ssg_8611)  ssg_8611,   MAX(qrmk.ssg_8612)  ssg_8612,
          MAX(qrmk.ssg_8613)  ssg_8613,   MAX(qrmk.ssg_8614)  ssg_8614
     FROM qrmk
 GROUP BY qrmk.bkng_nbr, qrmk.pax_id)
----------------------------------------------------------------------------------------------------------
-- General Comments
----------------------------------------------------------------------------------------------------------
   ,qcomt AS (
   SELECT qrmk2.bkng_nbr, qrmk2.pax_id,
          TRIM( TRIM(NVL(qrmk2.ssg1_8610,' '))||' '||  TRIM(NVL(qrmk2.ssg2_8610,' ')) ) AS remarks,
          TRIM( TRIM(NVL(qrmk2.ssg1_2501,' '))||' '||  TRIM(NVL(qrmk2.ssg2_2501,' '))||' '||
                TRIM(NVL(qrmk2.ssg1_2502,' '))||' '||  TRIM(NVL(qrmk2.ssg2_2502,' '))||' '||
                TRIM(NVL(qrmk2.ssg1_2602,' '))||' '||  TRIM(NVL(qrmk2.ssg2_2602,' '))||' '||
                TRIM(NVL(qrmk2.ssg1_2612,' '))||' '||  TRIM(NVL(qrmk2.ssg2_2612,' '))||' '||
                TRIM(NVL(qrmk2.ssg1_2613,' '))||' '||  TRIM(NVL(qrmk2.ssg2_2613,' '))||' '||
                TRIM(NVL(qrmk2.ssg1_2618,' '))||' '||  TRIM(NVL(qrmk2.ssg2_2618,' '))||' '||
                TRIM(NVL(qrmk2.ssg1_8610,' '))||' '||  TRIM(NVL(qrmk2.ssg2_8610,' '))||' '||
                TRIM(NVL(qrmk2.ssg1_8623,' '))||' '||  TRIM(NVL(qrmk2.ssg2_8623,' ')) ) AS comments
     FROM qrmk2)
-- comments specific to embarkation
  ,qc_emb AS (
   SELECT qrmk2.bkng_nbr, qrmk2.pax_id,
          TRIM( TRIM(NVL(qrmk2.ssg_8611,' ')) ||' '||  TRIM(NVL(qrmk2.ssg_8613,' '))  ) AS emb_comments
     FROM qrmk2)
-- comments specific to debarkation
  ,qc_deb AS (
   SELECT qrmk2.bkng_nbr, qrmk2.pax_id,
          TRIM( TRIM(NVL(qrmk2.ssg_8612,' ')) ||' '||  TRIM(NVL(qrmk2.ssg_8614,' '))  ) AS deb_comments
     FROM qrmk2)

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Get resource information specific to embarkation: transfers, transfer dates, Home Port Bus (HPB), Packages and Hotels
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
  ,r_cats AS (    
   SELECT bpi.bkng_id, bpi.rsc_cd, bpi.allocation_dt, bpi.rsc_desc, bpi.rsc_type, bpi.sequence_nbr, 
          bpi.tdef, bpi.pre_post_package_xfr, bpi.rsc_without_year, bpi.vendor_name, bpi.tour_split_cd,
          bpi.itin_duration_cd AS dur_cd, bpi.itin_duration_value AS dur_value,
          CASE WHEN bpi.rsc_type IN ('M' , 'R' , 'T' , 'X' , '+' , '-') AND bpi.rsc_pos_7_char_4 <> '-XXX' AND qbkg.vyd_sail_date   >= bpi.allocation_dt    THEN 'pre_em'
               WHEN bpi.rsc_type = 'H'                                                                     AND qbkg.vyd_sail_date   >= bpi.allocation_dt    THEN 'pre_htl'
               WHEN (company_code = 'S' AND bpi.rsc_type IN ('1', 'I')                                     AND qbkg.vyd_sail_date   >= bpi.allocation_dt )
                 OR (company_code = 'H' AND bpi.rsc_type IN ('1')                                          AND qbkg.vyd_sail_date   >= bpi.allocation_dt )  THEN 'pre_pkg'
               WHEN bpi.rsc_type IN ('M' , 'R' , 'T' , 'X' , '+' , '-') AND bpi.rsc_pos_7_char_4 <> '-XXX' AND qbkg.vyd_return_date <= bpi.allocation_dt    THEN 'post_de'
               WHEN bpi.rsc_type = 'H'                                                                     AND qbkg.vyd_return_date <= bpi.allocation_dt    THEN 'post_htl'
               WHEN (company_code = 'S' AND bpi.rsc_type IN ('2', 'I')                                     AND qbkg.vyd_return_date <= bpi.allocation_dt )
                 OR (company_code = 'H' AND bpi.rsc_type IN ('2')                                          AND qbkg.vyd_return_date <= bpi.allocation_dt )  THEN 'post_pkg'    
          END AS rsc_category,
          CASE WHEN r.package_id IS NOT NULL THEN 'Y' ELSE 'N' END AS hpb_flg
     FROM qbkg,
          rsc_psngr_itin bpi,
          dm_usr.usr_homeportbuslist r  
    WHERE qbkg.bkng_id = bpi.bkng_id(+)
      AND qbkg.vyd_voyage = bpi.voyage(+)
      AND bpi.rsc_without_year = r.package_id(+))   
        
----------------------------------------------------------------------------------------------------------
-- Reduce the rsc_category to the min/max sequence number to facilitate selections
----------------------------------------------------------------------------------------------------------          
  ,by_seq AS (
   SELECT rc.bkng_id,  rc.rsc_category,
          MIN(rc.sequence_nbr) AS min_sequence_nbr,
          MAX(rc.sequence_nbr) AS max_sequence_nbr
     FROM r_cats rc
 GROUP BY rc.bkng_id, rc.rsc_category ) 

     ,ops AS (
   SELECT rc.*, min_sequence_nbr, max_sequence_nbr,
          CASE WHEN rc.rsc_category = 'pre_em'   AND sequence_nbr = min_sequence_nbr AND sequence_nbr <> max_sequence_nbr THEN rsc_cd        END AS emb_xfr1_res,
          CASE WHEN rc.rsc_category = 'pre_em'   AND sequence_nbr = min_sequence_nbr AND sequence_nbr <> max_sequence_nbr THEN rsc_desc      END AS emb_xfr1,
          CASE WHEN rc.rsc_category = 'pre_em'   AND sequence_nbr = min_sequence_nbr AND sequence_nbr <> max_sequence_nbr THEN allocation_dt END AS emb_xfr1_date,
          CASE WHEN rc.rsc_category = 'pre_em'   AND sequence_nbr = min_sequence_nbr AND sequence_nbr <> max_sequence_nbr THEN vendor_name   END AS emb_xfr1_vendor,
          CASE WHEN rc.rsc_category = 'pre_htl'  AND sequence_nbr = min_sequence_nbr AND dur_cd = 'D' THEN dur_value                         END AS emb_hotel_duration,
          CASE WHEN rc.rsc_category = 'pre_htl'  AND sequence_nbr = min_sequence_nbr THEN rsc_desc                                           END AS emb_hotel,
          CASE WHEN rc.rsc_category = 'pre_htl'  AND sequence_nbr = min_sequence_nbr THEN rsc_cd                                             END AS emb_hotel_res,
          CASE WHEN rc.rsc_category = 'pre_htl'  AND sequence_nbr = min_sequence_nbr THEN allocation_dt                                      END AS emb_hotel_date,
          CASE WHEN rc.rsc_category = 'pre_em'   AND sequence_nbr = max_sequence_nbr THEN rsc_cd                                             END AS emb_xfr2_res,
          CASE WHEN rc.rsc_category = 'pre_em'   AND sequence_nbr = max_sequence_nbr THEN rsc_desc                                           END AS emb_xfr2,
          CASE WHEN rc.rsc_category = 'pre_em'   AND sequence_nbr = max_sequence_nbr THEN allocation_dt                                      END AS emb_xfr2_date,
          CASE WHEN rc.rsc_category = 'pre_em'   AND sequence_nbr = max_sequence_nbr THEN vendor_name                                        END AS emb_xfr2_vendor,
          CASE WHEN rc.rsc_category = 'pre_em'   AND rc.hpb_flg = 'Y' THEN rsc_without_year                                                  END AS hpb_res,
          CASE WHEN rc.rsc_category = 'pre_em'   AND rc.hpb_flg = 'Y' THEN rsc_desc                                                          END AS hpb_desc,
          CASE WHEN rc.rsc_category = 'pre_em'   AND rc.hpb_flg = 'Y' THEN pre_post_package_xfr                                              END AS hpb_xfr,
          CASE WHEN rc.rsc_category = 'pre_pkg'  AND sequence_nbr = max_sequence_nbr THEN rsc_desc                                           END AS pre_tour_package,
          CASE WHEN rc.rsc_category = 'pre_pkg'  AND sequence_nbr = max_sequence_nbr THEN rsc_cd                                             END AS pre_pkg_res,
          CASE WHEN rc.rsc_category = 'pre_pkg'  AND sequence_nbr = max_sequence_nbr THEN allocation_dt                                      END AS pre_pkg_date,
--
          CASE WHEN rc.rsc_category = 'post_de'  AND sequence_nbr = min_sequence_nbr THEN rsc_cd                                             END AS deb_xfr1_res,
          CASE WHEN rc.rsc_category = 'post_de'  AND sequence_nbr = min_sequence_nbr THEN rsc_desc                                           END AS deb_xfr1,
          CASE WHEN rc.rsc_category = 'post_de'  AND sequence_nbr = min_sequence_nbr THEN allocation_dt                                      END AS deb_xfr1_date,
          CASE WHEN rc.rsc_category = 'post_de'  AND sequence_nbr = min_sequence_nbr THEN vendor_name                                        END AS deb_xfr1_vendor,
          CASE WHEN rc.rsc_category = 'post_htl' AND sequence_nbr = min_sequence_nbr AND dur_cd = 'D' THEN dur_value                         END AS deb_hotel_duration,
          CASE WHEN rc.rsc_category = 'post_htl' AND sequence_nbr = min_sequence_nbr THEN rsc_desc                                           END AS deb_hotel,
          CASE WHEN rc.rsc_category = 'post_htl' AND sequence_nbr = min_sequence_nbr THEN rsc_cd                                             END AS deb_hotel_res,
          CASE WHEN rc.rsc_category = 'post_htl' AND sequence_nbr = min_sequence_nbr THEN allocation_dt                                      END AS deb_hotel_date,
          CASE WHEN rc.rsc_category = 'post_de'  AND sequence_nbr = max_sequence_nbr AND sequence_nbr <> min_sequence_nbr THEN rsc_cd        END AS deb_xfr2_res,
          CASE WHEN rc.rsc_category = 'post_de'  AND sequence_nbr = max_sequence_nbr AND sequence_nbr <> min_sequence_nbr THEN rsc_desc      END AS deb_xfr2,
          CASE WHEN rc.rsc_category = 'post_de'  AND sequence_nbr = max_sequence_nbr AND sequence_nbr <> min_sequence_nbr THEN allocation_dt END AS deb_xfr2_date,
          CASE WHEN rc.rsc_category = 'post_de'  AND sequence_nbr = max_sequence_nbr AND sequence_nbr <> min_sequence_nbr THEN vendor_name   END AS deb_xfr2_vendor,
          CASE WHEN rc.rsc_category = 'post_pkg' AND sequence_nbr = min_sequence_nbr THEN rsc_desc                                           END AS post_tour_package,
          CASE WHEN rc.rsc_category = 'post_pkg' AND sequence_nbr = min_sequence_nbr THEN rsc_cd                                             END AS post_pkg_res,
          CASE WHEN rc.rsc_category = 'post_pkg' AND sequence_nbr = min_sequence_nbr THEN allocation_dt                                      END AS post_pkg_date
     FROM r_cats rc, by_seq bs
    WHERE rc.bkng_id = bs.bkng_id
      AND rc.rsc_category = bs.rsc_category)
----------------------------------------------------------------------------------------------------------
--  Flatten the embarkation and debarkation data to one record per guest
----------------------------------------------------------------------------------------------------------   
    ,qres AS (
   SELECT ops.bkng_id, 
          MAX(ops.tdef)                AS tdef, 
          MAX(ops.tour_split_cd)       AS split_cd, 
          MAX(ops.hpb_desc)            AS hpb_desc,
          MAX(ops.hpb_xfr)             AS hpb_xfr, 
          MAX(ops.hpb_res)             AS hpb_res,
          MAX(ops.emb_xfr1_res)        AS emb_xfr1_res,
          MAX(ops.emb_xfr1)            AS emb_xfr1,
          MAX(ops.emb_xfr1_date)       AS emb_xfr1_dt,
          MAX(ops.emb_xfr1_vendor)     AS emb_xfr1_vendor, 
          MAX(ops.emb_hotel)           AS emb_hotel,
          MAX(ops.emb_hotel_duration)  AS emb_hotel_duration,
          MAX(ops.emb_hotel_date)      AS emb_hotel_date,
          MAX(ops.emb_hotel_res)       AS emb_hotel_res,
          MAX(ops.emb_xfr2_res)        AS emb_xfr2_res,  
          MAX(ops.emb_xfr2)            AS emb_xfr2, 
          MAX(ops.emb_xfr2_date)       AS emb_xfr2_dt,
          MAX(ops.emb_xfr2_vendor)     AS emb_xfr2_vendor,
          MAX(ops.pre_tour_package)    AS pre_tour_package,
          MAX(ops.pre_pkg_res)         AS pre_pkg_res,
          MAX(ops.pre_pkg_date)        AS pre_pkg_date,
          MAX(ops.deb_xfr1_res)        AS deb_xfr1_res,
          MAX(ops.deb_xfr1)            AS deb_xfr1, 
          MAX(ops.deb_xfr1_date)       AS deb_xfr1_dt,
          MAX(ops.deb_xfr1_vendor)     AS deb_xfr1_vendor,
          MAX(ops.deb_hotel)           AS deb_hotel,
          MAX(ops.deb_hotel_duration)  AS deb_hotel_duration,
          MAX(ops.deb_hotel_date)      AS deb_hotel_date, 
          MAX(ops.deb_hotel_res)       AS deb_hotel_res, 
          MAX(ops.deb_xfr2_res)        AS deb_xfr2_res, 
          MAX(ops.deb_xfr2)            AS deb_xfr2,
          MAX(ops.deb_xfr2_date)       AS deb_xfr2_dt,
          MAX(ops.deb_xfr2_vendor)     AS deb_xfr2_vendor,
          MAX(ops.post_tour_package)   AS post_tour_package,
          MAX(ops.post_pkg_res)        AS post_pkg_res,
          MAX(ops.post_pkg_date)       AS post_pkg_date
     FROM ops
 GROUP BY ops.bkng_id  )
----------------------------------------------------------------------------------------------------------
-- Get airline infomation prior to embarkation
--
-- Ranking is used to retrieve the appropriate piece of air info, based on the direction of flight.  
--  i.e.  We want the last leg for arrival to the ship and the first leg for departure from the ship
----------------------------------------------------------------------------------------------------------
   ,qair1 AS (
   SELECT qbkg.bkng_nbr, qbkg.pax_id, qbkg.vyd_voyage, qbkg.vyd_sail_date, qbkg.vyd_return_date, qbkg.vyd_trade, at.sequence_nbr,
          at.depart_dt t_dep_date, at.depart_time t_dep_time,
          at.arrival_dt t_arr_date, at.arrival_time t_arr_time, at.air_carrier t_air_carrier, at.flight_nbr t_flight_nbr, 
          at.from_city t_from_city, at.to_city t_to_city, at.flight_direction t_flt_dir, at.manual_air_flg t_man_flg,
        ( CASE WHEN SUBSTR(arrival_time,6,1) = 'P' THEN  
          CASE WHEN SUBSTR(arrival_time,1,2) = '01' THEN '13'
               WHEN SUBSTR(arrival_time,1,2) = '02' THEN '14'
               WHEN SUBSTR(arrival_time,1,2) = '03' THEN '15'
               WHEN SUBSTR(arrival_time,1,2) = '04' THEN '16'
               WHEN SUBSTR(arrival_time,1,2) = '05' THEN '17'
               WHEN SUBSTR(arrival_time,1,2) = '06' THEN '18'
               WHEN SUBSTR(arrival_time,1,2) = '07' THEN '19'
               WHEN SUBSTR(arrival_time,1,2) = '08' THEN '20'
               WHEN SUBSTR(arrival_time,1,2) = '09' THEN '21'
               WHEN SUBSTR(arrival_time,1,2) = '10' THEN '22'
               WHEN SUBSTR(arrival_time,1,2) = '11' THEN '23'
               WHEN SUBSTR(arrival_time,1,2) = '12' THEN '12'  
               ELSE NULL END   
                                   ELSE 
          CASE WHEN SUBSTR(arrival_time,6,1) = 'A' THEN 
          CASE WHEN SUBSTR(arrival_time,1,2) = '01' THEN '01'
               WHEN SUBSTR(arrival_time,1,2) = '02' THEN '02'
               WHEN SUBSTR(arrival_time,1,2) = '03' THEN '03'
               WHEN SUBSTR(arrival_time,1,2) = '04' THEN '04'
               WHEN SUBSTR(arrival_time,1,2) = '05' THEN '05'
               WHEN SUBSTR(arrival_time,1,2) = '06' THEN '06'
               WHEN SUBSTR(arrival_time,1,2) = '07' THEN '07'
               WHEN SUBSTR(arrival_time,1,2) = '08' THEN '08'
               WHEN SUBSTR(arrival_time,1,2) = '09' THEN '09'
               WHEN SUBSTR(arrival_time,1,2) = '10' THEN '10'
               WHEN SUBSTR(arrival_time,1,2) = '11' THEN '11'
               WHEN SUBSTR(arrival_time,1,2) = '12' THEN '00'
               ELSE NULL END   
              END  END) ||':'||substr(arrival_time,4,2) AS t_arr_24_time
     FROM qbkg,
          air_psngr_flight at
    WHERE qbkg.vyd_voyage = at.voyage(+)
      AND qbkg.bkng_nbr = at.bkng_nbr(+)
      AND qbkg.pax_id = at.guest_id(+)
      AND qbkg.vyd_sail_date >= at.arrival_dt(+)
      AND at.sequence_nbr IS NOT NULL     
      AND at.arrival_dt IS NOT NULL
      AND to_char(at.arrival_dt,'MM/DD/YYYY') <> '12/31/1900'
      AND at.air_carrier IS NOT NULL
      AND at.flight_nbr IS NOT NULL
      AND at.flight_direction in('I', 'T')     )   

,to_ranks AS (
   SELECT bkng_nbr, pax_id, vyd_voyage, vyd_sail_date, t_dep_date,  t_dep_time, t_arr_date,  t_arr_time, t_arr_24_time, t_air_carrier,
          t_flight_nbr, t_from_city,  t_to_city,  t_flt_dir, t_man_flg,  sequence_nbr,
          DENSE_RANK() OVER (PARTITION BY qair1.bkng_nbr, qair1.pax_id, qair1.vyd_voyage, t_flt_dir 
                                 ORDER BY qair1.bkng_nbr, qair1.pax_id, qair1.vyd_voyage, t_flt_dir, sequence_nbr, t_arr_date, t_arr_24_time ) ranking
     FROM qair1   )                                
             
,to_minmax AS 
  (SELECT bkng_nbr, pax_id, t_flt_dir, MIN(ranking) rank1, MAX(ranking) rank9  
     FROM to_ranks
 GROUP BY bkng_nbr, pax_id, t_flt_dir)  

-- Intra Air (going to the ship)
,t_rank10 AS (
   SELECT tr.* 
     FROM to_ranks tr, to_minmax mm
    WHERE tr.bkng_nbr = mm.bkng_nbr
      AND tr.pax_id = mm.pax_id
      AND tr.ranking = mm.rank9
      AND tr.t_flt_dir = mm.t_flt_dir
      AND tr.t_flt_dir = 'I')             

-- Std Air (going to the ship)  : last leg -- this is the one for reporting 
,t_rank11 AS (
   SELECT tr.* 
     FROM to_ranks tr, to_minmax mm
    WHERE tr.bkng_nbr = mm.bkng_nbr
      AND tr.pax_id   = mm.pax_id
      AND tr.ranking = mm.rank9
      AND tr.t_flt_dir = mm.t_flt_dir
      AND tr.t_flt_dir <> 'I')              
    
-- Std Air (going to the ship)  :  last leg - 1  
,t_rank12 AS (
   SELECT tr.* 
     FROM to_ranks tr, to_minmax mm
    WHERE tr.bkng_nbr = mm.bkng_nbr
      AND tr.pax_id   = mm.pax_id
      AND tr.ranking = mm.rank9 - 1
      AND tr.t_flt_dir = mm.t_flt_dir
      AND tr.t_flt_dir <> 'I')    
      
----------------------------------------------------------------------------------------------------------
-- Get airline information after debarkation
----------------------------------------------------------------------------------------------------------
   ,qair2 AS (
   SELECT qbkg.bkng_nbr, qbkg.pax_id, qbkg.vyd_voyage, qbkg.vyd_sail_date, qbkg.vyd_return_date, qbkg.vyd_trade,  at.sequence_nbr,  
          at.arrival_dt f_arr_date, at.arrival_time f_arr_time,                        
          at.depart_dt f_dep_date, at.depart_time f_dep_time, at.air_carrier f_air_carrier, at.flight_nbr f_flight_nbr, 
          at.from_city f_from_city, at.to_city f_to_city, at.flight_direction f_flt_dir, at.manual_air_flg f_man_flg,
        ( CASE WHEN SUBSTR(depart_time,6,1) = 'P'  THEN  
          CASE WHEN SUBSTR(depart_time,1,2) = '01' THEN '13'
               WHEN SUBSTR(depart_time,1,2) = '02' THEN '14'
               WHEN SUBSTR(depart_time,1,2) = '03' THEN '15'
               WHEN SUBSTR(depart_time,1,2) = '04' THEN '16'
               WHEN SUBSTR(depart_time,1,2) = '05' THEN '17'
               WHEN SUBSTR(depart_time,1,2) = '06' THEN '18'
               WHEN SUBSTR(depart_time,1,2) = '07' THEN '19'
               WHEN SUBSTR(depart_time,1,2) = '08' THEN '20'
               WHEN SUBSTR(depart_time,1,2) = '09' THEN '21'
               WHEN SUBSTR(depart_time,1,2) = '10' THEN '22'
               WHEN SUBSTR(depart_time,1,2) = '11' THEN '23'
               WHEN SUBSTR(depart_time,1,2) = '12' THEN '12'  
               ELSE NULL END   
                                   ELSE 
          CASE WHEN SUBSTR(depart_time,6,1) = 'A'   THEN 
          CASE WHEN SUBSTR(depart_time,1,2) = '01' THEN '01'
               WHEN SUBSTR(depart_time,1,2) = '02' THEN '02'
               WHEN SUBSTR(depart_time,1,2) = '03' THEN '03'
               WHEN SUBSTR(depart_time,1,2) = '04' THEN '04'
               WHEN SUBSTR(depart_time,1,2) = '05' THEN '05'
               WHEN SUBSTR(depart_time,1,2) = '06' THEN '06'
               WHEN SUBSTR(depart_time,1,2) = '07' THEN '07'
               WHEN SUBSTR(depart_time,1,2) = '08' THEN '08'
               WHEN SUBSTR(depart_time,1,2) = '09' THEN '09'
               WHEN SUBSTR(depart_time,1,2) = '10' THEN '10'
               WHEN SUBSTR(depart_time,1,2) = '11' THEN '11'
               WHEN SUBSTR(depart_time,1,2) = '12' THEN '00'
               ELSE NULL END   
              END  END) ||':'||substr(depart_time,4,2) AS f_dep_24_time
     FROM qbkg,
          air_psngr_flight at
    WHERE qbkg.vyd_voyage = at.voyage(+)
      AND qbkg.bkng_nbr = at.bkng_nbr(+)
      AND qbkg.pax_id = at.guest_id(+)
      AND qbkg.vyd_return_date <= at.depart_dt(+)
      AND at.sequence_nbr IS NOT NULL
      AND at.depart_dt IS NOT NULL
      AND to_char(at.depart_dt,'MM/DD/YYYY') <> '12/31/1900'
      AND at.air_carrier IS NOT NULL
      AND at.flight_nbr IS NOT NULL
      AND at.flight_direction in('I', 'F')     )  

,from_ranks AS (
   SELECT bkng_nbr, pax_id, vyd_voyage, vyd_sail_date, f_arr_date, f_arr_time, f_dep_date, f_dep_time, f_dep_24_time, f_air_carrier,
          f_flight_nbr, f_from_city, f_to_city,  f_flt_dir, f_man_flg, sequence_nbr,
          DENSE_RANK() OVER (PARTITION BY bkng_nbr, pax_id, vyd_voyage ,f_flt_dir
                                 ORDER BY bkng_nbr, pax_id, vyd_voyage, f_flt_dir, sequence_nbr, f_dep_date, f_dep_24_time   ) ranking
     FROM qair2      )              

,from_minmax AS (
   SELECT bkng_nbr, pax_id, f_flt_dir,  MIN(ranking) rank1, MAX(ranking) rank9  
     FROM from_ranks
 GROUP BY bkng_nbr, pax_id, f_flt_dir)              

-- Intra Air (going from the ship)  
,f_rank20 AS (
   SELECT fr.* 
     FROM from_ranks fr, from_minmax mm
    WHERE fr.bkng_nbr = mm.bkng_nbr
      AND fr.pax_id = mm.pax_id
      AND fr.ranking = mm.rank1
      AND fr.f_flt_dir = mm.f_flt_dir
      AND fr.f_flt_dir = 'I')                   

-- Std Air (going from the ship)  : 1st leg -- this is the one for reporting    
,f_rank21 AS (
   SELECT fr.* 
     FROM from_ranks fr, from_minmax mm
    WHERE fr.bkng_nbr = mm.bkng_nbr
      AND fr.pax_id = mm.pax_id
      AND fr.ranking = mm.rank1
      AND fr.f_flt_dir = mm.f_flt_dir
      AND fr.f_flt_dir <> 'I')                 

-- Std Air (going from the ship)  :  last leg  
,f_rank22 AS (
   SELECT fr.* 
    FROM from_ranks fr, from_minmax mm
   WHERE fr.bkng_nbr = mm.bkng_nbr
     AND fr.pax_id = mm.pax_id
     AND fr.ranking = mm.rank1 + 1
     AND fr.f_flt_dir = mm.f_flt_dir
     AND fr.f_flt_dir <> 'I')
----------------------------------------------------------------------------------------------------------
--  Gather all extracts together for final output
----------------------------------------------------------------------------------------------------------
    ,qfin AS (
   SELECT distinct 
          qvoy.company_code            AS company_cd, 
          qvoy.vyd_voyage              AS voyage, 
          qvoy.vyd_ship_code           AS ship_cd,
          qvoy.ship_name               AS ship_name,
          qvoy.vyd_sail_date           AS sail_dt,
          qvoy.vyd_return_date         AS return_dt,
          qvoy.vyd_trade               AS trade,
          qvoy.vyd_basic_from_port     AS from_port,
          qvoy.vyd_basic_to_port       AS to_port,
          qbkg.bkng_nbr                AS bkng_nbr, 
          qbkg.pax_id                  AS bkng_party_nbr,  
          qbkg.bkng_id                 AS bkng_id,
          qbkg.group_id                AS group_id,
          qbkg.b_group_code            AS group_cd,
          qbkg.b_category              AS category_cd,
          qbkg.apo_cd                  AS apo_cd,
          qbkg.b_cabin                 AS cabin_nbr,
          qbkg.b_p_surname             AS last_name,
          qbkg.b_p_forename            AS first_name,
          qbkg.b_p_title               AS title,
          qbkg.b_contact               AS bkng_contact,
          qbkg.b_travel_with_id        AS twid,
          qbkg.b_p_loyalty_pgm_code    AS loy_cd,
          qbkg.b_status                AS bkng_status,
          qbkg.b_link_id               AS link_id,
          qbkg.b_link_sequence_flag    AS link_seq_flg,
          qbkg.b_dir_flag              AS direct_flg,
          qbkg.b_dir_home_phone        AS direct_home_phone,
          qbkg.b_dir_email_address     AS direct_email,
          qbkg.b_tour_id               AS tour_id,
          qbkg.b_tour_start_date       AS tour_start_dt,
          qbkg.b_tour_end_date         AS tour_end_dt,
          qbkg.b_p_sex                 AS gender_cd,
          qbkg.grm_group_name          AS group_name,
          qbkg.b_p_air_home_city       AS home_city_air,
          qbkg.b_p_air_flag            AS air_flg,
          qbkg.b_p_air_city            AS air_city,
          qbkg.alien_resident_id       AS alien_resident_id,
          qbkg.passport_expire_date    AS passport_expire_dt,
          qbkg.passport                AS passport_nbr,
          qbkg.pp_issued_at            AS passport_issued_at,
          qbkg.birthdate               AS birth_dt,
          qbkg.cell_phone              AS cell_phone,
          qbkg.email_address           AS email,
          qbkg.home_address1           AS addr_line1,
          qbkg.home_address2           AS addr_line2,
          qbkg.home_city               AS city,
          qbkg.home_state              AS state,
          qbkg.home_zip                AS zip_cd,
          qbkg.home_phone              AS telephone,
          qbkg.home_country            AS home_cntry_cd,
          qbkg.nationality             AS nationality,
          qbkg.b_booking_agent         AS agency_nbr,
          qbkg.agy1_contact            AS agency_contact, 
          qbkg.agy1_telephone          AS agency_telephone,
          qbkg.agy1_name               AS agency_name,
          qbkg.agy2_email_address      AS agency_email,
          qbkg.cct_country_name        AS nationality_cntry,
          qbkg.intransit_voy_flag      AS intransit_voyage_flg,
          qcomt.comments               AS all_comments, 
          qcomt.remarks                AS remarks_8610,
          qc_emb.emb_comments          AS embark_comments, 
          qc_deb.deb_comments          AS debark_comments, 
          qwchoxy.wheelchair_o2        AS wheelchair_o2_comments,
          qres.tdef                    AS tdef,
          qres.split_cd                AS motorcoach_split_cd,
          qres.emb_xfr1_res            AS embark_xfr1_rsc_cd,
          qres.emb_xfr1                AS embark_xfr1_rsc_desc, 
          qres.emb_xfr1_dt             AS embark_xfr1_rsc_dt,
          qres.emb_xfr1_vendor         AS embark_xfr1_vendor, 
          qres.emb_hotel               AS embark_hotel_name,
          qres.emb_hotel_duration      AS embark_hotel_duration,
          qres.emb_hotel_date          AS embark_hotel_dt, 
          qres.emb_hotel_res           AS embark_hotel_rsc_cd,
          qres.hpb_desc                AS home_port_bus_desc,
          qres.hpb_xfr                 AS home_port_bus_xfr_cd,
          qres.hpb_res                 AS home_port_bus_rsc_cd,
          qres.pre_tour_package        AS pre_pkg_rsc_desc,
          qres.pre_pkg_date            AS pre_pkg_rsc_dt,
          qres.pre_pkg_res             AS pre_pkg_rsc_cd,
          qres.emb_xfr2_res            AS embark_xfr2_rsc_cd,
          qres.emb_xfr2                AS embark_xfr2_rsc_desc, 
          qres.emb_xfr2_dt             AS embark_xfr2_rsc_dt,
          qres.emb_xfr2_vendor         AS embark_xfr2_vendor, 
          qres.deb_xfr1_res            AS debark_xfr1_rsc_cd,
          qres.deb_xfr1                AS debark_xfr1_rsc_desc, 
          qres.deb_xfr1_dt             AS debark_xfr1_rsc_dt,
          qres.deb_xfr1_vendor         AS debark_xfr1_vendor, 
          qres.deb_hotel               AS debark_hotel_name,
          qres.deb_hotel_duration      AS debark_hotel_duration,
          qres.deb_hotel_date          AS debark_hotel_dt, 
          qres.deb_hotel_res           AS debark_hotel_rsc_cd, 
          qres.post_tour_package       AS post_pkg_rsc_desc,
          qres.post_pkg_date           AS post_pkg_rsc_dt,
          qres.post_pkg_res            AS post_pkg_rsc_cd,
          qres.deb_xfr2_res            AS debark_xfr2_rsc_cd,
          qres.deb_xfr2                AS debark_xfr2_rsc_desc, 
          qres.deb_xfr2_dt             AS debark_xfr2_rsc_dt,
          qres.deb_xfr2_vendor         AS debark_xfr2_vendor,
          t_rank10.t_flt_dir           AS to_intra_flt_dir,
          t_rank10.t_dep_date          AS to_intra_dep_dt, 
          t_rank10.t_dep_time          AS to_intra_dep_time,
          t_rank10.t_arr_date          AS to_intra_arr_dt , 
          t_rank10.t_arr_time          AS to_intra_arr_time, 
          t_rank10.t_air_carrier       AS to_intra_air_carrier, 
          t_rank10.t_flight_nbr        AS to_intra_flight_nbr, 
          t_rank10.t_from_city         AS to_intra_from_city,  
          t_rank10.t_to_city           AS to_intra_to_city, 
          t_rank11.t_flt_dir           AS to_flt_dir,
          t_rank11.t_dep_date          AS to_dep_dt, 
          t_rank11.t_dep_time          AS to_dep_time,
          t_rank11.t_arr_date          AS to_arr_dt, 
          t_rank11.t_arr_time          AS to_arr_time,
          t_rank11.t_arr_24_time       AS to_arr_24time, 
          t_rank11.t_air_carrier       AS to_air_carrier, 
          t_rank11.t_flight_nbr        AS to_flight_nbr, 
          t_rank11.t_from_city         AS to_from_city,  
          t_rank11.t_to_city           AS to_to_city, 
          f_rank20.f_flt_dir           AS fr_intra_flt_dir,
          f_rank20.f_dep_date          AS fr_intra_dep_dt, 
          f_rank20.f_dep_time          AS fr_intra_dep_time,
          f_rank20.f_arr_date          AS fr_intra_arr_dt , 
          f_rank20.f_arr_time          AS fr_intra_arr_time, 
          f_rank20.f_air_carrier       AS fr_intra_air_carrier, 
          f_rank20.f_flight_nbr        AS fr_intra_flight_nbr, 
          f_rank20.f_from_city         AS fr_intra_from_city,  
          f_rank20.f_to_city           AS fr_intra_to_city, 
          f_rank21.f_flt_dir           AS fr_flt_dir,
          f_rank21.f_dep_date          AS fr_dep_dt,
          f_rank21.f_dep_time          AS fr_dep_time,
          f_rank21.f_dep_24_time       AS fr_dep_24time,
          f_rank21.f_arr_date          AS fr_arr_dt, 
          f_rank21.f_arr_time          AS fr_arr_time,
          f_rank21.f_air_carrier       AS fr_air_carrier, 
          f_rank21.f_flight_nbr        AS fr_flight_nbr, 
          f_rank21.f_from_city         AS fr_from_city, 
          f_rank21.f_to_city           AS fr_to_city,
          CASE WHEN t_rank12.t_to_city = t_rank11.t_from_city THEN t_rank12.t_to_city END to_intransit_air_city,
          CASE WHEN f_rank21.f_to_city = f_rank22.f_from_city THEN f_rank21.f_to_city END fr_intransit_air_city,
          qbkg.sbn_amenity_cabin       AS sbn_amenity_cabin_flg,
          CASE WHEN qvoy.company_code = 'S' AND qbkg.b_p_air_flag = 'Y' AND t_rank11.t_arr_date = qvoy.vyd_sail_date AND (t_rank11.t_arr_24_time >= '05:00' and t_rank11.t_arr_24_time <= '09:00') THEN 'YES' ELSE 'NO' END sbn_meals,
          CASE WHEN qvoy.company_code = 'S' AND qbkg.b_p_air_flag = 'Y' AND t_rank11.t_arr_date = qvoy.vyd_sail_date AND (t_rank11.t_arr_24_time >= '00:01' and t_rank11.t_arr_24_time <= '04:59') THEN 'YES' ELSE 'NO' END sbn_dayroom
     FROM qvoy, qbkg, qcomt, qc_emb, qc_deb, qres, t_rank10, t_rank11, t_rank12, f_rank20, f_rank21, f_rank22, qwchoxy 
    WHERE qvoy.vyd_voyage = qbkg.vyd_voyage
      AND qbkg.bkng_id    = qres.bkng_id(+)
      AND qbkg.bkng_nbr   = qwchoxy.bkng_nbr(+)
      AND qbkg.pax_id     = qwchoxy.pax_id(+)
      AND qbkg.bkng_nbr   = qcomt.bkng_nbr(+)
      AND qbkg.pax_id     = qcomt.pax_id(+)
      AND qbkg.bkng_nbr   = qc_emb.bkng_nbr(+)
      AND qbkg.pax_id     = qc_emb.pax_id(+)
      AND qbkg.bkng_nbr   = qc_deb.bkng_nbr(+)
      AND qbkg.pax_id     = qc_deb.pax_id(+)
      AND qbkg.bkng_nbr   = t_rank10.bkng_nbr(+)
      AND qbkg.pax_id     = t_rank10.pax_id(+)
      AND qbkg.bkng_nbr   = t_rank11.bkng_nbr(+)
      AND qbkg.pax_id     = t_rank11.pax_id(+)
      AND qbkg.vyd_voyage = t_rank11.vyd_voyage(+)
      AND qbkg.bkng_nbr   = t_rank12.bkng_nbr(+)
      AND qbkg.pax_id     = t_rank12.pax_id(+)
      AND qbkg.vyd_voyage = t_rank12.vyd_voyage(+)
      AND qbkg.bkng_nbr   = f_rank20.bkng_nbr(+)
      AND qbkg.pax_id     = f_rank20.pax_id(+)
      AND qbkg.bkng_nbr   = f_rank21.bkng_nbr(+)
      AND qbkg.pax_id     = f_rank21.pax_id(+)
      AND qbkg.vyd_voyage = f_rank21.vyd_voyage(+)
      AND qbkg.bkng_nbr   = f_rank22.bkng_nbr(+)
      AND qbkg.pax_id     = f_rank22.pax_id(+)
      AND qbkg.vyd_voyage = f_rank22.vyd_voyage(+)
      )
  SELECT 
  company_cd, 
voyage, 
ship_cd,
ship_name,
sail_dt,
return_dt,
trade,
from_port,
to_port,
bkng_nbr, 
bkng_party_nbr,  
bkng_id,
group_id,
group_cd,
category_cd,
apo_cd,
cabin_nbr,
last_name,
first_name,
title,
bkng_contact,
twid,
loy_cd,
bkng_status,
link_id,
link_seq_flg,
direct_flg,
direct_home_phone,
direct_email,
tour_id,
tour_start_dt,
tour_end_dt,
gender_cd,
group_name,
home_city_air,
air_flg,
air_city,
alien_resident_id,
passport_expire_dt,
passport_nbr,
passport_issued_at,
birth_dt,
cell_phone,
email,
addr_line1,
addr_line2,
city,
state,
zip_cd,
telephone,
home_cntry_cd,
nationality,
agency_nbr,
agency_contact, 
agency_telephone,
agency_name,
agency_email,
nationality_cntry,
intransit_voyage_flg,
all_comments, 
remarks_8610,
embark_comments, 
debark_comments, 
wheelchair_o2_comments,
tdef,
motorcoach_split_cd,
embark_xfr1_rsc_cd,
embark_xfr1_rsc_desc, 
embark_xfr1_rsc_dt,
embark_xfr1_vendor, 
embark_hotel_name,
embark_hotel_duration,
embark_hotel_dt, 
embark_hotel_rsc_cd,
home_port_bus_desc,
home_port_bus_xfr_cd,
home_port_bus_rsc_cd,
pre_pkg_rsc_desc,
pre_pkg_rsc_dt,
pre_pkg_rsc_cd,
embark_xfr2_rsc_cd,
embark_xfr2_rsc_desc, 
embark_xfr2_rsc_dt,
embark_xfr2_vendor, 
debark_xfr1_rsc_cd,
debark_xfr1_rsc_desc, 
debark_xfr1_rsc_dt,
debark_xfr1_vendor, 
debark_hotel_name,
debark_hotel_duration,
debark_hotel_dt, 
debark_hotel_rsc_cd, 
post_pkg_rsc_desc,
post_pkg_rsc_dt,
post_pkg_rsc_cd,
debark_xfr2_rsc_cd,
debark_xfr2_rsc_desc, 
debark_xfr2_rsc_dt,
debark_xfr2_vendor,
to_intra_flt_dir,
to_intra_dep_dt, 
to_intra_dep_time,
to_intra_arr_dt , 
to_intra_arr_time, 
to_intra_air_carrier, 
to_intra_flight_nbr, 
to_intra_from_city,  
to_intra_to_city, 
to_flt_dir,
to_dep_dt, 
to_dep_time,
to_arr_dt, 
to_arr_time,
to_arr_24time, 
to_air_carrier, 
to_flight_nbr, 
to_from_city,  
to_to_city, 
fr_intra_flt_dir,
fr_intra_dep_dt, 
fr_intra_dep_time,
fr_intra_arr_dt , 
fr_intra_arr_time, 
fr_intra_air_carrier, 
fr_intra_flight_nbr, 
fr_intra_from_city,  
fr_intra_to_city, 
fr_flt_dir,
fr_dep_dt,
fr_dep_time,
fr_dep_24time,
fr_arr_dt, 
fr_arr_time,
fr_air_carrier, 
fr_flight_nbr, 
fr_from_city, 
fr_to_city,
to_intransit_air_city,
fr_intransit_air_city,
sbn_amenity_cabin_flg,
sbn_meals,
sbn_dayroom
 FROM qfin
-- ORDER BY qfin.company_cd, qfin.ship_cd, qfin.voyage, qfin.bkng_id

;
    
          x:=SQL%ROWCOUNT;        
          sComment  := 'trn_ops_manifest Inserts: '||x;
  
          EXECUTE IMMEDIATE 'ALTER TABLE trn_ops_manifest LOGGING';
          
          BEGIN
            SYS.DBMS_STATS.GATHER_TABLE_STATS (
            OwnName        => 'DM_TKR'
           ,TabName        => 'TRN_OPS_MANIFEST'
           ,Degree            => 4
           ,Cascade           => TRUE
           );
          END;           
              
          /* Record Completion in Log */
          IF  DM_INT.COMMON_JOBS.LOG_JOB (
                      io_Batch_ID => BatchID,   
                      in_Prog_Status => 'C',  
                      in_Comments => sComment,  
                      in_Error_Message => 'NA' 
                      ) != 0 THEN
              RAISE eCALL_FAILED;            
          END IF;      
    
          /* Record Completion in Control File */
          IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                      in_Batch_ID => BatchID,                       
                      in_Schema_Name => cSchema_NM,  
                      in_Package_Name => cPackage_NM, 
                      in_Program_Name => cProcedure_NM,   
                      in_Load_Type => 'D',         
                      in_Prog_Status => 'C', 
                      in_Start_Dt_Parm => dBeg_Date,             
                      in_End_Dt_Parm => dEnd_Date 
                      ) != 0 THEN
              RAISE eCALL_FAILED;            
          END IF; 
          
    EXCEPTION  
      WHEN eCALL_FAILED THEN
              ROLLBACK ;
              eErrMsg :=  'User Defined - Error in called sub-program';                  
              
              IF BatchID = 0 THEN                                                                           
                      x:=-1 ;
                      /* Record error against placeholder w/ batch of -1 since batch not recorded */
                      x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                  io_Batch_ID => x ,   
                                  in_Prog_Status => 'X',  
                                  in_Comments => 'Job Failed logged to placeholder  ', 
                                  in_Error_Message => eErrMsg          
                                  );
                      x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                              in_Batch_ID => BatchID ,                       
                              in_Schema_Name => cSchema_NM ,  
                              in_Package_Name => cPackage_NM, 
                              in_Program_Name => cProcedure_NM,   
                              in_Load_Type => 'D' ,         
                              in_Prog_Status => 'X' , 
                              in_Start_Dt_Parm => dBeg_Date ,      
                              in_End_Dt_Parm => dEnd_Date   
                              );
              ELSE
                      /* Record error w/ the assigned batch ID */ 
                      x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                              io_Batch_ID => BatchID,   
                              in_Prog_Status => 'F',  
                              in_Comments => 'Job Failed',  
                              in_Error_Message => eErrMsg 
                              );
                      x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                              in_Batch_ID => BatchID,  
                              in_Schema_Name => cSchema_NM,  
                              in_Package_Name => cPackage_NM, 
                              in_Program_Name => cProcedure_NM, 
                              in_Load_Type => 'D',         
                              in_Prog_Status  => 'F', 
                              in_Start_Dt_Parm => dBeg_Date,  
                              in_End_Dt_Parm => dEnd_Date  
                              );
              END IF ;
              COMMIT;
              RAISE eCALL_FAILED;     
               
        WHEN OTHERS THEN             
              /* Error trap and logging w/ generic handler */
              eErrMsg :=  SQLERRM;         
              dbms_output.put_line(eErrMSG);                                   
              ROLLBACK;
                          
              /* Record error w/ the assigned batch ID */
              x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                      io_Batch_ID => BatchID,   
                      in_Prog_Status => 'F',  
                      in_Comments => 'Job Failed',  
                      in_Error_Message => eErrMsg 
                      );  
              x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                      in_Batch_ID => BatchID,  
                      in_Schema_Name => cSchema_NM,  
                      in_Package_Name => cPackage_NM, 
                      in_Program_Name => cProcedure_NM, 
                      in_Load_Type => 'D',         
                      in_Prog_Status => 'F' , 
                      in_Start_Dt_Parm => dBeg_Date,  
                      in_End_Dt_Parm => dEnd_Date              
                      );
              COMMIT;
              RAISE;           
  END TRN_OPS_MANIFEST_LOAD;
 
   PROCEDURE WEB_BKNG_ACTIVITY_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'web_bkng_activity';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF; 

         
        EXECUTE IMMEDIATE 'ALTER TABLE web_bkng_activity NOLOGGING';
               
        EXECUTE IMMEDIATE 'ALTER TABLE web_bkng_activity NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE web_bkng_activity'; 

EXECUTE IMMEDIATE 'TRUNCATE TABLE web_bkng_activity'; 
        
INSERT INTO web_bkng_activity
with t1 as
(
SELECT
    TO_CHAR(TRUNC(SYSDATE,'MM') - 1,'DD-MON-YYYY') as beg_dt,
    TO_CHAR(TRUNC(SYSDATE,'MM'),'DD-MON-YYYY') as beg2_dt,
    TO_CHAR(TRUNC(SYSDATE -1),'DD-MON-YYYY') as end_dt,
    TO_CHAR (SYSDATE, 'YYYY') as current_year,
    TO_CHAR (SYSDATE, 'YYYY') - 1 as prior_year
FROM DUAL
)
, t2 as
(
select /*+ parallel */
    bpa.company_code,
    bd.rpt_year,
    vd.trade_description,
    bd.channel_type,
    case when bd.nationality in ('US','CA','NL','AU','GB','DE','BE') then bd.nationality else 'OT' end as country_code,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt,
    sum(nvl(case 
        when (bpa.effective_date <= t1.beg_dt and bpa.expiration_date > t1.beg_dt) then bpa.passenger_count
        else 0
    end,0)) as passenger_count_beg,
    sum(nvl(case 
        when (bpa.effective_date <= t1.beg_dt and bpa.expiration_date > t1.beg_dt) then bpa.passenger_count * vd.sea_days
        else 0
    end,0)) as pcd_beg,
    sum(nvl(case
        when (bpa.effective_date <= t1.beg_dt and bpa.expiration_date > t1.beg_dt) then bpa.ntr_fin_usd
        else 0
    end,0)) as ntr_fin_usd_beg,
    sum(nvl(case
        when (bpa.effective_date <= t1.beg_dt and bpa.expiration_date > t1.beg_dt) then (bpa.b_p_air_add_on + bpa.b_p_air_supplement)  * bpa.blended_conversion_rate
        else 0
    end,0))as air_rev_usd_beg,
    sum(nvl(case
        when (bpa.effective_date <= t1.beg_dt and bpa.expiration_date > t1.beg_dt) then bpa.b_p_waiver  * bpa.blended_conversion_rate
        else 0
    end,0)) as cpp_rev_usd_beg,
    sum(nvl(case
        when (bpa.effective_date <= t1.beg_dt and bpa.expiration_date > t1.beg_dt) then bpa.b_p_packages  * bpa.blended_conversion_rate
        else 0
    end,0)) as package_rev_usd_beg,
    sum(nvl(case 
        when (bpa.effective_date <= t1.end_dt and bpa.expiration_date > t1.end_dt) then bpa.passenger_count
        else 0
    end,0)) as passenger_count_end,
    sum(nvl(case 
        when (bpa.effective_date <= t1.end_dt and bpa.expiration_date > t1.end_dt) then bpa.passenger_count * vd.sea_days
        else 0
    end,0)) as pcd_end,
    sum(nvl(case
        when (bpa.effective_date <= t1.end_dt and bpa.expiration_date > t1.end_dt) then bpa.ntr_fin_usd
        else 0
    end,0)) as ntr_fin_usd_end,
    sum(nvl(case
        when (bpa.effective_date <= t1.end_dt and bpa.expiration_date > t1.end_dt) then (bpa.b_p_air_add_on + bpa.b_p_air_supplement)  * bpa.blended_conversion_rate
        else 0
    end,0)) as air_rev_usd_end,
    sum(nvl(case
        when (bpa.effective_date <= t1.end_dt and bpa.expiration_date > t1.end_dt) then bpa.b_p_waiver  * bpa.blended_conversion_rate
        else 0
    end,0)) as cpp_rev_usd_end,
    sum(nvl(case
        when (bpa.effective_date <= t1.end_dt and bpa.expiration_date > t1.end_dt) then bpa.b_p_packages  * bpa.blended_conversion_rate
        else 0
    end,0)) as package_rev_usd_end
from
    bkng_psngr_audit bpa,
    rdm_booking_d bd,
    rdm_voyage_d vd,
    t1
where bpa.b_p_passenger_status = 'A'
    and bpa.company_code = bd.company_code
    and bpa.b_bkng_nbr = bd.bkng_nbr
    and bpa.b_p_passenger_id = bd.pax_id
    and ((bpa.effective_date <= t1.beg_dt and bpa.expiration_date > t1.beg_dt)
        or (bpa.effective_date <= t1.end_dt and bpa.expiration_date > t1.end_dt))
    and bpa.company_code = vd.company_code
    and bpa.b_voyage = vd.voyage
    and bd.company_code = vd.company_code    
    and bpa.voyage_status in ('A', 'H')
    and bpa.cal_year >= t1.prior_year
    and bd.rpt_year >= t1.current_year
    and bd.channel_type = 'DIRECT WEB'
group by
    bpa.company_code,
    bd.rpt_year,
    vd.trade_description,
    bd.channel_type,
    case when bd.nationality in ('US','CA','NL','AU','GB','DE','BE') then bd.nationality else 'OT' end,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt
),
t3 as (
select t2.*,
    t2.passenger_count_end - t2.passenger_count_beg as passenger_count_gain,
    t2.pcd_end - t2.pcd_beg as pcd_gain,
    t2.ntr_fin_usd_end - t2.ntr_fin_usd_beg as ntr_fin_usd_gain,
    t2.air_rev_usd_end - t2.air_rev_usd_beg as air_rev_gain,
    t2.cpp_rev_usd_end - t2.cpp_rev_usd_beg as cpp_rev_gain,
    t2.package_rev_usd_end - t2.package_rev_usd_beg as package_rev_gain
from t2
),
t4 as (
select /*+ parallel */
    b.company_code,
    b.rpt_year,
    v.trade_description,
    b.channel_type,
    case when b.nationality in ('US','CA','NL','AU','GB','DE','BE') then b.nationality else 'OT' end as country_code,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt,
    sum(case when b.bkng_open_date between t1.beg2_dt and t1.end_dt then f.guest_count else 0 end) as guest_count_mo,
    sum(case when b.bkng_open_date between t1.beg2_dt and t1.end_dt then (f.guest_count * v.sea_days) else 0 end) as pcd_mo,
    sum(case when b.bkng_open_date between t1.beg2_dt and t1.end_dt then f.ntr_fin else 0 end) as ntr_fin_usd_mo,
    sum(case when b.bkng_open_date between t1.beg2_dt and t1.end_dt then f.hca_rev_usd_blended else 0 end) as air_rev_usd_mo,
    sum(case when b.bkng_open_date between t1.beg2_dt and t1.end_dt then f.cpp_rev_usd_blended else 0 end) as cpp_rev_usd_mo,
    sum(case when b.bkng_open_date between t1.beg2_dt and t1.end_dt then f.package_rev_usd_blended else 0 end) as package_rev_usd_mo,
    sum(f.guest_count) as guest_count,
    sum(f.guest_count * v.sea_days) as pcd,
    sum(f.ntr_fin) as ntr_fin_usd,
    sum(f.hca_rev_usd_blended) as air_rev_usd,
    sum(f.cpp_rev_usd_blended) as cpp_rev_usd,
    sum(f.package_rev_usd_blended) as package_rev_usd
from
    rdm_booking_d b,
    rdm_trans_f f,
    rdm_voyage_d v,
    t1
where
    b.bkng_id=f.bkng_id
    and v.voyage=f.voyage
    and b.bkng_status in ('B','G')
    and b.pax_status = 'A'
    and v.voyage_status in ('A','H')
    and v.rpt_year >= t1.current_year
    and f.rpt_year >= t1.current_year
    and v.rpt_year >= t1.current_year
    and b.channel_type = 'DIRECT WEB'
group by
    b.company_code,
    b.rpt_year,
    v.trade_description,
    b.channel_type,
    case when b.nationality in ('US','CA','NL','AU','GB','DE','BE') then b.nationality else 'OT' end,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt
),
t5 as (
select 
    shx.company_code,
    shx.rpt_year,
    vd.trade_description,
    'DIRECT WEB' as channel_type,
    case when bd.nationality in ('US','CA','NL','AU','GB','DE','BE') then bd.nationality else 'OT' end as country_code,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt,
    sum(case when shx.service_created between t1.beg2_dt and t1.end_dt then shx.shorex_rev else 0 end) as pp_shx_rev_mo,
    sum(shx.shorex_rev) as pp_shx_rev
from
        obr_prepaidshx_f shx,
        rdm_booking_d bd,
        rdm_voyage_d vd,
        t1
where shx.company_code = bd.company_code(+)
and shx.bkng_id = bd.bkng_id(+)
and shx.company_code = vd.company_code
and shx.voyage = vd.voyage
and shx.service_status in ('C','WP')
and shx.agent_id is null
and shx.rpt_year >= t1.current_year
and vd.rpt_year >= t1.current_year
group by
    shx.company_code,
    shx.rpt_year,
    vd.trade_description,
    'DIRECT WEB',
    case when bd.nationality in ('US','CA','NL','AU','GB','DE','BE') then bd.nationality else 'OT' end,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt
),
t6 as (
select 
    g.company_code,
    g.rpt_year,
    vd.trade_description,
    'DIRECT WEB' as channel_type,
    case when bd.nationality in ('US','CA','NL','AU','GB','DE','BE') then bd.nationality else 'OT' end as country_code,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt,
    sum(case when g.gift_created between t1.beg2_dt and t1.end_dt then g.gift_rev else 0 end) as pp_gift_rev_mo,
    sum(g.gift_rev) as pp_gift_rev
from
        obr_prepaidgift_f g,
        rdm_booking_d bd,
        rdm_voyage_d vd,
        t1
where g.company_code = bd.company_code(+)
and g.bkng_id = bd.bkng_id(+)
and g.company_code = vd.company_code
and g.voyage = vd.voyage
and g.gift_status in ('C','WP')
and g.agent_id is null
and g.rpt_year >= t1.current_year
and vd.rpt_year >= t1.current_year
group by
    g.company_code,
    g.rpt_year,
    vd.trade_description,
    'DIRECT WEB',
    case when bd.nationality in ('US','CA','NL','AU','GB','DE','BE') then bd.nationality else 'OT' end,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt
),
t7 as (
select 
    spa.company_code,
    vd.rpt_year,
    vd.trade_description,
    'DIRECT WEB' as channel_type,
    case when bd.nationality in ('US','CA','NL','AU','GB','DE','BE') then bd.nationality else 'OT' end as country_code,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt,
    sum(case when spa.service_created between t1.beg2_dt and t1.end_dt then spa.total_service_rev else 0 end) as pp_spa_rev_mo,
    sum(spa.total_service_rev) as pp_spa_rev
from
        obr_prepaidspa_f spa,
        rdm_voyage_d vd,
        rdm_booking_d bd,
        t1
where spa.company_code = vd.company_code
and spa.voyage = vd.voyage
and spa.company_code = bd.company_code(+)
and spa.bkng_id = bd.bkng_id(+)
and spa.service_status in ('C','WP')
and spa.agent_id is null
and vd.rpt_year >= t1.current_year
group by
    spa.company_code,
    vd.rpt_year,
    vd.trade_description,
    'DIRECT WEB',
    case when bd.nationality in ('US','CA','NL','AU','GB','DE','BE') then bd.nationality else 'OT' end,
    t1.beg_dt,
    t1.beg2_dt,
    t1.end_dt
),
t9 as (
select
    t3.company_code,
    t3.rpt_year,
    t3.trade_description,
    t3.channel_type,
    t3.country_code,
    t3.beg_dt,
    t3.beg2_dt,
    t3.end_dt,
    t3.passenger_count_beg,
    t3.pcd_beg,
    t3.ntr_fin_usd_beg,
    t3.air_rev_usd_beg,
    t3.cpp_rev_usd_beg,
    t3.package_rev_usd_beg,
    t3.passenger_count_end,
    t3.pcd_end,
    t3.ntr_fin_usd_end,
    t3.air_rev_usd_end,
    t3.cpp_rev_usd_end,
    t3.package_rev_usd_end,
    t3.passenger_count_gain,
    t3.pcd_gain,
    t3.ntr_fin_usd_gain,
    t3.air_rev_gain,
    t3.cpp_rev_gain,
    t3.package_rev_gain,
    0 as guest_count_mo,
    0 as pcd_mo,
    0 as ntr_fin_usd_mo,
    0 as air_rev_usd_mo,
    0 as cpp_rev_usd_mo,
    0 as package_rev_usd_mo,
    0 as pp_shx_rev_mo,
    0 as pp_gift_rev_mo,
    0 as pp_spa_rev_mo,
    0 as guest_count,
    0 as pcd,
    0 as ntr_fin_usd,
    0 as air_rev_usd,
    0 as cpp_rev_usd,
    0 as package_rev_usd,
    0 as pp_shx_rev,
    0 as pp_gift_rev,
    0 as pp_spa_rev
from t3
union all
select 
    t4.company_code,
    t4.rpt_year,
    t4.trade_description,
    t4.channel_type,
    t4.country_code,
    t4.beg_dt,
    t4.beg2_dt,
    t4.end_dt,
    0 as passenger_count_beg,
    0 as pcd_beg,
    0 as ntr_fin_usd_beg,
    0 as air_rev_usd_beg,
    0 as cpp_rev_usd_beg,
    0 as package_rev_usd_beg,
    0 as passenger_count_end,
    0 as pcd_end,
    0 as ntr_fin_usd_end,
    0 as air_rev_usd_end,
    0 as cpp_rev_usd_end,
    0 as package_rev_usd_end,
    0 as passenger_count_gain,
    0 as pcd_gain,
    0 as ntr_fin_usd_gain,
    0 as air_rev_gain,
    0 as cpp_rev_gain,
    0 as package_rev_gain,
    t4.guest_count_mo,
    t4.pcd_mo,
    t4.ntr_fin_usd_mo,
    t4.air_rev_usd_mo,
    t4.cpp_rev_usd_mo,
    t4.package_rev_usd_mo,
    0 as pp_shx_rev_mo,
    0 as pp_gift_rev_mo,
    0 as pp_spa_rev_mo,
    t4.guest_count,
    t4.pcd,
    t4.ntr_fin_usd,
    t4.air_rev_usd,
    t4.cpp_rev_usd,
    t4.package_rev_usd,
    0 as pp_shx_rev,
    0 as pp_gift_rev,
    0 as pp_spa_rev
from t4
union all
select 
    t5.company_code,
    t5.rpt_year,
    t5.trade_description,
    t5.channel_type,
    t5.country_code,
    t5.beg_dt,
    t5.beg2_dt,
    t5.end_dt,
    0 as passenger_count_beg,
    0 as pcd_beg,
    0 as ntr_fin_usd_beg,
    0 as air_rev_usd_beg,
    0 as cpp_rev_usd_beg,
    0 as package_rev_usd_beg,
    0 as passenger_count_end,
    0 as pcd_end,
    0 as ntr_fin_usd_end,
    0 as air_rev_usd_end,
    0 as cpp_rev_usd_end,
    0 as package_rev_usd_end,
    0 as passenger_count_gain,
    0 as pcd_gain,
    0 as ntr_fin_usd_gain,
    0 as air_rev_gain,
    0 as cpp_rev_gain,
    0 as package_rev_gain,
    0 as guest_count_mo,
    0 as pcd_mo,
    0 as ntr_fin_usd_mo,
    0 as air_rev_usd_mo,
    0 as cpp_rev_usd_mo,
    0 as package_rev_usd_mo,
    t5.pp_shx_rev_mo,
    0 as pp_gift_rev_mo,
    0 as pp_spa_rev_mo,
    0 as guest_count,
    0 as pcd,
    0 as ntr_fin_usd,
    0 as air_rev_usd,
    0 as cpp_rev_usd,
    0 as package_rev_usd,
    t5.pp_shx_rev,
    0 as pp_gift_rev,
    0 as pp_spa_rev
from t5
union all
select 
    t6.company_code,
    t6.rpt_year,
    t6.trade_description,
    t6.channel_type,
    t6.country_code,
    t6.beg_dt,
    t6.beg2_dt,
    t6.end_dt,
    0 as passenger_count_beg,
    0 as pcd_beg,
    0 as ntr_fin_usd_beg,
    0 as air_rev_usd_beg,
    0 as cpp_rev_usd_beg,
    0 as package_rev_usd_beg,
    0 as passenger_count_end,
    0 as pcd_end,
    0 as ntr_fin_usd_end,
    0 as air_rev_usd_end,
    0 as cpp_rev_usd_end,
    0 as package_rev_usd_end,
    0 as passenger_count_gain,
    0 as pcd_gain,
    0 as ntr_fin_usd_gain,
    0 as air_rev_gain,
    0 as cpp_rev_gain,
    0 as package_rev_gain,
    0 as guest_count_mo,
    0 as pcd_mo,
    0 as ntr_fin_usd_mo,
    0 as air_rev_usd_mo,
    0 as cpp_rev_usd_mo,
    0 as package_rev_usd_mo,
    0 as pp_shx_rev_mo,
    t6.pp_gift_rev_mo,
    0 as pp_spa_rev_mo,
    0 as guest_count,
    0 as pcd,
    0 as ntr_fin_usd,
    0 as air_rev_usd,
    0 as cpp_rev_usd,
    0 as package_rev,
    0 as pp_shx_rev,
    t6.pp_gift_rev,
    0 as pp_spa_rev
from t6
union all
select 
    t7.company_code,
    t7.rpt_year,
    t7.trade_description,
    t7.channel_type,
    t7.country_code,
    t7.beg_dt,
    t7.beg2_dt,
    t7.end_dt,
    0 as passenger_count_beg,
    0 as pcd_beg,
    0 as ntr_fin_usd_beg,
    0 as air_rev_usd_beg,
    0 as cpp_rev_usd_beg,
    0 as package_rev_usd_beg,
    0 as passenger_count_end,
    0 as pcd_end,
    0 as ntr_fin_usd_end,
    0 as air_rev_usd_end,
    0 as cpp_rev_usd_end,
    0 as package_rev_usd_end,
    0 as passenger_count_gain,
    0 as pcd_gain,
    0 as ntr_fin_usd_gain,
    0 as air_rev_gain,
    0 as cpp_rev_gain,
    0 as package_rev_gain,
    0 as guest_count_mo,
    0 as pcd_mo,
    0 as ntr_fin_usd_mo,
    0 as air_rev_usd_mo,
    0 as cpp_rev_usd_mo,
    0 as package_rev_usd_mo,
    0 as pp_shx_rev_mo,
    0 as pp_gift_rev_mo,
    t7.pp_spa_rev_mo,
    0 as guest_count,
    0 as pcd,
    0 as ntr_fin_usd,
    0 as air_rev_usd,
    0 as cpp_rev_usd,
    0 as package_rev_usd,
    0 as pp_shx_rev,
    0 as pp_gift_rev,
    t7.pp_spa_rev
from t7
)
select 
    t9.company_code,
    t9.rpt_year,
    t9.trade_description,
    t9.channel_type,
    t9.country_code,
    t9.beg_dt,
    t9.beg2_dt,
    t9.end_dt,
    sum(t9.passenger_count_beg) as passenger_count_beg,
    sum(t9.pcd_beg) as pcd_beg,
    sum(t9.ntr_fin_usd_beg) as ntr_fin_usd_beg,
    sum(t9.air_rev_usd_beg) as air_rev_usd_beg,
    sum(t9.cpp_rev_usd_beg) as cpp_rev_usd_beg,
    sum(t9.package_rev_usd_beg) as package_rev_usd_beg,
    sum(t9.passenger_count_end) as passenger_count_end,
    sum(t9.pcd_end) as pcd_end,
    sum(t9.ntr_fin_usd_end) as ntr_fin_usd_end,
    sum(t9.air_rev_usd_end) as air_rev_usd_end,
    sum(t9.cpp_rev_usd_end) as cpp_rev_usd_end,
    sum(t9.package_rev_usd_end) as package_rev_usd_end,
    sum(t9.passenger_count_gain) as passenger_count_gain,
    sum(t9.pcd_gain) as pcd_gain,
    sum(t9.ntr_fin_usd_gain) as ntr_fin_usd_gain,
    sum(t9.air_rev_gain) as air_rev_gain,
    sum(t9.cpp_rev_gain) as cpp_rev_gain,
    sum(t9.package_rev_gain) as package_rev_gain,
    sum(t9.guest_count_mo) as guest_count_mtd,
    sum(t9.pcd_mo) as pcd_mtd,
    sum(t9.ntr_fin_usd_mo) as ntr_fin_usd_mtd,
    sum(t9.air_rev_usd_mo) as air_rev_usd_mtd,
    sum(t9.cpp_rev_usd_mo) as cpp_rev_usd_mtd,
    sum(t9.package_rev_usd_mo) as package_rev_usd_mtd,
    sum(t9.pp_shx_rev_mo) as pp_shx_rev_mtd,
    sum(t9.pp_gift_rev_mo) as pp_gift_rev_mtd,
    sum(t9.pp_spa_rev_mo) as pp_spa_rev_mtd,
    sum(t9.guest_count) as guest_count_ytd,
    sum(t9.pcd) as pcd_ytd,
    sum(t9.ntr_fin_usd) as ntr_fin_usd_ytd,
    sum(t9.air_rev_usd) as air_rev_usd_ytd,
    sum(t9.cpp_rev_usd) as cpp_rev_usd_ytd,
    sum(t9.package_rev_usd) as package_rev_usd_ytd,
    sum(t9.pp_shx_rev) as pp_shx_rev_ytd,
    sum(t9.pp_gift_rev) as pp_gift_rev_ytd,
    sum(t9.pp_spa_rev) as pp_spa_rev_ytd
from t9
group by
    t9.company_code,
    t9.rpt_year,
    t9.trade_description,
    t9.channel_type,
    t9.country_code,
    t9.beg_dt,
    t9.beg2_dt,
    t9.end_dt    
order by
    t9.company_code,
    t9.rpt_year,
    t9.trade_description,
    t9.channel_type,
    t9.country_code,
    t9.beg_dt,
    t9.beg2_dt,
    t9.end_dt  ;      

 x:=SQL%ROWCOUNT ;        
        sComment  := 'web_bkng_activity Inserts: '||x;
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
                       
  END WEB_BKNG_ACTIVITY_LOAD; 
  
 PROCEDURE SLS_DIRECTOR_AGENCY_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'sls_director_agency';
        BatchID Number := 0;
         
  BEGIN
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF; 
         
        EXECUTE IMMEDIATE 'ALTER TABLE sls_director_agency NOLOGGING';
               
        EXECUTE IMMEDIATE 'ALTER TABLE sls_director_agency NOLOGGING';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE sls_director_agency'; 

        EXECUTE IMMEDIATE 'TRUNCATE TABLE sls_director_agency'; 

        INSERT INTO sls_director_agency 
        with t1 as
        (
        select 
            a.company_code,
            a.assoc_year,
            a.super_assoc,
            a.assoc_id,
            a.agency_id,
            a.agency_nbr,
            case
                when a.agency_nbr = s.agency_nbr then s.director else null
            end as director
        from rdm_agent_d a,
                dm_usr.usr_slsnatlacctselectionlist s
        where a.agency_nbr = s.agency_nbr(+)
        and a.assoc_year = (SELECT TO_CHAR (SYSDATE, 'YYYY') FROM DUAL)
        )
        , t2 as (
        select
            t1.company_code,
            t1.assoc_year,
            t1.super_assoc,
            t1.assoc_id,
            t1.agency_id,
            t1.agency_nbr,
            case
                when (t1.director is null and t1.assoc_id = s.assoc_id) then s.director else t1.director
            end as director
        from
            t1,
            dm_usr.usr_slsnatlacctselectionlist s
        where  t1.assoc_id = s.assoc_id(+)
        )
        , t3 as (
        select
            t2.company_code as company_cd,
            t2.agency_id,
            t2.agency_nbr,
            t2.assoc_year,
            t2.super_assoc,
            t2.assoc_id,
            case
                when (t2.director is null and t2.super_assoc = s.super_assoc) then s.director else t2.director
            end as director
        from
            t2,
            dm_usr.usr_slsnatlacctselectionlist s
        where t2.super_assoc = s.super_assoc(+)
        )
        select 
            t3.company_cd,
            t3.agency_id,
            t3.agency_nbr,
            t3.assoc_year,
            t3.super_assoc,
            t3.assoc_id,
            t3.director,
            case
                when t3.director is null then 'N' else 'Y'
            end as natl_acct_flg
        from t3
        order by
            t3.agency_id
        ;

        x:=SQL%ROWCOUNT ;        
        sComment  := 'sls_director_agency Inserts: '||x;
            
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
  
        /* Record Completion in Control File */
        IF  DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,                       
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM,   
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'C', 
                    in_Start_Dt_Parm => dBeg_Date,             
                    in_End_Dt_Parm => dEnd_Date 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF; 
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID ,                       
                            in_Schema_Name => cSchema_NM ,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM,   
                            in_Load_Type => 'D' ,         
                            in_Prog_Status => 'X' , 
                            in_Start_Dt_Parm => dBeg_Date ,      
                            in_End_Dt_Parm => dEnd_Date   
                            );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
                    x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                            in_Batch_ID => BatchID,  
                            in_Schema_Name => cSchema_NM,  
                            in_Package_Name => cPackage_NM, 
                            in_Program_Name => cProcedure_NM, 
                            in_Load_Type => 'D',         
                            in_Prog_Status  => 'F', 
                            in_Start_Dt_Parm => dBeg_Date,  
                            in_End_Dt_Parm => dEnd_Date  
                            );
            END IF ;
            COMMIT; 
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            x:=   DM_INT.COMMON_JOBS.LOG_PROC_CTRL (
                    in_Batch_ID => BatchID,  
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM, 
                    in_Program_Name => cProcedure_NM, 
                    in_Load_Type => 'D',         
                    in_Prog_Status => 'F' , 
                    in_Start_Dt_Parm => dBeg_Date,  
                    in_End_Dt_Parm => dEnd_Date              
                    );
            COMMIT;
            RAISE;
                       
  END SLS_DIRECTOR_AGENCY_LOAD; 

   PROCEDURE END_LOAD IS 
        cProcedure_NM  CONSTANT  Varchar2(30) := 'END_LOAD';
        BatchID Number := 0;
         
  BEGIN
  
         /* Log program start */ 
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,    
                    in_Schema_Name => cSchema_NM,  
                    in_Package_Name => cPackage_NM,
                    in_Program_Name => cProcedure_NM, 
                    in_Prog_Status => 'I',  
                    in_Comments => 'Job in Progress', 
                    in_Error_Message => 'NA'
                    ) != 0 THEN                  
            RAISE eCALL_FAILED;      
        END IF;
        
       sComment  := 'Revenue Data Mart load has ended ';        
        
        /* Record Completion in Log */
        IF  DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'C',  
                    in_Comments => sComment,  
                    in_Error_Message => 'NA' 
                    ) != 0 THEN
            RAISE eCALL_FAILED;            
        END IF;      
        
  EXCEPTION  
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';                  
            
            IF BatchID = 0 THEN                                                                           
                    x:=-1 ;
                    /* Record error against placeholder w/ batch of -1 since batch not recorded */
                    x:=  DM_INT.COMMON_JOBS.LOG_JOB (
                                io_Batch_ID => x ,   
                                in_Prog_Status => 'X',  
                                in_Comments => 'Job Failed logged to placeholder  ', 
                                in_Error_Message => eErrMsg          
                                );
            ELSE
                    /* Record error w/ the assigned batch ID */ 
                    x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                            io_Batch_ID => BatchID,   
                            in_Prog_Status => 'F',  
                            in_Comments => 'Job Failed',  
                            in_Error_Message => eErrMsg 
                            );
            END IF ;
            COMMIT;
            RAISE eCALL_FAILED;     
             
      WHEN OTHERS THEN             
            /* Error trap and logging w/ generic handler */
            eErrMsg :=  SQLERRM;         
            dbms_output.put_line(eErrMSG);                                   
            ROLLBACK;
                        
            /* Record error w/ the assigned batch ID */
            x:=   DM_INT.COMMON_JOBS.LOG_JOB (
                    io_Batch_ID => BatchID,   
                    in_Prog_Status => 'F',  
                    in_Comments => 'Job Failed',  
                    in_Error_Message => eErrMsg 
                    );  
            COMMIT;
            RAISE;  
               
END END_LOAD;   
  
END RDM_LOAD;
/