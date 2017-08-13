DECLARE 
x number;
s varchar2(300);
eErrMsg Varchar2(500);

BEGIN
dbms_output.enable;

MERGE INTO bkng_psngr_audit bpa
USING init_bpa_ntr_old upd
ON (bpa.b_bkng_nbr = upd.b_bkng_nbr
    AND bpa.b_voyage = upd.b_voyage
    AND bpa.b_p_passenger_id = upd.b_p_passenger_id
    AND bpa.effective_date = upd.effective_date
    AND bpa.company_code = upd.company_code)
WHEN MATCHED THEN
UPDATE SET bpa.npr_usd = upd.npr_usd,
          bpa.ntr_fin_usd = upd.ntr_fin_usd,
          bpa.ntr_fin_bkng_curr = upd.ntr_fin_bkng_curr,
          bpa.ntr_sls_usd = upd.ntr_sls_usd,
          bpa.ntr_sls_bkng_curr = upd.ntr_sls_bkng_curr;
          
x:=SQL%ROWCOUNT;  
dbms_output.put_line('Records Affected '||x);

EXCEPTION
WHEN OTHERS THEN
    s := SQLCODE ;
        eErrMsg :=  SQLERRM;
   dbms_output.put_line('Failed  '||s||'   '||eErrMsg );

RAISE;
     
END;
COMMIT;
/

---------------------------------------------------------------------------------
/* UPDATE and INSERT */

MERGE INTO VOYAGE_MSTR_STATUS VMS
USING (SELECT /*+ PARALLEL(T1,8) PARALLEL(T2,8) */ 
T1.VYD_VOYAGE, T1.COMPANY_CODE, T1.VYD_VOYAGE_STATUS, T1.FISCAL_SAIL_DATE 
FROM VOYAGE_MSTR T1,
     VOYAGE_ARCHIVE_STATUS T2
WHERE VOYAGE_ARCHIVE_STATUS <> 'P'
  AND T1.VYD_VOYAGE = T2.VYD_VOYAGE
  AND T1.COMPANY_CODE = T2.COMPANY_CODE
) VSL
ON (VMS.VYD_VOYAGE = VSL.VYD_VOYAGE
    AND VMS.COMPANY_CODE = VSL.COMPANY_CODE)
WHEN MATCHED THEN
UPDATE SET 
           VMS.VYD_VOYAGE_STATUS = VSL.VYD_VOYAGE_STATUS,
           VMS.FISCAL_SAIL_DATE = VSL.FISCAL_SAIL_DATE
WHEN NOT MATCHED THEN
INSERT (VMS.VYD_VOYAGE, VMS.COMPANY_CODE, VMS.VYD_VOYAGE_STATUS, VMS.FISCAL_SAIL_DATE)
VALUES (VSL.VYD_VOYAGE, VSL.COMPANY_CODE, VSL.VYD_VOYAGE_STATUS, VSL.FISCAL_SAIL_DATE);
---------------------------------------------------------------------------------
SQL 1 :
MERGE INTO prddw.w_HOUSEHOLD_D  t1
USING 
(SELECT   HOUSEHOLD_WID, CONTACT_WID, TOTAL_DAYS, LOY_RANK FROM (
SELECT  /*+PARALLEL(F2,8)*/
                   F2.HOUSEHOLD_WID,
                   F2.CONTACT_WID,
                   D2.TOTAL_DAYS,
                                      RANK() OVER (PARTITION BY F2.HOUSEHOLD_WID ORDER BY D2.TOTAL_DAYS DESC NULLS LAST, F2.X_HH_SEQ_NUM NULLS LAST, F2.ROW_WID) LOY_RANK,
                                      F2.X_HH_SEQ_NUM
          FROM   
                   PRDDW.W_HOUSEHOLD_F F2,
                   PRDDW.WC_LOYMEMBER_D D2
          WHERE  
                      F2.HOUSEHOLD_WID <> 0
          AND      F2.CONTACT_WID = D2.CONTACT_WID(+)
         and  F2.HOUSEHOLD_WID  in ( select distinct household_wid from w_household_f a, PRDDW.WC_PERSON_PCC b where A.CONTACT_WID = b.contact_wid AND  a.END_DT_WID > TO_CHAR(CURRENT_DATE,'YYYYMMDD')) 
                  AND      F2.X_HH_SEQ_NUM <> 999)
         WHERE LOY_RANK = 1 ) t2
ON (t1.row_wid = t2.HOUSEHOLD_WID  )
WHEN MATCHED THEN
UPDATE SET t1.X_PCCFLG  = 'Y';
---------------------------------------------------------------------------------
SQL 2:
MERGE INTO prddw.WC_LOYTXN_F t1
USING 
(
SELECT /*+parallel(F,6)*/
f.row_wid row_wid, p.pr_household_wid pr_household_wid
FROM prddw.w_person_d p,  prddw.WC_LOYTXN_F f
WHERE f.contact_wid = p.row_wid
AND f.household_wid <> p.pr_household_wid
) t2
ON (t1.row_wid = t2.row_wid )
WHEN MATCHED THEN
UPDATE SET t1.HOUSEHOLD_WID = t2.PR_HOUSEHOLD_WID;
/*************************UPDATE W_CAMP_HIST_F WITH PR_HOUSEHOLD_WID**************************************/
SQL3:

while v_min_date_wid  <= var_max_camp_drop_dt_wid    LOOP 
    
    BEGIN
    execute immediate 'truncate table w_camp_hist_fs_upd';
    END;
      
    insert into prddw.w_camp_hist_fs_upd
    select /*+parallel(F,6)*/
        f.row_wid row_wid, 
         f.x_camp_drop_dt_wid ,
        p.pr_household_wid pr_household_wid
        from  prddw.w_person_d p,  prddw.w_camp_hist_f f
    where f.contact_wid = p.row_wid
    and f.x_household_wid <> p.pr_household_wid
    and f.x_camp_drop_dt_wid >= v_min_date_wid
    and f.x_camp_drop_dt_wid <  v_max_date_wid;
    
    COMMIT;
    
    BEGIN
            DBMS_STATS.GATHER_TABLE_STATS(  'PRDDW',
                                            'W_CAMP_HIST_FS_UPD', 
                                            method_opt => 'FOR ALL INDEXED COLUMNS SIZE AUTO', 
                                            granularity => 'ALL', 
                                            ESTIMATE_PERCENT=> 60, 
                                            CASCADE=>TRUE, 
                                            DEGREE=> 2);    
    END;

    merge into (select * from w_camp_hist_f where x_camp_drop_dt_wid >= v_min_date_wid
    and x_camp_drop_dt_wid <  v_max_date_wid) t1
    using w_camp_hist_fs_upd t2
    on (t1.row_wid = t2.row_wid)
    when matched then
    update set t1.x_household_wid = t2.pr_household_wid;
---------------------------------------------------------------------------------------------------------------------------
--MERGE INTO bkng_psngr_audit bpa
MERGE INTO ROBERT_OLD_NTR_TESTING bpa
USING 
(
SELECT /*+ PARALLEL(bpa,8) PARALLEL(bm,8) PARALLEL(vm,8) */
         bpa.b_bkng_nbr,
         bpa.b_voyage,
         bpa.b_p_passenger_id,
         bpa.effective_date,
         bpa.company_code,
         bpa.expiration_date,
--NPR
         ROUND (
         bpa.b_p_fare * bpa.blended_conversion_rate
         + bpa.b_p_discount * bpa.blended_conversion_rate
         + bpa.b_p_fcc_discount * bpa.blended_conversion_rate
         + bpa.b_p_air_add_on * bpa.blended_conversion_rate
         + bpa.b_p_air_supplement * bpa.blended_conversion_rate
         + bpa.b_p_air_fees * bpa.blended_conversion_rate
          - bpa.b_p_actual_air_cost / bpa.b_local_per_base_rate
          - nvl(bpa.pcom_netf_base_amt,0) * bpa.blended_conversion_rate
          - nvl(bpa.pcom_netf_ovr_amt,0) * bpa.blended_conversion_rate
          - nvl(bpa.pcom_land_base_amt,0) * bpa.blended_conversion_rate
          - nvl(bpa.pcom_land_ovr_amt,0) * bpa.blended_conversion_rate
          - nvl(bpa.pcom_air_add_base_amt,0) * bpa.blended_conversion_rate
          - nvl(bpa.pcom_air_add_ovr_amt,0) * bpa.blended_conversion_rate
          - nvl(bpa.pcom_air_upg_base_amt,0) * bpa.blended_conversion_rate
          - nvl(bpa.pcom_air_upg_ovr_amt,0) * bpa.blended_conversion_rate
          - nvl(bpa.pcom_bonus_comm_amt,0) * bpa.blended_conversion_rate
          - bpa.std_land_cost / bpa.b_local_per_base_rate
          - bpa.b_p_onboard_credit_amt
          - bpa.rev_adj_act_gap
          - bpa.rev_adj_cocktail_pty
          - bpa.b_p_air_fees_taxes_to * bpa.blended_conversion_rate
          - bpa.b_p_air_fees_taxes_fr * bpa.blended_conversion_rate
          - nvl(bpa.b_p_wholesaler_comm_portion,0) * bpa.blended_conversion_rate
          - bpa.rev_adj_act_ammenities
          - bpa.rev_adj_proj_gap
          - bpa.rev_adj_proj_tc
          - nvl(bpa.rev_adj_proj_ovr_comm_amt,0),
          2) as npr_usd,
--NTR Finance
          ROUND (
                   (CASE
                      WHEN bm.b_tours_indicator IN ('T', 'L')
                      THEN
                         (  bpa.ct_ship_cost_amount
                          + bpa.ct_supplemental_cost
                          + bpa.ct_nda
                          + bpa.ct_tax_rev
                          - bpa.ct_tax_cost)
                         + (bpa.b_p_fuel_surcharge * bpa.blended_conversion_rate)
                         - (bpa.fuel_ta_fee / bpa.b_local_per_base_rate)
                      ELSE
                         (  bpa.b_p_fare
                         + bpa.b_p_discount
                         + bpa.b_p_fcc_discount
                         + bpa.b_p_air_add_on
                         + bpa.b_p_air_supplement
                         + bpa.b_p_air_fees
                          - bpa.b_p_air_fees_taxes_to
                          - bpa.b_p_air_fees_taxes_fr
                          - NVL (bpa.pcom_netf_base_amt, 0)
                          - NVL (bpa.pcom_netf_ovr_amt, 0)
                          - NVL (bpa.pcom_land_base_amt, 0)
                          - NVL (bpa.pcom_land_ovr_amt, 0)
                          - NVL (bpa.pcom_air_add_base_amt, 0)
                          - NVL (bpa.pcom_air_add_ovr_amt, 0)
                          - NVL (bpa.pcom_air_upg_base_amt, 0)
                          - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                          - NVL (bpa.pcom_bonus_comm_amt, 0)
                          - NVL (bpa.b_p_wholesaler_comm_portion, 0)) * bpa.blended_conversion_rate
                          - bpa.b_p_actual_air_cost / bpa.b_local_per_base_rate
                          - bpa.std_land_cost / bpa.b_local_per_base_rate
                          - bpa.b_p_onboard_credit_amt
                          - bpa.rev_adj_act_gap
                          - bpa.rev_adj_cocktail_pty
                          - bpa.rev_adj_act_ammenities
                          - bpa.rev_adj_proj_gap
                          - bpa.rev_adj_proj_tc
                          - NVL (bpa.rev_adj_proj_ovr_comm_amt, 0)
                          + bpa.tax_margin
                          + (bpa.b_p_port_charges * bpa.blended_conversion_rate)
                          + (bpa.b_p_fuel_surcharge * bpa.blended_conversion_rate)
                   END),
             2) AS ntr_fin_usd,
--NTR Finance in Booking Currency
          ROUND (
                      (CASE
                         WHEN bm.b_tours_indicator IN ('T', 'L')
                         THEN
                            (bpa.ct_ship_cost_amount
                             + bpa.ct_supplemental_cost
                             + bpa.ct_nda
                             + bpa.ct_tax_rev
                             - bpa.ct_tax_cost) / bpa.blended_conversion_rate
                            + bpa.b_p_fuel_surcharge
                            - bpa.fuel_ta_fee
                         ELSE
                           (  bpa.b_p_fare
                           + bpa.b_p_discount
                           + bpa.b_p_fcc_discount
                           + bpa.b_p_air_add_on
                           + bpa.b_p_air_supplement
                           + bpa.b_p_air_fees
                            - bpa.b_p_air_fees_taxes_to
                            - bpa.b_p_air_fees_taxes_fr
                            - NVL (bpa.pcom_netf_base_amt, 0)
                            - NVL (bpa.pcom_netf_ovr_amt, 0)
                            - NVL (bpa.pcom_land_base_amt, 0)
                            - NVL (bpa.pcom_land_ovr_amt, 0)
                            - NVL (bpa.pcom_air_add_base_amt, 0)
                            - NVL (bpa.pcom_air_add_ovr_amt, 0)
                            - NVL (bpa.pcom_air_upg_base_amt, 0)
                            - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                            - NVL (bpa.pcom_bonus_comm_amt, 0)
                            - NVL (bpa.b_p_wholesaler_comm_portion, 0))
                            - bpa.b_p_actual_air_cost
                            - bpa.std_land_cost
                            - bpa.b_p_onboard_credit_amt / bpa.blended_conversion_rate
                            - bpa.rev_adj_act_gap / bpa.blended_conversion_rate
                            - bpa.rev_adj_cocktail_pty / bpa.blended_conversion_rate
                            - bpa.rev_adj_act_ammenities / bpa.blended_conversion_rate 
                            - bpa.rev_adj_proj_gap / bpa.blended_conversion_rate
                            - bpa.rev_adj_proj_tc / bpa.blended_conversion_rate
                            - (nvl(bpa.rev_adj_proj_ovr_comm_amt,0) / bpa.blended_conversion_rate)
                            + bpa.tax_margin / bpa.blended_conversion_rate
                            + bpa.b_p_port_charges
                            + bpa.b_p_fuel_surcharge
                      END),
                2) AS ntr_fin_bkng_curr,
--NTR Sales
          ROUND (
             (CASE
                WHEN bm.b_tours_indicator IN ('T', 'L')
                THEN
                   (CASE
                      WHEN bm.b_fsc_refunded_flag = 'Y'
                      THEN
                         (  bpa.b_p_fare
                          + bpa.b_p_discount
                          + bpa.b_p_fcc_discount
                          + bpa.b_p_air_add_on
                          + bpa.b_p_air_supplement
                          + bpa.b_p_air_fees
                          - bpa.b_p_air_fees_taxes_to
                          - bpa.b_p_air_fees_taxes_fr
                          - NVL (bpa.pcom_netf_base_amt, 0)
                          - NVL (bpa.pcom_netf_ovr_amt, 0)
                          - NVL (bpa.pcom_land_base_amt, 0)
                          - NVL (bpa.pcom_land_ovr_amt, 0)
                          - NVL (bpa.pcom_air_add_base_amt, 0)
                          - NVL (bpa.pcom_air_add_ovr_amt, 0)
                          - NVL (bpa.pcom_air_upg_base_amt, 0)
                          - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                          - NVL (bpa.pcom_bonus_comm_amt, 0)
                          - NVL (bpa.b_p_wholesaler_comm_portion, 0)) * bpa.blended_conversion_rate
                         - bpa.b_p_actual_air_cost / bpa.b_local_per_base_rate
                         - bpa.std_land_cost / bpa.b_local_per_base_rate
                         - bpa.b_p_onboard_credit_amt
                         - bpa.rev_adj_act_gap
                         - bpa.rev_adj_cocktail_pty
                         - bpa.rev_adj_act_ammenities
                         - bpa.rev_adj_proj_gap
                         - bpa.rev_adj_proj_tc
                         - NVL (bpa.rev_adj_proj_ovr_comm_amt, 0)
                         + (bpa.b_p_fuel_surcharge * bpa.blended_conversion_rate)
                         + (bpa.b_p_port_charges + bpa.b_p_land_charges) * bpa.blended_conversion_rate
                         + bpa.std_land_cost / bpa.b_local_per_base_rate
                      ELSE
                         (  bpa.b_p_fare
                          + bpa.b_p_discount
                          + bpa.b_p_fcc_discount
                          + bpa.b_p_air_add_on
                          + bpa.b_p_air_supplement
                          + bpa.b_p_air_fees
                          - bpa.b_p_air_fees_taxes_to
                          - bpa.b_p_air_fees_taxes_fr
                          - NVL (bpa.pcom_netf_base_amt, 0)
                          - NVL (bpa.pcom_netf_ovr_amt, 0)
                          - NVL (bpa.pcom_land_base_amt, 0)
                          - NVL (bpa.pcom_land_ovr_amt, 0)
                          - NVL (bpa.pcom_air_add_base_amt, 0)
                          - NVL (bpa.pcom_air_add_ovr_amt, 0)
                          - NVL (bpa.pcom_air_upg_base_amt, 0)
                          - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                          - NVL (bpa.pcom_bonus_comm_amt, 0)
                          - NVL (bpa.b_p_wholesaler_comm_portion, 0)) * bpa.blended_conversion_rate
                         - bpa.b_p_actual_air_cost / bpa.b_local_per_base_rate
                         - bpa.std_land_cost / bpa.b_local_per_base_rate
                         - bpa.b_p_onboard_credit_amt
                         - bpa.rev_adj_act_gap
                         - bpa.rev_adj_cocktail_pty
                         - bpa.rev_adj_act_ammenities
                         - bpa.rev_adj_proj_gap
                         - bpa.rev_adj_proj_tc
                         - NVL (bpa.rev_adj_proj_ovr_comm_amt, 0)
                         + (bpa.b_p_port_charges + bpa.b_p_land_charges) * bpa.blended_conversion_rate
                         + bpa.std_land_cost / bpa.b_local_per_base_rate
                   END)
                ELSE
                   (CASE
                      WHEN bm.b_fsc_refunded_flag = 'Y'
                      THEN
                         (  bpa.b_p_fare
                          + bpa.b_p_discount
                          + bpa.b_p_fcc_discount
                          + bpa.b_p_air_add_on
                          + bpa.b_p_air_supplement
                          + bpa.b_p_air_fees
                          - bpa.b_p_air_fees_taxes_to
                          - bpa.b_p_air_fees_taxes_fr
                          - NVL (bpa.pcom_netf_base_amt, 0)
                          - NVL (bpa.pcom_netf_ovr_amt, 0)
                          - NVL (bpa.pcom_land_base_amt, 0)
                          - NVL (bpa.pcom_land_ovr_amt, 0)
                          - NVL (bpa.pcom_air_add_base_amt, 0)
                          - NVL (bpa.pcom_air_add_ovr_amt, 0)
                          - NVL (bpa.pcom_air_upg_base_amt, 0)
                          - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                          - NVL (bpa.pcom_bonus_comm_amt, 0)
                          - NVL (bpa.b_p_wholesaler_comm_portion, 0)) * bpa.blended_conversion_rate
                         - bpa.b_p_actual_air_cost / bpa.b_local_per_base_rate
                         - bpa.std_land_cost / bpa.b_local_per_base_rate
                         - bpa.b_p_onboard_credit_amt
                         - bpa.rev_adj_act_gap
                         - bpa.rev_adj_cocktail_pty
                         - bpa.rev_adj_act_ammenities
                         - bpa.rev_adj_proj_gap
                         - bpa.rev_adj_proj_tc
                         - NVL (bpa.rev_adj_proj_ovr_comm_amt, 0)
                         + (bpa.b_p_fuel_surcharge * bpa.blended_conversion_rate)
                         + (bpa.b_p_port_charges * bpa.blended_conversion_rate)
                         + bpa.tax_margin
                      ELSE
                         (  bpa.b_p_fare
                          + bpa.b_p_discount
                          + bpa.b_p_fcc_discount
                          + bpa.b_p_air_add_on
                          + bpa.b_p_air_supplement
                          + bpa.b_p_air_fees
                          - bpa.b_p_air_fees_taxes_to
                          - bpa.b_p_air_fees_taxes_fr
                          - NVL (bpa.pcom_netf_base_amt, 0)
                          - NVL (bpa.pcom_netf_ovr_amt, 0)
                          - NVL (bpa.pcom_land_base_amt, 0)
                          - NVL (bpa.pcom_land_ovr_amt, 0)
                          - NVL (bpa.pcom_air_add_base_amt, 0)
                          - NVL (bpa.pcom_air_add_ovr_amt, 0)
                          - NVL (bpa.pcom_air_upg_base_amt, 0)
                          - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                          - NVL (bpa.pcom_bonus_comm_amt, 0)
                          - NVL (bpa.b_p_wholesaler_comm_portion, 0))
                         * bpa.blended_conversion_rate
                         - bpa.b_p_actual_air_cost / bpa.b_local_per_base_rate
                         - bpa.std_land_cost / bpa.b_local_per_base_rate
                         - bpa.b_p_onboard_credit_amt
                         - bpa.rev_adj_act_gap
                         - bpa.rev_adj_cocktail_pty
                         - bpa.rev_adj_act_ammenities
                         - bpa.rev_adj_proj_gap
                         - bpa.rev_adj_proj_tc
                         - NVL (bpa.rev_adj_proj_ovr_comm_amt, 0)
                         + (bpa.b_p_port_charges * bpa.blended_conversion_rate)
                         + bpa.tax_margin
                   END)
             END),
             2) AS ntr_sls_usd,
--NTR Sales in Booking Currency
          ROUND (
             (CASE
                WHEN bm.b_tours_indicator IN ('T', 'L')
                THEN
                   (CASE
                      WHEN bm.b_fsc_refunded_flag = 'Y'
                      THEN
                           bpa.b_p_fare
                         + bpa.b_p_discount
                         + bpa.b_p_fcc_discount
                         + bpa.b_p_air_add_on
                         + bpa.b_p_air_supplement
                         + bpa.b_p_air_fees
                         - bpa.b_p_air_fees_taxes_to
                         - bpa.b_p_air_fees_taxes_fr
                         - NVL (bpa.pcom_netf_base_amt, 0)
                         - NVL (bpa.pcom_netf_ovr_amt, 0)
                         - NVL (bpa.pcom_land_base_amt, 0)
                         - NVL (bpa.pcom_land_ovr_amt, 0)
                         - NVL (bpa.pcom_air_add_base_amt, 0)
                         - NVL (bpa.pcom_air_add_ovr_amt, 0)
                         - NVL (bpa.pcom_air_upg_base_amt, 0)
                         - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                         - NVL (bpa.pcom_bonus_comm_amt, 0)
                         - NVL (bpa.b_p_wholesaler_comm_portion, 0)
                         - bpa.b_p_actual_air_cost
                         - bpa.std_land_cost
                         - bpa.b_p_onboard_credit_amt / bpa.blended_conversion_rate
                         - bpa.rev_adj_act_gap / bpa.blended_conversion_rate
                         - bpa.rev_adj_cocktail_pty / bpa.blended_conversion_rate
                         - bpa.rev_adj_act_ammenities / bpa.blended_conversion_rate
                         - bpa.rev_adj_proj_gap / bpa.blended_conversion_rate
                         - bpa.rev_adj_proj_tc / bpa.blended_conversion_rate
                         - NVL (bpa.rev_adj_proj_ovr_comm_amt, 0) / bpa.blended_conversion_rate
                         + bpa.b_p_fuel_surcharge
                         + bpa.b_p_port_charges
                         + bpa.b_p_land_charges
                         + bpa.std_land_cost
                      ELSE
                           bpa.b_p_fare
                         + bpa.b_p_discount
                         + bpa.b_p_fcc_discount
                         + bpa.b_p_air_add_on
                         + bpa.b_p_air_supplement
                         + bpa.b_p_air_fees
                         - bpa.b_p_air_fees_taxes_to
                         - bpa.b_p_air_fees_taxes_fr
                         - NVL (bpa.pcom_netf_base_amt, 0)
                         - NVL (bpa.pcom_netf_ovr_amt, 0)
                         - NVL (bpa.pcom_land_base_amt, 0)
                         - NVL (bpa.pcom_land_ovr_amt, 0)
                         - NVL (bpa.pcom_air_add_base_amt, 0)
                         - NVL (bpa.pcom_air_add_ovr_amt, 0)
                         - NVL (bpa.pcom_air_upg_base_amt, 0)
                         - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                         - NVL (bpa.pcom_bonus_comm_amt, 0)
                         - NVL (bpa.b_p_wholesaler_comm_portion, 0)
                         - bpa.b_p_actual_air_cost
                         - bpa.std_land_cost
                         - bpa.b_p_onboard_credit_amt / bpa.blended_conversion_rate
                         - bpa.rev_adj_act_gap / bpa.blended_conversion_rate
                         - bpa.rev_adj_cocktail_pty / bpa.blended_conversion_rate
                         - bpa.rev_adj_act_ammenities / bpa.blended_conversion_rate
                         - bpa.rev_adj_proj_gap / bpa.blended_conversion_rate
                         - bpa.rev_adj_proj_tc / bpa.blended_conversion_rate
                         - NVL (bpa.rev_adj_proj_ovr_comm_amt, 0) / bpa.blended_conversion_rate
                         + bpa.b_p_port_charges
                         + bpa.b_p_land_charges
                         + bpa.std_land_cost
                   END)
                ELSE
                   (CASE
                      WHEN bm.b_fsc_refunded_flag = 'Y'
                      THEN
                           bpa.b_p_fare
                         + bpa.b_p_discount
                         + bpa.b_p_fcc_discount
                         + bpa.b_p_air_add_on
                         + bpa.b_p_air_supplement
                         + bpa.b_p_air_fees
                         - bpa.b_p_air_fees_taxes_to
                         - bpa.b_p_air_fees_taxes_fr
                         - NVL (bpa.pcom_netf_base_amt, 0)
                         - NVL (bpa.pcom_netf_ovr_amt, 0)
                         - NVL (bpa.pcom_land_base_amt, 0)
                         - NVL (bpa.pcom_land_ovr_amt, 0)
                         - NVL (bpa.pcom_air_add_base_amt, 0)
                         - NVL (bpa.pcom_air_add_ovr_amt, 0)
                         - NVL (bpa.pcom_air_upg_base_amt, 0)
                         - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                         - NVL (bpa.pcom_bonus_comm_amt, 0)
                         - NVL (bpa.b_p_wholesaler_comm_portion, 0)
                         - bpa.b_p_actual_air_cost
                         - bpa.std_land_cost
                         - bpa.b_p_onboard_credit_amt / bpa.blended_conversion_rate
                         - bpa.rev_adj_act_gap / bpa.blended_conversion_rate
                         - bpa.rev_adj_cocktail_pty / bpa.blended_conversion_rate
                         - bpa.rev_adj_act_ammenities / bpa.blended_conversion_rate
                         - bpa.rev_adj_proj_gap / bpa.blended_conversion_rate
                         - bpa.rev_adj_proj_tc / bpa.blended_conversion_rate
                         - NVL (bpa.rev_adj_proj_ovr_comm_amt, 0) / bpa.blended_conversion_rate
                         + bpa.tax_margin / bpa.blended_conversion_rate
                         + bpa.b_p_fuel_surcharge
                         + bpa.b_p_port_charges
                      ELSE
                           bpa.b_p_fare
                         + bpa.b_p_discount
                         + bpa.b_p_fcc_discount
                         + bpa.b_p_air_add_on
                         + bpa.b_p_air_supplement
                         + bpa.b_p_air_fees
                         - bpa.b_p_air_fees_taxes_to
                         - bpa.b_p_air_fees_taxes_fr
                         - NVL (bpa.pcom_netf_base_amt, 0)
                         - NVL (bpa.pcom_netf_ovr_amt, 0)
                         - NVL (bpa.pcom_land_base_amt, 0)
                         - NVL (bpa.pcom_land_ovr_amt, 0)
                         - NVL (bpa.pcom_air_add_base_amt, 0)
                         - NVL (bpa.pcom_air_add_ovr_amt, 0)
                         - NVL (bpa.pcom_air_upg_base_amt, 0)
                         - NVL (bpa.pcom_air_upg_ovr_amt, 0)
                         - NVL (bpa.pcom_bonus_comm_amt, 0)
                         - NVL (bpa.b_p_wholesaler_comm_portion, 0)
                         - bpa.b_p_actual_air_cost
                         - bpa.std_land_cost
                         - bpa.b_p_onboard_credit_amt / bpa.blended_conversion_rate
                         - bpa.rev_adj_act_gap / bpa.blended_conversion_rate
                         - bpa.rev_adj_cocktail_pty / bpa.blended_conversion_rate
                         - bpa.rev_adj_act_ammenities / bpa.blended_conversion_rate
                         - bpa.rev_adj_proj_gap / bpa.blended_conversion_rate
                         - bpa.rev_adj_proj_tc / bpa.blended_conversion_rate
                         - NVL (bpa.rev_adj_proj_ovr_comm_amt, 0) / bpa.blended_conversion_rate
                         + bpa.tax_margin / bpa.blended_conversion_rate
                         + bpa.b_p_port_charges
                   END)
             END),
             2) AS ntr_sls_bkng_curr
     FROM bkng_psngr_audit bpa,
          bkng_mstr bm,
          voyage_mstr vm
    WHERE  bpa.b_voyage = bm.b_voyage
       AND bpa.b_bkng_nbr = bm.b_bkng_nbr
       AND bpa.company_code = bm.company_code
       AND bpa.b_voyage = vm.vyd_voyage
       AND bpa.company_code = vm.company_code
       AND vm.vyd_ship_code <> 'ZZ'
       AND bpa.effective_date >= '16-JAN-2011'
       AND bpa.b_bkng_nbr = '9T7RXL'
) upd
ON (bpa.b_bkng_nbr = upd.b_bkng_nbr
    AND bpa.b_voyage = upd.b_voyage
    AND bpa.b_p_passenger_id = upd.b_p_passenger_id
    AND bpa.effective_date = upd.effective_date
    AND bpa.company_code = upd.company_code)
WHEN MATCHED THEN
UPDATE SET bpa.npr_usd = upd.npr_usd,
          bpa.ntr_fin_usd = upd.ntr_fin_usd,
          bpa.ntr_fin_bkng_curr = upd.ntr_fin_bkng_curr,
          bpa.ntr_sls_usd = upd.ntr_sls_usd,
          bpa.ntr_sls_bkng_curr = upd.ntr_sls_bkng_curr;
