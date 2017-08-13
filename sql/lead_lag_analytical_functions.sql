--without NVL
LEAD(effdt,1,'31-DEC-9999') OVER (PARTITION BY jobcode ORDER BY effdt) AS EXPDT,

/* Prior Record - LAG */
LAG(end_date,1) OVER (PARTITION BY prom_voyage, prom_currency, prorat_cat ORDER BY end_date) AS START_DATE_LAG

/* Next Record - LEAD */
LEAD(end_date,1) OVER (PARTITION BY prom_voyage, prom_currency, prorat_cat ORDER BY end_date) AS START_DATE_LEAD

--*********************************************************************
/* Example */
WITH a AS (
SELECT vm.vyd_ship_code, pm.prom_voyage, vm.vyd_sail_date, vm.company_code, pm.prom_promo_code, pm.prom_currency, pm.prom_campaign_nbr,
            pr.prorat_eff_date as EFFECTIVE_DATE, pr.prorat_eff_time as EFFECTIVE_TIME, pm.prom_shut_off_date as EXPIRATION_DATE,
            '000000' as EXPIRATION_TIME, pr.prorat_cat, pr.prorat_cruz_amt_double, pr.prorat_cruz_amt_single, vm.vyd_web_itinerary_code, sp.eob_data_date,
            row_number() over (partition by vyd_ship_code, prom_voyage, vyd_sail_date, prom_promo_code, prorat_cat order by prorat_eff_date desc, prorat_eff_time desc) as ROW_WID
FROM    voyage_mstr vm,
            voyage_promotions pm,
            voyage_promotions_rates pr,
            voyage_promotions_by_eff_date pe,
            dw_system_parms sp
WHERE vm.vyd_voyage = pr.prorat_voyage
    AND vm.company_code = pr.company_code
    AND pm.prom_promo_code = pr.prorat_promo_code
    AND vm.vyd_voyage = pm.prom_voyage
    AND vm.company_code = pm.company_code
    AND pm.prom_voyage = pe.proeff_voyage
    AND pm.prom_promo_code = pe.proeff_promo_code
    AND pm.company_code = pe.company_code
    AND vm.vyd_trade = 'A'
    AND vm.rpt_year >= '2012'
    AND pm.prom_tours_eligible_ind = 'N'
    AND pm.prom_promo_code LIKE 'RH%'      
    AND pm.prom_promo_status = 'O'
    AND pm.prom_currency = 'USD'      
--    AND pm.prom_voyage = 'X231'
    AND pm.prom_voyage IN ('X231', 'X232')
--    AND pr.prorat_cat = 'A'
    AND pr.prorat_cat IN ('A', 'AA')    
ORDER BY vm.vyd_ship_code, pm.prom_voyage, vm.vyd_sail_date, pm.prom_promo_code, pr.prorat_cat, vm.company_code
),
b AS (
SELECT t1.vyd_ship_code, t1.company_code, t1.prom_voyage, t1.vyd_sail_date, t1.prom_promo_code, 
            t1.prom_currency, t1.prom_campaign_nbr, t1.effective_date, t1.effective_time, t1.eob_data_date,
            CASE WHEN t1.row_wid = 1 THEN t1.expiration_date ELSE t2.effective_date END as END_DATE,
            CASE WHEN t1.row_wid = 1 THEN t1.expiration_time ELSE t2.effective_time END as END_TIME,
            t1.prorat_cat, t1.prorat_cruz_amt_double, t1.prorat_cruz_amt_single, t1.vyd_web_itinerary_code
FROM a t1, 
          a t2
WHERE t1.row_wid-1 = t2.row_wid(+)
    AND t1.vyd_ship_code = t2.vyd_ship_code(+)
    AND t1.prom_voyage = t2.prom_voyage(+)
    AND t1.vyd_sail_date = t2.vyd_sail_date(+)
    AND t1.prom_promo_code = t2.prom_promo_code(+)
    AND t1.prorat_cat = t2.prorat_cat(+)
ORDER BY t1.vyd_ship_code, t1.prom_voyage, t1.vyd_sail_date, t1.prom_promo_code, t1.prorat_cat, t1.company_code
)
SELECT    
    prom_voyage,
    prom_currency,
    prorat_cat,
    effective_date AS START_DATE_OLD,
    NVL(LAG(end_date,1) OVER (PARTITION BY prom_voyage, prom_currency, prorat_cat ORDER BY prom_voyage, prom_currency, prorat_cat, end_date),effective_date) START_DATE_LAG,
    NVL(LEAD(end_date,1) OVER (PARTITION BY prom_voyage, prom_currency, prorat_cat ORDER BY prom_voyage, prom_currency, prorat_cat, end_date),effective_date) START_DATE_LEAD,
    end_date,
    prorat_cruz_amt_double,
    prorat_cruz_amt_single,
    prom_promo_code,
    vyd_ship_code
FROM b 
WHERE effective_date <> end_date
ORDER BY prom_voyage, prorat_cat, end_date;