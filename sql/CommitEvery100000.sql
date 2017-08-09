DECLARE
  
   TYPE t_record IS RECORD (
     b_voyage                     bp_update_tmp.b_voyage%TYPE,
     b_bkng_nbr                   bp_update_tmp.b_bkng_nbr%TYPE,
     b_p_passenger_id             bp_update_tmp.b_p_passenger_id%TYPE,
     ntr_fin_usd                  bp_update_tmp.ntr_fin_usd%TYPE,
     ntr_fin_bkng_curr            bp_update_tmp.ntr_fin_bkng_curr%TYPE,
     company_code                 bkng_psngr.company_code%TYPE
   );
   TYPE t_record_table IS TABLE OF t_record;
   v_record_table t_record_table;
   
   /* COMMIT INTERVAL */
   v_limit number := 100000;

   /* Cursor Query - Get rows to update */
   CURSOR c_data IS
    SELECT /*+ PARALLEL */
        s.b_voyage, 
        s.b_bkng_nbr,
        s.b_p_passenger_id,
        s.ntr_fin_usd,
        s.ntr_fin_bkng_curr,
        f.company_code
    from bp_update_tmp s, bkng_psngr f;
       
      
   
   BEGIN
      open c_data;
      LOOP
      fetch c_data BULK COLLECT INTO v_record_table LIMIT v_limit;
      IF (v_record_table.COUNT > 0) THEN
         FORALL i IN v_record_table.FIRST..v_record_table.LAST
        
            UPDATE bkng_psngr
            SET 
              ntr_fin_usd        = v_record_table(i).ntr_fin_usd,
              ntr_fin_bkng_curr  = v_record_table(i).ntr_fin_bkng_curr,
              net_ticket_revenue = v_record_table(i).ntr_fin_usd
            WHERE
              company_code = v_record_table(i).company_code AND 
              b_voyage = v_record_table(i).b_voyage AND 
              b_bkng_nbr = v_record_table(i).b_bkng_nbr AND 
              b_p_passenger_id = v_record_table(i).b_p_passenger_id;   
         -- Save changes
         COMMIT;
      END IF;
      
   EXIT WHEN v_record_table.COUNT < v_limit;
   END LOOP;
   close c_data;

   EXCEPTION
      WHEN OTHERS THEN
        RAISE;
   END;
/   

