CREATE OR REPLACE PACKAGE BODY DM_OBR.OBR_LOAD AS
/******************************************************************************
   NAME:       OBR_LOAD
   PURPOSE:  PL/SQL code to Load OBR data mart

   REVISIONS:
   Ver        Date             Author           Description
   ---------  ----------  ---------------  ------------------------------------

******************************************************************************/

    /* Package Variables */
    cSchema_NM    CONSTANT Varchar2(15) := 'DM_OBR';
    cPackage_NM    CONSTANT Varchar2(50) := 'OBR_LOAD';
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

   PROCEDURE RUN_OBR_LOAD IS
        cProcedure_NM  CONSTANT  Varchar2(30) := 'RUN_OBR_LOAD';
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

      ----------------------------------
        START_OBR_LOAD;
        WC_ITEM_MASTER_D_LOAD;
        WC_OBRDEP_D_LOAD;
        WC_SHIP_SCHED_D_LOAD;
        WC_OBRVOYAGE_D_LOAD;
        WC_EPO_D_LOAD;
        WC_OBRPAX_D_LOAD;
        WC_SHOREX_D_LOAD;
        WC_SHOREX_F_LOAD;
        WC_SHOREX_MARGIN_F_LOAD;
        WC_SHOREX_PAX_F_LOAD;
        WC_OBRTXN_F_LOAD;
        WC_OBRREV_A_LOAD;
        WC_VOYPAX_F_LOAD;
        WC_OBRTGT_F_HAL_LOAD;
        WC_OBRTGT_F_SBN_LOAD;
        WC_ITEM_TXN_F_LOAD;
        WC_PAXLEG_F_LOAD;
        END_OBR_LOAD;
      ----------------------------------

      sComment := 'Job ran successfully';

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

END RUN_OBR_LOAD;

   PROCEDURE START_OBR_LOAD IS
        cProcedure_NM  CONSTANT  Varchar2(30) := 'START_OBR_LOAD';
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

   sComment  := 'OBR Data Mart load has started ';

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

END START_OBR_LOAD;

PROCEDURE WC_ITEM_MASTER_D_LOAD IS
        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_ITEM_MASTER_D_LOAD';
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

    execute immediate 'truncate table wc_item_master_d reuse storage';

/*BEGIN reload item master*/

    INSERT /*+append*/ INTO wc_item_master_d
    SELECT max(fam_id), fam_number, max(fam_name),
           TO_NUMBER (SUBSTR (mim_maj_id, 3)) maj_id, maj_number, maj_name, mim_shp_id,
           shp_name, mim_id, mim_number, mim_name, mim_rate,
           c2.class_depend_id mim_group_id, c1.class_description mim_group_desc,
           mim_class_id, c2.class_description mim_class_desc, '00000' dep_no
      FROM
        mim@prdfms_prdbi1.hq.halw.com,
        fam@prdfms_prdbi1.hq.halw.com,
        maj@prdfms_prdbi1.hq.halw.com,
        shp@prdfms_prdbi1.hq.halw.com,
        classifications@prdfms_prdbi1.hq.halw.com c1,
        classifications@prdfms_prdbi1.hq.halw.com c2
     WHERE TO_NUMBER (SUBSTR (mim_fam_id, 3)) = TO_NUMBER (fam_number)
       AND mim_shp_id = fam_shp_id
       AND TO_NUMBER (SUBSTR (mim_maj_id, 3)) = TO_NUMBER (maj_number(+))
       AND mim_shp_id = maj_shp_id
       AND mim_shp_id = shp_id
       AND mim_class_id = c2.class_id
       AND c2.class_depend_id = c1.class_id(+)
     GROUP BY
            fam_number, TO_NUMBER (SUBSTR (mim_maj_id, 3)), maj_number, maj_name, mim_shp_id,
           shp_name, mim_id, mim_number, mim_name, mim_rate,
           c2.class_depend_id, c1.class_description,
           mim_class_id, c2.class_description, '00000';

        x:=SQL%ROWCOUNT;
        sComment  := 'WC_ITEM_MASTER_D Inserts: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
END WC_ITEM_MASTER_D_LOAD;

PROCEDURE WC_OBRDEP_D_LOAD IS
        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_OBRDEP_D_LOAD';
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

    insert into wc_obrdep_d
    with wid as (select max(row_wid) max_wid from wc_obrdep_d)
    select  wid.max_wid+rownum,
        trunc(d.dep_transdate),
        trunc(d.dep_transdate),
        1,
        1,
        dep.dep_id,
        dep.SHIP_ID,
        dep.SHIP_NAME,
        dep.DEM_ID,
        dep.DEM_NO,
        dep.DEM_DESC ,
        dep.DEM_CLASS_ID,
        nvl(bd.DEM_TYPE, d.dep_flag),
        dep.DEM_CLASS_DESC,
        dep.DEM_GROUP_ID,
        dep.DEM_GROUP_DESC,
        dep.DEP_ID,
        dep.DEP_NO,
        dep.DEP_DESC,
        dep.DEP_CLASS_ID,
        dep.DEP_CLASS_DESC,
        nvl(bd.DEP_TYPE, d.dep_flag),
        dep.DEP_GROUP_ID,
        dep.DEP_GROUP_DESC,
        bd.maingroup,
        bd.subgroup,
        bd.additional,
        bd.ordermain,
        bd.ordersub,
        bd.recap,
        bd.orderrecap,
        bd.dalrec_type,
        bd.allmatches,
        bd.ordermain_net,
        case    when bd.dalrec_type = 'R' then '1'||dep.dep_no --always sort revenue before cost
                when bd.dalrec_type = 'C' then '2'||dep.dep_no
                else '3'||dep.dep_no
        end as dep_order,
        null as end_date,
        bd.scorecard_a,
        bd.scorecard_b,
        nvl(l.rev_ctr_cd, 0)
    from    dep@prdfms_prdbi1.hq.halw.com d,
            department_classifications@prdfms_prdbi1.hq.halw.com dep,
            wc_obrdep_d w,
            bd_dalrec_d bd,
            wid,
            wc_sales_loc_d l
    where   d.dep_id = dep.dep_id
    and     d.dep_transdate >= trunc(current_date) - 14
    and     dep.dep_id = w.dep_id (+)
    and     dep.dep_no = bd.dep_no (+)
    and     substr(dep.dem_no,1,3) = l.rev_ctr_cd (+)
    and     w.row_wid is null;

        x:=SQL%ROWCOUNT;
        sComment  := 'WC_OBRDEP_D Inserts: '||x;

commit;

        MERGE INTO
            wc_obrdep_d t1
        USING
            (SELECT
                 dep_no,
                 ship_id,
                 MIN (row_wid) min_wid,
                 MAX (w_insert_dt) - 1 end_date
             FROM
                 dm_obr.wc_obrdep_d
             WHERE
                 end_date IS NULL
             GROUP BY
                 dep_no, ship_id
             HAVING
                 COUNT (*) > 1) t2
        ON
            (t1.row_wid = t2.min_wid)
        WHEN MATCHED THEN
            UPDATE SET t1.end_date = t2.end_date;

        x:=SQL%ROWCOUNT;
        sComment  := sComment || '; Dupes End Dated: '||x;

commit;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
  END WC_OBRDEP_D_LOAD;

PROCEDURE WC_SHIP_SCHED_D_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_SHIP_SCHED_D_LOAD';
        BatchID Number := 0;
        v_max_sched_wid Number;

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

    execute immediate 'truncate table wc_ship_sched_ds';

SELECT MAX(ROW_WID)
    into v_max_sched_wid
    from dm_obr.WC_SHIP_SCHED_D;

    BEGIN

        insert into wc_ship_sched_ds
        with cd as (
        SELECT  nvl(ss.row_wid, 1000000+rownum) row_wid,
            cruiseid AS cruise_id,
            shipid AS ship_id,
            cruise AS cruise_description,
            port AS port_name,
            cruisedate AS cruise_date,
            cruiseday AS cruise_day,
            cruisestart AS cruise_start_date,
            cruiseend AS cruise_end_date,
            currency AS cruise_currency,
            cruisestatus AS cruise_status,
            paxcount AS pax_count,
            childcount AS child_count,
            crewcount AS crew_count,
            dayscomment AS port_details,
            cruiseitinarydesc AS cruise_itinerary,
            cruiseseasondesc AS cruise_season,
            CASE WHEN ss.row_wid is NULL THEN 'I' ELSE 'U' END AS insert_update
        FROM  cruise_details fms, wc_ship_sched_d ss
        WHERE  cruiseactive = 1
        AND    cruisestart < trunc(current_date)
        AND    cruisestart > trunc(current_date) - 120
        AND    NVL(cruiseend,trunc(current_date)-1) >= trunc(current_date) - 14
        AND    fms.cruisestart = ss.cruise_start_date (+)
        AND    FMS.SHIPID = ss.ship_id (+)
        AND    fms.cruisedate = ss.cruise_date (+)
        AND    fms.cruisedate < trunc(current_date) + 120
        ),
        a as (
        select  row_wid,
            (w.ship_ID || to_char(cruise_date, 'yyyymmdd')) as SchedID,
            shipname,
            cruise_id,
            ship_id,
            cruise_description,
            port_name,
            cruise_date,
            cruise_day,
            cruise_start_date,
            nvl(cruise_end_date, max(cruise_date) over (partition by cruise_id)) as cruise_end_date,
            cruise_currency,
            cruise_status,
            pax_count,
            child_count,
            crew_count,
            port_details,
            cruise_itinerary,
            cruise_season,
            cast(Cruise_date - Cruise_start_Date + 1 as int)
            / cast(nvl(cruise_end_date,max(cruise_date) over (partition by cruise_id)) - cruise_start_date as int)
            as CruiseComplete,
            sum(pax_count) over (partition by cruise_id order by cruise_date) as pcd,
            cast(Cruise_date - Cruise_start_Date + 1 as int) as day_number,
            'All' as AllMatches,
            case when pax_count > 1 then 1 else 0
            end as DayComp,
            insert_update
        from        cd w, wc_ship_d s
        where       w.ship_id = s.shipid
        )
        select  a.*,
            sum(DayComp) over (partition by cruise_id order by cruise_date)/(a.Cruise_end_date - a.cruise_Start_Date) as CompDay
        from a;

        x:=SQL%ROWCOUNT;

        commit;

    /*delete turnaround days because they belong to next cruise*/
        delete from wc_ship_sched_ds
        where cruisecomplete > 1;

        x:=x-SQL%ROWCOUNT;

        commit;

    /*delete dupes*/
        delete from wc_ship_sched_ds
        where pax_count = 0
        and (ship_id, cruise_date) in
        (select ship_id, cruise_date
        from wc_ship_sched_ds
        group by ship_id, cruise_date
        having count(*) > 1);

        commit;

        x:=x-SQL%ROWCOUNT;
        sComment  := 'WC_SHIP_SCHED_DS Inserts: '||x;

    --delete rows from target table so that updates can be processed as inserts
        delete from wc_ship_sched_d where (ship_id, cruise_date) in
        (select ship_id, cruise_date from wc_ship_sched_ds);

        COMMIT;

    --insert new and updated rows
        insert into wc_ship_sched_d
        select
        ROW_WID,
        SCHEDID,
        SHIPNAME,
        CRUISE_ID,
        SHIP_ID,
        CRUISE_DESCRIPTION,
        PORT_NAME,
        CRUISE_DATE,
        CRUISE_DAY,
        CRUISE_START_DATE,
        CRUISE_END_DATE,
        CRUISE_CURRENCY,
        CRUISE_STATUS,
        PAX_COUNT,
        CHILD_COUNT,
        CREW_COUNT,
        PORT_DETAILS,
        CRUISE_ITINERARY,
        CRUISE_SEASON,
        CRUISECOMPLETE,
        PCD,
        DAY_NUMBER,
        ALLMATCHES,
        DAYCOMP,
        COMPDAY,
        0 VOYAGE_WID,
        to_char(cruise_date,'YYYYMMDD') VOYAGE_DT_WID,
        to_char(cruise_date,'YYYYMMDD') * 100 + SHIP_ID SCHED_KEY
        from
        wc_ship_sched_ds
        where insert_update = 'U'
        UNION
        select
        rownum + v_max_sched_wid,
        SCHEDID,
        SHIPNAME,
        CRUISE_ID,
        SHIP_ID,
        CRUISE_DESCRIPTION,
        PORT_NAME,
        CRUISE_DATE,
        CRUISE_DAY,
        CRUISE_START_DATE,
        CRUISE_END_DATE,
        CRUISE_CURRENCY,
        CRUISE_STATUS,
        PAX_COUNT,
        CHILD_COUNT,
        CREW_COUNT,
        PORT_DETAILS,
        CRUISE_ITINERARY,
        CRUISE_SEASON,
        CRUISECOMPLETE,
        PCD,
        DAY_NUMBER,
        ALLMATCHES,
        DAYCOMP,
        COMPDAY,
        0 VOYAGE_WID,
        to_char(cruise_date,'YYYYMMDD') VOYAGE_DT_WID,
        to_char(cruise_date,'YYYYMMDD') * 100 + SHIP_ID SCHED_KEY
        from
        wc_ship_sched_ds
        where insert_update = 'I';

        x:=SQL%ROWCOUNT;
        sComment  := sComment|| ', WC_SHIP_SCHED_D Inserts: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
       END;
  END WC_SHIP_SCHED_D_LOAD;

PROCEDURE WC_OBRVOYAGE_D_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_OBRVOYAGE_D_LOAD';
        BatchID Number := 0;
        v_max_voyage_wid Number;

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

    execute immediate 'truncate table wc_obrvoyage_ds';

SELECT MAX(ROW_WID)
into v_max_voyage_wid
from dm_obr.WC_OBRVOYAGE_D;

BEGIN

--Now create distinct voyages from the schedule
    insert into wc_obrvoyage_ds
    select
        null,
        s.shipname,
        s.shipclassdesc,
        ss.Cruise_ID,
        ss.Ship_ID,
        ss.Cruise_Description,
        ss.Cruise_start_date,
        ss.cruise_end_date,
        ss.Cruise_itinerary,
        ss.Cruise_season,
        sum(ss.pax_count) as PCD,
        ss.Cruise_End_Date - ss.Cruise_Start_Date as CruiseLength,
        round(sum(ss.pax_count)/(ss.Cruise_End_Date - ss.Cruise_Start_Date),0) as Pax,
        to_Number(to_char(ss.Cruise_end_date,'MM')) as VoyMonth,
        to_Number(to_char(ss.Cruise_end_date,'YYYY')) as VoyYear,
        to_char(ss.Cruise_end_date,'YYYYMM') as VoyTime,
        s.shiporder,
        null,
        ss.cruise_status
    From        wc_ship_sched_d ss, wc_ship_d s, wc_ship_sched_ds ds
    where       ss.ship_id = s.shipid and ss.cruise_date = ds.cruise_date and ss.ship_id = ds.ship_id
    Group By    s.shipname, s.shipclassdesc, ss.Cruise_ID, ss.Ship_ID, ss.Cruise_Description, ss.Cruise_start_date,
            ss.cruise_end_date, ss.Cruise_itinerary, ss.Cruise_season, s.shiporder, ss.cruise_status
    order by    ss.ship_id, ss.cruise_id;

        x:=SQL%ROWCOUNT;
        sComment  := 'WC_OBRVOYAGE_DS Inserts: '||x;

    COMMIT;

--Change calendar year to fiscal year for december voyages --
    update wc_obrvoyage_ds
    set voyyear = voyyear+1
    where (voytime/100)-.12 = voyyear;

    commit;

--Set the row_wid from the wc_obrvoyage_d to maintain data integrity--
    update wc_obrvoyage_ds ds
    set ds.row_wid = (select v.row_wid from wc_obrvoyage_d v where v.ship_id = ds.ship_id and v.cruise_start_date = ds.cruise_start_date)
    where exists (select 'x' from wc_obrvoyage_d v where v.ship_id = ds.ship_id and v.cruise_start_date = ds.cruise_start_date);

    COMMIT;

--remove matching rows from target
    delete from wc_obrvoyage_d
    where row_wid in (select row_wid from wc_obrvoyage_ds);

    COMMIT;

    insert into wc_obrvoyage_d
    select
        ds.row_wid,
        ds.shipname,
        ds.shipclassdesc,
        ds.Cruise_ID,
        ds.Ship_ID,
        ds.Cruise_Description,
        ds.Cruise_start_date,
        ds.cruise_end_date,
        ds.Cruise_itinerary,
        ds.Cruise_season,
        ds.PCD,
        ds.CruiseLength,
        ds.Pax,
        ds.VoyMonth,
        ds.VoyYear,
        ds.VoyTime,
        ds.shiporder,
        ds.voyage_cd,
        b.budget_pcd,
        to_char(ds.cruise_start_date,'MM/DD/YYYY')||' '||ds.cruise_description,
        ds.cruise_status,
        dd.row_wid*-1 cruise_end_sort,
        null
    from
        wc_obrvoyage_ds ds,
        w_day_d dd,
        wc_budget_voyage_f b
    where
        ds.row_wid is not null
        and ds.ship_id = b.ship_wid (+)
        and ds.cruise_start_date = b.cruise_start_date (+)
        and ds.cruise_end_date = dd.calendar_date;

        x:=SQL%ROWCOUNT;
        sComment  := sComment|| ', WC_OBRVOYAGE_D Updates: '||x;

    COMMIT;

    INSERT INTO WC_OBRVOYAGE_D
    select
        rownum + v_max_voyage_wid,
        ds.shipname,
        ds.shipclassdesc,
        ds.Cruise_ID,
        ds.Ship_ID,
        ds.Cruise_Description,
        ds.Cruise_start_date,
        ds.cruise_end_date,
        ds.Cruise_itinerary,
        ds.Cruise_season,
        ds.PCD,
        ds.CruiseLength,
        ds.Pax,
        ds.VoyMonth,
        ds.VoyYear,
        ds.VoyTime,
        ds.shiporder,
        ds.voyage_cd,
        b.budget_pcd,
        to_char(ds.cruise_start_date,'MM/DD/YYYY')||' '||ds.cruise_description,
        ds.cruise_status,
        dd.row_wid*-1 cruise_end_sort,
        null
    from
        wc_obrvoyage_ds ds,
        w_day_d dd,
        wc_budget_voyage_f b
    where
        ds.row_wid is null
        and ds.ship_id = b.ship_wid (+)
        and ds.cruise_start_date = b.cruise_start_date (+)
        and ds.cruise_end_date = dd.calendar_date;

        x:=SQL%ROWCOUNT;
        sComment  := sComment|| ', WC_OBRVOYAGE_D Inserts: '||x;

    COMMIT;

--Update polar voyage codes--
    merge into wc_obrvoyage_d v
    using (select * from dm_tkr.rdm_voyage_d where voyage_status <> 'C') x
    on (
        UPPER(v.shipname) = UPPER(x.ship_name)
        AND v.cruise_start_date = x.sail_date
        AND v.cruise_end_date = x.return_date
        )
    when matched then update set
        v.voyage_cd = x.voyage,
        v.voyage_cd_physical = x.voyage_physical;

    COMMIT;

--Update voyage wid on wc_ship_sched_d--
    update wc_ship_sched_d ss
    set ss.voyage_wid = (select v.row_wid from wc_obrvoyage_d v
                        where v.cruise_start_date = ss.cruise_start_date
                        and v.ship_id = ss.ship_id)
    where ss.voyage_wid = 0
    and exists (select 'x' from wc_obrvoyage_d v
                where v.cruise_start_date = ss.cruise_start_date
                and v.ship_id = ss.ship_id);

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
       END;
  END WC_OBRVOYAGE_D_LOAD;

PROCEDURE WC_EPO_D_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_EPO_D_LOAD';
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

    execute immediate 'truncate table wc_epo_ds';

    insert /*+append*/ into wc_epo_ds
    select
      EPO_ID,
      EPO_POS_ID,
      EPO_ESE_ID,
      EPO_PDAT,
      EPO_SALESPRICE,
      trunc(EPO_TRANSDATE),
      EPO_QUANT,
      EPO_PREPAID,
      EPO_KIOSK,
      epo_shp_id * 10000000000 + epo_cruise,
      EPO_BOOKING_ORIGIN,
      EPO_TYPE
    from
        EPO@prdfms_prdbi1.hq.halw.com t1,
        ESE@prdfms_prdbi1.hq.halw.com t2
    where t1.epo_ese_id = t2.ese_id
    and t2.ese_date >= trunc(current_date) - 14
    and t1.epo_void = 0;

    x:=SQL%ROWCOUNT;
    sComment:='WC_EPO_DS Inserts: '||x;

    COMMIT;

    delete from wc_epo_d
    where epo_pos_id in (select epo_pos_id from wc_epo_ds);

    COMMIT;

    insert into wc_epo_d
    SELECT
      EPO_ID,
      EPO_POS_ID,
      EPO_ESE_ID,
      EPO_PDAT,
      EPO_SALESPRICE,
      EPO_TRANSDATE,
      EPO_QUANT,
      EPO_PREPAID,
      EPO_KIOSK,
      EPO_CRUISE,
      EPO_BOOKING_ORIGIN,
      EPO_TYPE
    FROM
        (select
          EPO_ID,
          EPO_POS_ID,
          EPO_ESE_ID,
          EPO_PDAT,
          EPO_SALESPRICE,
          EPO_TRANSDATE,
          EPO_QUANT,
          EPO_PREPAID,
          EPO_KIOSK,
          EPO_CRUISE,
          EPO_BOOKING_ORIGIN,
          EPO_TYPE,
          RANK() OVER (PARTITION BY EPO_POS_ID ORDER BY EPO_ID DESC) R1
        from
            WC_EPO_DS
        where
            EPO_QUANT > 0
            AND EPO_POS_ID > 0
        ) t1
    WHERE t1.R1 = 1;

    x:=SQL%ROWCOUNT;
    sComment:=sComment || ', WC_EPO_D Inserts: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

  END WC_EPO_D_LOAD;

PROCEDURE WC_OBRPAX_D_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_OBRPAX_D_LOAD';
        BatchID Number := 0;
        v_max_pax_wid Number;

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

    execute immediate 'truncate table wc_obrpax_ds';

--get starting point for inserts
SELECT MAX(ROW_WID)
into v_max_pax_wid
from dm_obr.WC_OBRPAX_D;

    BEGIN

--Now create distinct voyages from the schedule
  INSERT INTO WC_OBRPAX_DS
    SELECT  nvl(p.row_wid, 10000000+rownum),
            acc_id AS pax_spms_id,
            acx_fleet_id AS pax_fleet_id,
            DECODE (acx_a_frq_cardno, '000000000', '', '999999999', '', '222222222', '', acx_a_frq_cardno) AS pax_mariner_no,
            CASE when acc_tag = 'P' then
                        CASE when acc_shp_id between 51 and 56 then nvl(acc_loyalty,'CM')
                                else nvl(acc_loyalty,'0S')
                        END
                     else null
            END as pax_loyalty,
            acc_shp_id AS pax_ship_id,
            acc_status AS pax_status,
            (acc_shp_id * 10000000000) + acc_cruise AS pax_cruise_id,
            acc_name AS pax_last_name,
            acc_fstn AS pax_first_name,
            acc_mname AS pax_middle_name,
            acx_title AS pax_title,
            TRUNC (acx_birthd) AS pax_birth_date,
            acx_age AS pax_age,
            acx_sex AS pax_gender,
            cab_no AS pax_cabin,
            SUBSTR (acx_nation, 9, 2) AS pax_nation_code,
            Nation_name AS pax_nation,
            acx_passno AS pax_pass_number,
            acc_reserv_id AS pax_booking_no,
            TRUNC (acx_passex) AS pax_pass_expiry_date,
            TRUNC (acx_passdi) AS pax_pass_issue_date,
            acx_passpi AS pax_pass_issue_place,
            acx_telno AS pax_tel_no,
            SUBSTR (acx_passpi_country, 9, 2) AS pax_pass_issue_ctry,
            acc_cons_id AS pax_reservation_id,
            acc_pos_credit AS pax_pos_credit,
            acc_pos_debit AS pax_pos_debit,
            acx_street AS pax_street_1,
            acx_street2 AS pax_street_2,
            acx_street3 AS pax_street_3,
            acx_city AS pax_city,
            acx_state AS pax_state,
            acx_zip AS pax_zip,
            SUBSTR (acx_country, 9, 2) AS pax_country_code,
            acx_email AS pax_email,
            case when acc_tag in ('P','C')
                    then nvl(cab_design, acc_tag)
                    else acc_tag
            end as pax_type,
            acc_tag,
            acc_emb_a AS pax_embark_date,
            nvl(acc_dis_a,acc_dis_e) AS pax_debark_date,
            acc_sys_acc,
            'N' back_to_back,
            CASE WHEN p.row_wid is NULL THEN 'I' ELSE 'U' END AS insert_update,
            acc_grp,
            NULL AS X_META,
            regexp_replace( RTRIM(ACC.ACC_PPD_TAG , 'GEN') , '[0-9]') PPD,
            ACC_BOARDCC,
            ACX_ORDER_EMP_NO
      FROM      acc@prdfms_prdbi1.hq.halw.com acc,
            acx@prdfms_prdbi1.hq.halw.com,
            shp@prdfms_prdbi1.hq.halw.com,
            cab@prdfms_prdbi1.hq.halw.com,
            geo_nations@prdfms_prdbi1.hq.halw.com,
            (select pax_spms_id, row_wid, pax_debark_date from wc_obrpax_d) p
     WHERE  acc_id = P.PAX_SPMS_ID (+)
            AND acc_id = acx_id (+)
            AND acc_shp_id = shp_id (+)
            AND acc_cab_id = cab_id (+)
            AND SUBSTR (acx_nation, 9, 2) = iso2(+)
            AND trunc(acc_transdate) >= trunc(current_date) - 14
            AND nvl(acc_dis_a,trunc(current_date)) >= trunc(current_date) - 14;

        x:=SQL%ROWCOUNT;
        sComment  := 'WC_OBRPAX_DS Inserts: '||x;

    COMMIT;

-- Update the x_meta attribute in staging table
    MERGE INTO wc_obrpax_ds  stage
    USING
        (
        SELECT
        R.META, R.CABIN, D.SHIPID
        FROM DM_TKR.RDM_CABIN_D R, WC_SHIP_D D
        WHERE D.SHIPPOLAR = R.SHIP_CODE
        ) M
    ON (STAGE.pax_cabin = M.cabin and STAGE.pax_ship_id = M.shipid )
    WHEN MATCHED THEN
     UPDATE SET STAGE.X_META = M.META;

 commit;


    --flag back to back cruisers--
    update wc_obrpax_ds
    set back_to_back = 'Y'
    where pax_type = 'P'
    and pax_spms_id in
        (select leg_acc_id
        from leg@prdfms_prdbi1.hq.halw.com
        where leg_emb >= trunc(current_date) - 180
        and leg_deleted = 0
        group by leg_acc_id having count(*) > 1);

    COMMIT;

    DELETE FROM dm_obr.wc_obrpax_d
    WHERE pax_spms_id IN (SELECT pax_spms_id
                 FROM dm_obr.wc_obrpax_ds);

    COMMIT;

    INSERT INTO dm_obr.wc_obrpax_d
        SELECT
        ROW_WID,
        PAX_SPMS_ID,
        PAX_FLEET_ID,
        PAX_MARINER_NO,
        PAX_LOYALTY,
        PAX_SHIP_ID,
        PAX_STATUS,
        PAX_CRUISE_ID,
        PAX_LAST_NAME,
        PAX_FIRST_NAME,
        PAX_MIDDLE_NAME,
        PAX_TITLE,
        PAX_BIRTH_DATE,
        PAX_AGE,
        PAX_GENDER,
        PAX_CABIN,
        PAX_NATION_CODE,
        PAX_NATION,
        PAX_PASS_NUMBER,
        PAX_BOOKING_NO,
        PAX_PASS_EXPIRY_DATE,
        PAX_PASS_ISSUE_DATE,
        PAX_PASS_ISSUE_PLACE,
        PAX_TEL_NO,
        PAX_PASS_ISSUE_CTRY,
        PAX_RESERVATION_ID,
        PAX_POS_CREDIT,
        PAX_POS_DEBIT,
        PAX_STREET_1,
        PAX_STREET_2,
        PAX_STREET_3,
        PAX_CITY,
        PAX_STATE,
        PAX_ZIP,
        PAX_COUNTRY_CODE,
        PAX_EMAIL,
        PAX_TYPE,
        ACC_TAG,
        PAX_EMBARK_DATE,
        PAX_DEBARK_DATE,
        ACC_SYS_ACC,
        BACK_TO_BACK,
        ACC_GRP,
        X_META,
        PPD,
        TRUNC(CURRENT_DATE) LOAD_DT,
        ACC_BOARDCC PAX_PLAYER_CARD,
        ACX_ORDER_EMP_NO SALES_ASSOC_CD,
        0 PAR_PAX_WID
        FROM
        dm_obr.wc_obrpax_ds
        WHERE
        insert_update = 'U';

        x:=SQL%ROWCOUNT;
        sComment  := sComment || ', WC_OBRPAX_D Updates: '||x;

        COMMIT;

    INSERT INTO dm_obr.wc_obrpax_d
        SELECT
        rownum + v_max_pax_wid,
        PAX_SPMS_ID,
        PAX_FLEET_ID,
        PAX_MARINER_NO,
        PAX_LOYALTY,
        PAX_SHIP_ID,
        PAX_STATUS,
        PAX_CRUISE_ID,
        PAX_LAST_NAME,
        PAX_FIRST_NAME,
        PAX_MIDDLE_NAME,
        PAX_TITLE,
        PAX_BIRTH_DATE,
        PAX_AGE,
        PAX_GENDER,
        PAX_CABIN,
        PAX_NATION_CODE,
        PAX_NATION,
        PAX_PASS_NUMBER,
        PAX_BOOKING_NO,
        PAX_PASS_EXPIRY_DATE,
        PAX_PASS_ISSUE_DATE,
        PAX_PASS_ISSUE_PLACE,
        PAX_TEL_NO,
        PAX_PASS_ISSUE_CTRY,
        PAX_RESERVATION_ID,
        PAX_POS_CREDIT,
        PAX_POS_DEBIT,
        PAX_STREET_1,
        PAX_STREET_2,
        PAX_STREET_3,
        PAX_CITY,
        PAX_STATE,
        PAX_ZIP,
        PAX_COUNTRY_CODE,
        PAX_EMAIL,
        PAX_TYPE,
        ACC_TAG,
        PAX_EMBARK_DATE,
        PAX_DEBARK_DATE,
        ACC_SYS_ACC,
        BACK_TO_BACK,
        ACC_GRP,
        X_META,
        PPD,
        TRUNC(CURRENT_DATE) LOAD_DT,
        ACC_BOARDCC PAX_PLAYER_CARD,
        ACX_ORDER_EMP_NO SALES_ASSOC_CD,
        0 PAR_PAX_WID
        FROM
        dm_obr.wc_obrpax_ds
        WHERE
        insert_update = 'I';

        x:=SQL%ROWCOUNT;
        sComment  := sComment|| ', WC_OBRPAX_D Inserts: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
       END;
END WC_OBRPAX_D_LOAD;

PROCEDURE WC_SHOREX_D_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_SHOREX_D_LOAD';
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

    execute immediate 'truncate table wc_shorex_ds';

    BEGIN

--Now create distinct voyages from the schedule
insert /*+append*/ into wc_shorex_ds
select
    ese_shp_id,
    ese_id,
    ese_no,
    ese_name,
    ese_port,
    ese_maxper,
    ese_date,
    ese_fromtime,
    ese_totime,
    ese_duration,
    ese_price,
    ese_price_child,
    ese_transdate
from
    ese@prdfms_prdbi1.hq.halw.com
where
    ese_date >= trunc(current_date) - 14;

        x:=SQL%ROWCOUNT;
        sComment  := 'WC_SHOREX_DS Inserts: '||x;

    COMMIT;

--*****************************
--UPDATES TO FMS SOURCED SHOREX
--*****************************
merge into
    (select * from wc_shorex_d
     where shorex_date >= trunc(current_date) - 14) t1
using
    (select   ds.ese_id,
              v.cruise_id,
              CASE
                WHEN ds.ese_date = v.cruise_end_date
                THEN ds.ese_date - 1
                ELSE ds.ese_date
              END as pax_count_date
     from   wc_obrvoyage_d v, wc_shorex_ds ds
     where  ds.ese_date between v.cruise_start_date+1 and v.cruise_end_date
     and    ds.ese_shp_id = v.ship_id) t2
on (t1.ese_id = t2.ese_id)
when matched then update set
     t1.cruise_id = t2.cruise_id,
     t1.pax_count_date = t2.pax_count_date;

commit;

--*****************************
--UPDATES TO MXP SOURCED SHOREX
--*****************************
merge into
    (select * from wc_shorex_d
    where shorex_date >= trunc(current_date) - 14
    and ese_id = 0 and inactive_flg = 'N') t1
using
    (select   ds.ese_id,
              v.cruise_id,
              ds.ese_no,
              ds.ese_date,
              ds.ese_shp_id,
              ds.ese_fromtime,
              ds.ese_totime,
              ds.ese_duration,
              ds.ese_price,
              ds.ese_price_child,
              ds.ese_maxper,
              CASE
                WHEN ds.ese_date = v.cruise_end_date
                THEN ds.ese_date - 1
                ELSE ds.ese_date
              END as pax_count_date
     from   wc_obrvoyage_d v, wc_shorex_ds ds
     where  ds.ese_date >= trunc(current_date) - 14
     and    ds.ese_date > v.cruise_start_date (+) and ds.ese_date <= v.cruise_end_date (+)
     and    ds.ese_shp_id = v.ship_id (+)) t2
on (t1.shorex_no = t2.ese_no and t1.shorex_date = t2.ese_date and t1.ship_id = t2.ese_shp_id)
when matched then update set
    t1.cruise_id = t2.cruise_id,
    t1.ese_id = t2.ese_id,
    t1.start_time = t2.ese_fromtime,
    t1.end_time = t2.ese_totime,
    t1.duration = t2.ese_duration,
    t1.adult_price = t2.ese_price,
    t1.child_price = t2.ese_price_child,
    t1.ticket_max_count = t2.ese_maxper,
    t1.pax_count_date = t2.pax_count_date,
    t1.mxp_match = 'Y';

commit;

--****************
--INSERTS FROM FMS
--****************
insert into wc_shorex_d
with max_wid as (select max(row_wid) wid from wc_shorex_d)
select
    max_wid.wid+rownum,
    ds.ese_shp_id,
    s.shipcode,
    s.shipname,
    nvl(v.cruise_id,0),
    ds.ese_id,
    ds.ese_name,
    ds.ese_date,
    ds.ese_port,
    ds.ese_port,
    ds.ese_fromtime start_time,
    ds.ese_totime end_time,
    ds.ese_duration duration,
    ds.ese_price adult_price,
    ds.ese_price_child child_price,
    ds.ese_maxper ticket_max_count,
    case when ds.ese_date = v.cruise_end_date
         then ds.ese_date - 1
         else ds.ese_date
    end,
    ds.ese_no,
    case when length(ds.ese_no) > 5 then substr(ds.ese_no,1,length(ds.ese_no)-2)
         else ds.ese_no
    end,
    ds.ese_transdate,
    'N',
    null,
    'N',
    0,
    null,
    null
from
    wc_shorex_ds ds,
    wc_shorex_d d,
    wc_ship_d s,
    wc_obrvoyage_d v,
    max_wid
where
    ds.ese_id = d.ese_id (+)
    and ds.ese_shp_id = s.shipid
    and ds.ese_date > v.cruise_start_date (+)
    and ds.ese_date <= v.cruise_end_date (+)
    and ds.ese_shp_id = v.ship_id (+)
    and d.row_wid is null;

        x:=SQL%ROWCOUNT;
        sComment  := sComment || ', WC_SHOREX_D Inserts: '||x;

COMMIT;
--**************************************
--UPDATES TO FMS SOURCED SHOREX FROM MXP
--**************************************
merge into
    (select *
            from wc_shorex_d
            where shorex_date between trunc(current_date) -30 and trunc(current_date) -1
            and mxp_match = 'N') t1
using
    (select * from  (
                    select shipcode, tourdate, departurecode, portcode, portname, interfacemappingkey id_tour,
                        tourprogramactivitylevel code_activity_level,
                        case when collectionid is null then collectionid
                             else collectionid || ' - ' || collection
                        end as collection,
                        row_number() over (partition by shipcode, tourdate, departurecode order by numberoftickets desc, departuretime nulls last, tourprogramid) r1
                    from MXP_SHOREX_COST
                    where tourdate between trunc(current_date) -30 and trunc(current_date) -1)
     where r1 = 1) t2
on
    (t1.ship_code = t2.shipcode and t1.shorex_date = t2.tourdate and t1.shorex_no = t2.departurecode)
when matched then update set
    t1.shorex_port = t2.portcode,
    t1.port_description = t2.portname,
    t1.id_tour = t2.id_tour,
    t1.code_activity_level = t2.code_activity_level,
    t1.collection = t2.collection,
    t1.mxp_match = 'Y';

commit;

--****************************
--DEDUPE BY MARKING INACTIVES
--****************************
--find dupes where one has been used in WC_SHOREX_F and the other has not

update wc_shorex_d set inactive_flg = 'Y'
where inactive_flg = 'N' and shorex_date between trunc(current_date) - 14 and trunc(current_date) - 1
and row_wid in
(
with
t2 as (--not used in wc_shorex_f
    select
        x.row_wid,
        x.ship_id,
        x.shorex_date,
        x.shorex_no
    from
        wc_shorex_d x,
        (select distinct shorex_wid
            from wc_shorex_f
            where shorex_dt_wid >= to_char(trunc(current_date) - 14,'YYYYMMDD')
            and pos_id is not null) f
    where
        x.inactive_flg = 'N'
        and x.shorex_date between trunc(current_date) - 14 and trunc(current_date) - 1
        and x.row_wid = f.shorex_wid (+)
        and f.shorex_wid is null),
t3 as (--used in wc_shorex_f
    select
        x.row_wid,
        x.ship_id,
        x.shorex_date,
        x.shorex_no
    from
        wc_shorex_d x,
        (select distinct shorex_wid
            from wc_shorex_f
            where shorex_dt_wid >= to_char(trunc(current_date)-14,'YYYYMMDD')
            and pos_id is not null) f
    where
        x.inactive_flg = 'N'
        and x.shorex_date between trunc(current_date) - 14 and trunc(current_date) - 1
        and x.row_wid = f.shorex_wid)
select
    t2.row_wid
from
    t2,t3
where
    t2.ship_id = t3.ship_id
    and t2.shorex_date = t3.shorex_date
    and t2.shorex_no = t3.shorex_no
);

commit;

--*********************
--INSERT NEW MXP SHOREX
--*********************
insert into wc_shorex_d
with max_wid as (select max(row_wid) wid from wc_shorex_d)
select
    max_wid.wid + rownum,
    c.shipid,
    c.shipcode,
    c.shipname,
    v.cruise_id,
    0,
    c.tourname,
    c.tourdate,
    c.portcode,
    c.portname,
    to_char(c.departuretime,'HH24:MI'),
    null,
    null,
    null,
    null,
    0,
    case when c.tourdate = v.cruise_end_date then c.tourdate - 1
         else c.tourdate
    end,
    c.departurecode,
    c.tourcode,
    null,
    'N',
    null,
    'N',
    c.id_tour,
    c.code_activity_level,
    c.collection
from
    (select shipid, t1.shipcode, shipname, tourdate, departurecode, tourcode, MAX(departuretime) departuretime,
        MAX(tourname) tourname, MAX(portcode) portcode, MAX(portname) portname, SUM(totalcost), MAX(interfacemappingkey) id_tour,
        MAX(tourprogramactivitylevel) code_activity_level,
        MAX(case when collectionid is null then collectionid
                 else collectionid || ' - ' || collection
            end) collection
     from mxp_shorex_cost t1, wc_ship_d t2
     where t1.shipcode = t2.shipcode
     and tourdate >= trunc(current_date) - 30
     group by shipid, t1.shipcode, shipname, tourdate, departurecode, tourcode
     having SUM(totalcost) > 0) c,
    wc_obrvoyage_d v,
    wc_shorex_d x,
    max_wid
where
    c.tourdate between v.cruise_start_date+1 and v.cruise_end_date
    and c.shipid = v.ship_id
    and c.shipid = x.ship_id (+)
    and c.tourdate = x.shorex_date (+)
    and c.departurecode = x.shorex_no (+)
    and x.ese_id is null;

        x:=SQL%ROWCOUNT;
        sComment  := sComment || ', WC_SHOREX_D Inserts from MXP: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
       END;
  END WC_SHOREX_D_LOAD;

PROCEDURE WC_SHOREX_F_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_SHOREX_F_LOAD';
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

    execute immediate 'truncate table wc_shorex_fs';

    BEGIN

    insert into wc_shorex_fs
    select
        epo_id,
        epo_ese_id,
        epo_shp_id,
        epo_control,
        epo_pos_id,
        epo_salesprice,
        epo_shp_id * 10000000000 + epo_buyer epo_buyer,
        epo_quant,
        epo_prepaid,
        epo_kiosk,
        epo_void,
        epo_cancelfee,
        epo_shp_id * 10000000000 + epo_cancelfee_id epo_cancel_pos_id,
        epo_shp_id * 10000000000 + epo_cruise epo_cruise,
        epo_pdat,
        epo_booking_origin,
        epo_pos_dept
    from
        epo@prdfms_prdbi1.hq.halw.com,
        ese@prdfms_prdbi1.hq.halw.com
    where
        epo_ese_id = ese_id
        and ese_date >= trunc(current_date) - 14;

        x:=SQL%ROWCOUNT;
        sComment  := 'WC_SHOREX_FS Inserts: '||x;

    COMMIT;

    --****************
    --INSERTS FROM FMS
    --****************
    insert into wc_shorex_f
    select
        fs.epo_id,
        fs.epo_shp_id ship_wid,
        fs.epo_pos_id pos_id,
        to_char(x.shorex_date,'YYYYMMDD') shorex_dt_wid,
        p.row_wid pax_wid,
        v.row_wid voyage_wid,
        x.row_wid shorex_wid,
        fs.epo_quant ticket_qty,
        fs.epo_salesprice rev_amt,
        fs.epo_cancel_pos_id cancel_pos_id,
        fs.epo_cancelfee cancel_flg,
        case when fs.epo_void = -1
                then 'Y'
             else 'N'
        end as void_flg,
        case when fs.epo_void > 0
                then epo_shp_id * 10000000000 + epo_void
             when fs.epo_void = -1
                then fs.epo_id
             else epo_void
        end as void_epo_id,
        epo_booking_origin booking_origin,
        epo_prepaid prepaid_flg,
        epo_kiosk kiosk_flg,
        fs.epo_control txn_group,
        epo_pdat posting_date,
        epo_pos_dept pos_dep_no
    from
        wc_shorex_fs fs,
        wc_obrpax_d p,
        wc_obrvoyage_d v,
        wc_shorex_d x,
        wc_shorex_f f
    where
        fs.epo_buyer = p.pax_spms_id (+)
        and fs.epo_cruise = v.cruise_id (+)
        and fs.epo_ese_id = x.ese_id (+)
        and fs.epo_id = f.epo_id (+)
        and f.epo_id is null;

            x:=SQL%ROWCOUNT;
            sComment  := sComment || ', WC_SHOREX_F Inserts: '||x;

    COMMIT;

    /* Process Updates to Pax and Voyage */
    MERGE INTO
        (SELECT
             *
         FROM
             wc_shorex_f
         WHERE
             pax_wid IS NULL
         AND shorex_dt_wid >= TO_CHAR (SYSDATE - 14, 'YYYYMMDD')) t1
    USING
        (SELECT
             p.row_wid pax_wid, fs.epo_id
         FROM
             wc_obrpax_d p, wc_shorex_fs fs
         WHERE
             p.pax_spms_id = fs.epo_buyer) t2
    ON
        (t1.epo_id = t2.epo_id)
    WHEN MATCHED THEN
        UPDATE SET t1.pax_wid = t2.pax_wid;

    COMMIT;

    MERGE INTO
        (SELECT
             *
         FROM
             wc_shorex_f
         WHERE
             voyage_wid IS NULL
         AND shorex_dt_wid >= TO_CHAR (SYSDATE - 14, 'YYYYMMDD')) t1
    USING
        (SELECT
             v.row_wid voyage_wid, fs.epo_id, fs.epo_pos_id
         FROM
             wc_obrvoyage_d v, wc_shorex_fs fs
         WHERE
             v.cruise_id = fs.epo_cruise) t2
    ON
        (t1.epo_id = t2.epo_id)
    WHEN MATCHED THEN
        UPDATE SET t1.voyage_wid = t2.voyage_wid, t1.pos_id = t2.epo_pos_id;


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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
       END;
  END WC_SHOREX_F_LOAD;

PROCEDURE WC_SHOREX_MARGIN_F_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_SHOREX_MARGIN_F_LOAD';
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

    execute immediate 'truncate table wc_shorex_margin_f_new';

    insert into wc_shorex_margin_f_new
    with t1 as (
        select /*+parallel(f,8)*/
            x.shorex_date, x.shorex_no, x.ship_code, f.ship_wid, f.shorex_dt_wid, f.shorex_wid, sum(f.rev_amt) total_rev, sum(f.ticket_qty) ticket_qty
        from
            wc_shorex_f f, wc_shorex_d x
        where
            f.shorex_wid = x.row_wid and x.inactive_flg = 'N'
        group by
            x.shorex_date, x.shorex_no, x.ship_code, f.ship_wid, f.shorex_dt_wid, f.shorex_wid
        ),
    t2 as (
        select
            shipcode, departurecode, tourdate, sum(nvl(totalcost,0)) total_cost
        from
            mxp_shorex_cost
        group by
            shipcode, departurecode, tourdate
        )
    select
        t1.ship_wid, t1.shorex_dt_wid, t1.shorex_wid, nvl(t1.total_rev,0) total_rev, t1.ticket_qty, nvl(t2.total_cost,0) total_cost,
        case when (nvl(t2.total_cost,0) = 0 or nvl(t1.total_rev,0) = 0) then 0
             else (t1.total_rev - t2.total_cost)/t1.total_rev
        end as gross_margin
    from
        t1, t2
    where
        t1.ship_code = t2.shipcode (+)
        and t1.shorex_no = t2.departurecode (+)
        and t1.shorex_date = t2.tourdate (+);

        x:=SQL%ROWCOUNT;

    COMMIT;

      insert into wc_shorex_margin_f_new
    select x.ship_id, to_char(m.tourdate,'YYYYMMDD'), x.row_wid, 0, sum(nvl(m.numberoftickets,0)), sum(nvl(m.totalcost,0)), 0
    from
        mxp_shorex_cost m, wc_shorex_d x
    where
        m.shipcode = x.ship_code
        and m.tourdate = x.shorex_date
        and m.departurecode = x.shorex_no
        and x.mxp_match = 'N'
        and x.ese_id = 0
    group by
        x.ship_id, to_char(m.tourdate,'YYYYMMDD'), x.row_wid;

            x:=x+SQL%ROWCOUNT;
            sComment  := 'WC_SHOREX_MARGIN_F Inserts: '||x;

    COMMIT;

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

    execute immediate 'alter table wc_shorex_margin_f rename to wc_shorex_margin_f_old';

    execute immediate 'alter table wc_shorex_margin_f_new rename to wc_shorex_margin_f';

    execute immediate 'alter table wc_shorex_margin_f_old rename to wc_shorex_margin_f_new';

  EXCEPTION
    WHEN eCALL_FAILED THEN
            ROLLBACK ;
            eErrMsg :=  'User Defined - Error in called sub-program';

            IF BatchID = 0 THEN
                    x:=-1 ;
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

  END WC_SHOREX_MARGIN_F_LOAD;

PROCEDURE WC_SHOREX_PAX_F_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_SHOREX_PAX_F_LOAD';
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

    execute immediate 'truncate table wc_shorex_pax_f';

 insert /*+append*/ into wc_shorex_pax_f
    with t1 as (
        select /*+parallel(f,4)*/ ship_wid, shorex_wid, shorex_dt_wid, pax_wid, sum(ticket_qty) pax_qty, sum(rev_amt) pax_rev
        from wc_shorex_f f
        group by ship_wid, shorex_wid, shorex_dt_wid, pax_wid
        having sum(ticket_qty) > 0)
    select
        mf.ship_wid,
        mf.shorex_wid,
        mf.shorex_dt_wid,
        d.row_wid obrdep_wid,
        nvl(t1.pax_wid,0) pax_wid,
        nvl(t1.pax_qty,0) pax_qty,
        nvl(t1.pax_rev,0) pax_rev,
        case
            when (t1.shorex_wid is null or mf.ticket_qty = 0)
            then mf.total_cost
            else (t1.pax_qty / mf.ticket_qty) * mf.total_cost
        end pax_cost
    from
        t1,
        wc_shorex_margin_f mf,
        wc_obrdep_d d
    where
        t1.shorex_wid (+) = mf.shorex_wid
        and mf.ship_wid = d.fms_id;

             x:=SQL%ROWCOUNT;
            sComment  := 'WC_SHOREX_PAX_F Inserts: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

  END WC_SHOREX_PAX_F_LOAD;

PROCEDURE WC_OBRTXN_F_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_OBRTXN_F_LOAD';
        BatchID Number := 0;
        v_ship_id       number;
        v_max_ship_id   number;
        v_min_posting_dt_wid number;

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

    execute immediate 'truncate table wc_obrtxn_fs';

    select min(shipid) into v_ship_id from wc_ship_d;
    select max(shipid) into v_max_ship_id from wc_ship_d;
    x:=0;

     while v_ship_id <= v_max_ship_id loop
    INSERT /*+append*/ INTO wc_obrtxn_fs
    SELECT  pos.pos_id,
            pos.pos_pdat posting_date,
            pos.pos_value,
            pos.pos_cruise,
            pos.pos_dep_id,
            pos.pos_shp_id,
            pos.pos_buyer,
            null load_dt,
            pos.pos_odat,
            pos.pos_trans_id
    FROM pos@prdfms_prdbi1.HQ.HALW.COM pos
    WHERE trunc(pos.pos_transdate) between trunc(current_date)-14 and trunc(current_date)-1
    AND pos.pos_shp_id = v_ship_id;

    x:=x+SQL%ROWCOUNT;

    commit;

    v_ship_id := v_ship_id + 1;

    end loop;

            sComment  := 'WC_OBRTXN_FS Inserts: '||x;

/*WC_OBRTXN_F load scenarios*/
    execute immediate 'truncate table wc_obrtxn_fs_delete';

/*Delete rows already in WC_OBRTXN_F*/
    INSERT INTO
        wc_obrtxn_fs_delete
        SELECT
            fs.pos_id
        FROM
            wc_obrtxn_fs fs, wc_obrtxn_f f
        WHERE
            fs.pos_id = f.pos_id
        AND ABS (fs.pos_value) = ABS (f.amount)
        AND TO_CHAR (fs.posting_dt, 'YYYYMMDD') = f.posting_dt_wid;

        COMMIT;

        delete from wc_obrtxn_fs
        where pos_id in (select pos_id from wc_obrtxn_fs_delete);

         x:=SQL%ROWCOUNT;
         sComment  := sComment || ', WC_OBRTXN_FS Dupes Removed: '||x;

        COMMIT;

        MERGE INTO WC_OBRTXN_FS t1
        USING (SELECT PAX_SPMS_ID
                    FROM WC_OBRPAX_D
                    WHERE PAX_DEBARK_DATE >= TRUNC(CURRENT_DATE) - 14) t2
        ON (t1.pos_buyer = t2.pax_spms_id)
        WHEN MATCHED THEN UPDATE SET t1.load_dt = TRUNC(CURRENT_DATE);

        COMMIT;

    select to_char(min(nvl(original_dt,posting_dt)),'YYYYMMDD')
    into v_min_posting_dt_wid
    from wc_obrtxn_fs;

    BEGIN

/*Process WC_OBRTXN_F updates*/
    MERGE INTO
        (SELECT
             pos_id,
             posting_dt_wid,
             voyage_wid,
             sched_wid,
             load_dt,
             amount
         FROM
             wc_obrtxn_f
         WHERE
             posting_dt_wid >= v_min_posting_dt_wid) t1
    USING
        (SELECT
             fs.pos_id,
             TO_CHAR (fs.posting_dt, 'YYYYMMDD') posting_dt_wid,
             ss.voyage_wid,
             ss.row_wid sched_wid,
             fs.load_dt,
             CASE
                 WHEN DD.DEP_TYPE = 'C' THEN -fs.pos_value
                 ELSE fs.pos_value
             END
                 txn_amt
         FROM
             wc_obrtxn_fs fs, wc_ship_sched_d ss, wc_obrdep_d dd
         WHERE
             fs.pos_cruise = ss.cruise_id
         AND fs.posting_dt = ss.cruise_date
         AND fs.pos_dep_id = dd.fms_id
         UNION ALL
         SELECT
             fs.pos_id,
             TO_CHAR (fs.posting_dt, 'YYYYMMDD') posting_dt_wid,
             ss.voyage_wid,
             ss.row_wid sched_wid,
             fs.load_dt,
             CASE
                 WHEN DD.DEP_TYPE = 'C' THEN -fs.pos_value
                 ELSE fs.pos_value
             END
                 txn_amt
         FROM
             wc_obrtxn_fs fs, wc_ship_sched_d ss, wc_obrdep_d dd
         WHERE
             fs.pos_cruise = ss.cruise_id
         AND fs.posting_dt < ss.cruise_start_date
         AND ss.cruise_date = ss.cruise_start_date
         AND fs.pos_dep_id = dd.fms_id
         UNION ALL
         SELECT
             fs.pos_id,
             TO_CHAR (fs.posting_dt, 'YYYYMMDD') posting_dt_wid,
             ss.voyage_wid,
             ss.row_wid sched_wid,
             fs.load_dt,
             CASE
                 WHEN DD.DEP_TYPE = 'C' THEN -fs.pos_value
                 ELSE fs.pos_value
             END
                 txn_amt
         FROM
             wc_obrtxn_fs fs, wc_ship_sched_d ss, wc_obrdep_d dd
         WHERE
             fs.pos_cruise = ss.cruise_id
         AND fs.posting_dt >= ss.cruise_end_date
         AND ss.cruise_date + 1 = ss.cruise_end_date
         AND fs.pos_dep_id = dd.fms_id) t2
    ON
        (t1.pos_id = t2.pos_id)
    WHEN MATCHED THEN
        UPDATE SET t1.posting_dt_wid = t2.posting_dt_wid,
                   t1.voyage_wid = t2.voyage_wid,
                   t1.sched_wid = t2.sched_wid,
                   t1.amount = t2.txn_amt,
                   t1.load_dt = nvl(t2.load_dt,t1.load_dt);

    COMMIT;

/*Delete rows just updated in WC_OBRTXN_F*/
    execute immediate 'truncate table wc_obrtxn_fs_delete';

        insert /*+append*/ into wc_obrtxn_fs_delete
        select f.pos_id
        from wc_obrtxn_fs fs, wc_obrtxn_f f
        where fs.pos_id = f.pos_id
        and f.posting_dt_wid >= v_min_posting_dt_wid;

        COMMIT;

        delete from wc_obrtxn_fs
        where pos_id in (select pos_id from wc_obrtxn_fs_delete);

         x:=SQL%ROWCOUNT;
         sComment  := sComment || ', WC_OBRTXN_F Updates: '||x;

        COMMIT;

/*Process remaining WC_OBRTXN_FS rows as WC_OBRTXN_F inserts*/
    /*load txns between cruise start and cruise end - 1*/
    INSERT INTO WC_OBRTXN_F
    SELECT
          wid.max_wid + ROWNUM,
          ts.pos_id,
          ts.pos_shp_id,
          TO_CHAR (ts.posting_dt, 'YYYYMMDD'),
          dd.row_wid,
          trunc(current_date),
          case  when DD.DEP_TYPE = 'C'
                then -ts.pos_value
                else ts.pos_value
          end,
          0,
          ss.row_wid,
          ps.row_wid,
          v.row_wid
     FROM DM_OBR.wc_obrtxn_fs ts,
          DM_OBR.wc_obrpax_d ps,
          DM_OBR.wc_obrdep_d dd,
          DM_OBR.wc_ship_sched_d ss,
          DM_OBR.wc_obrvoyage_d v,
          (select max(row_wid) max_wid from wc_obrtxn_f) wid
    WHERE ts.pos_buyer = ps.pax_spms_id (+)
      AND ts.pos_dep_id = dd.fms_id
      AND ts.pos_cruise = v.cruise_id
      AND ss.voyage_wid = v.row_wid
      AND ts.posting_dt = ss.cruise_date;

        x:=SQL%ROWCOUNT;

commit;

--load txns before cruise start as occuring on the cruise start
    INSERT INTO WC_OBRTXN_F
    SELECT
          wid.max_wid + ROWNUM,
          ts.pos_id,
          ts.pos_shp_id,
          TO_CHAR (ss.cruise_date, 'YYYYMMDD'),
          dd.row_wid,
          trunc(current_date),
          case  when DD.DEP_TYPE = 'C'
                then -ts.pos_value
                else ts.pos_value
          end,
          0,
          ss.row_wid,
          ps.row_wid,
          v.row_wid
     FROM DM_OBR.wc_obrtxn_fs ts,
          DM_OBR.wc_obrpax_d ps,
          DM_OBR.wc_obrdep_d dd,
          DM_OBR.wc_ship_sched_d ss,
          DM_OBR.wc_obrvoyage_d v,
          (select max(row_wid) max_wid from wc_obrtxn_f) wid
    WHERE ts.pos_buyer = ps.pax_spms_id (+)
      AND ts.pos_dep_id = dd.fms_id
      AND ts.pos_cruise = v.cruise_id
      AND ss.voyage_wid = v.row_wid
      AND ts.posting_dt < ss.cruise_start_date
      AND ss.cruise_date = ss.cruise_start_date;

         x:=x+SQL%ROWCOUNT;

commit;

--load txns on or after cruise end as occuring on cruise end - 1
    INSERT INTO WC_OBRTXN_F
    SELECT
          wid.max_wid + ROWNUM,
          ts.pos_id,
          ts.pos_shp_id,
          TO_CHAR (ss.cruise_date, 'YYYYMMDD'),
          dd.row_wid,
          trunc(current_date),
          case  when DD.DEP_TYPE = 'C'
                then -ts.pos_value
                else ts.pos_value
          end,
          0,
          ss.row_wid,
          ps.row_wid,
          v.row_wid
     FROM DM_OBR.wc_obrtxn_fs ts,
          DM_OBR.wc_obrpax_d ps,
          DM_OBR.wc_obrdep_d dd,
          DM_OBR.wc_ship_sched_d ss,
          DM_OBR.wc_obrvoyage_d v,
          (select max(row_wid) max_wid from wc_obrtxn_f) wid
    WHERE ts.pos_buyer = ps.pax_spms_id (+)
      AND ts.pos_dep_id = dd.fms_id
      AND ts.pos_cruise = v.cruise_id
      AND ss.voyage_wid = v.row_wid
      AND ts.posting_dt >= ss.cruise_end_date
      AND ss.cruise_date = ss.cruise_end_date - 1;

         x:=x+SQL%ROWCOUNT;
         sComment  := sComment || ', WC_OBRTXN_F Inserts: '||x;

commit;

--associate gift cards to their owners
    MERGE INTO
        (SELECT
             pax_spms_id, par_pax_wid
         FROM
             wc_obrpax_d
         WHERE
             pax_type = 'Z'
             ) t1 --gift cards
    USING
        (SELECT
             s2.pos_buyer, p.row_wid
         FROM
             wc_obrtxn_fs s1,
             (SELECT
                  fs.pos_trans_id, fs.pos_buyer, ROW_NUMBER () OVER (PARTITION BY fs.pos_buyer, d.dep_no ORDER BY fs.pos_trans_id) r1
              FROM
                  wc_obrtxn_fs fs, wc_obrdep_d d
              WHERE
                  pos_dep_id = d.dep_id and d.dep_no = '99100') s2,
             wc_obrpax_d p,
             wc_obrdep_d d
         WHERE
             s1.pos_trans_id = s2.pos_trans_id
         AND s1.pos_buyer = p.pax_spms_id
         and s1.pos_dep_id = d.dep_id
         AND p.pax_type in ('P','G')
         AND d.dep_no = '82001'
         AND s2.r1 = 1) t2 --guests or groups who activated gift cards
    ON
        (t1.pax_spms_id = t2.pos_buyer)
    WHEN MATCHED THEN
        UPDATE SET t1.par_pax_wid = t2.row_wid;  --assign guest/group to the gift card

commit;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
    END;
END WC_OBRTXN_F_LOAD;

PROCEDURE WC_OBRREV_A_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_OBRREV_A_LOAD';
        BatchID Number := 0;
        v_min_posting_dt_wid number;

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

    select to_char(min(nvl(original_dt,posting_dt))-14,'YYYYMMDD')
    into v_min_posting_dt_wid
    from wc_obrtxn_fs;
    x:=0;

/*Remove existing WC_OBRREV_A rows for last 14 days*/
    delete from wc_obrrev_a
    where posting_dt_wid >= v_min_posting_dt_wid;

    commit;

/*Load current daily ship totals by department into WC_OBRREV_A for last 14 days*/
    insert /*+append*/ into wc_obrrev_a
    select
            f.sched_wid,
            f.posting_dt_wid,
            f.obrdep_wid,
            f.voyage_wid,
            sum(f.amount) day_amount,
            f.pos_shp_id ship_wid,
            ss.voyage_dt_wid
        from wc_obrtxn_f f, wc_ship_sched_d ss
        where f.sched_wid = ss.row_wid
        and f.posting_dt_wid >= v_min_posting_dt_wid
        group by
            f.sched_wid,
            f.posting_dt_wid,
            f.obrdep_wid,
            f.voyage_wid,
            f.pos_shp_id,
            ss.voyage_dt_wid;

            x:=SQL%ROWCOUNT;
            sComment  := 'WC_OBRREV_A Inserts: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

  END WC_OBRREV_A_LOAD;

PROCEDURE WC_VOYPAX_F_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_VOYPAX_F_LOAD';
        BatchID Number := 0;
        v_min_dt_wid number;

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

        select min(voyage_dt_wid)
        into v_min_dt_wid
        from wc_ship_sched_d
        where cruise_end_date >= trunc(current_date) -14
        and cruise_start_date < trunc(current_date);

        x:=0;

/*Remove existing WC_VOYPAX_F rows for last 14 days*/
    delete from wc_voypax_f
    where voyage_wid in (
                    select row_wid
                    from wc_obrvoyage_d
                    where cruise_end_date >= trunc(current_date) -14
                    and cruise_start_date < trunc(current_date))
    and voyage_dt_wid >= v_min_dt_wid;

    commit;

--load rows for past dates on active voyages or voyages just ended in the last seven days
    insert into wc_voypax_f
    with wid as (select max(row_wid) max_wid from wc_voypax_f)
    select
        wid.max_wid+rownum row_wid,
        p.row_wid pax_wid,
        s.voyage_wid,
        s.voyage_dt_wid,
        s.ship_id ship_wid,
        s.row_wid sched_wid
    from
        wc_obrpax_d p,
        wc_ship_sched_d s,
        wid
    where
        p.pax_ship_id = s.ship_id
        and s.cruise_date between p.pax_embark_date and nvl(p.pax_debark_date,trunc(current_date))-1
        and p.pax_type = 'P'
        and p.acc_tag in ('P','C')
        and p.pax_embark_date < nvl(p.pax_debark_date,trunc(current_date))
        and nvl(p.pax_debark_date,trunc(current_date)) > s.cruise_start_date
        and s.cruise_date < trunc(current_date)
        and s.voyage_wid in (select row_wid
                    from wc_obrvoyage_d
                    where cruise_end_date >= trunc(current_date) -14
                    and cruise_start_date < trunc(current_date));

            x:=SQL%ROWCOUNT;
            sComment  := 'WC_VOYPAX_F Inserts: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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


    --now adjust the ship schedule PCDs
    merge into (select * from wc_ship_sched_d
                where cruise_end_date >= trunc(current_date) -14
                and cruise_start_date < trunc(current_date)) ss
    using (select
            sched_wid,
            pax_count,
            sum(vp.pax_count) over (partition by voyage_wid order by voyage_dt_wid) as pcd
           from
            (select f.voyage_wid, f.sched_wid, f.voyage_dt_wid, count(f.row_wid) pax_count
            from wc_voypax_f f, wc_obrvoyage_d v
            where f.voyage_wid = v.row_wid
            and v.cruise_start_date < trunc(current_date) and v.cruise_end_date >= trunc(current_date) - 14
            and f.voyage_dt_wid >= to_char(trunc(current_date) - 120,'YYYYMMDD')
            group by f.voyage_wid, f.sched_wid, f.voyage_dt_wid
            order by f.sched_wid) vp) pcd
    on (ss.row_wid = pcd.sched_wid)
    when matched then
    update set ss.pax_count = pcd.pax_count, ss.pcd = pcd.pcd;

    commit;

  END WC_VOYPAX_F_LOAD;

PROCEDURE WC_OBRTGT_F_HAL_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_OBRTGT_F_HAL_LOAD';
        BatchID Number := 0;
        v_load_start_date   date;
        v_load_end_date     date;
        v_load_date         date;
        v_ship_id           number;
        v_max_ship_id       number;
        v_min_start_date    date;

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

        select min(shipid)
        into v_ship_id
        from wc_ship_d
        where brand_code = 'H';

        select max(shipid)
        into v_max_ship_id
        from wc_ship_d
        where brand_code = 'H';

        v_min_start_date := trunc(current_date) - 1;

        x:=0;
        sComment:='WC_OBRTGT_F HAL Inserts: ';

    /*loop thru each ship to reload targets by voyage*/
    while v_ship_id <= v_max_ship_id loop

    /*assign date range variables*/
    --v_load_start_date
    with t1 as (
        select v.ship_id, min(v.cruise_start_date) load_start_date
        from obr_targets_load_status ts, wc_obrvoyage_d v
        where
            v.ship_id = v_ship_id
            and ts.ship_wid = v.ship_id
            and ts.max_load_dt -7 between v.cruise_start_date and v.cruise_end_date - 1
        group by v.ship_id)
    select
        case    when t2.max_load_dt = t2.max_budget_dt and t2.max_budget_dt < trunc(current_date) -14
                    then trunc(current_date)
                    else t1.load_start_date
        end into v_load_start_date
    from
        t1, obr_targets_load_status t2
    where
        t1.ship_id = t2.ship_wid;

    --v_load_end_date
    with t1 as (
        select v.ship_id, min(v.cruise_start_date) load_start_date
        from obr_targets_load_status ts, wc_obrvoyage_d v
        where
            v.ship_id = v_ship_id
            and ts.ship_wid = v.ship_id
            and ts.max_load_dt -7 between v.cruise_start_date and v.cruise_end_date - 1
        group by v.ship_id)
    select
        case    when t2.max_load_dt = t2.max_budget_dt and t2.max_budget_dt < trunc(current_date) -14
                    then trunc(current_date) - 1
                    else least(t2.max_budget_dt, trunc(current_date) - 1)
        end into v_load_end_date
    from
        t1, obr_targets_load_status t2
    where
        t1.ship_id = t2.ship_wid;

    --v_load_date
    v_load_date := v_load_start_date;

    /*delete incomplete voyage targets */
        delete from wc_obrtgt_f
        where ship_wid = v_ship_id
        and cruise_date >= v_load_start_date;

        commit;


    /*loop through each voyage date to load actuals and targets */
        while v_load_date <= v_load_end_date loop
                insert into wc_obrtgt_f
                with max_wid as (
                select max(row_wid) wid from wc_obrtgt_f
                ),
                a as ( --get ship pcd and pax count actuals
                select
                    s.schedid,
                    s.pcd,
                    s.pax_count,
                    s.cruise_id,
                    s.voyage_dt_wid,
                    s.cruise_date,
                    s.cruise_start_date,
                    s.cruise_end_date,
                    s.cruisecomplete,
                    s.voyage_wid,
                    s.row_wid sched_wid,
                    s.ship_id,
                    d.dep_id, d.dep_no, d.dep_desc, d.Maingroup, d.Subgroup, d.Ordermain, d.Ordersub, d.dalrec_type, d.row_wid dep_wid
                from            wc_ship_sched_d s, wc_obrdep_d d
                where           s.ship_id = d.ship_id
                    and s.ship_id = v_ship_id       --per ship
                    and s.cruise_date = v_load_date --per cruise date
                    and d.dalrec_type in ('R','C')
                    and d.end_date is null
                ),
                b as ( --get daily and to-date spend actuals
                select          a.schedid, a.cruise_id, a.pcd, a.pax_count,
                    a.dep_id, a.dep_no, a.dep_desc, a.cruise_date,
                    a.cruise_start_date, a.cruise_end_date, a.cruisecomplete,
                    a.Maingroup, a.Subgroup, a.Ordermain, a.Ordersub, a.dalrec_type,
                    a.dep_wid, a.voyage_wid, a.sched_wid,
                    a.ship_id,
                    sum(case    when nvl(t.voyage_dt_wid,a.voyage_dt_wid) = a.voyage_dt_wid
                                then nvl(t.day_amount,0)
                                else 0
                        end) as AmountDay,
                    sum(case    when nvl(t.voyage_dt_wid,a.voyage_dt_wid) <= a.voyage_dt_wid
                                then nvl(t.day_amount,0)
                                else 0
                        end) as AmountToDate,
                    sum(nvl(t.day_amount,0)) as amountcruise
                from            a, wc_obrrev_a t
                where
                    a.dep_wid = t.obrdep_wid (+)
                    and a.voyage_wid = t.voyage_wid (+)
                group by        a.schedid, a.cruise_id, a.pcd, a.pax_count,
                    a.dep_id, a.dep_no, a.dep_desc, a.cruise_date,
                    a.cruise_start_date, a.cruise_end_date, a.cruisecomplete,
                    a.Maingroup, a.Subgroup, a.Ordermain, a.Ordersub, a.dalrec_type,
                    a.dep_wid, a.voyage_wid, a.ship_id, a.sched_wid
                ),
                c as ( --get budgets and to-date budget PCDs
                select
                    b.*,
                    nvl(bud.budget_per_pcd,0) as Budget,
                    (b.cruise_date - b.cruise_start_date + 1) * nvl(bud.budget_pax,0) as budget_pcd,
                    nvl(bud.budget_pax,0) as budget_pax
                from
                    b,
                    wc_budget_f bud
                where
                    b.dep_wid = bud.obrdep_wid(+)
                    and b.ship_id = bud.ship_wid(+)
                    and b.cruise_date = bud.cruise_date(+)
                ),
                shipdata as ( --get voyage totals
                select
                    voyage_wid,
                    sum(amountday) as ActualActivity,
                    sum(budget) as BudgetActivity
                from c
                group by
                    voyage_wid
                ),
                shipdatasummary as ( --mark to exclude voyage with no activity
                select
                    voyage_wid,
                    case    when ActualActivity is null or actualactivity = 0 or budgetactivity is null or budgetactivity = 0
                            then 'No'
                            else 'Yes'
                    end as ReportStatus
                from            shipdata
                ),
                stat as ( --include voyages with activity
                select distinct c.voyage_wid, pcd, pax_count
                from            c, shipdatasummary
                where           c.voyage_wid = shipdatasummary.voyage_wid
                    and reportstatus = 'Yes'
                ),
                statsummary as ( --PCD totals across all voyages with activity
                select          sum(pcd) as pcdTotal,
                    sum (pax_count) as pax_counttotal
                from            stat
                ),
                output as ( --final to-date and per PCD amounts per voyage per ship dept per day.  Includes allocation of shorex spend across the voyage days
                select          cruise_date,
                    c.cruise_id,
                    schedid,
                    MainGroup,
                    Subgroup,
                    Dep_no,
                    Dep_desc,
                    pcd,
                    pax_count,
                    cruisecomplete,
                    cruise_start_date,
                    case    when maingroup = 'Shore Excursions'
                            then sum(AmountToDate)*cruisecomplete
                            else sum(AmountToDate)
                    end as AmountToDate,
                    case    when pcd is null or pcd = 0
                            then 0
                            else    case    when maingroup = 'Shore Excursions'
                                            then sum(AmountToDate)*cruisecomplete
                                            else sum(AmountToDate)
                                    end
                            /pcd
                    end as PerPCD,
                    sum(AmountDay) as AmountDay,
                    case    when pax_count is null or pax_count = 0
                            then 0
                            else    case    when maingroup = 'Shore Excursions'
                                            then sum(AmountDay)*cruisecomplete
                                            else sum(AmountDay)
                                    end
                            /pax_count
                    end as PerPCDDay,
                    sum(Budget) as Budget,
                    sum(Budget)*nvl(pcd,0) as BudgetAmount,
                    sum(Budget)*nvl(pax_count,0) as BudgetAmountDay,
                    pcdtotal,
                    pax_counttotal,
                    reportstatus,
                    case    when pcdtotal is null or pcdtotal = 0
                            then 0
                            else    case    when maingroup = 'Shore Excursions'
                                            then sum(AmountToDate)*cruisecomplete
                                            else sum(AmountToDate)
                                    end
                            /pcdtotal
                    end as PerPCDtotal,
                    case    when pax_counttotal is null or pax_counttotal = 0
                            then 0
                            else    case    when maingroup = 'Shore Excursions'
                                            then sum(AmountDay)*cruisecomplete
                                            else sum(AmountDay)
                                    end
                            /pax_counttotal
                    end as PerPCDDaytotal,
                    c.dep_wid,
                    c.sched_wid,
                    c.voyage_wid,
                    to_char(cruise_date,'YYYYMMDD'),
                    budget_pcd,
                    budget_pax,
                    0 budget_month_pcd,
                    sum(Budget)*nvl(budget_pax,0) as day_budget,
                    cruise_end_date,
                    c.ship_id
                from            c, statsummary, shipdatasummary
                where           c.voyage_wid = shipdatasummary.voyage_wid(+)
                group by        cruise_date, c.cruise_id, schedid, MainGroup, Subgroup, Dep_no, Dep_desc, pcd, pax_count,
                    cruisecomplete, cruise_start_date, cruise_end_date, Ordermain, Ordersub, pcdtotal, pax_counttotal, reportstatus,
                    budget_pcd, c.dep_wid, c.sched_wid, c.voyage_wid, budget_pax, c.ship_id
                order by        c.Cruise_id, Ordermain, Ordersub
                )
                select rownum+max_wid.wid,
                output.*
                from output, max_wid;

                x:=x+SQL%ROWCOUNT;

                COMMIT;

            v_load_date := v_load_date + 1;

            v_min_start_date := least(v_min_start_date,v_load_start_date);

        end loop;

    sComment  := sComment ||', Ship ID ' || v_ship_id || ' : '||x;
    v_ship_id := v_ship_id + 1;
    x := 0;

    end loop;

/*update load status with most current load date*/
    merge into obr_targets_load_status t1
    using (    select /*+parallel(f,4)*/ ship_wid, max(cruise_date) max_load_date
                    from wc_obrtgt_f f
                    where ship_wid between 11 and 25
                    group by ship_wid) t2
    on (t1.ship_wid = t2.ship_wid)
    when matched then update set t1.max_load_dt = t2.max_load_date;

    commit;

/*for each cruise date that still involves an active cruise, recalculate the to-date PCD grand total across all ships*/
    merge into  (select pcdtotal, voyage_dt_wid
                from wc_obrtgt_f
                where cruise_date between v_min_start_date and trunc(current_date) - 1
                and ship_wid between 11 and 25
                ) t1
    using       (select voyage_dt_wid, sum(pcd) pcdtot
                from wc_obrtgt_f
                where cruise_date between v_min_start_date and trunc(current_date) - 1
                and dep_no = '50001'
                and ship_wid between 11 and 25
                group by voyage_dt_wid
                ) t2
    on (t1.voyage_dt_wid = t2.voyage_dt_wid)
    when matched then update set t1.pcdtotal = t2.pcdtot;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

  END WC_OBRTGT_F_HAL_LOAD;

PROCEDURE WC_OBRTGT_F_SBN_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_OBRTGT_F_SBN_LOAD';
        BatchID Number := 0;
        v_load_start_date   date;
        v_load_end_date     date;
        v_load_date         date;
        v_ship_id           number;
        v_max_ship_id       number;
        v_min_start_date    date;

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

        select min(shipid)
        into v_ship_id
        from wc_ship_d
        where brand_code = 'S';

        select max(shipid)
        into v_max_ship_id
        from wc_ship_d
        where brand_code = 'S';

        v_min_start_date := trunc(current_date) - 1;

        x:=0;
        sComment:='WC_OBRTGT_F SBN Inserts: ';

    /*loop thru each ship to reload targets by voyage*/
    while v_ship_id <= v_max_ship_id loop

    /*assign date range variables*/
    --v_load_start_date
    with t1 as (
        select v.ship_id, min(v.cruise_start_date) load_start_date
        from obr_targets_load_status ts, wc_obrvoyage_d v
        where
            v.ship_id = v_ship_id
            and ts.ship_wid = v.ship_id
            and ts.max_load_dt -7 between v.cruise_start_date and v.cruise_end_date - 1
        group by v.ship_id)
    select
        case    when t2.max_load_dt = t2.max_budget_dt and t2.max_budget_dt < trunc(current_date) -14
                    then trunc(current_date)
                    else t1.load_start_date
        end into v_load_start_date
    from
        t1, obr_targets_load_status t2
    where
        t1.ship_id = t2.ship_wid;

    --v_load_end_date
    with t1 as (
        select v.ship_id, min(v.cruise_start_date) load_start_date
        from obr_targets_load_status ts, wc_obrvoyage_d v
        where
            v.ship_id = v_ship_id
            and ts.ship_wid = v.ship_id
            and ts.max_load_dt -7 between v.cruise_start_date and v.cruise_end_date - 1
        group by v.ship_id)
    select
        case    when t2.max_load_dt = t2.max_budget_dt and t2.max_budget_dt < trunc(current_date) -14
                    then trunc(current_date) - 1
                    else least(t2.max_budget_dt, trunc(current_date) - 1)
        end into v_load_end_date
    from
        t1, obr_targets_load_status t2
    where
        t1.ship_id = t2.ship_wid;

    --v_load_date
    v_load_date := v_load_start_date;

    /*delete incomplete voyage targets */
        delete from wc_obrtgt_f
        where ship_wid = v_ship_id
        and cruise_date >= v_load_start_date;

        commit;

    --loop through each voyage date to load actuals and targets
        while v_load_date <= v_load_end_date loop
                insert into wc_obrtgt_f
                with max_wid as (
                select max(row_wid) wid from wc_obrtgt_f
                ),
                a as ( --get ship pcd and pax count actuals
                select
                    s.schedid,
                    s.pcd,
                    s.pax_count,
                    s.cruise_id,
                    s.voyage_dt_wid,
                    s.cruise_date,
                    s.cruise_start_date,
                    s.cruise_end_date,
                    s.cruisecomplete,
                    s.voyage_wid,
                    s.row_wid sched_wid,
                    s.ship_id,
                    d.dep_id, d.dep_no, d.dep_desc, d.Maingroup, d.Subgroup, d.Ordermain, d.Ordersub, d.dalrec_type, d.row_wid dep_wid
                from            wc_ship_sched_d s, wc_obrdep_d d
                where           s.ship_id = d.ship_id
                    and s.ship_id = v_ship_id       --per ship
                    and s.cruise_date = v_load_date --per cruise date
                    and d.dalrec_type in ('R','C')
                    and d.end_date is null
                ),
                b as ( --get daily and to-date spend actuals
                select          a.schedid, a.cruise_id, a.pcd, a.pax_count,
                    a.dep_id, a.dep_no, a.dep_desc, a.cruise_date,
                    a.cruise_start_date, a.cruise_end_date, a.cruisecomplete,
                    a.Maingroup, a.Subgroup, a.Ordermain, a.Ordersub, a.dalrec_type,
                    a.dep_wid, a.voyage_wid, a.sched_wid,
                    a.ship_id,
                    sum(case    when nvl(t.voyage_dt_wid,a.voyage_dt_wid) = a.voyage_dt_wid
                                then nvl(t.day_amount,0)
                                else 0
                        end) as AmountDay,
                    sum(case    when nvl(t.voyage_dt_wid,a.voyage_dt_wid) <= a.voyage_dt_wid
                                then nvl(t.day_amount,0)
                                else 0
                        end) as AmountToDate,
                    sum(nvl(t.day_amount,0)) as amountcruise
                from            a, wc_obrrev_a t
                where
                    a.dep_wid = t.obrdep_wid (+)
                    and a.voyage_wid = t.voyage_wid (+)
                group by        a.schedid, a.cruise_id, a.pcd, a.pax_count,
                    a.dep_id, a.dep_no, a.dep_desc, a.cruise_date,
                    a.cruise_start_date, a.cruise_end_date, a.cruisecomplete,
                    a.Maingroup, a.Subgroup, a.Ordermain, a.Ordersub, a.dalrec_type,
                    a.dep_wid, a.voyage_wid, a.ship_id, a.sched_wid
                ),
                c as ( --get budgets and to-date budget PCDs
                select
                    b.*,
                    nvl(bud.budget_per_pcd,0) as Budget,
                    (b.cruise_date - b.cruise_start_date + 1) * nvl(bud.budget_pax,0) as budget_pcd,
                    nvl(bud.budget_pax,0) as budget_pax
                from
                    b,
                    wc_budget_f bud
                where
                    b.dep_wid = bud.obrdep_wid(+)
                    and b.ship_id = bud.ship_wid(+)
                    and b.cruise_date = bud.cruise_date(+)
                ),
                shipdata as ( --get voyage totals
                select
                    voyage_wid,
                    sum(amountday) as ActualActivity,
                    sum(budget) as BudgetActivity
                from c
                group by
                    voyage_wid
                ),
                shipdatasummary as ( --mark to exclude voyage with no activity
                select
                    voyage_wid,
                    case    when ActualActivity is null or actualactivity = 0 or budgetactivity is null or budgetactivity = 0
                            then 'No'
                            else 'Yes'
                    end as ReportStatus
                from            shipdata
                ),
                stat as ( --include voyages with activity
                select distinct c.voyage_wid, pcd, pax_count
                from            c, shipdatasummary
                where           c.voyage_wid = shipdatasummary.voyage_wid
                    and reportstatus = 'Yes'
                ),
                statsummary as ( --PCD totals across all voyages with activity
                select          sum(pcd) as pcdTotal,
                    sum (pax_count) as pax_counttotal
                from            stat
                ),
                output as ( --final to-date and per PCD amounts per voyage per ship dept per day.  Includes allocation of shorex spend across the voyage days
                select          cruise_date,
                    c.cruise_id,
                    schedid,
                    MainGroup,
                    Subgroup,
                    Dep_no,
                    Dep_desc,
                    pcd,
                    pax_count,
                    cruisecomplete,
                    cruise_start_date,
                    case    when maingroup = 'Shore Excursions'
                            then sum(AmountToDate)*cruisecomplete
                            else sum(AmountToDate)
                    end as AmountToDate,
                    case    when pcd is null or pcd = 0
                            then 0
                            else    case    when maingroup = 'Shore Excursions'
                                            then sum(AmountToDate)*cruisecomplete
                                            else sum(AmountToDate)
                                    end
                            /pcd
                    end as PerPCD,
                    sum(AmountDay) as AmountDay,
                    case    when pax_count is null or pax_count = 0
                            then 0
                            else    case    when maingroup = 'Shore Excursions'
                                            then sum(AmountDay)*cruisecomplete
                                            else sum(AmountDay)
                                    end
                            /pax_count
                    end as PerPCDDay,
                    sum(Budget) as Budget,
                    sum(Budget)*nvl(pcd,0) as BudgetAmount,
                    sum(Budget)*nvl(pax_count,0) as BudgetAmountDay,
                    pcdtotal,
                    pax_counttotal,
                    reportstatus,
                    case    when pcdtotal is null or pcdtotal = 0
                            then 0
                            else    case    when maingroup = 'Shore Excursions'
                                            then sum(AmountToDate)*cruisecomplete
                                            else sum(AmountToDate)
                                    end
                            /pcdtotal
                    end as PerPCDtotal,
                    case    when pax_counttotal is null or pax_counttotal = 0
                            then 0
                            else    case    when maingroup = 'Shore Excursions'
                                            then sum(AmountDay)*cruisecomplete
                                            else sum(AmountDay)
                                    end
                            /pax_counttotal
                    end as PerPCDDaytotal,
                    c.dep_wid,
                    c.sched_wid,
                    c.voyage_wid,
                    to_char(cruise_date,'YYYYMMDD'),
                    budget_pcd,
                    budget_pax,
                    0 budget_month_pcd,
                    sum(Budget)*nvl(budget_pax,0) as day_budget,
                    cruise_end_date,
                    c.ship_id
                from            c, statsummary, shipdatasummary
                where           c.voyage_wid = shipdatasummary.voyage_wid(+)
                group by        cruise_date, c.cruise_id, schedid, MainGroup, Subgroup, Dep_no, Dep_desc, pcd, pax_count,
                    cruisecomplete, cruise_start_date, cruise_end_date, Ordermain, Ordersub, pcdtotal, pax_counttotal, reportstatus,
                    budget_pcd, c.dep_wid, c.sched_wid, c.voyage_wid, budget_pax, c.ship_id
                order by        c.Cruise_id, Ordermain, Ordersub
                )
                select rownum+max_wid.wid,
                output.*
                from output, max_wid;

                x:=x+SQL%ROWCOUNT;

                COMMIT;

            v_load_date := v_load_date + 1;

            v_min_start_date := least(v_min_start_date,v_load_start_date);

        end loop;

    sComment  := sComment ||', Ship ID ' || v_ship_id || ' : '||x;
    v_ship_id := v_ship_id + 1;
    x := 0;

    end loop;

/*update load status with most current load date*/
    merge into obr_targets_load_status t1
    using (    select /*+parallel(f,4)*/ ship_wid, max(cruise_date) max_load_date
                    from wc_obrtgt_f f
                    where ship_wid between 51 and 56
                    group by ship_wid) t2
    on (t1.ship_wid = t2.ship_wid)
    when matched then update set t1.max_load_dt = t2.max_load_date;

    commit;

    --for each cruise date that still involves an active cruise, recalculate the to-date PCD grand total across all ships
merge into  (select pcdtotal, voyage_dt_wid
            from wc_obrtgt_f
            where cruise_date between v_min_start_date and v_load_end_date
            and ship_wid between 51 and 56
            ) t1
using       (select voyage_dt_wid, sum(pcd) pcdtot
            from wc_obrtgt_f
            where cruise_date between v_min_start_date and v_load_end_date
            and dep_no = '50001'
            and ship_wid between 51 and 56
            group by voyage_dt_wid
            ) t2
on (t1.voyage_dt_wid = t2.voyage_dt_wid)
when matched then update set t1.pcdtotal = t2.pcdtot;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

  END WC_OBRTGT_F_SBN_LOAD;

PROCEDURE WC_ITEM_TXN_F_LOAD IS

        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_ITEM_TXN_F_LOAD';
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

    execute immediate 'truncate table wc_item_txn_fs';

        x:=0;

    /*stage and load item transaction details*/
    INSERT /*+append*/ INTO wc_item_txn_fs
    SELECT
        sscd_id,
        ssci_shp_id,
        ssci_cruise,
        ssci_account_id,
        ssci_check_id,
        ssci_track,
        sscd_item_id,
        sscd_sold,
        sscd_price,
        sscd_total,
        ssci_id,
        ssci_server_id,
        ssci_server_name,
        ssci_xrvc_code,
        ssci_system_date,
        ssci_check_date,
        sscd_item_description
    FROM
        sscd@prdfms_prdbi1.hq.halw.com sscd,
        ssci@prdfms_prdbi1.hq.halw.com ssci
    WHERE
        sscd_shp_id in (select distinct shipid from wc_ship_d)
        and sscd_ssci_id = ssci_id
        and sscd_shp_id = ssci_shp_id
        and ssci_check_date >= trunc(current_date) - 14;

                x:=SQL%ROWCOUNT;

    COMMIT;

            sComment  := 'WC_ITEM_TXN_FS Inserts: '||x;

    /*Item Server inserts*/
    INSERT
          INTO
        wc_item_server_d
        SELECT
            ROWNUM + t3.max_wid,
            t1.ship_wid,
            t1.server_id,
            t1.server_name,
            t1.start_ts,
            t1.end_ts
        FROM
            (SELECT
                 ssci_shp_id ship_wid,
                 ssci_server_id server_id,
                 UPPER (nvl(ssci_server_name,ssci_server_id)) server_name,
                 MIN (ssci_check_date) start_ts,
                 MAX (ssci_check_date) end_ts
             FROM
                 wc_item_txn_fs
             GROUP BY
                 ssci_shp_id, ssci_server_id, UPPER (nvl(ssci_server_name,ssci_server_id))) t1,
            wc_item_server_d t2,
            (select max(row_wid) max_wid from wc_item_server_d) t3
        WHERE
            t1.ship_wid = t2.ship_wid(+) AND
            t1.server_id = t2.server_id(+) AND
            t1.server_name = t2.server_name(+) AND
            t2.row_wid IS NULL;

    commit;

    /*Item Server Updates*/
MERGE INTO
    wc_item_server_d t1
USING
    (SELECT
         ssci_shp_id ship_wid,
         ssci_server_id server_id,
         UPPER (nvl(ssci_server_name,ssci_server_id)) server_name,
         MAX (ssci_check_date) end_ts
     FROM
         wc_item_txn_fs
     GROUP BY
         ssci_shp_id, ssci_server_id, UPPER (nvl(ssci_server_name,ssci_server_id))) t2
ON
    (t1.ship_wid = t2.ship_wid AND
     t1.server_id = t2.server_id AND
     t1.server_name = t2.server_name)
WHEN MATCHED THEN
    UPDATE SET t1.end_ts = t2.end_ts;

    commit;

    /*Item Txn Updates*/
    merge into (select /*+parallel(f, 4)*/ bill_dtl_id, pax_wid, voyage_wid from wc_item_txn_f f) t1
    using (
        select fs.sscd_id, p.row_wid pax_wid, v.row_wid voyage_wid
        from  wc_obrvoyage_d v, wc_obrpax_d p, wc_item_txn_fs fs
        where fs.ssci_account_id = p.pax_spms_id (+) and  fs.ssci_cruise = v.cruise_id (+)
        ) t2
    on (t1.bill_dtl_id = t2.sscd_id)
    when matched then update set t1.pax_wid = t2.pax_wid, t1.voyage_wid = t2.voyage_wid;

 commit;

    /*Item Txn Inserts*/
    insert into wc_item_txn_f
    select /*+parallel (f,4)*/
        fs.sscd_id bill_dtl_id,
        fs.ssci_id bill_id,
        fs.ssci_check_id bill_no,
        fs.ssci_shp_id ship_wid,
        fs.ssci_track sales_loc,
        fs.sscd_item_id item_number,
        fs.sscd_sold item_qty,
        fs.sscd_price item_price,
        fs.sscd_total item_total,
        fs.ssci_xrvc_code rev_ctr_cd,
        trunc(fs.ssci_check_date) bill_sys_dt,
        fs.ssci_check_date  bill_sys_ts,
        v.row_wid voyage_wid,
        p.row_wid pax_wid,
        case when fs.sscd_item_description like '* %'
             then SUBSTR(fs.sscd_item_description,3, length(fs.sscd_item_description) - 2)
             else fs.sscd_item_description
        end,
        case when fs.ssci_system_date >= v.cruise_end_date then to_char(v.cruise_end_date - 1,'YYYYMMDD') * 100 + v.ship_id
                when fs.ssci_system_date < v.cruise_start_date then to_char(v.cruise_start_date,'YYYYMMDD') * 100 + v.ship_id
                else to_char(fs.ssci_system_date,'YYYYMMDD') * 100 + fs.ssci_shp_id
        end,
        to_char(fs.ssci_check_date, 'HH24') bill_time_of_day,
        nvl(d.row_wid, 0) server_wid
    from
        wc_item_txn_fs fs,
        wc_obrpax_d p,
        wc_obrvoyage_d v,
        wc_item_txn_f f,
        wc_item_server_d d
    where
        fs.ssci_cruise = v.cruise_id (+)
        and fs.ssci_account_id = p.pax_spms_id (+)
        and fs.ssci_shp_id = p.pax_ship_id (+)
        and fs.sscd_id = f.bill_dtl_id (+)
        and fs.ssci_server_id = d.server_id (+)
        and fs.ssci_shp_id = d.ship_wid (+)
        and UPPER (nvl(ssci_server_name,ssci_server_id)) = d.server_name (+)
        and fs.ssci_check_date between d.start_ts (+) and d.end_ts (+)
        and f.bill_dtl_id is null;

                x:=SQL%ROWCOUNT;
                sComment  := sComment || ', WC_ITEM_TXN_F Inserts: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

  END WC_ITEM_TXN_F_LOAD;

PROCEDURE WC_PAXLEG_F_LOAD IS
        cProcedure_NM  CONSTANT  Varchar2(30) := 'WC_PAXLEG_F_LOAD';
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

    execute immediate 'truncate table wc_paxleg_fs';

    BEGIN

        insert into wc_paxleg_fs
        select
            leg_id,
            leg_shp_id,
            leg_acc_id,
            leg_emb,
            leg_dis,
            leg_number,
            leg_shorebookid,
            leg_shorevoyageid,
            leg_cruiseid,
            leg_deleted,
            substr(leg_v_guestid,8,1) bkng_party_nbr
        from
            leg@prdfms_prdbi1.hq.halw.com
        where
            leg_emb >= trunc(current_date) - 14;

        x:=SQL%ROWCOUNT;
        sComment  := 'WC_PAXLEG_FS Inserts: '||x;

        commit;

        merge into wc_paxleg_f t1
        using (
            select
                fs.leg_id,
                fs.leg_shp_id ship_wid,
                p.row_wid pax_wid,
                v.row_wid voyage_wid,
                fs.leg_emb embark_dt,
                fs.leg_dis debark_dt,
                fs.leg_number leg_nbr,
                fs.leg_deleted deleted_flg,
                fs.leg_shorebookid bkng_nbr,
                fs.leg_shorevoyageid voyage_cd,
                fs.bkng_party_nbr
            from
                wc_paxleg_fs fs,
                wc_obrpax_d p,
                wc_obrvoyage_d v
            where
                fs.leg_acc_id = p.pax_spms_id
                and fs.leg_cruiseid = v.cruise_id) t2
        on (t1.leg_id = t2.leg_id)
        when matched then update set
            t1.pax_wid = t2.pax_wid,
            t1.voyage_wid = t2.voyage_wid,
            t1.embark_dt = t2.embark_dt,
            t1.debark_dt = t2.debark_dt,
            t1.leg_nbr = t2.leg_nbr,
            t1.deleted_flg = t2.deleted_flg,
            t1.bkng_nbr = t2.bkng_nbr,
            t1.voyage_cd = t2.voyage_cd,
            t1.bkng_party_nbr = t2.bkng_party_nbr
        when not matched then insert
                VALUES(t2.leg_id,
                t2.ship_wid,
                t2.pax_wid,
                t2.voyage_wid,
                t2.embark_dt,
                t2.debark_dt,
                t2.leg_nbr,
                t2.deleted_flg,
                t2.bkng_nbr,
                t2.voyage_cd,
                t2.bkng_party_nbr);

        x:=SQL%ROWCOUNT;
        sComment  := sComment || ', WC_PAXLEG_F Inserts: '||x;

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
    END;
END WC_PAXLEG_F_LOAD;

PROCEDURE MS_PARTITION_DATES_LOAD IS
        cProcedure_NM  CONSTANT  Varchar2(30) := 'MS_PARTITION_DATES_LOAD';
        BatchID Number := 0;
        v_partition_date Date := null;

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

       IF to_char(current_date, 'DAY') = 'SUNDAY'
       THEN
            v_partition_date := trunc(current_date);
       END IF;

        UPDATE MS_PARTITION_LOAD_SCHEDULE
        SET CURRENT_PARTITION_START_DT = NVL(v_partition_date, CURRENT_PARTITION_START_DT)
        WHERE CUBE_NAME = 'OBR_FULL';

       COMMIT;

        sComment  := 'MS_PARTITION_LOAD_SCHEDULE dates updated';

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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
END MS_PARTITION_DATES_LOAD;

  PROCEDURE END_OBR_LOAD IS
        cProcedure_NM  CONSTANT  Varchar2(30) := 'END_OBR_LOAD';
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

       sComment  := 'OBR Data Mart load has ended ';

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
                    /* Record error against palceholder w/ batch of -1 since batch not recorded */
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

END END_OBR_LOAD;

END;
/