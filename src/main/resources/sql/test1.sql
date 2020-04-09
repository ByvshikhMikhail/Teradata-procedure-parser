INSERT INTO prd_db_stg.gtt_oprt_lost(
             src_system_id
           , oprt_type_id
           , day_id
           , whs_id
           , art_id
           , lost_whs_type_id
           , oprt_id
           , src_oprt_art_id
           , cntr_id
           , is_suppl_define
           , suppl_type
           , is_priority
           , qnty
           , oprt_sum
           , oprt_sum_wo_nds
           , oprt_sum_tz
           , oprt_sum_tz_wo_nds)
        SELECT
                COALESCE(rc.src_system_id, op.dwh_src_system_id)       AS SRC_SYSTEM_ID
              , op.dwh_oprt_type_id                                    AS OPRT_TYPE_ID
              , op.oprt_dt                                             AS DAY_ID
              , COALESCE(rc.whs_prn_id, op.dwh_whs_id)                 AS WHS_ID
              , op.dwh_art_id                                          AS ART_ID
              , acw.lost_whs_type_id                                   AS LOST_WHS_TYPE_ID
              , op.dwh_oprt_id                                         AS OPRT_ID
              , op.src_oprt_art_id                                     AS SRC_OPRT_ART_ID
              , op.dwh_cntr_id                                         AS CNTR_ID
              , lcr.is_suppl_define                                    AS IS_SUPPL_DEFINE
              , lcr.suppl_type                                         AS SUPPL_TYPE
              , lcr.is_priority                                        AS IS_PRIORITY
              , (u.corr_qnty (FLOAT)) * lcr.qnty_coef (DECIMAL(18, 6)) AS QNTY
              , (u.corr_oprt_sum (FLOAT)) * lcr.opsum_coef + (((u.corr_qnty (FLOAT)) * ZEROIFNULL(op.pricebuy (FLOAT))) (FLOAT)) * lcr.cp_sum_coef (DECIMAL(18,6)) AS OPRT_SUM
              , (get_opsum_wo_nds(NULL,NULL,NULL, op.nds_fact_pct, u.corr_oprt_sum) (FLOAT)) * lcr.opsum_coef
                + ((u.corr_qnty (DECIMAL(18,6))) * ZEROIFNULL(op.pricebuy_wo_nds (FLOAT)) (DECIMAL(18,6))) * lcr.cp_sum_coef  AS OPRT_SUM_WO_NDS
              , (u.corr_oprt_sum (FLOAT)) * lcr.opsum_coef + (u.corr_qnty * (COALESCE(op.pricebuy_tz, op.pricebuy, 0) (FLOAT)) (FLOAT)) * lcr.cp_sum_coef AS OPRT_SUM_TZ
              , (get_opsum_wo_nds(NULL,NULL,NULL, nds_fact_pct, u.corr_oprt_sum) (FLOAT)) * lcr.opsum_coef
                + (u.corr_qnty * (COALESCE(op.pricebuy_tz_wo_nds, op.pricebuy_wo_nds, 0) (FLOAT)) (FLOAT)) * lcr.cp_sum_coef AS OPRT_SUM_TZ_WO_NDS
          FROM (SELECT u.day_id
                     , u.oprt_id
                     , u.src_oprt_art_id
                     , MAX(u.corr_qnty) AS CORR_QNTY
                     , MAX(u.corr_oprt_sum) AS CORR_OPRT_SUM
                  FROM prd_db_dm.d_oprt_lost_corr u
                 WHERE u.is_use = 1
                   AND u.day_id BETWEEN v_calc_begin_dt AND v_calc_end_dt
                   AND u.lost_corr_type_id = 1
                   AND u.is_prc_delete = 0
                   AND u.is_user_delete = 0
                   AND (u.resp_qnty IS NULL
                    OR u.resp_oprt_sum IS NULL
                    OR u.resp_oprt_sum_wo_nds IS NULL
                    OR u.resp_oprt_sum_tz IS NULL
                    OR u.resp_oprt_sum_tz_wo_nds IS NULL)
                 GROUP BY
                       u.day_id
                     , u.oprt_id
                     , u.src_oprt_art_id) u
         INNER JOIN prd_vd_rdv.v_oprt_art_l op ON op.oprt_dt = u.day_id
                                              AND op.dwh_oprt_id = u.oprt_id
                                              AND op.src_oprt_art_id = u.src_oprt_art_id
         INNER JOIN prd_db_stg.gtt_art_cross_whs acw ON op.dwh_art_id = acw.art_id
                                                    AND op.dwh_whs_id = acw.whs_id
                                                    AND op.dwh_src_system_id = acw.src_system_id
         INNER JOIN prd_db_stg.gtt_lost_struct lcr   ON op.dwh_oprt_type_id = lcr.oprt_type_id
                                                    AND acw.lost_whs_type_id = lcr.lost_whs_type_id
                                                    AND acw.grp_art_id = lcr.grp_art_id
                                                    AND (lcr.is_sp = -1 OR (lcr.is_sp <> -1 AND lcr.is_sp = acw.is_sp))
          LEFT JOIN prd_db_stg.gtt_lost_rc rc        ON acw.whs_id = rc.whs_id
                                                    AND acw.lost_whs_type_id = 0
          LEFT JOIN prd_db_stg.gtt_lost_cntr cntr    ON (lcr.cntr_type > 0
                                                     OR (lcr.is_suppl_define = 0
                                                    AND lcr.suppl_type > 0))
                                                    AND COALESCE(op.dwh_cntr_id, -1) = cntr.cntr_id
         WHERE op.dwh_oprt_type_prn_id IN (30, 42, 44, 32)
           AND (lcr.cntr_type = 0
            OR (lcr.cntr_type > 0
           AND (lcr.cntr_type = cntr.is_external_cntr OR lcr.cntr_type = cntr.is_rc_cntr OR lcr.cntr_type = cntr.is_whs_cntr)))
           AND (lcr.is_suppl_define = 1
                OR (lcr.is_suppl_define = 0 AND lcr.suppl_type = 0)
                OR (lcr.is_suppl_define = 0
                    AND lcr.suppl_type > 0
                    AND COALESCE(op.dwh_cntr_id, -1) <> -1
                    AND (lcr.suppl_type = COALESCE(cntr.is_rc_cntr, -1)
                         OR lcr.suppl_type = COALESCE(cntr.is_external_cntr, -1)
                         OR lcr.suppl_type = COALESCE(cntr.is_whs_cntr, -1))
                    ))
           AND op.oprt_dt BETWEEN v_calc_begin_dt AND v_calc_end_dt;