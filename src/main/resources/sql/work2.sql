set he = dsf;
INSERT INTO prd_db_stg.gtt_oprt_lost_corr(
            lost_corr_type_id
          , oprt_type_id
          , day_id
          , whs_id
          , art_id
          , oprt_id
          , src_oprt_art_id
          , resp_whs_id
          , qnty
          , oprt_sum
          , corr_qnty
          , corr_oprt_sum
          , description
          , is_use
          , is_user_delete
          , user_name
          , resp_qnty
          , resp_oprt_sum
          , resp_oprt_sum_wo_nds
          , resp_oprt_sum_tz
          , resp_oprt_sum_tz_wo_nds)
        SELECT u.lost_corr_type_id
             , ol.oprt_type_id
             , u.day_id
             , ol.whs_id
             , u.art_id
             , u.oprt_id
             , ol.src_oprt_art_id
             , ol.resp_whs_id
             , oa.qnty
             , oa.oprt_sum
             , u.corr_qnty
             , u.corr_oprt_sum
             , u.description
             , u.is_use
             , u.is_delete
             , u.user_name
             , CASE WHEN u.is_use = 0 THEN 0 ELSE (u.corr_qnty (FLOAT)) / NULLIFZERO(oa.qnty) * ol.qnty END AS RESP_QNTY   
             , CASE WHEN u.is_use = 0 THEN 0 ELSE (u.corr_oprt_sum (FLOAT)) / NULLIFZERO(oa.oprt_sum) * ol.oprt_sum END AS RESP_OPRT_SUM
             , CASE WHEN u.is_use = 0 THEN 0 ELSE (u.corr_oprt_sum (FLOAT)) / NULLIFZERO(oa.oprt_sum) * ol.oprt_sum_wo_nds END AS RESP_OPRT_SUM_WO_NDS
             , CASE WHEN u.is_use = 0 THEN 0 ELSE (u.corr_oprt_sum (FLOAT)) / NULLIFZERO(oa.oprt_sum) * ol.oprt_sum_tz END AS RESP_OPRT_SUM_TZ
             , CASE WHEN u.is_use = 0 THEN 0 ELSE (u.corr_oprt_sum (FLOAT)) / NULLIFZERO(oa.oprt_sum) * ol.oprt_sum_tz_wo_nds END AS RESP_OPRT_SUM_TZ_WO_NDS
          FROM user_param.lost_corr u
         INNER JOIN prd_vd_rdv.v_oprt_art_l oa ON u.oprt_type_id = oa.dwh_oprt_type_id
                                               AND u.oprt_id = oa.dwh_oprt_id
                                               AND u.day_id = oa.oprt_dt
                                               AND u.whs_id = oa.dwh_whs_id
                                               AND u.art_id = oa.dwh_art_id
         INNER JOIN prd_db_dm.d_oprt_lost ol ON oa.oprt_dt = ol.day_id
                                            AND oa.dwh_oprt_id = ol.oprt_id
                                            AND oa.src_oprt_art_id = ol.src_oprt_art_id
         WHERE u.day_id BETWEEN v_calc_begin_dt AND v_calc_end_dt
           AND oa.oprt_dt BETWEEN v_calc_begin_dt AND v_calc_end_dt
           AND ol.day_id BETWEEN v_calc_begin_dt AND v_calc_end_dt
           AND oa.dwh_oprt_type_prn_id IN (30, 42, 44, 32);