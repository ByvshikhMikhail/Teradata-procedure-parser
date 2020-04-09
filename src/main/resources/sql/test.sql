SELECT u.day_id
                     , u.oprt_id
                     , u.src_oprt_art_id  
                     , MAX(u.corr_qnty) AS CORR_QNTY
                     , MAX(u.corr_oprt_sum) AS CORR_OPRT_SUM
                  FROM prd_db_dm.d_oprt_lost_corr u 
INNER JOIN prd_db_stg.gtt_art_cross_whs acw ON u.dwh_art_id = acw.art_id
                                                    AND u.dwh_whs_id = acw.whs_id
                                                    AND u.dwh_src_system_id = acw.src_system_id