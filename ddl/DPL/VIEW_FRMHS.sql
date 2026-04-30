-- DPL.VIEW_FRMHS source

CREATE OR REPLACE FORCE VIEW "DPL"."VIEW_FRMHS" ("FRMHS_NO", "LSIND_REGIST_NO", "FRMHS_NM", "FRMHS_AR", "BSNS_STTUS_SE", "FRMHS_FCLTY_SE", "CTTPC_CN", "CTTPC_DETAIL_CN", "POST_SN", "LEGALDONG_CL", "MNTN_LC", "BON_LC", "BU_LC", "HP_NO", "TEL_NO", "BLDNG_MNG_NO", "ROAD_NM_CL", "ROAD_NM", "UNDR_SE", "BLDNG_BON", "BLDNG_BU", "BLDNG_NM", "DETAIL_BLDNG_NM", "FDB_SE", "FDB_FRMHS_SN", "FRST_CRTR_ID", "FRST_CREAT_DT", "FRST_CRTR_IP_NM", "LAST_CHANGER_ID", "LAST_CHANGE_DT", "LAST_CHANGER_IP_NM", "AROW", "BROW", "CROW", "DROW", "TATTOO_NO", "MG_REG_NO", "POSESN_STLE_CODE") AS
SELECT /*+ USE_NL(a,b,c,d) */
    a.frmhs_no,
    a.lsind_regist_no,
    a.frmhs_nm,
    a.frmhs_ar,
    a.bsns_sttus_se,
    a.frmhs_fclty_se,
    b.cttpc_cn,
    b.cttpc_detail_cn,
    b.post_sn,
    b.legaldong_cl,
    b.mntn_lc,
    b.bon_lc,
    b.bu_lc,
    c.cttpc_cn     AS hp_no,
    d.cttpc_cn     AS tel_no,
    b.bldng_mng_no,
    b.road_nm_cl,
    b.road_nm,
    b.undr_se,
    b.bldng_bon,
    b.bldng_bu,
    b.bldng_nm,
    b.detail_bldng_nm,
    a.fdb_se,
    a.fdb_frmhs_sn,
    a.frst_crtr_id,
    a.frst_creat_dt,
    a.frst_crtr_ip_nm,
    a.last_changer_id,
    a.last_change_dt,
    a.last_changer_ip_nm,
    a.ROWID        AS arow,
    b.ROWID        AS brow,
    c.ROWID        AS crow,
    d.ROWID        AS drow,
    a.TATTOO_NO,
    a.MG_REG_NO,
    a.POSESN_STLE_CODE
FROM tn_stkrs_frmhs  a                                       /* 농장기초정보*/
        ,
     tn_frmhs_cttpc  b                                        /* 농장주소 */
        ,
     tn_frmhs_cttpc  c                                        /* 휴대전화 */
        ,
--            (SELECT *
--               FROM (SELECT T.*,
--                            ROW_NUMBER ()
--                                OVER (PARTITION BY T.FRMHS_NO
--                                      ORDER BY T.CTTPC_SN DESC)    AS RN
--                       FROM tn_frmhs_cttpc T
--                      WHERE T.CTTPC_SE = '2' AND T.USE_AT = 'Y')
--              WHERE 1 = 1 AND RN = '1') c,
     tn_frmhs_cttpc  d                                        /* 전화번호 */
WHERE     a.frmhs_no = b.frmhs_No(+)
  AND a.frmhs_no = c.frmhs_no(+)
  AND a.frmhs_no = d.frmhs_no(+)
  AND b.cttpc_se(+) = '1'
  AND b.reprsnt_sn(+) = 1
  AND c.cttpc_se(+) = '2'
  AND c.reprsnt_sn(+) = 1
  AND d.cttpc_se(+) = '3'
  AND d.reprsnt_sn(+) = 1;

GRANT SELECT ON "DPL"."VIEW_FRMHS" TO "BQS";