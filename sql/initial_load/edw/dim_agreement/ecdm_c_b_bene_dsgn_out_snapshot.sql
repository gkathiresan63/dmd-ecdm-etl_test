TRUNCATE TABLE EDW_STAGING.ECDM_C_B_BENE_DSGN_OUT_SNAPSHOT;


INSERT INTO EDW_STAGING.ECDM_C_B_BENE_DSGN_OUT_SNAPSHOT
(
    BATCH_NR,
    ADD_UPD_DEL_IND,
    MEMBER_ID,
    CARR_ADMN_SYS_CD,
    HLDG_KEY_PFX,
    AGRMNT_NUM,
    HLDG_KEY_SFX,
    ROLE_TP,
    SUB_ROLE_TP,
    EFF_END_DATE,
    EFF_START_DATE,
    BENE_CLASS_CD,
    BENE_TYP_CD,
    BENE_SUB_CLASS_CD,
    REL_TO_INSD_CD,
    DIST_PCT,
    ISS_PER_STIRPES_CD,
    FREE_FORM_DSGN_DESC,
    DIST_DOL_AMT,
    BENE_SUB_CLASS_NR,
    HUB_STATE_IND,
    LST_UPDT_OPER_CD,
    SRC_LST_MOD_TS,
    ECDM_END_TS,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    SOURCE_SYSTEM_ID,
    CURRENT_BATCH
)
SELECT
    0 AS BATCH_NR,
    'I' AS ADD_UPD_DEL_IND,
    CLEAN_STRING(SRC.SRC_SYS_PRTY_ID) AS MEMBER_ID,
    CLEAN_STRING(SRC.CARR_ADMIN_SYS_CD) AS CARR_ADMN_SYS_CD,
    CLEAN_STRING(SRC.HLDG_KEY_PFX) AS HLDG_KEY_PFX,
    CLEAN_STRING(SRC.HLDG_KEY) AS AGRMNT_NUM,
    CLEAN_STRING(SRC.HLDG_KEY_SFX) AS HLDG_KEY_SFX,
    CLEAN_STRING(SRC.PRTY_AGMT_RLE_CD) AS ROLE_TP,
    CLEAN_STRING(SRC.PRTY_AGMT_RLE_STYP_CD) AS SUB_ROLE_TP,
    SRC.BUS_END_DT AS EFF_END_DATE,
    SRC.BUS_STRT_DT AS EFF_START_DATE,
    CLEAN_STRING(SRC.BENE_CLS_CD) AS BENE_CLASS_CD,
    CLEAN_STRING(SRC.BENE_TYP_CD) AS BENE_TYP_CD,
    CLEAN_STRING(SRC.BENE_SUB_CLS_CD) AS BENE_SUB_CLASS_CD,
    CLEAN_STRING(SRC.BENE_REL_NM) AS REL_TO_INSD_CD,
    SRC.BENE_ALLOC_PCT AS DIST_PCT,
    CLEAN_STRING(SRC.BENE_ISS_PER_STIRPES_CD) AS ISS_PER_STIRPES_CD,
    CLEAN_STRING(SRC.BENE_ARGMT_TXT) AS FREE_FORM_DSGN_DESC,
    SRC.BENE_ALLOC_AMT AS DIST_DOL_AMT,
    SRC.BENE_SUB_CLS_NR AS BENE_SUB_CLASS_NR,
    NULL AS HUB_STATE_IND,
    NULL AS LST_UPDT_OPER_CD,
    NULL AS SRC_LST_MOD_TS,
    NULL AS ECDM_END_TS,
    NULL AS ROW_PROCESS_DTM,
    NULL AS AUDIT_ID,
    45 AS SOURCE_SYSTEM_ID,
    TRUE AS CURRENT_BATCH
FROM PROD_STND_PRTY_VW_TERSUN.PRTY_AGMT_BENE_DATA_VW SRC;
