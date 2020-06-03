TRUNCATE TABLE EDW_WORK.ECDM_REL_PARTY_ADDRESS;


TRUNCATE TABLE EDW_STAGING.ECDM_REL_PARTY_ADDRESS_PRE_WORK;


-- For rel table we only need the insert and update
-- We need first to move source into prework table
INSERT INTO EDW_STAGING.ECDM_REL_PARTY_ADDRESS_PRE_WORK
(
    DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    PRIMARY_ADDRESS_IND,
    OPERATOR_IND,
    BEGIN_DT,
    BEGIN_DTM,
    END_DT,
    END_DTM,
    SRC_LAST_MOD_TS
)

SELECT
    UUID_GEN(PREHASH_VALUE(
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_1),
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_2),
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_3),
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_4),
        CLEAN_STRING(ADDR_SNAPSHOT.CITY),
        CLEAN_STRING(ADDR_SNAPSHOT.STATE),
        CLEAN_STRING(ADDR_SNAPSHOT.POSTAL_CD),
        CLEAN_STRING(ADDR_SNAPSHOT.POSTAL_EXT_CD),
        CLEAN_STRING(ADDR_SNAPSHOT.ZIP_10_13_NR),
        CLEAN_STRING(ADDR_SNAPSHOT.COUNTRY_CD)))::UUID AS DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING(SRC.PARTY_ADDR_TP)))::UUID AS REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING('ECDM')))::UUID AS REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING(SRC.MEMBER_ID)))::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
    NULL AS PRIMARY_ADDRESS_IND,
    SRC.ADD_UPD_DEL_IND AS OPERATOR_IND,
    SRC.SRC_LST_MOD_TS::DATE AS BEGIN_DT,
    SRC.SRC_LST_MOD_TS::TIMESTAMP AS BEGIN_DTM,
    SRC.SRC_LST_MOD_TS::DATE  - INTERVAL '1 day' AS END_DT,
    (SRC.SRC_LST_MOD_TS  - INTERVAL '1 sec')::TIMESTAMP AS END_DTM,
    SRC.SRC_LST_MOD_TS AS SRC_LAST_MOD_TS


FROM EDW_STAGING.ECDM_PARTY_ADDR_OUT SRC
    LEFT JOIN EDW_STAGING.ECDM_C_B_ADDR_OUT_SNAPSHOT ADDR_SNAPSHOT
ON SRC.ADDR_HASH_KEY = ADDR_SNAPSHOT.ADDR_HASH_KEY;


INSERT INTO EDW_STAGING.ECDM_REL_PARTY_ADDRESS_PRE_WORK
(
    DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    PRIMARY_ADDRESS_IND,
    OPERATOR_IND,
    BEGIN_DT,
    BEGIN_DTM,
    END_DT,
    END_DTM,
    SRC_LAST_MOD_TS
)

SELECT DISTINCT
    UUID_GEN(PREHASH_VALUE(
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_1),
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_2),
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_3),
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_4),
        CLEAN_STRING(ADDR_SNAPSHOT.CITY),
        CLEAN_STRING(ADDR_SNAPSHOT.STATE),
        CLEAN_STRING(ADDR_SNAPSHOT.POSTAL_CD),
        CLEAN_STRING(ADDR_SNAPSHOT.POSTAL_EXT_CD),
        CLEAN_STRING(ADDR_SNAPSHOT.ZIP_10_13_NR),
        CLEAN_STRING(ADDR_SNAPSHOT.COUNTRY_CD)))::UUID AS DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING(ADDR_TP)))::UUID AS REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING(CARR_ADMN_SYS_CD)))::UUID AS REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING(MEMBER_ID)))::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
    NULL AS PRIMARY_ADDRESS_IND,
    SRC.ADD_UPD_DEL_IND AS OPERATOR_IND,
    SRC.SRC_LST_MOD_TS::DATE AS BEGIN_DT,
    SRC.SRC_LST_MOD_TS::TIMESTAMP AS BEGIN_DTM,
    SRC.SRC_LST_MOD_TS::DATE  - INTERVAL '1 day' AS END_DT,
    (SRC.SRC_LST_MOD_TS  - INTERVAL '1 sec')::TIMESTAMP AS END_DTM,
    SRC.SRC_LST_MOD_TS AS SRC_LAST_MOD_TS

FROM EDW_STAGING.ECDM_PARTY_AGMT_ADDR_OUT SRC
    LEFT JOIN EDW_STAGING.ECDM_C_B_ADDR_OUT_SNAPSHOT ADDR_SNAPSHOT
ON SRC.ADDR_HASH_KEY = ADDR_SNAPSHOT.ADDR_HASH_KEY WHERE SRC.ADD_UPD_DEL_IND != 'D';


INSERT INTO EDW_STAGING.ECDM_REL_PARTY_ADDRESS_PRE_WORK
(
    DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    PRIMARY_ADDRESS_IND,
    OPERATOR_IND,
    BEGIN_DT,
    BEGIN_DTM,
    END_DT,
    END_DTM,
    SRC_LAST_MOD_TS
)

SELECT DISTINCT
    UUID_GEN(PREHASH_VALUE(
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_1),
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_2),
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_3),
        CLEAN_STRING(ADDR_SNAPSHOT.ADDR_LINE_4),
        CLEAN_STRING(ADDR_SNAPSHOT.CITY),
        CLEAN_STRING(ADDR_SNAPSHOT.STATE),
        CLEAN_STRING(ADDR_SNAPSHOT.POSTAL_CD),
        CLEAN_STRING(ADDR_SNAPSHOT.POSTAL_EXT_CD),
        CLEAN_STRING(ADDR_SNAPSHOT.ZIP_10_13_NR),
        CLEAN_STRING(ADDR_SNAPSHOT.COUNTRY_CD)))::UUID AS DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING(ADDR_TP)))::UUID AS REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING(CARR_ADMN_SYS_CD)))::UUID AS REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING(MEMBER_ID)))::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
    NULL AS PRIMARY_ADDRESS_IND,
    SRC.ADD_UPD_DEL_IND AS OPERATOR_IND,
    SRC.SRC_LST_MOD_TS::DATE AS BEGIN_DT,
    SRC.SRC_LST_MOD_TS::TIMESTAMP AS BEGIN_DTM,
    SRC.SRC_LST_MOD_TS::DATE  - INTERVAL '1 day' AS END_DT,
    (SRC.SRC_LST_MOD_TS  - INTERVAL '1 sec')::TIMESTAMP AS END_DTM,
    SRC.SRC_LST_MOD_TS AS SRC_LAST_MOD_TS

FROM EDW_STAGING.ECDM_PARTY_AGMT_ADDR_OUT SRC
    LEFT JOIN EDW_STAGING.ECDM_C_B_ADDR_OUT_SNAPSHOT ADDR_SNAPSHOT
ON SRC.ADDR_HASH_KEY = ADDR_SNAPSHOT.ADDR_HASH_KEY
    WHERE SRC.ADD_UPD_DEL_IND = 'D' AND SRC.ADDR_HASH_KEY NOT IN
    (SELECT ADDR_HASH_KEY FROM EDW_STAGING.ECDM_PRTY_AGMT_ADDR_OUT_SNAPSHOT WHERE ADD_UPD_DEL_IND != 'D');


CREATE LOCAL TEMPORARY TABLE ECDM_REL_PARTY_ADDRESS_DEDUP_SRC ON COMMIT PRESERVE ROWS AS
    SELECT * FROM
    (
        SELECT
            SRC.DIM_ADDRESS_NATURAL_KEY_HASH_UUID AS DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
            SRC.REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID AS REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
            SRC.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID AS REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
            SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
            SRC.PRIMARY_ADDRESS_IND AS PRIMARY_ADDRESS_IND,
            SRC.OPERATOR_IND AS ADD_UPD_DEL_IND,
            SRC.BEGIN_DT AS BEGIN_DT,
            SRC.BEGIN_DTM AS BEGIN_DTM,
            SRC.END_DT AS END_DT,
            SRC.END_DTM AS END_DTM,
            SRC.SRC_LAST_MOD_TS AS SRC_LAST_MOD_TS,
            ROW_NUMBER() OVER (PARTITION BY
                SRC.DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
                SRC.REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
                SRC.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
                SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
                ORDER BY
                SRC_LAST_MOD_TS ASC) AS RNK
        FROM EDW_STAGING.ECDM_REL_PARTY_ADDRESS_PRE_WORK SRC
    ) SRC_DUP
    WHERE SRC_DUP.RNK = 1;

-- Handle end
INSERT INTO EDW_WORK.ECDM_REL_PARTY_ADDRESS (
    DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    PRIMARY_ADDRESS_IND,
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    CHECK_SUM,
    CURRENT_ROW_IND,
    END_DT,
    END_DTM,
    SOURCE_SYSTEM_ID,
    RESTRICTED_ROW_IND,
    UPDATE_AUDIT_ID
)
SELECT
    CASE WHEN TGT.DIM_ADDRESS_NATURAL_KEY_HASH_UUID IS NULL THEN SRC.DIM_ADDRESS_NATURAL_KEY_HASH_UUID ELSE TGT.DIM_ADDRESS_NATURAL_KEY_HASH_UUID END AS DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    CASE WHEN TGT.REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID IS NULL THEN SRC.REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID ELSE TGT.REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID END AS REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    CASE WHEN TGT.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID IS NULL THEN SRC.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID END AS REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    CASE WHEN TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID IS NULL THEN SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID ELSE TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID END AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
    CASE WHEN TGT.PRIMARY_ADDRESS_IND IS NULL THEN SRC.PRIMARY_ADDRESS_IND ELSE TGT.PRIMARY_ADDRESS_IND END AS PRIMARY_ADDRESS_IND,
    CASE WHEN TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID IS NOT NULL THEN TGT.BEGIN_DT ELSE '0001-01-01' END AS BEGIN_DT,
    CASE WHEN TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID IS NOT NULL THEN TGT.BEGIN_DTM ELSE '0001-01-01'::TIMESTAMP END AS BEGIN_DTM,
    CURRENT_TIMESTAMP(6) AS ROW_PROCESS_DTM,
    CASE WHEN TGT.AUDIT_ID IS NULL THEN :audit_id ELSE TGT.AUDIT_ID END AS AUDIT_ID,
    FALSE AS LOGICAL_DELETE_IND,
    CASE WHEN TGT.CHECK_SUM IS NULL THEN SRC.CHECK_SUM ELSE TGT.CHECK_SUM END AS CHECK_SUM,
    FALSE AS CURRENT_ROW_IND,
    CASE WHEN TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID IS NOT NULL THEN TGT.END_DT ELSE SRC.BEGIN_DT END AS END_DT,
    CASE WHEN TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID IS NOT NULL THEN TGT.END_DTM ELSE SRC.BEGIN_DTM END AS END_DTM,
    '45' AS SOURCE_SYSTEM_ID,
    FALSE AS RESTRICTED_ROW_IND,
    :audit_id AS UPDATE_AUDIT_ID

FROM EDW_STAGING.ECDM_REL_PARTY_ADDRESS_PRE_WORK SRC

LEFT JOIN EDW_TDSUNSET.REL_PARTY_ADDRESS TGT
ON SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID = TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID AND
SRC.REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID = TGT.REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID AND
SRC.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID = TGT.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID AND
SRC.DIM_ADDRESS_NATURAL_KEY_HASH_UUID = TGT.DIM_ADDRESS_NATURAL_KEY_HASH_UUID
WHERE SRC.OPERATOR_IND = 'D';



-- Handle insert
INSERT INTO EDW_WORK.ECDM_REL_PARTY_ADDRESS (
    DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    PRIMARY_ADDRESS_IND,
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    CHECK_SUM,
    CURRENT_ROW_IND,
    END_DT,
    END_DTM,
    SOURCE_SYSTEM_ID,
    RESTRICTED_ROW_IND,
    UPDATE_AUDIT_ID
)
SELECT
    SRC.DIM_ADDRESS_NATURAL_KEY_HASH_UUID AS DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    SRC.REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID AS REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    SRC.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID AS REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
    SRC.PRIMARY_ADDRESS_IND AS PRIMARY_ADDRESS_IND,
    '0001-01-01' AS BEGIN_DT,
    '0001-01-01'::TIMESTAMP AS BEGIN_DTM,
    CURRENT_TIMESTAMP(6) AS ROW_PROCESS_DTM,
    :audit_id AS AUDIT_ID,
    FALSE AS LOGICAL_DELETE_IND,
    UUID_GEN(SRC.PRIMARY_ADDRESS_IND)::UUID AS CHECK_SUM,
    TRUE AS CURRENT_ROW_IND,
    '9999-12-31' AS END_DT,
    '9999-12-31'::TIMESTAMP AS END_DTM,
    '45' AS SOURCE_SYSTEM_ID,
    FALSE AS RESTRICTED_ROW_IND,
    :audit_id AS UPDATE_AUDIT_ID

FROM EDW_STAGING.ECDM_REL_PARTY_ADDRESS_PRE_WORK  SRC
WHERE (SRC.DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
SRC.REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
SRC.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID)
NOT IN (
SELECT DISTINCT DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID,
    REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID,
    DIM_PARTY_NATURAL_KEY_HASH_UUID
    FROM EDW_TDSUNSET.REL_PARTY_ADDRESS WHERE CURRENT_ROW_IND = TRUE) AND SRC.OPERATOR_IND != 'D';
