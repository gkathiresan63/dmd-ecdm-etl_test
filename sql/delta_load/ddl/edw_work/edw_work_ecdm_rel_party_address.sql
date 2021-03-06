CREATE TABLE EDW_WORK.ECDM_REL_PARTY_ADDRESS
(
    DIM_ADDRESS_NATURAL_KEY_HASH_UUID UUID ,
    REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID UUID ,
    REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID UUID ,
    DIM_PARTY_NATURAL_KEY_HASH_UUID UUID ,
    PRIMARY_ADDRESS_IND BOOLEAN,
    BEGIN_DT DATE  DEFAULT '0001-01-01'::DATE,
    BEGIN_DTM TIMESTAMP(6)  DEFAULT '0001-01-01 00:00:00'::TIMESTAMP,
    ROW_PROCESS_DTM TIMESTAMP(6)  DEFAULT (NOW())::TIMESTAMPTZ(6),
    AUDIT_ID INT  DEFAULT 0,
    LOGICAL_DELETE_IND BOOLEAN  DEFAULT FALSE,
    CHECK_SUM UUID ,
    CURRENT_ROW_IND BOOLEAN  DEFAULT TRUE,
    END_DT DATE  DEFAULT '9999-12-31'::DATE,
    END_DTM TIMESTAMP(6)  DEFAULT '9999-12-31 23:59:59.999999'::TIMESTAMP,
    SOURCE_SYSTEM_ID VARCHAR(50) ,
    RESTRICTED_ROW_IND BOOLEAN  DEFAULT FALSE,
    UPDATE_AUDIT_ID INT  DEFAULT 0
);
