CREATE TABLE EDW_WORK.PARTY_MASTER_OF_MASTERS_XREF
(
    DIM_PARTY_NATURAL_KEY_HASH_UUID       UUID,
    DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID UUID,
    PARTY_ID                              VARCHAR(200),
    PARTY_PRIOR_ID                        VARCHAR(200),
    SOR_PARTY_ID                          VARCHAR(200),
    PARTY_ID_TYPE_CDE                     VARCHAR(50),
    BEGIN_DT                              DATE DEFAULT '0001-01-01'::DATE,
    BEGIN_DTM                             TIMESTAMP(6) DEFAULT '0001-01-01 00:00:00'::TIMESTAMP,
    ROW_PROCESS_DTM                       TIMESTAMP(6) DEFAULT (NOW())::TIMESTAMPTZ(6),
    AUDIT_ID                              INT DEFAULT 0,
    LOGICAL_DELETE_IND                    BOOLEAN DEFAULT FALSE,
    CHECK_SUM                             UUID,
    CURRENT_ROW_IND                       BOOLEAN DEFAULT TRUE,
    END_DT                                DATE DEFAULT '9999-12-31'::DATE,
    END_DTM                               TIMESTAMP(6) DEFAULT '9999-12-31 23:59:59.999999'::TIMESTAMP,
    SOURCE_SYSTEM_ID                      VARCHAR(50),
    RESTRICTED_ROW_IND                    BOOLEAN DEFAULT FALSE,
    ROW_SID                               IDENTITY(1,1) ,
    PARTY_ID_STATUS_TYPE_CDE              VARCHAR(50),
    PARTY_ID_CLASS_TYPE_CDE               VARCHAR(50)
);