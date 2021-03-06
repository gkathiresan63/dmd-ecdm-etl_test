CREATE TABLE EDW_STAGING.ECDM_REL_PARTY_ADDRESS_PRE_WORK
(
    DIM_ADDRESS_NATURAL_KEY_HASH_UUID UUID,
    REF_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID UUID,
    REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID UUID,
    DIM_PARTY_NATURAL_KEY_HASH_UUID UUID,
    PRIMARY_ADDRESS_IND BOOLEAN,
    OPERATOR_IND varchar(1),
    BEGIN_DT DATE,
    END_DT DATE,
    BEGIN_DTM TIMESTAMP,
    END_DTM TIMESTAMP,
    SRC_LAST_MOD_TS TIMESTAMP
);
