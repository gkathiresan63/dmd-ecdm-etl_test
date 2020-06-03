INSERT INTO EDAP_MDC.ETL_TRANSFORM_DEFINITION
(
    MAPPING_ID,
    DEFINED_BY_FILE,
    DEFINITION_FILE_LOCATION,
    DEFINITION_SQL
)
SELECT
    6,
    'Y',
    'sql/delta_load/work_load/ecdm_dim_address.sql',
    ''
;

INSERT INTO EDAP_MDC.ETL_TRANSFORM_DEFINITION
(
    MAPPING_ID,
    DEFINED_BY_FILE,
    DEFINITION_FILE_LOCATION,
    DEFINITION_SQL
)
SELECT
    8,
    'Y',
    'sql/delta_load/work_load/ecdm_rel_party_address.sql',
    ''
;

INSERT INTO EDAP_MDC.ETL_TRANSFORM_DEFINITION
(
    MAPPING_ID,
    DEFINED_BY_FILE,
    DEFINITION_FILE_LOCATION,
    DEFINITION_SQL
)
SELECT
    8,
    'Y',
    'sql/delta_load/work_load/ecdm_dim_agreement.sql',
    ''
;
