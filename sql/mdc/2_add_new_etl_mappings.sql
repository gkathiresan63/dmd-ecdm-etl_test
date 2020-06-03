INSERT INTO EDAP_MDC.ETL_MAPPING
(
	NAME,
    DESCRIPTION,
    PROJECT_ID,
    EDW_TABLE_NAME,
    EDW_TARGET_SCHEMA,
    EDW_TABLE_TYPE_ID,
    EDW_MERGE_APPEND_ONLY,
    EDW_MERGE_COLUMNS
)
SELECT
	'dim_address',
	'etl to generate type2 table dim_agreement',
	2, -- that will map to the step1 created row's id
	'dim_address',
	'edw_tdsunset',
	2, -- that will map to edw_table_type's table type 2 -> Type2
	false, -- because this is a type to we need to set it to false, if it is true, it will append all data and will not do SCD type2
	'DIM_ADDRESS_NATURAL_KEY_HASH_UUID' -- when we are doing the type2 merge, we need to use some UUID to make sure we are updating the correct columns, some of the table may have more keys it should be <key1>,<key2>

--mapping id = 6

INSERT INTO EDAP_MDC.ETL_MAPPING
(
	NAME,
    DESCRIPTION,
    PROJECT_ID,
    EDW_TABLE_NAME,
    EDW_TARGET_SCHEMA,
    EDW_TABLE_TYPE_ID,
    EDW_MERGE_APPEND_ONLY,
    EDW_MERGE_COLUMNS
)
SELECT
	'rel_party_address',
	'etl to generate type2 table rel_party_address',
	2, -- that will map to the step1 created row's id
	'rel_party_address',
	'edw_tdsunset',
	3, -- that will map to edw_table_type's table type 2 -> Type2
	true, -- because this is a type to we need to set it to false, if it is true, it will append all data and will not do SCD type2
	'' -- when we are doing the type2 merge, we need to use some UUID to make sure we are updating the correct columns, some of the table may have more keys it should be <key1>,<key2>

--mapping id = 8

INSERT INTO EDAP_MDC.ETL_MAPPING
(
	NAME,
    DESCRIPTION,
    PROJECT_ID,
    EDW_TABLE_NAME,
    EDW_TARGET_SCHEMA,
    EDW_TABLE_TYPE_ID,
    EDW_MERGE_APPEND_ONLY,
    EDW_MERGE_COLUMNS
)
SELECT
	'ecdm_dim_agreement',
	'etl to generate type2 table dim_agreement',
	2, -- that will map to the step1 created row's id
	'dim_agreement',
	'edw_tdsunset',
	2, -- that will map to edw_table_type's table type 2 -> Type2
	false, -- because this is a type to we need to set it to false, if it is true, it will append all data and will not do SCD type2
	'DIM_AGREEMENT_NATURAL_KEY_HASH_UUID' -- when we are doing the type2 merge, we need to use some UUID to make sure we are updating the correct columns, some of the table may have more keys it should be <key1>,<key2>

--mapping id = 10
