INSERT INTO EDAP_MDC.ETL_PROJECT (
	NAME,
	SHORT_NAME,
	BATCH_USER,
	DESCRIPTION
)
SELECT
	'Enterprise_Customer_Data_Management',
	'ECDM',
	'edw_ecdm_batch',
	'ecdm Tera data migration EDW ETL project';

-- get etl project id = 2
