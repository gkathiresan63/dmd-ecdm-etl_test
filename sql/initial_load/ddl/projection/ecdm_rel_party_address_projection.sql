CREATE PROJECTION IF NOT EXISTS EDW_STAGING.ST_AD_VW_GET_DIM_ADDRESS_KEY_BY_SRC_SYS_AD_ID
(
	AD_L1_TXT,
	AD_L2_TXT,
	AD_L3_TXT,
	AD_L4_TXT,
	CITY,
	STATE,
	ZIP_1_5_NR,
	ZIP_6_9_NR,
	ZIP_10_13_NR,
	CTRY_CD,
	SRC_SYS_AD_ID
)
AS SELECT
	ADDR.AD_L1_TXT,
	ADDR.AD_L2_TXT,
	ADDR.AD_L3_TXT,
	ADDR.AD_L4_TXT,
	ADDR.CITY,
	ADDR.STATE,
	ADDR.ZIP_1_5_NR,
	ADDR.ZIP_6_9_NR,
	ADDR.ZIP_10_13_NR,
	ADDR.CTRY_CD,
	SRC_SYS_AD_ID
	FROM EDW_STAGING.ECDM_ST_AD_VW ADDR
	ORDER BY SRC_SYS_AD_ID SEGMENTED BY HASH(SRC_SYS_AD_ID) ALL NODES;


CREATE PROJECTION IF NOT EXISTS EDW_STAGING.ST_PRTY_AGMT_AD_VW_GET_REL_PARTY_ADDRESS_PRTY_AGMT
(
	AD_TYP_CD,
	CARR_ADMN_SYS_CD,
	SRC_SYS_PRTY_ID,
	PRTY_AGMT_AD_FR_DT,
	SRC_DEL_IND,
	CURR_IND,
	PRTY_AGMT_AD_TO_DT,
	SRC_SYS_ID,
	SRC_SYS_AD_ID
)
AS
SELECT
	AD_TYP_CD,
	CARR_ADMIN_SYS_CD,
	SRC_SYS_PRTY_ID,
	PRTY_AGMT_AD_FR_DT,
	SRC_DEL_IND,
	CURR_IND,
	PRTY_AGMT_AD_TO_DT,
	SRC_SYS_ID,
	SRC_SYS_AD_ID
FROM EDW_STAGING.ECDM_PRTY_AGMT_AD_VW
	ORDER BY SRC_SYS_AD_ID SEGMENTED BY HASH(SRC_SYS_AD_ID) ALL NODES; _ID SEGMENTED BY HASH(SRC_SYS_AD_ID) ALL NODES;
