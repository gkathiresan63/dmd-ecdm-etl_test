---- HIST Table Load ---
 
-- check whether the begin and end date should be PRTY_ALT_ID_FR_DT and PRTY_ALT_ID_TO_DT -- confirm with team 
 
Truncate table EDW_WORK.PARTY_MASTER_OF_MASTERS_XREF;
 
INSERT INTO EDW_WORK.PARTY_MASTER_OF_MASTERS_XREF
(
    DIM_PARTY_NATURAL_KEY_HASH_UUID,     
    DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    PARTY_ID,                             
    PARTY_PRIOR_ID,                      
    SOR_PARTY_ID,                        
    PARTY_ID_TYPE_CDE,                  
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
    PARTY_ID_STATUS_TYPE_CDE,           
    PARTY_ID_CLASS_TYPE_CDE
      
)
SELECT 
    UUID_GEN(PREHASH_VALUE(SRC.SRC_SYS_PRTY_ID))::UUID   AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
    NULL                                                       AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    SRC.SRC_SYS_PRTY_ID                                                 AS PARTY_ID,
    NULL                                                       AS PARTY_PRIOR_ID,
    CLEAN_STRING(SRC.ALT_ID)                                   AS SOR_PARTY_ID,
    CLEAN_STRING(SRC.ALT_ID_TYP_CD)                              AS PARTY_ID_TYPE_CDE,
    '0001-01-01'                                               AS BEGIN_DT,
    '0001-01-01 00:00:00'                                      AS BEGIN_DTM,
    CURRENT_TIMESTAMP(6)                                       AS ROW_PROCESS_DTM,
    0                                                  AS AUDIT_ID,
    FALSE                                                      AS LOGICAL_DELETE_IND,
    UUID_GEN(PREHASH_VALUE(
        CLEAN_STRING(SRC.ALT_ID),
        CLEAN_STRING(SRC.ALT_ID_TYP_CD),
        CLEAN_STRING(SRC.STUS_CD),
        CLEAN_STRING(SRC.ALT_ID_STYP_CD)
        ))::UUID                                                AS CHECK_SUM,
    TRUE                                                        AS CURRENT_ROW_IND,
    '9999-12-31'                                                AS END_DT,
    '9999-12-31'::TIMESTAMP                                     AS END_DTM,
    '45'                                                        AS SOURCE_SYSTEM_ID,
    FALSE                                                       AS RESTRICTED_ROW_IND,
    CLEAN_STRING(SRC.STUS_CD)                                   AS PARTY_ID_STATUS_TYPE_CDE,
    CLEAN_STRING(SRC.ALT_ID_STYP_CD)                            AS PARTY_ID_STATUS_TYPE_CDE
   -- SRC.ADD_UPD_DEL_IND                                         AS ADD_UPD_DEL_IND
FROM
(
SELECT
    SRC_DUP.*,
    ROW_NUMBER() OVER (PARTITION BY SRC_DUP.SRC_SYS_PRTY_ID ORDER BY SRC_DUP.TRANS_DT DESC) AS RNK
 
FROM 
   PROD_STND_PRTY_VW_TERSUN.PRTY_ALT_ID_VW SRC_DUP
)SRC
WHERE SRC.RNK = 1
 
 
---- Target table load ----
 
INSERT INTO EDW_TDSUNSET.PARTY_MASTER_OF_MASTERS_XREF 
(
    DIM_PARTY_NATURAL_KEY_HASH_UUID,     
    DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    PARTY_ID,                             
    PARTY_PRIOR_ID,                      
    SOR_PARTY_ID,                        
    PARTY_ID_TYPE_CDE,                  
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
    PARTY_ID_STATUS_TYPE_CDE,           
    PARTY_ID_CLASS_TYPE_CDE
      
)
SELECT
 
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID,     
    SRC.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    SRC.PARTY_ID,                             
    SRC.PARTY_PRIOR_ID,                      
    SRC.SOR_PARTY_ID,                        
    SRC.PARTY_ID_TYPE_CDE,                  
    SRC.BEGIN_DT,                         
    SRC.BEGIN_DTM,                         
    SRC.ROW_PROCESS_DTM,                     
    SRC.AUDIT_ID,                          
    SRC.LOGICAL_DELETE_IND,                   
    SRC.CHECK_SUM,                      
    SRC.CURRENT_ROW_IND,                    
    SRC.END_DT,                           
    SRC.END_DTM,                           
    SRC.SOURCE_SYSTEM_ID,                  
    SRC.RESTRICTED_ROW_IND,                
    SRC.PARTY_ID_STATUS_TYPE_CDE,           
    SRC.PARTY_ID_CLASS_TYPE_CDE
FROM 
EDW_WORK.PARTY_MASTER_OF_MASTERS_XREF SRC WHERE DIM_PARTY_NATURAL_KEY_HASH_UUID NOT IN
    (SELECT DISTINCT DIM_PARTY_NATURAL_KEY_HASH_UUID FROM EDW_TDSUNSET.PARTY_MASTER_OF_MASTERS_XREF);
 
