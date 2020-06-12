TRUNCATE TABLE EDW_WORK.PARTY_MASTER_OF_MASTERS_XREF;
TRUNCATE TABLE EDW_STAGING.PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK;


-- Populate the Pre work table 
/* Pre work logic 
    
     Dedup based on key and load pre_work_table 
     Begin_dt = source begin_date (for xref)
     end_date = 9999.               */

INSERT INTO EDW_STAGING.PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK
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
    PARTY_ID_CLASS_TYPE_CDE,
    ADD_UPD_DEL_IND          
)
SELECT 
    UUID_GEN(PREHASH_VALUE(CLEAN_STRING(SRC.MEMBER_ID)))::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
    NULL                                                       AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    CLEAN_STRING(SRC.MEMBER_ID)                                AS PARTY_ID,
    NULL                                                       AS PARTY_PRIOR_ID,
    CLEAN_STRING(SRC.FED_GOV_ID)                               AS SOR_PARTY_ID,
    CLEAN_STRING(SRC.FED_GOV_ID_TP)                            AS PARTY_ID_TYPE_CDE,
    SRC.SRC_LST_MOD_TS::DATE                                   AS BEGIN_DT,
    SRC.SRC_LST_MOD_TS::TIMESTAMP                              AS BEGIN_DTM,
    CURRENT_TIMESTAMP(6)                                       AS ROW_PROCESS_DTM,
    :audit_id                                                  AS AUDIT_ID,
    FALSE                                                      AS LOGICAL_DELETE_IND,
    UUID_GEN(PREHASH_VALUE(
        CLEAN_STRING(SRC.FED_GOV_ID),
        CLEAN_STRING(SRC.FED_GOV_ID_TP),
        CLEAN_STRING(SRC.GOVT_ID_STS_CD),
        CLEAN_STRING(SRC.GOVT_ID_CLS_CD)
        ))::UUID                                                AS CHECK_SUM,
    TRUE                                                        AS CURRENT_ROW_IND,
    '9999-12-31'                                                AS END_DT,
    '9999-12-31'::TIMESTAMP                                     AS END_DTM,
    '45'                                                        AS SOURCE_SYSTEM_ID,
    FALSE                                                       AS RESTRICTED_ROW_IND,
    CLEAN_STRING(SRC.GOVT_ID_STS_CD)                            AS PARTY_ID_STATUS_TYPE_CDE,
    CLEAN_STRING(SRC.GOVT_ID_CLS_CD)                            AS PARTY_ID_CLASS_TYPE_CDE,
    SRC.ADD_UPD_DEL_IND                                         AS ADD_UPD_DEL_IND
FROM
EDW_STAGING.ECDM_C_B_PARTY_OUT SRC
/*(
SELECT
    SRC_DUP.*,
    ROW_NUMBER() OVER (PARTITION BY CLEAN_STRING(SRC_DUP.MEMBER_ID) ORDER BY SRC_DUP.SRC_LST_MOD_TS DESC) AS RNK

FROM 
   EDW_STAGING.ECDM_C_B_PARTY_OUT SRC_DUP
)SRC
WHERE SRC.RNK = 1;*/




-- Apply delete records
/* Delete logic 
     
     Logical delete records which are presnet in Source with ADD_UPD_DEL_IND = 'D' . Please note CURRENT_ROW_IND will be set to True for Logical delete records.
     Begin_date - Set from target if record presnet , else from source
     End_date  - 9999 
     CURRENT_ROW_IND = TRUE *
     will there be any case where */
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
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID         AS DIM_PARTY_NATURAL_KEY_HASH_UUID,     
    SRC.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID   AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    SRC.PARTY_ID                                AS PARTY_ID,                             
    SRC.PARTY_PRIOR_ID                          AS PARTY_PRIOR_ID,                      
    SRC.SOR_PARTY_ID                            AS SOR_PARTY_ID,                        
    SRC.PARTY_ID_TYPE_CDE                       AS PARTY_ID_TYPE_CDE,                  
    --CASE WHEN TGT.BEGIN_DT IS NULL THEN '0001-01-01' ELSE TGT.BEGIN_DT END AS BEGIN_DT,
    --CASE WHEN TGT.BEGIN_DTM IS NULL THEN '0001-01-01' ELSE TGT.BEGIN_DTM END AS BEGIN_DTM,
    CASE WHEN TGT.BEGIN_DT IS NULL THEN  SRC.BEGIN_DT  ELSE TGT.BEGIN_DT END AS BEGIN_DT, -- case check to handle in case if the source delete record not present in target and when you already have a deleted record with curent_ind = false
    CASE WHEN TGT.BEGIN_DTM IS NULL THEN SRC.BEGIN_DTM ELSE TGT.BEGIN_DTM END AS BEGIN_DTM,
    SRC.ROW_PROCESS_DTM                         AS ROW_PROCESS_DTM,                     
    SRC.AUDIT_ID                                AS AUDIT_ID,                          
    TRUE                                        AS LOGICAL_DELETE_IND,                   
    SRC.CHECK_SUM                               AS CHECK_SUM,                      
    TRUE                                        AS CURRENT_ROW_IND,                    
    SRC.END_DT                                  AS END_DT,                           
    SRC.END_DTM                                 AS END_DTM,                           
    SRC.SOURCE_SYSTEM_ID                        AS SOURCE_SYSTEM_ID,                  
    SRC.RESTRICTED_ROW_IND                      AS RESTRICTED_ROW_IND,                
    SRC.PARTY_ID_STATUS_TYPE_CDE                AS PARTY_ID_STATUS_TYPE_CDE,           
    SRC.PARTY_ID_CLASS_TYPE_CDE                 AS PARTY_ID_CLASS_TYPE_CDE     

FROM
--  Apply Delete logic . If record present use tgt begin date ele src begin date
EDW_STAGING.PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK SRC
LEFT JOIN EDW_TDSUNSET.PARTY_MASTER_OF_MASTERS_XREF TGT
ON SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID::VARCHAR = TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID::VARCHAR 
AND TGT.CURRENT_ROW_IND = TRUE
WHERE SRC.ADD_UPD_DEL_IND = 'D';
-- AND TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID IS NOT NULL  -- check whether this is needed



-- Apply insert part for all records
/* Insert logic 
     records which comes as 'U' which are not present in target is considered as Insert record.
     Insert records which are not presnet in target for Source with ADD_UPD_DEL_IND IN ('I','U')
     Begin_date - 0000 or from source(check this)
     End_date  - 9999  */
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
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID         AS DIM_PARTY_NATURAL_KEY_HASH_UUID,     
    SRC.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID   AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    SRC.PARTY_ID                                AS PARTY_ID,                             
    SRC.PARTY_PRIOR_ID                          AS PARTY_PRIOR_ID,                      
    SRC.SOR_PARTY_ID                            AS SOR_PARTY_ID,                        
    SRC.PARTY_ID_TYPE_CDE                       AS PARTY_ID_TYPE_CDE,                  
    --CASE WHEN TGT.BEGIN_DT IS NULL THEN '0001-01-01' ELSE TGT.BEGIN_DT END AS BEGIN_DT,
    --CASE WHEN TGT.BEGIN_DTM IS NULL THEN '0001-01-01' ELSE TGT.BEGIN_DTM END AS BEGIN_DTM,
    '0001-01-01'                                AS BEGIN_DT,
    '0001-01-01 00:00:00'                       AS BEGIN_DTM,
    SRC.ROW_PROCESS_DTM                         AS ROW_PROCESS_DTM,                     
    SRC.AUDIT_ID                                AS AUDIT_ID,                          
    FALSE                                       AS LOGICAL_DELETE_IND,                   
    SRC.CHECK_SUM                               AS CHECK_SUM,                      
    TRUE                                        AS CURRENT_ROW_IND,                    
    SRC.END_DT                                  AS END_DT,
    SRC.END_DTM                                 AS END_DTM,                        
    SRC.SOURCE_SYSTEM_ID                        AS SOURCE_SYSTEM_ID,                  
    SRC.RESTRICTED_ROW_IND                      AS RESTRICTED_ROW_IND,                
    SRC.PARTY_ID_STATUS_TYPE_CDE                AS PARTY_ID_STATUS_TYPE_CDE,           
    SRC.PARTY_ID_CLASS_TYPE_CDE                 AS PARTY_ID_CLASS_TYPE_CDE     

FROM
--  Apply INSERT logic . If begin date AS 0001-01-01
EDW_STAGING.PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK SRC
WHERE
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
        NOT IN (SELECT DISTINCT DIM_PARTY_NATURAL_KEY_HASH_UUID FROM EDW_TDSUNSET.PARTY_MASTER_OF_MASTERS_XREF WHERE LOGICAL_DELETE_IND = FALSE)
AND SRC.ADD_UPD_DEL_IND != 'D';   -- what happens when a already deleted record(locical_src = false and curren_ind = false) comes again as new record . will it be inserted with begin date as '00000'




-- For update we need to have two records, one for prev another for curr
/* Curr Update logic 
     
     Insert records which are presnet in target for Source with ADD_UPD_DEL_IND = 'U','I'
     Logical delete records(current_ind = true) will be considered as new record and inserted in this step.
     Expected case - Records with 'I' will not be present in the target
     Begin_date - source begin date
     End_date  - 9999  
     Current_row_ind = True */

---------------- check the below curr update is needed or can be take can with insert/update --------
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
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID         AS DIM_PARTY_NATURAL_KEY_HASH_UUID,     
    SRC.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID   AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    SRC.PARTY_ID                                AS PARTY_ID,                             
    SRC.PARTY_PRIOR_ID                          AS PARTY_PRIOR_ID,                      
    SRC.SOR_PARTY_ID                            AS SOR_PARTY_ID,                        
    SRC.PARTY_ID_TYPE_CDE                       AS PARTY_ID_TYPE_CDE,                  
    --CASE WHEN TGT.BEGIN_DT IS NULL THEN '0001-01-01' ELSE TGT.BEGIN_DT END AS BEGIN_DT,
    --CASE WHEN TGT.BEGIN_DTM IS NULL THEN '0001-01-01' ELSE TGT.BEGIN_DTM END AS BEGIN_DTM,
    SRC.BEGIN_DT                                AS BEGIN_DT,
    SRC.BEGIN_DTM                               AS BEGIN_DTM,
    SRC.ROW_PROCESS_DTM                         AS ROW_PROCESS_DTM,                     
    SRC.AUDIT_ID                                AS AUDIT_ID,                          
    FALSE                                       AS LOGICAL_DELETE_IND,                   
    SRC.CHECK_SUM                               AS CHECK_SUM,                      
    TRUE                                        AS CURRENT_ROW_IND,                    
    SRC.END_DT                                  AS END_DT,
    SRC.END_DTM                                 AS END_DTM,                         
    SRC.SOURCE_SYSTEM_ID                        AS SOURCE_SYSTEM_ID,                  
    SRC.RESTRICTED_ROW_IND                      AS RESTRICTED_ROW_IND,                
    SRC.PARTY_ID_STATUS_TYPE_CDE                AS PARTY_ID_STATUS_TYPE_CDE,           
    SRC.PARTY_ID_CLASS_TYPE_CDE                 AS PARTY_ID_CLASS_TYPE_CDE     

FROM
--  Apply UPDATE logic . If begin date = SRC_DATE and end_date = 9999 for current records.
EDW_STAGING.PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK SRC
WHERE
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
        IN (SELECT DISTINCT DIM_PARTY_NATURAL_KEY_HASH_UUID FROM EDW_TDSUNSET.PARTY_MASTER_OF_MASTERS_XREF WHERE CURRENT_ROW_IND = TRUE)
AND SRC.ADD_UPD_DEL_IND != 'D';

-- Prev
/* Prev Update logic 
     
     Insert records which are presnet in target for Source with ADD_UPD_DEL_IND = 'U', 'I'
     Expected case - Records with I will not be present in Target and if presnet will be considered as update.
     TGT.LOGICAL_DELETE_IND = FALSE check is added to ensure , the logical Deleted record end date is not updated in this step.
     Begin_date - target begin date
     End_date  - source_date - 1  
     Current_row_ind = False */


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
    TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID         AS DIM_PARTY_NATURAL_KEY_HASH_UUID,     
    TGT.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID   AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    TGT.PARTY_ID                                AS PARTY_ID,                             
    TGT.PARTY_PRIOR_ID                          AS PARTY_PRIOR_ID,                      
    TGT.SOR_PARTY_ID                            AS SOR_PARTY_ID,                        
    TGT.PARTY_ID_TYPE_CDE                       AS PARTY_ID_TYPE_CDE,                  
    --CASE WHEN TGT.BEGIN_DT IS NULL THEN '0001-01-01' ELSE TGT.BEGIN_DT END AS BEGIN_DT,
    --CASE WHEN TGT.BEGIN_DTM IS NULL THEN '0001-01-01' ELSE TGT.BEGIN_DTM END AS BEGIN_DTM,
    TGT.BEGIN_DT                                AS BEGIN_DT,
    TGT.BEGIN_DTM                               AS BEGIN_DTM,
    SRC.ROW_PROCESS_DTM                         AS ROW_PROCESS_DTM,                     
    SRC.AUDIT_ID                                AS AUDIT_ID,                          
    FALSE                                       AS LOGICAL_DELETE_IND,                   
    TGT.CHECK_SUM                               AS CHECK_SUM,                      
    FALSE                                       AS CURRENT_ROW_IND,                    
    SRC.BEGIN_DT - INTERVAL '1 day'             AS END_DT,
    SRC.BEGIN_DTM - INTERVAL '1 second'         AS END_DTM,                          
    TGT.SOURCE_SYSTEM_ID                        AS SOURCE_SYSTEM_ID,                  
    TGT.RESTRICTED_ROW_IND                      AS RESTRICTED_ROW_IND,                
    TGT.PARTY_ID_STATUS_TYPE_CDE                AS PARTY_ID_STATUS_TYPE_CDE,           
    TGT.PARTY_ID_CLASS_TYPE_CDE                 AS PARTY_ID_CLASS_TYPE_CDE     

     
    FROM
--  Apply UPDATE logic . If begin date = SRC_DATE and end_date = 9999 for current records.
EDW_STAGING.PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK SRC

LEFT JOIN EDW_TDSUNSET.PARTY_MASTER_OF_MASTERS_XREF TGT 
ON SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID = TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND TGT.CURRENT_ROW_IND = TRUE
AND TGT.LOGICAL_DELETE_IND = FALSE
WHERE
   TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID IS NOT NULL
   AND SRC.ADD_UPD_DEL_IND != 'D';




































































