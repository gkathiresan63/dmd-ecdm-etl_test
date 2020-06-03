CREATE TABLE edw_staging.ECDM_DIM_AGREEMENT_PRE_WORK
(
    DIM_AGREEMENT_NATURAL_KEY_HASH_UUID uuid,
    AGREEMENT_NR_PFX varchar(50),
    AGREEMENT_NR varchar(150),
    AGREEMENT_NR_SFX varchar(50),
    AGREEMENT_SOURCE_CDE varchar(50),
    AGREEMENT_TYPE_CDE varchar(50),
    MASTER_CONTRACT_GROUP_ID varchar(50),
    SPONSOR_ID varchar(50),
    PLAN_ID varchar(50),
    SUBSCRIBER_ID varchar(50),
    PARTICIPANT_ID varchar(50),
    ICU_ID varchar(50),
    MASTER_CONTRACT_GROUP_NM_NATIONAL_DESC varchar(500),
    PLAN_NM_1_TXT varchar(150),
    PLAN_NM_2_TXT varchar(150),
    PLAN_NM_3_TXT varchar(150),
    SUBSCRIBER_NR int,
    AGREEMENT_STATUS_CDE varchar(50),
    AGREEMENT_STATUS_REASON_CDE varchar(50),
    ALLOW_ROLLOVER_IND boolean,
    APPLICATION_SUBMIT_DT date,
    BENEFICIARIES_RECORDED_IND boolean,
    CONVERSION_EXPIRED_IND boolean,
    CONVERSION_ELIGIBILITY_START_DT date,
    CONVERSION_ELIGIBILITY_END_DT date,
    DEFAULT_VOLUNTARY_DEFERRAL_RT numeric(9,6),
    ENROLLMENT_DT date,
    ENROLLMENT_ELIGIBILITY_DT date,
    ENROLLMENT_START_DT date,
    ENROLLMENT_END_DT date,
    ENROLLMENT_STATUS_CDE varchar(50),
    ENROLLMENT_TYPE_CDE varchar(50),
    FACE_AMT numeric(17,4),
    INTERNAL_PLAN_ID varchar(50),
    ISSUE_DT date,
    PARTICIPATION_DT date,
    SOLICITATION_DT date,
    SOLICITATION_IND boolean,
    TAFT_HARTLY_IND boolean,
    ELECTRONIC_CONTRIBUTION_CHANGE_IND boolean,
    PRE_TAX_AUTO_INCREASE_CONTRIBUTION_RT numeric(9,6),
    NEXT_PAYMENT_DT date,
    NEXT_PAYMENT_AMT numeric(17,4),
    CURRENT_PRINCIPAL_AMT numeric(17,4),
    LOAN_BOOKED_COUPON_RT numeric(9,6),
    LOAN_BOOKED_AMT numeric(17,4),
    LOAN_REQUESTED_COUPON_RT numeric(9,6),
    LOAN_REQUESTED_AMT numeric(17,4),
    LOAN_MONTHS_TO_MATURITY_VAL int,
    LOAN_DEGREE_PROGRAM_DESC varchar(50),
    DECISIONING_ANNUAL_INCOME_AMT numeric(17,4),
    APPLICATION_STATUS_DT date,
    LOAN_REPORT_UNIQUE_HASH_ID varchar(32),
    ESERVICE_IND boolean,
    AGREEMENT_EFFECTIVE_DT date,
    TERMINATED_PARTICIPANT_SERVICING_IND boolean,
    MANAGED_ACCOUNTS_ELIGIBILITY_IND boolean,
    MANAGED_ACCOUNTS_PARTICIPATION_IND boolean,
    OPERATOR_IND varchar(1),
    CHECK_SUM UUID,
    BEGIN_DT DATE,
    END_DT DATE,
    BEGIN_DTM TIMESTAMP,
    END_DTM TIMESTAMP
);