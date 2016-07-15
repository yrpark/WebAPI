SET IMPLICIT_TRANSACTIONS OFF
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cca_auc] (
    [execution_id] int NULL, 
    [auc] float NULL
)
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cca_matched_pop] (
    [execution_id] int NULL, 
    [row_id] bigint NULL, 
    [subject_id] bigint NULL, 
    [treatment] int NULL, 
    [cohort_start_date] date NULL, 
    [days_from_obs_start] int NULL, 
    [days_to_cohort_end] int NULL, 
    [days_to_obs_end] int NULL, 
    [outcome_count] int NULL, 
    [time_at_risk] int NULL, 
    [days_to_event] int NULL, 
    [survival_time] int NULL, 
    [propensity_score] float NULL, 
    [stratum_id] int NULL
)
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cca_ps_model] (
    [execution_id] int NULL, 
    [coefficient] float NULL, 
    [id] bigint NULL, 
    [covariate_name] varchar(255) COLLATE Latin1_General_100_CI_AS_KS_WS NULL
)
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cca_ps_model_agg] (
    [execution_id] int NULL, 
    [ps] float NULL, 
    [treatment] int NULL, 
    [comparator] int NULL
)
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cca_attrition] (
    [execution_id] int NULL, 
    [description] varchar(255) COLLATE Latin1_General_100_CI_AS_KS_WS NULL, 
    [treated_persons] int NULL, 
    [comparator_persons] int NULL,
    [treated_exposures] int NULL, 
    [comparator_exposures] int NULL,
    [attrition_order] int NULL
)
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cca_balance] (
    [execution_id] int NULL, 
    [covariate_id] bigint NULL,
    [before_matching_mean_treated] float NULL,
    [before_matching_mean_comparator] float NULL,
    [before_matching_sum_treated] int NULL,
    [before_matching_sum_comparator] int NULL,
    [before_matching_sd] float NULL,
    [after_matching_mean_treated] float NULL,
    [after_matching_mean_comparator] float NULL,
    [after_matching_sum_treated] int NULL,
    [after_matching_sum_comparator] int NULL,
    [after_matching_sd] float NULL,
    [covariate_name] varchar(255) COLLATE Latin1_General_100_CI_AS_KS_WS NULL, 
    [analysis_id] int NULL, 
    [concept_id] bigint NULL,
    [before_matching_std_diff] float NULL, 
    [after_matching_std_diff] float NULL
)
GO

