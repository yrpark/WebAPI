SET IMPLICIT_TRANSACTIONS OFF
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cohort_inclusion](
  [cohort_definition_id] [int] NOT NULL,
  [rule_sequence] [int] NOT NULL,
  [name] [varchar](255) NULL,
  [description] [varchar](1000) NULL
) WITH (clustered columnstore index, DISTRIBUTION = HASH(cohort_definition_id))
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cohort_inclusion_result](
  [cohort_definition_id] [int] NOT NULL,
  [inclusion_rule_mask] [bigint] NOT NULL,
  [person_count] [bigint] NOT NULL
) WITH (clustered columnstore index, DISTRIBUTION = HASH(cohort_definition_id))
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cohort_summary_stats](
  [cohort_definition_id] [int] NOT NULL,
  [base_count] [bigint] NOT NULL,
  [final_count] [bigint] NOT NULL
) WITH (clustered columnstore index, DISTRIBUTION = HASH(cohort_definition_id))
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cohort_inclusion_stats](
  [cohort_definition_id] [int] NOT NULL,
  [rule_sequence] [int] NOT NULL,
  [person_count] [bigint] NOT NULL,
  [gain_count] [bigint] NOT NULL,
  [person_total] [bigint] NOT NULL
) WITH (clustered columnstore index, DISTRIBUTION = HASH(cohort_definition_id))
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[heracles_results](
	cohort_definition_id int,
	analysis_id int,
	stratum_1 varchar(255),
	stratum_2 varchar(255),
	stratum_3 varchar(255),
	stratum_4 varchar(255),
	stratum_5 varchar(255),
	count_value bigint,
	last_update_time datetime 
) WITH ( CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION =  HASH(analysis_id));
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[heracles_results_dist](
	cohort_definition_id int,
	analysis_id int,
	stratum_1 varchar(255),
	stratum_2 varchar(255),
	stratum_3 varchar(255),
	stratum_4 varchar(255),
	stratum_5 varchar(255),
	count_value bigint,
	min_value float,
	max_value float,
	avg_value float,
	stdev_value float,
	median_value float,
	p10_value float,
	p25_value float,
	p75_value float,
	p90_value float,
	last_update_time datetime
)
WITH ( CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION =  HASH(analysis_id));
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[heracles_heel_results](
	cohort_definition_id int, 
	analysis_id INT, 
	HERACLES_HEEL_warning VARCHAR(255) 
)
WITH ( CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION =  HASH(analysis_id))
GO

IF XACT_STATE() = 1 COMMIT
GO