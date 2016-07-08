CREATE TABLE [${ohdsiSchema}].[cca](
  [cca_id] [int] IDENTITY (1, 1) NOT NULL,
  [name] [varchar](255) NULL,
  [treatment_id] [int] NOT NULL,
  [comparator_id] [int] NOT NULL,
  [outcome_id] [int] NOT NULL,
  [exclusion_id] [int] NOT NULL,
  [time_at_risk] [int] NOT NULL,
  [created] [datetime] NOT NULL,
  [modified] [datetime] NOT NULL,
  [sec_user_id] [int] NOT NULL,
  [locked] [int] NOT NULL
);

CREATE TABLE [${ohdsiSchema}].[cca_execution](
  [cca_execution_id] [int] IDENTITY (1, 1) NOT NULL,
  [cca_id] [int] NOT NULL,
  [source_key] [varchar](50) NOT NULL,
  [treatment_id] [int] NOT NULL,
  [comparator_id] [int] NOT NULL,
  [outcome_id] [int] NOT NULL,
  [exclusion_id] [int] NOT NULL,
  [time_at_risk] [int] NOT NULL,
  [executed] [datetime] NOT NULL,
  [execution_duration] [int] NOT NULL,
  [execution_status] [int] NOT NULL,
  [sec_user_id] [int] NOT NULL
);

