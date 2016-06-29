SET IMPLICIT_TRANSACTIONS OFF
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cca_auc] (
    [executionId] float NULL, 
    [auc] float NULL
)
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cca_strat_pop] (
    [executionId] float NULL, 
    [rowId] float NULL, 
    [subjectId] float NULL, 
    [treatment] float NULL, 
    [cohortStartDate] date NULL, 
    [daysFromObsStart] float NULL, 
    [daysToCohortEnd] float NULL, 
    [daysToObsEnd] float NULL, 
    [outcomeCount] float NULL, 
    [timeAtRisk] float NULL, 
    [daysToEvent] float NULL, 
    [survivalTime] float NULL, 
    [propensityScore] float NULL, 
    [stratumId] float NULL
)
GO

IF XACT_STATE() = 1 COMMIT
GO

CREATE TABLE ${resultSchema}.[cca_ps_model] (
    [executionId] float NULL, 
    [coefficient] float NULL, 
    [id] float NULL, 
    [covariateName] varchar(255) COLLATE Latin1_General_100_CI_AS_KS_WS NULL
)
GO

IF XACT_STATE() = 1 COMMIT
GO