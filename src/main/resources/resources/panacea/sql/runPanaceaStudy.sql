{DEFAULT @cdm_schema = 'OMOPV5_DE'}
{DEFAULT @results_schema = 'OHDSI'}
{DEFAULT @ohdsi_schema = 'OHDSI'}
{DEFAULT @cohortDefId = 915}
{DEFAULT @studyId = 18}
{DEFAULT @drugConceptId = '1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315'}
{DEFAULT @procedureConceptId = '1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315'}

IF OBJECT_ID('tempdb..#_pnc_ptsq_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_ptsq_ct;

CREATE TABLE #_pnc_ptsq_ct
(
  study_id INT,
  source_id INT,
  person_id INT,
  tx_seq INT,
  concept_id INT,
  concept_name VARCHAR(255),
  idx_start_date DATE,
  idx_end_date DATE,
  duration_days INT
);

--from drug_era
INSERT INTO #_pnc_ptsq_ct (study_id, person_id, source_id, concept_id, concept_name, idx_start_date, idx_end_date, duration_days)
SELECT distinct study.study_id AS study_id, myCohort.person_id AS person_id, @sourceId AS source_id, era.drug_concept_id,
  myConcept.concept_name, era.drug_era_start_date, era.drug_era_end_date, era.drug_era_end_date - era.drug_era_start_date + 1
FROM @results_schema.panacea_study study
INNER JOIN (SELECT DISTINCT COHORT_DEFINITION_ID COHORT_DEFINITION_ID, subject_id person_id, COHORT_START_DATE cohort_start_date, cohort_end_date cohort_end_date FROM @ohdsi_schema.cohort 
--       WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id in (2000000030415658, 2000000032622347))  myCohort
--        WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id in (2000000030415658, 2000000032622347, 2000000000085043,2000000000090467,2000000000118598,2000000000125769,2000000000125769,2000000000239227,2000000000239227,2000000000239227,2000000000239227,2000000000631458,2000000000959184,2000000000959184,2000000000959184,2000000001023133,2000000001050023,2000000001198966,2000000001198966,2000000001328233,2000000001328233,2000000001556222,2000000001572262,2000000001598664,2000000001663228,2000000001705565,2000000001705565,2000000001724335,2000000001913150,2000000001913150,2000000001915668,2000000001915668,2000000001953187,2000000001978178,2000000002067964,2000000002120363,2000000002265649,2000000002382712,2000000002382712,2000000002403404,2000000002857369,2000000002975421,2000000003048921,2000000003175220,2000000003395250,2000000003613126,2000000003622138,2000000008400409,2000000008400723,2000000008419771,2000000008419771,2000000008587433))  myCohort
--		WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id in (2000000030415658, 2000000032622347, 2000000000085043,2000000000090467,2000000000118598,2000000000125769,2000000000125769,2000000000239227,2000000000239227,2000000000239227,2000000000239227,2000000000631458,2000000000959184,2000000000959184,2000000000959184,2000000001023133,2000000001050023,2000000001198966,2000000001198966,2000000001328233,2000000001328233,2000000001556222,2000000001572262,2000000001598664,2000000001663228,2000000001705565,2000000001705565,2000000001724335,2000000001913150,2000000001913150,2000000001915668,2000000001915668,2000000001953187,2000000001978178,2000000002067964,2000000002120363,2000000002265649,2000000002382712,2000000002382712,2000000002403404,2000000002857369,2000000002975421,2000000003048921,2000000003175220,2000000003395250,2000000003613126,2000000003622138,2000000008400409,2000000008400723,2000000008419771,2000000008419771,2000000008587433,2000000003395250,2000000026715825,2000000028349554,2000000045463331,2000000049663233,2000000050900029,2000000091077892,2000000144174555,2000000220342782))  myCohort
        WHERE COHORT_DEFINITION_ID = @cohortDefId)  myCohort
ON myCohort.COHORT_DEFINITION_ID = study.COHORT_DEFINITION_ID
INNER JOIN @cdm_schema.drug_era era   
  ON myCohort.person_id = era.person_id
  AND era.drug_concept_id in (@drugConceptId)
  AND (era.DRUG_ERA_START_DATE > myCohort.COHORT_START_DATE OR era.DRUG_ERA_START_DATE = myCohort.COHORT_START_DATE) 
  AND (era.DRUG_ERA_START_DATE < myCohort.COHORT_START_DATE + study.STUDY_DURATION OR era.DRUG_ERA_START_DATE = myCohort.COHORT_START_DATE + study.STUDY_DURATION) 
  @drugEraStudyOptionalDateConstraint
INNER JOIN @cdm_schema.concept myConcept
ON era.drug_concept_id = myConcept.concept_id
WHERE
    study.study_id = @studyId
ORDER BY person_id, drug_era_start_date, drug_era_end_date;

--from procedure_occurrence
INSERT INTO #_pnc_ptsq_ct (study_id, person_id, source_id, concept_id, concept_name, idx_start_date, idx_end_date, duration_days)
SELECT distinct study.study_id AS study_id, myCohort.person_id AS person_id, @sourceId AS source_id, proc.procedure_concept_id,
  myConcept.concept_name, proc.procedure_date, myObservation.end_date, myObservation.end_date - proc.procedure_date + 1
FROM @results_schema.panacea_study study
INNER JOIN (SELECT DISTINCT COHORT_DEFINITION_ID COHORT_DEFINITION_ID, subject_id person_id, COHORT_START_DATE cohort_start_date, cohort_end_date cohort_end_date FROM @ohdsi_schema.cohort
--		WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id in (2000000030415658, 2000000032622347, 2000000000085043,2000000000090467,2000000000118598,2000000000125769,2000000000125769,2000000000239227,2000000000239227,2000000000239227,2000000000239227,2000000000631458,2000000000959184,2000000000959184,2000000000959184,2000000001023133,2000000001050023,2000000001198966,2000000001198966,2000000001328233,2000000001328233,2000000001556222,2000000001572262,2000000001598664,2000000001663228,2000000001705565,2000000001705565,2000000001724335,2000000001913150,2000000001913150,2000000001915668,2000000001915668,2000000001953187,2000000001978178,2000000002067964,2000000002120363,2000000002265649,2000000002382712,2000000002382712,2000000002403404,2000000002857369,2000000002975421,2000000003048921,2000000003175220,2000000003395250,2000000003613126,2000000003622138,2000000008400409,2000000008400723,2000000008419771,2000000008419771,2000000008587433,2000000003395250,2000000026715825,2000000028349554,2000000045463331,2000000049663233,2000000050900029,2000000091077892,2000000144174555,2000000220342782))  myCohort
        WHERE COHORT_DEFINITION_ID = @cohortDefId)  myCohort
ON myCohort.COHORT_DEFINITION_ID = study.COHORT_DEFINITION_ID
INNER JOIN @cdm_schema.procedure_occurrence proc
  ON myCohort.person_id = proc.person_id
  AND proc.procedure_concept_id in (@procedureConceptId)
  AND (proc.PROCEDURE_DATE > myCohort.COHORT_START_DATE OR proc.PROCEDURE_DATE = myCohort.COHORT_START_DATE) 
  AND (proc.PROCEDURE_DATE < myCohort.COHORT_START_DATE + study.STUDY_DURATION OR proc.PROCEDURE_DATE = myCohort.COHORT_START_DATE + study.STUDY_DURATION) 
  @procedureStudyOptionalDateConstraint
INNER JOIN @cdm_schema.concept myConcept
ON proc.procedure_concept_id = myConcept.concept_id
INNER JOIN (SELECT observationPeriod.PERSON_ID PERSON_ID, max(OBSERVATION_PERIOD_END_DATE) end_date 
	from @cdm_schema.observation_period observationPeriod
	where observationPeriod.person_id in (SELECT DISTINCT subject_id FROM @ohdsi_schema.cohort
    WHERE  COHORT_DEFINITION_ID = @cohortDefId)
	group by observationPeriod.person_id) myObservation
ON myObservation.PERSON_ID = myCohort.person_id
WHERE
    study.study_id = @studyId
ORDER BY person_id, procedure_date;

--INSERT INTO #_pnc_ptsq_ct (study_id, person_id, source_id, concept_id, concept_name, idx_start_date, idx_end_date, duration_days)
--SELECT distinct study.study_id AS study_id, myCohort.person_id AS person_id, @sourceId AS source_id, era.drug_concept_id,
--  myConcept.concept_name, era.drug_era_start_date, era.drug_era_end_date, era.drug_era_end_date - era.drug_era_start_date + 1
--FROM @results_schema.panacea_study study
--INNER JOIN (SELECT DISTINCT subject_id person_id, COHORT_START_DATE cohort_start_date, cohort_end_date cohort_end_date FROM @ohdsi_schema.cohort 
----       WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id in (2000000030415658, 2000000032622347))  myCohort
----        WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id in (2000000030415658, 2000000032622347, 2000000000085043,2000000000090467,2000000000118598,2000000000125769,2000000000125769,2000000000239227,2000000000239227,2000000000239227,2000000000239227,2000000000631458,2000000000959184,2000000000959184,2000000000959184,2000000001023133,2000000001050023,2000000001198966,2000000001198966,2000000001328233,2000000001328233,2000000001556222,2000000001572262,2000000001598664,2000000001663228,2000000001705565,2000000001705565,2000000001724335,2000000001913150,2000000001913150,2000000001915668,2000000001915668,2000000001953187,2000000001978178,2000000002067964,2000000002120363,2000000002265649,2000000002382712,2000000002382712,2000000002403404,2000000002857369,2000000002975421,2000000003048921,2000000003175220,2000000003395250,2000000003613126,2000000003622138,2000000008400409,2000000008400723,2000000008419771,2000000008419771,2000000008587433))  myCohort
--        WHERE COHORT_DEFINITION_ID = @cohortDefId)  myCohort
--ON myCohort.cohort_start_date > study.start_date
--   AND myCohort.cohort_start_date < study.end_date
--   AND myCohort.cohort_end_date < study.end_date
--INNER JOIN @cdm_schema.drug_era era   
--ON myCohort.cohort_start_date < era.drug_era_start_date
--  AND era.drug_era_start_date < myCohort.cohort_end_date
--  AND era.drug_era_start_date > study.start_date
----ON era.drug_era_start_date > study.start_date
--  AND era.drug_era_start_date < study.end_date
--  AND era.drug_era_end_date < (era.drug_era_start_date + study.study_duration)
--  AND myCohort.person_id = era.person_id
--  AND era.drug_concept_id in (@drugConceptId)
--INNER JOIN @cdm_schema.concept myConcept
--ON era.drug_concept_id = myConcept.concept_id
--WHERE
--    study.study_id = @studyId
--ORDER BY person_id, drug_era_start_date, drug_era_end_date;

--INSERT INTO #_pnc_ptsq_ct (study_id, person_id, source_id, concept_id, concept_name, idx_start_date, idx_end_date, duration_days)
--SELECT distinct study.study_id AS study_id, myCohort.person_id AS person_id, @sourceId AS source_id, era.drug_concept_id,
-- myConcept.concept_name, era.drug_era_start_date, era.drug_era_end_date, era.drug_era_end_date - era.drug_era_start_date + 1
--FROM @results_schema.panacea_study study
--INNER JOIN (SELECT DISTINCT subject_id person_id, COHORT_START_DATE cohort_start_date, cohort_end_date cohort_end_date FROM @ohdsi_schema.cohort
----	WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id in (2000001019583262))  myCohort
----        WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id in (2000000030415658, 2000000032622347))  myCohort
----        WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id in (2000000030415658, 2000000032622347, 2000000000085043,2000000000090467,2000000000118598,2000000000125769,2000000000125769,2000000000239227,2000000000239227,2000000000239227,2000000000239227,2000000000631458,2000000000959184,2000000000959184,2000000000959184,2000000001023133,2000000001050023,2000000001198966,2000000001198966,2000000001328233,2000000001328233,2000000001556222,2000000001572262,2000000001598664,2000000001663228,2000000001705565,2000000001705565,2000000001724335,2000000001913150,2000000001913150,2000000001915668,2000000001915668,2000000001953187,2000000001978178,2000000002067964,2000000002120363,2000000002265649,2000000002382712,2000000002382712,2000000002403404,2000000002857369,2000000002975421,2000000003048921,2000000003175220,2000000003395250,2000000003613126,2000000003622138,2000000008400409,2000000008400723,2000000008419771,2000000008419771,2000000008587433))  myCohort
--        WHERE COHORT_DEFINITION_ID = @cohortDefId)  myCohort
--ON myCohort.cohort_start_date > study.start_date
--   AND myCohort.cohort_start_date < study.end_date
--   AND myCohort.cohort_end_date < study.end_date
--INNER JOIN @cdm_schema.drug_era era   
----ON myCohort.cohort_start_date < era.drug_era_start_date
----  AND era.drug_era_start_date < myCohort.cohort_end_date
----  AND era.drug_era_start_date > study.start_date
--ON era.drug_era_start_date > study.start_date
--  AND era.drug_era_start_date < study.end_date
--  AND era.drug_era_end_date < (era.drug_era_start_date + study.study_duration)
--  AND myCohort.person_id = era.person_id
--  AND era.drug_concept_id in (@drugConceptId)
--INNER JOIN @cdm_schema.concept myConcept
--ON era.drug_concept_id = myConcept.concept_id
--WHERE
--    study.study_id = @studyId
--ORDER BY person_id, drug_era_start_date, drug_era_end_date;
 
MERGE INTO #_pnc_ptsq_ct ptsq
USING
(SELECT rank() OVER (PARTITION BY person_id
  ORDER BY person_id, idx_start_date, idx_end_date, ROWNUM) real_tx_seq,
  rowid AS the_rowid
  FROM #_pnc_ptsq_ct 
) ptsq1
ON
(
     ptsq.rowid = ptsq1.the_rowid
)
WHEN MATCHED THEN UPDATE SET ptsq.tx_seq = ptsq1.real_tx_seq;

IF OBJECT_ID('tempdb..#_pnc_ptstg_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_ptstg_ct;

CREATE TABLE #_pnc_ptstg_ct
(
  study_id INT,
  source_id INT,
  person_id INT,
  tx_stg_cmb_id INT,
  tx_seq INT,
  stg_start_date DATE,
  stg_end_date DATE,
  stg_duration_days INT
);

--IF OBJECT_ID('tempdb..#_pnc_sngl_cmb', 'U') IS NOT NULL
--  DROP TABLE #_pnc_sngl_cmb;

--CREATE TABLE #_pnc_sngl_cmb
--(
--  tx_stg_cmb_id INT,
--  concept_id INT,
--  concept_name VARCHAR(255)
--);

--get single concept combo
--insert into #_pnc_sngl_cmb (tx_stg_cmb_id, concept_id, concept_name)
--(select comb.pnc_tx_stg_cmb_id, combmap.concept_id, combmap.concept_name from @results_schema.pnc_tx_stage_combination comb
--join @results_schema.pnc_tx_stage_combination_map combMap 
--on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--join 
--(select comb.pnc_tx_stg_cmb_id, count(*) from @results_schema.pnc_tx_stage_combination comb
--join @results_schema.pnc_tx_stage_combination_map combMap 
--on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--where comb.study_id = @studyId
--group by comb.pnc_tx_stg_cmb_id
--having count(*) = 1) multiple_ids_combo
--on multiple_ids_combo.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--);


--MERGE INTO @results_schema.pnc_tx_stage_combination_map combo
--USING
--  (SELECT DISTINCT ptsq.concept_id concept_id, ptsq.concept_name concept_name FROM #_pnc_ptsq_ct ptsq
--    WHERE ptsq.concept_id NOT IN 
--      (select exist_combo.concept_id from 
--        (SELECT comb_map.concept_id concept_id FROM @results_schema.pnc_tx_stage_combination comb
--        JOIN @results_schema.pnc_tx_stage_combination_map comb_map
--        ON comb_map.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--        WHERE comb.study_id = @studyId) exist_combo
--      )
--  ) adding_concept
--  ON
--  (
--    1 = 0
--  )
--WHEN NOT MATCHED THEN INSERT (PNC_TX_STG_CMB_MP_ID, PNC_TX_STG_CMB_ID, CONCEPT_ID, CONCEPT_NAME)
--VALUES (@results_schema.seq_pnc_tx_stg_cmb_mp.NEXTVAL, @results_schema.seq_pnc_tx_stg_cmb.NEXTVAL, adding_concept.concept_id, adding_concept.concept_name);

MERGE INTO @results_schema.pnc_tx_stage_combination_map combo
USING
  (SELECT DISTINCT myConcept.concept_id concept_id, myConcept.concept_name concept_name FROM @cdm_schema.concept myConcept
    where myconcept.concept_id in (@drugConceptId, @procedureConceptId)
    and myConcept.concept_id NOT IN
--change to add all concepts into pnc_tx_stage_combination_map and pnc_tx_stage_combination instead of dynamically add not exising concepts in #_pnc_ptsq_ct, per Jon  
--  (SELECT DISTINCT ptsq.concept_id concept_id, ptsq.concept_name concept_name FROM #_pnc_ptsq_ct ptsq
--    WHERE ptsq.concept_id NOT IN 
--      (select distinct concept_id from #_pnc_sngl_cmb 
--      )
      (select distinct concept_id from 
        (select comb.pnc_tx_stg_cmb_id as comb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name from @results_schema.pnc_tx_stage_combination comb
          join @results_schema.pnc_tx_stage_combination_map combMap 
          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
          join 
          (select comb.pnc_tx_stg_cmb_id, count(*) from @results_schema.pnc_tx_stage_combination comb
          join @results_schema.pnc_tx_stage_combination_map combMap 
          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
          where comb.study_id = @studyId
          group by comb.pnc_tx_stg_cmb_id
          having count(*) = 1) multiple_ids_combo
          on multiple_ids_combo.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
        )
      )
  ) adding_concept
  ON
  (
    1 = 0
  )
WHEN NOT MATCHED THEN INSERT (PNC_TX_STG_CMB_MP_ID, PNC_TX_STG_CMB_ID, CONCEPT_ID, CONCEPT_NAME)
VALUES (@results_schema.seq_pnc_tx_stg_cmb_mp.NEXTVAL, @results_schema.seq_pnc_tx_stg_cmb.NEXTVAL, adding_concept.concept_id, adding_concept.concept_name);


MERGE INTO @results_schema.pnc_tx_stage_combination comb
USING
  (
    SELECT combo_map.pnc_tx_stg_cmb_id pnc_tx_stg_cmb_id FROM @results_schema.pnc_tx_stage_combination_map combo_map
  ) adding_combo
  ON
  (
    comb.pnc_tx_stg_cmb_id = adding_combo.pnc_tx_stg_cmb_id
  )
WHEN NOT MATCHED THEN INSERT (PNC_TX_STG_CMB_ID,STUDY_ID)
VALUES (adding_combo.pnc_tx_stg_cmb_id, @studyId);


-- insert from #_pnc_ptsq_ct ptsq into #_pnc_ptstg_ct (remove same patient/same drug small time window inside large time window. EX: 1/2/2015 ~ 1/31/2015 inside 1/1/2015 ~ 3/1/2015)
-- use single concept combo and avoid duplicate combo for the same concept if there's multiple single combo for same concept by min() value 
insert into #_pnc_ptstg_ct (study_id, source_id, person_id, tx_stg_cmb_id, stg_start_date, stg_end_date, stg_duration_days)
select insertingPTSQ.study_id, insertingPTSQ.source_id, insertingPTSQ.person_id, insertingPTSQ.pnc_tx_stg_cmb_id, insertingPTSQ.idx_start_date, insertingPTSQ.idx_end_date, insertingPTSQ.duration_days
from (select ptsq.study_id, ptsq.source_id, ptsq.person_id, ptsq.idx_start_date, ptsq.idx_end_date, ptsq.duration_days, combo.pnc_tx_stg_cmb_id from #_pnc_ptsq_ct ptsq,
  (select min(comb.pnc_tx_stg_cmb_id) pnc_tx_stg_cmb_id, combmap.concept_id concept_id, combmap.concept_name concept_name from @results_schema.pnc_tx_stage_combination comb
  	join @results_schema.pnc_tx_stage_combination_map combMap 
	on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
	join 
	(select comb.pnc_tx_stg_cmb_id, count(*) from @results_schema.pnc_tx_stage_combination comb
	join @results_schema.pnc_tx_stage_combination_map combMap 
	on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
	where comb.study_id = @studyId
	group by comb.pnc_tx_stg_cmb_id
	having count(*) = 1) multiple_ids_combo
	on multiple_ids_combo.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
	group by combmap.concept_id, combmap.concept_name
  ) combo
where ptsq.rowid not in
  (select ptsq2.rowid from #_pnc_ptsq_ct ptsq1
    join #_pnc_ptsq_ct ptsq2
    on ptsq1.person_id = ptsq2.person_id
    and ptsq1.concept_id = ptsq2.concept_id
    where ((ptsq2.idx_start_date > ptsq1.idx_start_date)
      and (ptsq2.idx_end_date < ptsq1.idx_end_date
      or ptsq2.idx_end_date = ptsq1.idx_end_date))
    or ((ptsq2.idx_start_date > ptsq1.idx_start_date
      or ptsq2.idx_start_date = ptsq1.idx_start_date
      ) and (ptsq2.idx_end_date < ptsq1.idx_end_date))
  )
and ptsq.study_id = @studyId
AND combo.concept_id = ptsq.concept_id
order by ptsq.person_id, ptsq.idx_start_date, ptsq.idx_end_date
) insertingPTSQ;


-- take care of expanded time window for same patient/same drug. 
-- EX: 2/1/2015 ~ 4/1/2015 ptstg2, 1/1/2015 ~ 3/1/2015 ptstg1. Update ptstg1 with later end date and delete ptstg2   
merge into #_pnc_ptstg_ct ptstg
using
  (
    select updateRowID updateRowID, max(realEndDate) as realEndDate from 
    (
      select ptstg2.rowid deleteRowId, ptstg1.rowid updateRowID,
        case 
          when ptstg1.stg_end_date > ptstg2.stg_end_date then ptstg1.stg_end_date
          when ptstg2.stg_end_date > ptstg1.stg_end_date then ptstg2.stg_end_date
          when ptstg2.stg_end_date = ptstg1.stg_end_date then ptstg2.stg_end_date
        end as realEndDate
      from #_pnc_ptstg_ct ptstg1
      join #_pnc_ptstg_ct ptstg2
      on ptstg1.person_id = ptstg2.person_id
      and ptstg1.tx_stg_cmb_id = ptstg2.tx_stg_cmb_id
      where ptstg2.stg_start_date < ptstg1.stg_end_date
        and ptstg2.stg_start_date > ptstg1.stg_start_date
    ) group by updateRowID
  ) ptstgExpandDate
  on
  (
     ptstg.rowid = ptstgExpandDate.updateRowID
  )
  WHEN MATCHED then update set ptstg.stg_end_date = ptstgExpandDate.realEndDate,
    ptstg.stg_duration_days = (ptstgExpandDate.realEndDate - ptstg.stg_start_date + 1);

    
delete from #_pnc_ptstg_ct ptstg
where ptstg.rowid in 
  (
    select ptstg2.rowid deleteRowId
    from #_pnc_ptstg_ct ptstg1
    join #_pnc_ptstg_ct ptstg2
    on ptstg1.person_id = ptstg2.person_id
    and ptstg1.tx_stg_cmb_id = ptstg2.tx_stg_cmb_id
    where ptstg2.stg_start_date < ptstg1.stg_end_date
      and ptstg2.stg_start_date > ptstg1.stg_start_date
  );

IF OBJECT_ID('tempdb..#_pnc_tmp_cmb_sq_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_tmp_cmb_sq_ct;

CREATE TABLE #_pnc_tmp_cmb_sq_ct
(
	person_id INT,
	combo_ids VARCHAR(255),
	tx_seq INT,	
	combo_seq VARCHAR(400),
    start_date date,
    end_date date,
    combo_duration INT,
    result_version INT,
    gap_days INT
);

--TRUNCATE TABLE #_pnc_ptsq_ct;
--DROP TABLE #_pnc_ptsq_ct;

--TRUNCATE TABLE #_pnc_ptstg_ct;
--DROP TABLE #_pnc_ptstg_ct;

--TRUNCATE TABLE #_pnc_sngl_cmb;
--DROP TABLE #_pnc_sngl_cmb;

--TRUNCATE TABLE #_pnc_tmp_cmb_sq_ct;
--DROP TABLE #_pnc_tmp_cmb_sq_ct;
