{DEFAULT @cdm_schema = 'OMOPV5_DE'}
{DEFAULT @results_schema = 'OHDSI'}
{DEFAULT @ohdsi_schema = 'OHDSI'}
{DEFAULT @cohortDefId = 915}
{DEFAULT @studyId = 18}
{DEFAULT @drugConceptId = '1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315'}

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


INSERT INTO #_pnc_ptsq_ct (study_id, person_id, source_id, concept_id, concept_name, idx_start_date, idx_end_date, duration_days)
SELECT distinct study.study_id AS study_id, myCohort.person_id AS person_id, @sourceId AS source_id, era.drug_concept_id,
  myConcept.concept_name, era.drug_era_start_date, era.drug_era_end_date, era.drug_era_end_date - era.drug_era_start_date + 1
FROM @results_schema.panacea_study study
INNER JOIN (SELECT DISTINCT subject_id person_id, COHORT_START_DATE cohort_start_date, cohort_end_date cohort_end_date FROM @ohdsi_schema.cohort 
        WHERE COHORT_DEFINITION_ID = 915 AND subject_id in (2000000030415658, 2000000032622347))  myCohort
ON myCohort.cohort_start_date > study.start_date
   AND myCohort.cohort_start_date < study.end_date
   AND myCohort.cohort_end_date < study.end_date
INNER JOIN @cdm_schema.drug_era era   
ON myCohort.cohort_start_date < era.drug_era_start_date
  AND era.drug_era_start_date < myCohort.cohort_end_date
  AND era.drug_era_start_date > study.start_date
  AND era.drug_era_start_date < study.end_date
  AND era.drug_era_end_date < (era.drug_era_start_date + study.study_duration)
  AND myCohort.person_id = era.person_id
  AND era.drug_concept_id in (1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315)
INNER JOIN @cdm_schema.concept myConcept
ON era.drug_concept_id = myConcept.concept_id
WHERE
    study.study_id = @studyId
ORDER BY person_id, drug_era_start_date, drug_era_end_date;


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


MERGE INTO @results_schema.pnc_tx_stage_combination_map combo
USING
  (SELECT DISTINCT ptsq.concept_id concept_id, ptsq.concept_name concept_name FROM #_pnc_ptsq_ct ptsq
    WHERE ptsq.concept_id NOT IN 
      (select exist_combo.concept_id from 
        (SELECT comb_map.concept_id concept_id FROM @results_schema.pnc_tx_stage_combination comb
        JOIN @results_schema.pnc_tx_stage_combination_map comb_map
        ON comb_map.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
        WHERE comb.study_id = @studyId) exist_combo
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


TRUNCATE TABLE #_pnc_ptsq_ct;
DROP TABLE #_pnc_ptsq_ct;

TRUNCATE TABLE #_pnc_ptstg_ct;
DROP TABLE #_pnc_ptstg_ct;
