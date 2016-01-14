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


-- insert from #_pnc_ptsq_ct ptsq into #_pnc_ptstg_ct (remove same patient/same drug small time window inside large time window. EX: 1/2/2015 ~ 1/31/2015 inside 1/1/2015 ~ 3/1/2015) 
insert into #_pnc_ptstg_ct (study_id, source_id, person_id, tx_stg_cmb_id, stg_start_date, stg_end_date, stg_duration_days)
select insertingPTSQ.study_id, insertingPTSQ.source_id, insertingPTSQ.person_id, insertingPTSQ.pnc_tx_stg_cmb_id, insertingPTSQ.idx_start_date, insertingPTSQ.idx_end_date, insertingPTSQ.duration_days
from (select ptsq.study_id, ptsq.source_id, ptsq.person_id, ptsq.idx_start_date, ptsq.idx_end_date, ptsq.duration_days, combo.pnc_tx_stg_cmb_id from #_pnc_ptsq_ct ptsq,
  (SELECT comb.pnc_tx_stg_cmb_id, combmap.concept_id FROM @results_schema.pnc_tx_stage_combination comb
    JOIN @results_schema.pnc_tx_stage_combination_map combMap
    ON combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
    where comb.study_id = @studyId
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

  
--TRUNCATE TABLE #_pnc_ptsq_ct;
--DROP TABLE #_pnc_ptsq_ct;

--TRUNCATE TABLE #_pnc_ptstg_ct;
--DROP TABLE #_pnc_ptstg_ct;
