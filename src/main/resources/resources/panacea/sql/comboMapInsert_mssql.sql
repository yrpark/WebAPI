

insert into @results_schema.pnc_tx_stage_combination_map (pnc_tx_stg_cmb_mp_id, pnc_tx_stg_cmb_id, concept_id, concept_name)
select NEXT VALUE FOR @results_schema.seq_pnc_tx_stg_cmb_mp, NEXT VALUE FOR @results_schema.seq_pnc_tx_stg_cmb, concept_id, concept_name from
  (SELECT DISTINCT myConcept.concept_id concept_id, myConcept.concept_name concept_name FROM @cdm_schema.concept myConcept
    where myconcept.concept_id in (@allConceptIdsStr)
    and myConcept.concept_id NOT IN
      (select distinct concept_id from 
        (select comb.pnc_tx_stg_cmb_id as comb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name from @results_schema.pnc_tx_stage_combination comb
          join @results_schema.pnc_tx_stage_combination_map combMap 
          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
          join 
          (select comb.pnc_tx_stg_cmb_id pnc_tx_stg_cmb_id, count(*) cnt from @results_schema.pnc_tx_stage_combination comb
          join @results_schema.pnc_tx_stage_combination_map combMap 
          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
          where comb.study_id = 1
          group by comb.pnc_tx_stg_cmb_id
          having count(*) = 1) multiple_ids_combo
          on multiple_ids_combo.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
        ) distinctConcept
      )
  ) adding_concept;
