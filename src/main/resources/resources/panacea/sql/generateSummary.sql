delete from @results_schema.pnc_study_summary_path where study_id = @studyId and source_id = @sourceId;

insert into @results_schema.pnc_study_summary_path(pnc_stdy_smry_id, study_id, source_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_cnt, tx_stg_avg_dr)
select seq_pnc_stdy_smry.nextval, @studyId, @sourceId, null, aggregatePath.combo_ids, aggregatePath.combo_seq, aggregatePath.tx_seq, aggregatePath.patientCount, aggregatePath.averageDurationDays 
from
  (select combo_ids combo_ids, combo_seq combo_seq, tx_seq tx_seq, count(*) patientCount, avg(combo_duration) averageDurationDays from #_pnc_tmp_cmb_sq_ct ptTxPath
    group by combo_ids, combo_seq, tx_seq) aggregatePath;
    
merge into @results_schema.pnc_study_summary_path  m
using
  (
    select pathsum.rowid as the_rowid, parentpath.pnc_stdy_smry_id as parentKey, updateParentPath.parentPath pPath from @results_schema.pnc_study_summary_path pathSum
    join (select rowid, SUBSTR(tx_stg_cmb_pth , 0 , length(tx_stg_cmb_pth) - length(tx_stg_cmb) - 2 ) as parentPath
    from @results_schema.pnc_study_summary_path) updateParentPath
    on updateParentPath.rowid = pathSum.rowid
    join pnc_study_summary_path parentPath
    on updateParentPath.parentPath = parentPath.tx_stg_cmb_pth
  ) m1
  on
  (
     m.rowid = m1.the_rowid
  )
WHEN MATCHED then update set m.tx_path_parent_key = m1.parentKey;
