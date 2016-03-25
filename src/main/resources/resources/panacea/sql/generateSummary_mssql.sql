delete from @results_schema.pnc_study_summary_path where study_id = @studyId and source_id = @sourceId;

insert into @results_schema.pnc_study_summary_path (pnc_stdy_smry_id, study_id, source_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_cnt, tx_stg_avg_dr, tx_stg_avg_gap, tx_rslt_version)
select seq_pnc_stdy_smry.nextval, @studyId, @sourceId, null, aggregatePath.combo_ids, aggregatePath.combo_seq, aggregatePath.tx_seq, aggregatePath.patientCount, aggregatePath.averageDurationDays, aggregatePath.averageGapDays, aggregatePath.result_version 
from
  (select combo_ids combo_ids, combo_seq combo_seq, tx_seq tx_seq, count(*) patientCount, avg(combo_duration) averageDurationDays, avg(gap_days) averageGapDays, result_version result_version from #_PNC_TMP_CMB_SQ_CT ptTxPath
--    where result_version = 1
    group by combo_ids, combo_seq, tx_seq, result_version) aggregatePath;

-- version = 1
merge into @results_schema.pnc_study_summary_path  m
using
  (
	select pathsum.rowid as the_rowid, parentpath.pnc_stdy_smry_id as parentKey, updateParentPath.parentPath pPath, 
    parentPath.tx_stg_cnt parentCount, pathSum.tx_stg_cnt childCount, NVL(ROUND(pathSum.tx_stg_cnt/parentPath.tx_stg_cnt * 100,2),0) percentage
    from @results_schema.pnc_study_summary_path pathSum
    join (select rowid, SUBSTR(tx_stg_cmb_pth , 0 , length(tx_stg_cmb_pth) - length(tx_stg_cmb) - 1 ) as parentPath
    from @results_schema.pnc_study_summary_path
    where study_id = @studyId and source_id = @sourceId and tx_rslt_version = 1 
    ) updateParentPath
    on updateParentPath.rowid = pathSum.rowid
    join @results_schema.pnc_study_summary_path parentPath
    on updateParentPath.parentPath = parentPath.tx_stg_cmb_pth
    and parentPath.study_id = @studyId
    and parentPath.tx_rslt_version = 1
    and parentPath.source_id = @sourceId
    where pathSum.study_id = @studyId and pathSum.source_id = @sourceId
    and pathSum.tx_rslt_version = 1 
    and parentPath.tx_rslt_version = 1 
    group by pathsum.rowid, parentpath.pnc_stdy_smry_id, updateParentPath.parentPath, parentPath.tx_stg_cnt, pathSum.tx_stg_cnt
  ) m1
  on
  (
     m.rowid = m1.the_rowid
  )
  WHEN MATCHED then update set m.tx_path_parent_key = m1.parentKey, m.tx_stg_percentage = m1.percentage;


merge into @results_schema.pnc_study_summary_path  m
using
  (
    select pathsum.rowid as the_rowid, rootCount.totalRootCount,
    rootCount.totalRootCount parentCount, pathSum.tx_stg_cnt childCount, NVL(ROUND(pathSum.tx_stg_cnt/rootCount.totalRootCount * 100,2),0) percentage
    from @results_schema.pnc_study_summary_path pathSum, (select sum(tx_stg_cnt) totalRootCount from @results_schema.pnc_study_summary_path
    where tx_path_parent_key is null and tx_rslt_version = 1
      and study_id = @studyId and source_id = @sourceId
      ) rootCount
    where tx_path_parent_key is null
    and pathSum.study_id = @studyId and pathSum.source_id = @sourceId
    and pathsum.tx_rslt_version = 1
  ) m1
  on
  (
     m.rowid = m1.the_rowid
  )
  WHEN MATCHED then update set m.tx_stg_percentage = m1.percentage;

-- version = 2
merge into @results_schema.pnc_study_summary_path  m
using
  (
	select pathsum.rowid as the_rowid, parentpath.pnc_stdy_smry_id as parentKey, updateParentPath.parentPath pPath, 
    parentPath.tx_stg_cnt parentCount, pathSum.tx_stg_cnt childCount, NVL(ROUND(pathSum.tx_stg_cnt/parentPath.tx_stg_cnt * 100,2),0) percentage
    from @results_schema.pnc_study_summary_path pathSum
    join (select rowid, SUBSTR(tx_stg_cmb_pth , 0 , length(tx_stg_cmb_pth) - length(tx_stg_cmb) - 1 ) as parentPath
    from @results_schema.pnc_study_summary_path
    where study_id = @studyId and source_id = @sourceId and tx_rslt_version = 2 
    ) updateParentPath
    on updateParentPath.rowid = pathSum.rowid
    join @results_schema.pnc_study_summary_path parentPath
    on updateParentPath.parentPath = parentPath.tx_stg_cmb_pth
    and parentPath.study_id = @studyId
    and parentPath.tx_rslt_version = 2
    and parentPath.source_id = @sourceId
    where pathSum.study_id = @studyId and pathSum.source_id = @sourceId
    and pathSum.tx_rslt_version = 2 
    and parentPath.tx_rslt_version = 2 
    group by pathsum.rowid, parentpath.pnc_stdy_smry_id, updateParentPath.parentPath, parentPath.tx_stg_cnt, pathSum.tx_stg_cnt
  ) m1
  on
  (
     m.rowid = m1.the_rowid
  )
  WHEN MATCHED then update set m.tx_path_parent_key = m1.parentKey, m.tx_stg_percentage = m1.percentage;

merge into @results_schema.pnc_study_summary_path  m
using
  (
    select pathsum.rowid as the_rowid, rootCount.totalRootCount,
    rootCount.totalRootCount parentCount, pathSum.tx_stg_cnt childCount, NVL(ROUND(pathSum.tx_stg_cnt/rootCount.totalRootCount * 100,2),0) percentage
    from @results_schema.pnc_study_summary_path pathSum, (select sum(tx_stg_cnt) totalRootCount from @results_schema.pnc_study_summary_path
    where tx_path_parent_key is null and tx_rslt_version = 2
      and study_id = @studyId and source_id = @sourceId
      ) rootCount
    where tx_path_parent_key is null
    and pathSum.study_id = @studyId and pathSum.source_id = @sourceId
    and pathsum.tx_rslt_version = 2
  ) m1
  on
  (
     m.rowid = m1.the_rowid
  )
  WHEN MATCHED then update set m.tx_stg_percentage = m1.percentage;

delete from @results_schema.pnc_study_summary where study_id = @studyId and source_id = @sourceId;


---------------ms sql collapse/merge multiple rows to concatenate strings (JSON string for conceptsArrary and conceptsName) ------
IF OBJECT_ID('tempdb..#_pnc_smry_msql_cmb', 'U') IS NOT NULL
  DROP TABLE #_pnc_smry_msql_cmb;
 
CREATE TABLE #_pnc_smry_msql_cmb
(
    pnc_tx_stg_cmb_id int,
    conceptsArray text,
	conceptsName text    
);

insert into #_pnc_smry_msql_cmb (pnc_tx_stg_cmb_id, conceptsArray, conceptsName)
select pnc_tx_stg_cmb_id,  conceptsArray, conceptsName 
from
(
select distinct tab1.pnc_tx_stg_cmb_id,
  '[' + STUFF((SELECT distinct '{"innerConceptName":' + '"' + tab2.concept_name + '"' + 
    ',"innerConceptId":' + convert(varchar, tab2.concept_id) + '}'
         from 
         (select comb.study_id as study_id, comb.pnc_tx_stg_cmb_id as pnc_tx_stg_cmb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name
            from @results_schema.pnc_tx_stage_combination comb
            join @results_schema.pnc_tx_stage_combination_map combMap 
              on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
              where comb.study_id = @studyId
         ) tab2
         where tab1.pnc_tx_stg_cmb_id = tab2.pnc_tx_stg_cmb_id
            FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)') 
        ,1,0,'') +  ']' conceptsArray,
  STUFF((SELECT distinct tab2.concept_name + ','
         from 
         (select comb.study_id as study_id, comb.pnc_tx_stg_cmb_id as pnc_tx_stg_cmb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name
            from @results_schema.pnc_tx_stage_combination comb
            join @results_schema.pnc_tx_stage_combination_map combMap 
              on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
              where comb.study_id = @studyId
         ) tab2
         where tab1.pnc_tx_stg_cmb_id = tab2.pnc_tx_stg_cmb_id
            FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)') 
        ,1,0,'') conceptsName
from (select comb.study_id as study_id, comb.pnc_tx_stg_cmb_id as pnc_tx_stg_cmb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name
        from @results_schema.pnc_tx_stage_combination comb
        join @results_schema.pnc_tx_stage_combination_map combMap 
          on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
          where comb.study_id = @studyId) tab1
) studyCombo;


-----------------generate rows of JSON (based on hierarchical data, without using oracle connect/level, each path is a row) insert into temp table----------------------
IF OBJECT_ID('tempdb..#_pnc_smry_msql_cmb', 'U') IS NOT NULL
  DROP TABLE #_pnc_smry_msql_indvdl_json;
 
CREATE TABLE #_pnc_smry_msql_indvdl_json
(
    rnum float,
    table_row_id int,
	rslt_vesion int,
	JSON text
);


-------------------------------version 1 insert into temp table----------------------------------------------
insert into #_pnc_smry_msql_indvdl_json(rnum, table_row_id, rslt_vesion, JSON)
select rnum, table_row_id, rslt_vesion, JSON 
from
(
select allRoots.rnum rnum, 1 table_row_id, 1 rslt_vesion, 
CASE 
    WHEN rnum = 1 THEN '{"comboId": "root","children": [' + substr(JSON_SNIPPET, 2, length(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
from 
(
  select 
  rnum rnum,
  CASE 
    WHEN Lvl = 1 THEN ',{'
    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{' 
    ELSE ',{' 
  END 
  + ' "comboId" : ' + combo_id + ' '
  + ' ,"conceptName" : "' + concept_names + '" '  
  + ' ,"patientCount" : ' + pt_count + ' '
  + ' ,"percentage" : "' + pt_percentage + '" '  
  + ' ,"avgDuration" : ' + avg_duration + ' '
  + ',"concepts" : ' + combo_concepts 
  + CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0 
     THEN '}' + rpad( ' ', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)), ']}' )
     ELSE NULL 
  END as JSON_SNIPPET
from 
(
   select 
     smry.rnum                               as rnum
    ,smry.tx_stg_cmb                           as combo_id
    ,smry.tx_stg_cmb_pth                       as current_path
    ,smry.tx_seq                               as path_seq
    ,smry.tx_stg_avg_dr                        as avg_duration
    ,smry.tx_stg_cnt                           as pt_count
    ,smry.tx_stg_percentage                    as pt_percentage
    ,concepts.conceptsName                as concept_names
    ,concepts.conceptsArray               as combo_concepts
    ,smry.lvl                                as Lvl
  FROM 
    (WITH t1( pnc_stdy_smry_id, tx_path_parent_key, lvl, tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, tx_stg_percentage) AS (
        SELECT 
           pnc_stdy_smry_id, tx_path_parent_key,
           1 AS lvl,
           tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, tx_stg_percentage
          FROM   @results_schema.pnc_study_summary_path
        WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id from @results_schema.pnc_study_summary_path
              where 
              study_id = @studyId
              and source_id = @sourceId
              and tx_rslt_version = 1
              and tx_path_parent_key is null)
        UNION ALL
        SELECT 
              t2.pnc_stdy_smry_id, t2.tx_path_parent_key,
              lvl+1,
              t2.tx_stg_cmb, t2.tx_stg_cmb_pth, t2.tx_seq, t2.tx_stg_avg_dr, t2.tx_stg_cnt, t2.tx_stg_percentage
        FROM   @results_schema.pnc_study_summary_path t2, t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
      )
      SEARCH DEPTH FIRST BY pnc_stdy_smry_id SET order1
      SELECT rownum as rnum, pnc_stdy_smry_id, tx_path_parent_key, lvl, tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, tx_stg_percentage
      FROM   t1
    order by order1) smry
  join #_pnc_smry_msql_cmb concepts 
  on concepts.comb_id = smry.tx_stg_cmb
) connect_by_query
order by rnum) allRoots
union all
select rnum as rnum, table_row_id as table_row_id, 1 rslt_vesion, ']}' as JSON from (
	select distinct 1/0F as rnum, 1 as table_row_id from @results_schema.pnc_study_summary_path)
) individualJsonRows;

-------------------------------------version 1 into summary table-------------------------------------
insert into @results_schema.pnc_study_summary (study_id, source_id, study_results)
select @studyId, @sourceId, JSON from (
	select distinct t1.table_row_id,
  	STUFF((SELECT distinct '' + t2.JSON
    	     from #_pnc_smry_msql_indvdl_json t2
        	 where t1.table_row_id = t2.table_row_id
            	FOR XML PATH(''), TYPE
	            ).value('.', 'NVARCHAR(MAX)') 
    	    ,1,0,'') json
	from #_pnc_smry_msql_indvdl_json t1
	where t1.rslt_version = 1
	order by t1.rnum
) mergeJsonRowsTable;

----------------------------------version 2 into temp table ------------------------------------------
insert into #_pnc_smry_msql_indvdl_json(rnum, table_row_id, rslt_vesion, JSON)
select rnum, table_row_id, rslt_vesion, JSON 
from
(
select allRoots.rnum rnum, 1 table_row_id, 2 rslt_vesion, 
CASE 
    WHEN rnum = 1 THEN '{"comboId": "root","children": [' + substr(JSON_SNIPPET, 2, length(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
from 
(
  select 
  rnum rnum,
  CASE 
    WHEN Lvl = 1 THEN ',{'
    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{' 
    ELSE ',{' 
  END
  + ' "comboId" : ' + combo_id + ' '
  + ' ,"conceptName" : "' + concept_names + '" '  
  + ' ,"patientCount" : ' + pt_count + ' '
  + ' ,"percentage" : "' + pt_percentage + '" '  
  + ' ,"avgDuration" : ' + avg_duration + ' '
  + ' ,"avgGapDay" : ' + avg_gap + ' '
  + ' ,"gapPercent" : "' + gap_pcnt + '" '  
  + ',"concepts" : ' + combo_concepts 
  + CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0 
     THEN '}' + rpad( ' ', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)), ']}' )
     ELSE NULL 
  END as JSON_SNIPPET
from 
(
  SELECT 
     smry.ROWNUM                               as rnum
    ,smry.tx_stg_cmb                           as combo_id
    ,smry.tx_stg_cmb_pth                       as current_path
    ,smry.tx_seq                               as path_seq
    ,smry.tx_stg_avg_dr                        as avg_duration
    ,smry.tx_stg_avg_gap                       as avg_gap
    ,NVL(ROUND(smry.tx_stg_avg_gap/smry.tx_stg_avg_dr * 100,2),0)   as gap_pcnt
    ,smry.tx_stg_cnt                           as pt_count
    ,smry.tx_stg_percentage                    as pt_percentage
    ,concepts.conceptsName                as concept_names
    ,concepts.conceptsArray               as combo_concepts
    ,smry.lvl                                as Lvl
  FROM 
    (WITH t1( pnc_stdy_smry_id, tx_path_parent_key, lvl, tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_gap) AS (
        SELECT 
           pnc_stdy_smry_id, tx_path_parent_key,
           1 AS lvl,
           tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_gap
          FROM   @results_schema.pnc_study_summary_path
        WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id from @results_schema.pnc_study_summary_path
              where 
              study_id = @studyId
              and source_id = @sourceId
              and tx_rslt_version = 2
              and tx_path_parent_key is null)
        UNION ALL
        SELECT 
              t2.pnc_stdy_smry_id, t2.tx_path_parent_key,
              lvl+1,
              t2.tx_stg_cmb, t2.tx_stg_cmb_pth, t2.tx_seq, t2.tx_stg_avg_dr, t2.tx_stg_cnt, t2.tx_stg_percentage, t2.tx_stg_avg_gap
        FROM   @results_schema.pnc_study_summary_path t2, t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
      )
      SEARCH DEPTH FIRST BY pnc_stdy_smry_id SET order1
      SELECT rownum as rnum, pnc_stdy_smry_id, tx_path_parent_key, lvl, tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_gap
      FROM   t1
    order by order1) smry
  join #_pnc_smry_msql_cmb concepts 
  on concepts.comb_id = smry.tx_stg_cmb
) connect_by_query
order by rnum) allRoots
union all
select rnum as rnum, table_row_id as table_row_id, 2 rslt_vesion, ']}' as JSON from (
	select distinct 1/0F as rnum, 1 as table_row_id from @results_schema.pnc_study_summary_path)
) individualJsonRows;


-------------------------------------version 2 into summary table-------------------------------------
update @results_schema.pnc_study_summary set study_results_2 = ( select JSON from (
select @studyId, @sourceId, JSON from (
	select distinct t1.table_row_id,
  	STUFF((SELECT distinct '' + t2.JSON
    	     from #_pnc_smry_msql_indvdl_json t2
        	 where t1.table_row_id = t2.table_row_id
            	FOR XML PATH(''), TYPE
	            ).value('.', 'NVARCHAR(MAX)') 
    	    ,1,0,'') json
	from #_pnc_smry_msql_indvdl_json t1
	where t1.rslt_version = 2
	order by t1.rnum
) mergeJsonRowsTable) mergedJson),
last_update_time = CURRENT_TIMESTAMP 
where study_id = @studyId and source_id = @sourceId;
