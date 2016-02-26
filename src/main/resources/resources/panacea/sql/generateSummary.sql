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

-------------------------------------version 1 -------------------------------------
insert into @results_schema.pnc_study_summary (study_id, source_id, study_results)
select @studyId, @sourceId, JSON from (
select JSON from (
SELECT
   table_row_id,
   DBMS_XMLGEN.CONVERT (
     EXTRACT(
       xmltype('<?xml version="1.0"?><document>' ||
               XMLAGG(
                 XMLTYPE('<V>' || DBMS_XMLGEN.CONVERT(JSON)|| '</V>')
                 order by rnum).getclobval() || '</document>'),
               '/document/V/text()').getclobval(),1) AS JSON
FROM (select allRoots.rnum rnum, 1 table_row_id,
CASE 
    WHEN rnum = 1 THEN '{"combo_id": "root","children": [' || substr(JSON_SNIPPET, 2, length(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
from 
(WITH connect_by_query as (
  SELECT 
     ROWNUM                               as rnum
    ,tx_stg_cmb                           as combo_id
    ,tx_stg_cmb_pth                       as current_path
    ,tx_seq                               as path_seq
    ,tx_stg_avg_dr                        as avg_duration
    ,tx_stg_cnt                           as pt_count
    ,tx_stg_percentage                    as pt_percentage
    ,concepts.conceptsName                as concept_names
    ,concepts.conceptsArray               as combo_concepts
    ,LEVEL                                as Lvl
  FROM @results_schema.pnc_study_summary_path smry
  join
  (select comb.pnc_tx_stg_cmb_id comb_id,
    '[' || wm_concat('{"conceptName":' || '"' || combMap.concept_name  || '"' || 
    ',"conceptId":' || combMap.concept_id || '}') || ']' conceptsArray,
    wm_concat(combMap.concept_name) conceptsName
    from @results_schema.pnc_tx_stage_combination comb
    join @results_schema.pnc_tx_stage_combination_map combMap 
    on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
    where comb.study_id = @studyId
    group by comb.pnc_tx_stg_cmb_id
  ) concepts
  on concepts.comb_id = smry.tx_stg_cmb
  START WITH pnc_stdy_smry_id in (select pnc_stdy_smry_id from @results_schema.pnc_study_summary_path
        where 
        study_id = @studyId
        and source_id = @sourceId
        and tx_rslt_version = 1
        and tx_path_parent_key is null)
  CONNECT BY PRIOR pnc_stdy_smry_id = tx_path_parent_key
  ORDER SIBLINGS BY pnc_stdy_smry_id
)
select 
  rnum rnum,
  CASE 
    WHEN Lvl = 1 THEN ',{'
    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{' 
    ELSE ',{' 
  END 
  || ' "combo_id" : ' || combo_id || ' '
  || ' ,"concept_names" : "' || concept_names || '" '  
  || ' ,"patient_counts" : ' || pt_count || ' '
  || ' ,"percentage" : "' || pt_percentage || '" '  
  || ' ,"average_duration" : ' || avg_duration || ' '
  || ',"concepts" : ' || combo_concepts 
  || CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0 
     THEN '}' || rpad( ' ', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)), ']}' )
     ELSE NULL 
  END as JSON_SNIPPET
from connect_by_query
order by rnum) allRoots
union all
select rnum as rnum, table_row_id as table_row_id, to_clob(']}') as JSON from (
	select distinct 1/0F as rnum, 1 as table_row_id from @results_schema.pnc_study_summary_path)
--  select distinct 1000000 as rnum, 1 as table_row_id from pnc_study_summary_path)
--sql render remove "dual", so I have to trick by using a real table(pnc_study_summary_path) select 1000000  as rnum, 1 as table_row_id, to_clob(']}') as JSON from dual
)
GROUP BY
   table_row_id));

----------------------------------version 2
update @results_schema.pnc_study_summary set study_results_2 = (select JSON from (
select JSON from (
SELECT
   table_row_id,
   DBMS_XMLGEN.CONVERT (
     EXTRACT(
       xmltype('<?xml version="1.0"?><document>' ||
               XMLAGG(
                 XMLTYPE('<V>' || DBMS_XMLGEN.CONVERT(JSON)|| '</V>')
                 order by rnum).getclobval() || '</document>'),
               '/document/V/text()').getclobval(),1) AS JSON
FROM (select allRoots.rnum rnum, 1 table_row_id,
CASE 
    WHEN rnum = 1 THEN '{"combo_id": "root","children": [' || substr(JSON_SNIPPET, 2, length(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
from 
(WITH connect_by_query as (
  SELECT 
     ROWNUM                               as rnum
    ,tx_stg_cmb                           as combo_id
    ,tx_stg_cmb_pth                       as current_path
    ,tx_seq                               as path_seq
    ,tx_stg_avg_dr                        as avg_duration
    ,tx_stg_avg_gap                       as avg_gap
    ,NVL(ROUND(tx_stg_avg_gap/tx_stg_avg_dr * 100,2),0)   as gap_pcnt
    ,tx_stg_cnt                           as pt_count
    ,tx_stg_percentage                    as pt_percentage
    ,concepts.conceptsName                as concept_names
    ,concepts.conceptsArray               as combo_concepts
    ,LEVEL                                as Lvl
  FROM @results_schema.pnc_study_summary_path smry
  join
  (select comb.pnc_tx_stg_cmb_id comb_id,
    '[' || wm_concat('{"conceptName":' || '"' || combMap.concept_name  || '"' || 
    ',"conceptId":' || combMap.concept_id || '}') || ']' conceptsArray,
    wm_concat(combMap.concept_name) conceptsName
    from @results_schema.pnc_tx_stage_combination comb
    join @results_schema.pnc_tx_stage_combination_map combMap 
    on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
    where comb.study_id = @studyId
    group by comb.pnc_tx_stg_cmb_id
  ) concepts
  on concepts.comb_id = smry.tx_stg_cmb
  START WITH pnc_stdy_smry_id in (select pnc_stdy_smry_id from @results_schema.pnc_study_summary_path
        where 
        study_id = @studyId
        and source_id = @sourceId
        and tx_rslt_version = 2
        and tx_path_parent_key is null)
  CONNECT BY PRIOR pnc_stdy_smry_id = tx_path_parent_key
  ORDER SIBLINGS BY pnc_stdy_smry_id
)
select 
  rnum rnum,
  CASE 
    WHEN Lvl = 1 THEN ',{'
    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{' 
    ELSE ',{' 
  END 
  || ' "combo_id" : ' || combo_id || ' '
  || ' ,"concept_names" : "' || concept_names || '" '  
  || ' ,"patient_counts" : ' || pt_count || ' '
  || ' ,"percentage" : "' || pt_percentage || '" '  
  || ' ,"average_duration" : ' || avg_duration || ' '
  || ' ,"average_gap_days" : ' || avg_gap || ' '
  || ' ,"gap_percent" : "' || gap_pcnt || '" '  
  || ',"concepts" : ' || combo_concepts 
  || CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0 
     THEN '}' || rpad( ' ', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)), ']}' )
     ELSE NULL 
  END as JSON_SNIPPET
from connect_by_query
order by rnum) allRoots
union all
select rnum as rnum, table_row_id as table_row_id, to_clob(']}') as JSON from (
	select distinct 1/0F as rnum, 1 as table_row_id from @results_schema.pnc_study_summary_path)
)
GROUP BY
   table_row_id))) 
where study_id = @studyId and source_id = @sourceId;


-------------------------filtering based on filter out conditions -----------------------
IF OBJECT_ID('tempdb..#_pnc_smrypth_fltr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smrypth_fltr;

CREATE TABLE #_pnc_smrypth_fltr
(
    pnc_stdy_smry_id    NUMBER(*,0),
    study_id    NUMBER(*,0),
    source_id NUMBER(*,0),
    tx_path_parent_key  NUMBER(*,0),
    tx_stg_cmb    VARCHAR2(255 BYTE),
    tx_stg_cmb_pth VARCHAR2(4000 BYTE),
    tx_seq          NUMBER(*,0),
    tx_stg_cnt      NUMBER(*,0),
    tx_stg_percentage NUMBER(*,2),
    tx_stg_avg_dr   NUMBER(*,0),
    tx_stg_avg_gap   NUMBER(*,0),
    tx_rslt_version NUMBER(*, 0)
);

insert into #_pnc_smrypth_fltr 
select pnc_stdy_smry_id, study_id, source_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth,
    tx_seq,
    tx_stg_cnt,
    tx_stg_percentage,
    tx_stg_avg_dr,
    tx_stg_avg_gap,
    tx_rslt_version 
    from @results_schema.pnc_study_summary_path
        where 
        study_id = @studyId
        and source_id = @sourceId
        and tx_rslt_version = 2;

----------- delete rows that do not qualify the conditions for fitlering out-----------
delete from #_pnc_smrypth_fltr where pnc_stdy_smry_id not in (
    select pnc_stdy_smry_id from #_pnc_smrypth_fltr qualified
--TODO!!!!!! change this with real condition string
--    where tx_stg_avg_dr >= 50);
    where tx_stg_avg_gap < 150);


--table to hold null parent ids (which have been deleted from #_pnc_smrypth_fltr as not qualified rows) and all their ancestor_id with levels
IF OBJECT_ID('tempdb..#_pnc_smry_ancstr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smry_ancstr;

CREATE TABLE #_pnc_smry_ancstr
(
    pnc_stdy_parent_id    NUMBER(*,0),
    pnc_ancestor_id    NUMBER(*,0),
    reallevel number
);

insert into #_pnc_smry_ancstr
select nullParentKey, pnc_stdy_smry_id, realLevel
from (
  select validancestor.nullParentKey, validancestor.ancestorlevel, 
  case 
      when path.pnc_stdy_smry_id is not null then validancestor.ancestorlevel
      when path.pnc_stdy_smry_id is null then 1000000
    end as realLevel,
  validancestor.parentid, path.pnc_stdy_smry_id
  from #_pnc_smrypth_fltr path
  right join
  (select smry.tx_path_parent_key nullParentKey, nullParentAncestors.l ancestorLevel, nullParentAncestors.parent parentId from #_pnc_smrypth_fltr smry
    join
    (SELECT pnc_stdy_smry_id, ancestor AS parent, l
      FROM
      (
        SELECT pnc_stdy_smry_id, tx_path_parent_key, LEVEL-1 l, 
        connect_by_root pnc_stdy_smry_id ancestor
        FROM @results_schema.pnc_study_summary_path
        where 
        study_id = @studyId
        and source_id = @sourceId
        and tx_rslt_version = 2
      	CONNECT BY PRIOR pnc_stdy_smry_id = tx_path_parent_key
      ) t
      WHERE t.ancestor <> t.pnc_stdy_smry_id
      and t.pnc_stdy_smry_id in 
      (select tx_path_parent_key from #_pnc_smrypth_fltr where tx_path_parent_key not in (select PNC_STDY_SMRY_ID from #_pnc_smrypth_fltr))
    ) nullParentAncestors
    on smry.tx_path_parent_key = nullParentAncestors.pnc_stdy_smry_id) validAncestor
  on path.pnc_stdy_smry_id = validAncestor.parentId);

--update null parent key in #_pnc_smrypth_fltr with valid ancestor id which exists in #_pnc_smrypth_fltr or null (null is from level set to 1000000 from table of #_pnc_smry_ancstr)
merge into #_pnc_smrypth_fltr m
using
  (
    select path.pnc_stdy_smry_id, updateParent.pnc_ancestor_id from #_pnc_smrypth_fltr path,
    (select pnc_stdy_parent_id, pnc_ancestor_id
    	from (select pnc_stdy_parent_id, pnc_ancestor_id, reallevel, 
    	row_number() over (partition by pnc_stdy_parent_id order by reallevel) rn
    	from #_pnc_smry_ancstr)
    where rn = 1) updateParent
    where path.tx_path_parent_key = updateParent.pnc_stdy_parent_id
  ) m1
  on
  (
     m.pnc_stdy_smry_id = m1.pnc_stdy_smry_id
  )
  WHEN MATCHED then update set m.tx_path_parent_key = m1.pnc_ancestor_id;

------------------------version 2 of filtered JSON into summary table-----------------
update @results_schema.pnc_study_summary set study_results_filtered = (select JSON from (
select JSON from (
SELECT
   table_row_id,
   DBMS_XMLGEN.CONVERT (
     EXTRACT(
       xmltype('<?xml version="1.0"?><document>' ||
               XMLAGG(
                 XMLTYPE('<V>' || DBMS_XMLGEN.CONVERT(JSON)|| '</V>')
                 order by rnum).getclobval() || '</document>'),
               '/document/V/text()').getclobval(),1) AS JSON
FROM (select allRoots.rnum rnum, 1 table_row_id,
CASE 
    WHEN rnum = 1 THEN '{"combo_id": "root","children": [' || substr(JSON_SNIPPET, 2, length(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
from 
(WITH connect_by_query as (
  SELECT 
     ROWNUM                               as rnum
    ,tx_stg_cmb                           as combo_id
    ,tx_stg_cmb_pth                       as current_path
    ,tx_seq                               as path_seq
    ,tx_stg_avg_dr                        as avg_duration
    ,tx_stg_avg_gap                       as avg_gap
    ,NVL(ROUND(tx_stg_avg_gap/tx_stg_avg_dr * 100,2),0)   as gap_pcnt
    ,tx_stg_cnt                           as pt_count
    ,tx_stg_percentage                    as pt_percentage
    ,concepts.conceptsName                as concept_names
    ,concepts.conceptsArray               as combo_concepts
    ,LEVEL                                as Lvl
  FROM #_pnc_smrypth_fltr smry
  join
  (select comb.pnc_tx_stg_cmb_id comb_id,
    '[' || wm_concat('{"conceptName":' || '"' || combMap.concept_name  || '"' || 
    ',"conceptId":' || combMap.concept_id || '}') || ']' conceptsArray,
    wm_concat(combMap.concept_name) conceptsName
    from @results_schema.pnc_tx_stage_combination comb
    join @results_schema.pnc_tx_stage_combination_map combMap 
    on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
    where comb.study_id = @studyId
    group by comb.pnc_tx_stg_cmb_id
  ) concepts
  on concepts.comb_id = smry.tx_stg_cmb
  START WITH pnc_stdy_smry_id in (select pnc_stdy_smry_id from #_pnc_smrypth_fltr
        where 
--        study_id = 19
--        and source_id = 2
--        and tx_rslt_version = 2
        tx_path_parent_key is null)
  CONNECT BY PRIOR pnc_stdy_smry_id = tx_path_parent_key
  ORDER SIBLINGS BY pnc_stdy_smry_id
)
select 
  rnum rnum,
  CASE 
    WHEN Lvl = 1 THEN ',{'
    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{' 
    ELSE ',{' 
  END 
  || ' "combo_id" : ' || combo_id || ' '
  || ' ,"concept_names" : "' || concept_names || '" '  
  || ' ,"patient_counts" : ' || pt_count || ' '
  || ' ,"percentage" : "' || pt_percentage || '" '  
  || ' ,"average_duration" : ' || avg_duration || ' '
  || ' ,"average_gap_days" : ' || avg_gap || ' '
  || ' ,"gap_percent" : "' || gap_pcnt || '" '
  || ',"concepts" : ' || combo_concepts 
  || CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0 
     THEN '}' || rpad( ' ', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)), ']}' )
     ELSE NULL 
  END as JSON_SNIPPET
from connect_by_query
order by rnum) allRoots
union all
select rnum as rnum, table_row_id as table_row_id, to_clob(']}') as JSON from (
	select distinct 1/0F as rnum, 1 as table_row_id from #_pnc_smrypth_fltr)
)
GROUP BY
   table_row_id)))
where study_id = @studyId and source_id = @sourceId;