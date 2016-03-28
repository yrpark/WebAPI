-------------------------filtering based on filter out conditions -----------------------
IF OBJECT_ID('tempdb..#_pnc_smrypth_fltr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smrypth_fltr;

CREATE TABLE #_pnc_smrypth_fltr
(
    pnc_stdy_smry_id int,
    study_id    int,
    source_id int,
    tx_path_parent_key  int,
    tx_stg_cmb    VARCHAR(255),
    tx_stg_cmb_pth VARCHAR(4000),
    tx_seq          int,
    tx_stg_cnt      int,
    tx_stg_percentage float,
    tx_stg_avg_dr   int,
    tx_stg_avg_gap   int,
    tx_rslt_version int
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
--    where tx_stg_avg_gap < 150
@constraintSql
);


--table to hold null parent ids (which have been deleted from #_pnc_smrypth_fltr as not qualified rows) and all their ancestor_id with levels
IF OBJECT_ID('tempdb..#_pnc_smry_ancstr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smry_ancstr;

CREATE TABLE #_pnc_smry_ancstr
(
    pnc_stdy_parent_id    int,
    pnc_ancestor_id    int,
    reallevel int
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
delete from #_pnc_smry_msql_indvdl_json;

insert into #_pnc_smry_msql_indvdl_json(rnum, table_row_id, JSON)
select rnum, table_row_id, JSON 
from
(
select allRoots.rnum rnum, 1 table_row_id, 
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
    (WITH RECURSIVE t1( pnc_stdy_smry_id, tx_path_parent_key, lvl, tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_gap) AS (
        SELECT 
           pnc_stdy_smry_id, tx_path_parent_key,
           1 AS lvl,
           tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_gap
          FROM #_pnc_smrypth_fltr
        WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id from #_pnc_smrypth_fltr
              where 
		      	tx_path_parent_key is null)
        UNION ALL
        SELECT 
              t2.pnc_stdy_smry_id, t2.tx_path_parent_key,
              lvl+1,
              t2.tx_stg_cmb, t2.tx_stg_cmb_pth, t2.tx_seq, t2.tx_stg_avg_dr, t2.tx_stg_cnt, t2.tx_stg_percentage, t2.tx_stg_avg_gap
        FROM   #_pnc_smrypth_fltr t2, t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
      )
--      SEARCH DEPTH FIRST BY pnc_stdy_smry_id SET order1
      SELECT rownum as rnum, pnc_stdy_smry_id, tx_path_parent_key, lvl, tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_gap
      FROM   t1
      order by tx_stg_cmb_pth) smry
--    order by order1) smry
  join #_pnc_smry_msql_cmb concepts 
  on concepts.comb_id = smry.tx_stg_cmb
) connect_by_query
order by rnum) allRoots
union all
select 'Infinity'::float as rnum, 1 as table_row_id, ']}' as JSON
) individualJsonRows;



update @results_schema.pnc_study_summary set study_results_filtered = (select JSON from (
select @studyId, @sourceId, JSON from (
	select individualResult.table_row_id,
		array_to_string(array_agg(individualResult.JSON), '')  JSON
	from #_pnc_smry_msql_indvdl_json individualResult
	group by individualResult.table_row_id
	order by individualResult.rnum
) mergeJsonRowsTable) mergedJson),
last_update_time = CURRENT_TIMESTAMP 
where study_id = @studyId and source_id = @sourceId;
  


IF OBJECT_ID('tempdb..#_pnc_smrypth_fltr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smrypth_fltr;
IF OBJECT_ID('tempdb..#_pnc_smry_ancstr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smry_ancstr;
IF OBJECT_ID('tempdb..#_pnc_ptsq_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_ptsq_ct;
IF OBJECT_ID('tempdb..#_pnc_ptstg_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_ptstg_ct;
IF OBJECT_ID('tempdb..#_pnc_tmp_cmb_sq_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_tmp_cmb_sq_ct;
