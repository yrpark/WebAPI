/**
 * The contents of this file are subject to the Regenstrief Public License
 * Version 1.0 (the "License"); you may not use this file except in compliance with the License.
 * Please contact Regenstrief Institute if you would like to obtain a copy of the license.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) Regenstrief Institute.  All Rights Reserved.
 */
package org.ohdsi.webapi.panacea.repository.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 *
 */
public class PanaceaPatientDrugComboTasklet implements Tasklet {
    
    private static final Log log = LogFactory.getLog(PanaceaPatientDrugComboTasklet.class);
    
    private static final int String = 0;
    
    private static final int List = 0;
    
    private JdbcTemplate jdbcTemplate;
    
    private TransactionTemplate transactionTemplate;
    
    private final Comparator patientStageCombinationCountDateComparator = new Comparator<PatientStageCombinationCount>() {
        
        @Override
        public int compare(final PatientStageCombinationCount pscc1, final PatientStageCombinationCount pscc2) {
            if ((pscc1 != null) && (pscc2 != null) && (pscc1.getStartDate() != null) && (pscc2.getStartDate() != null)) {
                return pscc1.getStartDate().before(pscc2.getStartDate()) ? -1 : 1;
            }
            return 0;
        }
    };
    
    /**
     * @param jdbcTemplate
     * @param transactionTemplate
     * @param pncService
     * @param pncStudy
     */
    public PanaceaPatientDrugComboTasklet(final JdbcTemplate jdbcTemplate, final TransactionTemplate transactionTemplate) {
        super();
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }
    
    /**
     * @see org.springframework.batch.core.step.tasklet.Tasklet#execute(org.springframework.batch.core.StepContribution,
     *      org.springframework.batch.core.scope.context.ChunkContext)
     */
    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
        try {
            final Map<String, Object> jobParams = chunkContext.getStepContext().getJobParameters();
            
            final String sql = this.getSql(jobParams, chunkContext);
            
            log.debug("PanaceaPatientDrugComboTasklet.execute, begin... ");
            
            final List<PatientStageCount> patientStageCountList = this.jdbcTemplate.query(sql,
                new RowMapper<PatientStageCount>() {
                    
                    @Override
                    public PatientStageCount mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                        final PatientStageCount patientStageCount = new PatientStageCount();
                        
                        patientStageCount.setPersonId(rs.getLong("person_id"));
                        patientStageCount.setCmbId(rs.getLong("cmb_id"));
                        patientStageCount.setStartDate(rs.getDate("start_date"));
                        patientStageCount.setEndDate(rs.getDate("end_date"));
                        
                        return patientStageCount;
                    }
                });
            
            log.debug("PanaceaPatientDrugComboTasklet.execute, returned size -- " + patientStageCountList.size());
            
            final List<PatientStageCombinationCount> calculatedOverlappingPSCCList = mergeComboOverlapWindow(patientStageCountList);
            
            if (calculatedOverlappingPSCCList != null) {
                calculatedOverlappingPSCCList.toString();
            }
            
            return RepeatStatus.FINISHED;
        } catch (final Exception e) {
            e.printStackTrace();
            
            //TODO -- consider this bad? and terminate the job?
            //return RepeatStatus.CONTINUABLE;
            return RepeatStatus.FINISHED;
        } finally {
            //TODO
            final DefaultTransactionDefinition completeTx = new DefaultTransactionDefinition();
            completeTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            final TransactionStatus completeStatus = this.transactionTemplate.getTransactionManager().getTransaction(
                completeTx);
            this.transactionTemplate.getTransactionManager().commit(completeStatus);
        }
        
    }
    
    /**
     * @return the jdbcTemplate
     */
    public JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }
    
    /**
     * @param jdbcTemplate the jdbcTemplate to set
     */
    public void setJdbcTemplate(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    /**
     * @return the transactionTemplate
     */
    public TransactionTemplate getTransactionTemplate() {
        return this.transactionTemplate;
    }
    
    /**
     * @param transactionTemplate the transactionTemplate to set
     */
    public void setTransactionTemplate(final TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }
    
    private String getSql(final Map<String, Object> jobParams, final ChunkContext chunkContext) {
        //String sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/getPersonIds.sql");
        String sql = "select ptstg.person_id as person_id, ptstg.tx_stg_cmb_id cmb_id, ptstg.stg_start_date start_date, ptstg.stg_end_date end_date "
                + "from HN31JHWQ_PNC_PTSTG_CT  ptstg "
                + "where "
                + "person_id in (@allDistinctPersonId) "
                + "order by person_id, stg_start_date, stg_end_date";
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        final List<String> allDistinctPersonId = (List<String>) chunkContext.getStepContext().getJobExecutionContext()
                .get("allDistinctPersonId");
        String allDistinctPersonIdStr = "";
        if (allDistinctPersonId != null) {
            boolean firstId = true;
            for (final String ids : allDistinctPersonId) {
                allDistinctPersonIdStr = firstId ? allDistinctPersonIdStr.concat(ids) : allDistinctPersonIdStr.concat(","
                        + ids);
                firstId = false;
            }
        }
        
        final String[] params = new String[] { "cdm_schema", "ohdsi_schema", "cohortDefId", "drugConceptId", "sourceId",
                "allDistinctPersonId" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, cohortDefId, drugConceptId,
                sourceId, allDistinctPersonIdStr };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        return sql;
    }
    
    private List<PatientStageCombinationCount> mergeComboOverlapWindow(final List<PatientStageCount> patientStageCountList) {
        if ((patientStageCountList != null) && (patientStageCountList.size() > 0)) {
            final Map<Long, List<PatientStageCombinationCount>> mergedComboPatientMap = new HashMap<Long, List<PatientStageCombinationCount>>();
            
            Long currentPersonId = patientStageCountList.get(0).getPersonId();
            final List<PatientStageCombinationCount> mergedList = new ArrayList<PatientStageCombinationCount>();
            final List<PatientStageCombinationCount> truncatedList = new ArrayList<PatientStageCombinationCount>();
            for (final PatientStageCount psc : patientStageCountList) {
                if (psc.getPersonId().equals(currentPersonId)) {
                    //from same patient
                    while ((truncatedList.size() > 0) && truncatedList.get(0).getStartDate().before(psc.getStartDate())) {
                        popAndMergeList(mergedList, truncatedList, null);
                    }
                    
                    final PatientStageCombinationCount newPSCC = new PatientStageCombinationCount();
                    newPSCC.setPersonId(psc.getPersonId());
                    newPSCC.setComboIds(psc.getCmbId().toString());
                    newPSCC.setStartDate(psc.getStartDate());
                    newPSCC.setEndDate(psc.getEndDate());
                    
                    popAndMergeList(mergedList, truncatedList, newPSCC);
                    
                    if (patientStageCountList.indexOf(psc) == (patientStageCountList.size() - 1)) {
                        //last object in the original list
                        
                        while (truncatedList.size() > 0) {
                            popAndMergeList(mergedList, truncatedList, null);
                        }
                        
                        final List<PatientStageCombinationCount> currentPersonIdMergedList = new ArrayList<PatientStageCombinationCount>();
                        currentPersonIdMergedList.addAll(mergedList);
                        mergedComboPatientMap.put(currentPersonId, currentPersonIdMergedList);
                        
                        mergedList.clear();
                        truncatedList.clear();
                    }
                } else {
                    //read to roll to next patient after popping all from truncatedList 
                    while (truncatedList.size() > 0) {
                        popAndMergeList(mergedList, truncatedList, null);
                    }
                    
                    final List<PatientStageCombinationCount> currentPersonIdMergedList = new ArrayList<PatientStageCombinationCount>();
                    currentPersonIdMergedList.addAll(mergedList);
                    mergedComboPatientMap.put(currentPersonId, currentPersonIdMergedList);
                    
                    mergedList.clear();
                    truncatedList.clear();
                    
                    //first object for next patient
                    currentPersonId = psc.getPersonId();
                    
                    final PatientStageCombinationCount newPSCC = new PatientStageCombinationCount();
                    newPSCC.setPersonId(psc.getPersonId());
                    newPSCC.setComboIds(psc.getCmbId().toString());
                    newPSCC.setStartDate(psc.getStartDate());
                    newPSCC.setEndDate(psc.getEndDate());
                    
                    popAndMergeList(mergedList, truncatedList, newPSCC);
                }
            }
            
            final List<PatientStageCombinationCount> returnPSCCList = new ArrayList(mergedComboPatientMap.values());
            
            return returnPSCCList;
            
        } else {
            //TODO - error logging
            return null;
        }
    }
    
    private void popAndMergeList(final List<PatientStageCombinationCount> mergedList,
                                 final List<PatientStageCombinationCount> truncatedList,
                                 final PatientStageCombinationCount newConstructedPSCC) {
        if ((mergedList != null) && (truncatedList != null)) {
            PatientStageCombinationCount poppingPSCC = null;
            boolean newPSCCFromOriginalList = false;
            if (newConstructedPSCC == null) {
                poppingPSCC = truncatedList.get(0);
            } else {
                poppingPSCC = newConstructedPSCC;
                newPSCCFromOriginalList = true;
            }
            
            if (mergedList.size() > 0) {
                //mergedList has elements
                final PatientStageCombinationCount lastMergedPSCC = mergedList.get(mergedList.size() - 1);
                
                if (lastMergedPSCC.getStartDate().after(poppingPSCC.getStartDate())) {
                    
                    log.error("Error in popAndMergeList -- starting date wrong in popAndMergeList");
                }
                
                if (poppingPSCC.getStartDate().before(lastMergedPSCC.getEndDate())) {
                    //overlapping
                    
                    if (poppingPSCC.getEndDate().before(lastMergedPSCC.getEndDate())) {
                        //poping time window is "within" last merged object
                        final PatientStageCombinationCount newPSCC = new PatientStageCombinationCount();
                        newPSCC.setPersonId(poppingPSCC.getPersonId());
                        newPSCC.setComboIds(lastMergedPSCC.getComboIds());
                        newPSCC.setStartDate(poppingPSCC.getEndDate());
                        newPSCC.setEndDate(lastMergedPSCC.getEndDate());
                        
                        poppingPSCC.setComboIds(mergeComboIds(lastMergedPSCC, poppingPSCC));
                        
                        lastMergedPSCC.setEndDate(poppingPSCC.getStartDate());
                        
                        //TODO - verify this more!!!
                        if (lastMergedPSCC.getStartDate().equals(lastMergedPSCC.getEndDate())) {
                            mergedList.remove(mergedList.size() - 1);
                        }
                        
                        mergedList.add(poppingPSCC);
                        
                        if (!newPSCCFromOriginalList) {
                            truncatedList.remove(0);
                        }
                        
                        truncatedList.add(newPSCC);
                        
                        Collections.sort(truncatedList, this.patientStageCombinationCountDateComparator);
                        
                    } else if (poppingPSCC.getEndDate().after(lastMergedPSCC.getEndDate())) {
                        //poping object end date is after last merged object
                        final PatientStageCombinationCount newPSCC = new PatientStageCombinationCount();
                        newPSCC.setPersonId(poppingPSCC.getPersonId());
                        newPSCC.setComboIds(poppingPSCC.getComboIds());
                        newPSCC.setStartDate(lastMergedPSCC.getEndDate());
                        newPSCC.setEndDate(poppingPSCC.getEndDate());
                        
                        poppingPSCC.setComboIds(mergeComboIds(lastMergedPSCC, poppingPSCC));
                        poppingPSCC.setEndDate(lastMergedPSCC.getEndDate());
                        
                        lastMergedPSCC.setEndDate(poppingPSCC.getStartDate());
                        
                        //TODO - verify this more!!!
                        if (lastMergedPSCC.getStartDate().equals(lastMergedPSCC.getEndDate())) {
                            mergedList.remove(mergedList.size() - 1);
                        }
                        
                        mergedList.add(poppingPSCC);
                        
                        if (!newPSCCFromOriginalList) {
                            truncatedList.remove(0);
                        }
                        
                        truncatedList.add(newPSCC);
                        
                        Collections.sort(truncatedList, this.patientStageCombinationCountDateComparator);
                    } else if (poppingPSCC.getEndDate().equals(lastMergedPSCC.getEndDate())) {
                        //poping object end date is the same as last merged object
                        poppingPSCC.setComboIds(mergeComboIds(lastMergedPSCC, poppingPSCC));
                        
                        lastMergedPSCC.setEndDate(poppingPSCC.getStartDate());
                        
                        mergedList.add(poppingPSCC);
                        
                        if (!newPSCCFromOriginalList) {
                            truncatedList.remove(0);
                        }
                    }
                } else {
                    //no overlapping, just pop
                    mergedList.add(poppingPSCC);
                    
                    if (!newPSCCFromOriginalList) {
                        truncatedList.remove(0);
                    }
                }
            } else {
                //mergedList has no elements, just add the first one
                mergedList.add(poppingPSCC);
                
                //TODO -- check if still needed
                if (!newPSCCFromOriginalList) {
                    truncatedList.remove(0);
                }
            }
        } else {
            //TODO -- error logging
        }
    }
    
    private String mergeComboIds(final PatientStageCombinationCount pscc1, final PatientStageCombinationCount pscc2) {
        
        if ((pscc1 != null) && (pscc2 != null) && (pscc1.getComboIds() != null) && (pscc2.getComboIds() != null)) {
            final String[] pscc1ComboStringArray = pscc1.getComboIds().split("\\|");
            final List<String> psccCombos = new ArrayList<String>();
            psccCombos.addAll(Arrays.asList(pscc1ComboStringArray));
            
            final String[] pscc2ComboStringArray = pscc2.getComboIds().split("\\|");
            psccCombos.addAll(Arrays.asList(pscc2ComboStringArray));
            
            final Set<String> comboSet = new HashSet<String>(psccCombos);
            
            psccCombos.clear();
            psccCombos.addAll(comboSet);
            
            Collections.sort(psccCombos);
            
            return StringUtils.join(psccCombos, "|");
        }
        
        return null;
    }
}
