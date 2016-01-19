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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            
            mergeComboOverlapWindow(patientStageCountList);
            
            return RepeatStatus.FINISHED;
        } catch (final Exception e) {
            e.printStackTrace();
            
            return RepeatStatus.CONTINUABLE;
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
    
    private void mergeComboOverlapWindow(final List<PatientStageCount> patientStageCountList) {
        if (patientStageCountList != null) {
            final Map<Long, List<PatientStageCombinationCount>> mergedComboPatientMap = new HashMap<Long, List<PatientStageCombinationCount>>();
            final Map<Long, List<PatientStageCombinationCount>> truncatedComboPatientMap = new HashMap<Long, List<PatientStageCombinationCount>>();
            
            for (final PatientStageCount psc : patientStageCountList) {
                if ((psc != null) && (psc.getPersonId() != null) && (psc.getCmbId() != null) && (psc.getStartDate() != null)
                        && (psc.getEndDate() != null)) {
                    if (mergedComboPatientMap.containsKey(psc.getPersonId())) {
                        popCurrentPatientStageCount(mergedComboPatientMap.get(psc.getPersonId()),
                            truncatedComboPatientMap.get(psc.getPersonId()), psc);
                    } else {
                        final PatientStageCombinationCount pscc = new PatientStageCombinationCount();
                        pscc.setPersonId(psc.getPersonId());
                        pscc.setComboIds(psc.getCmbId().toString());
                        pscc.setStartDate(psc.getStartDate());
                        pscc.setEndDate(psc.getEndDate());
                        
                        final List<PatientStageCombinationCount> psccList = new ArrayList<PatientStageCombinationCount>();
                        psccList.add(pscc);
                        
                        mergedComboPatientMap.put(psc.getPersonId(), psccList);
                    }
                }
            }
        } else {
            //TODO - error logging
        }
    }
    
    private void popCurrentPatientStageCount(final List<PatientStageCombinationCount> mergedComboPatientCountList,
                                             final List<PatientStageCombinationCount> truncatedComboPatientCountList,
                                             final PatientStageCount psc) {
        
    }
}
