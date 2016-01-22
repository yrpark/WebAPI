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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlSplit;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.helper.ResourceHelper;
import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 *
 */
public class PanaceaSummaryGenerateTasklet implements Tasklet {
    
    private static final Log log = LogFactory.getLog(PanaceaSummaryGenerateTasklet.class);
    
    private JdbcTemplate jdbcTemplate;
    
    private TransactionTemplate transactionTemplate;
    
    private final PanaceaStudy pncStudy;
    
    /**
     * @param jdbcTemplate
     * @param transactionTemplate
     * @param pncService
     * @param pncStudy
     */
    public PanaceaSummaryGenerateTasklet(final JdbcTemplate jdbcTemplate, final TransactionTemplate transactionTemplate,
        final PanaceaStudy pncStudy) {
        super();
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.pncStudy = pncStudy;
    }
    
    /**
     * @see org.springframework.batch.core.step.tasklet.Tasklet#execute(org.springframework.batch.core.StepContribution,
     *      org.springframework.batch.core.scope.context.ChunkContext)
     */
    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
        try {
            final Map<String, Object> jobParams = chunkContext.getStepContext().getJobParameters();
            
            final String sql = this.getSql(jobParams);
            
            final int[] ret = this.transactionTemplate.execute(new TransactionCallback<int[]>() {
                
                @Override
                public int[] doInTransaction(final TransactionStatus status) {
                    
                    final String[] stmts = SqlSplit.splitSql(sql);
                    
                    return PanaceaSummaryGenerateTasklet.this.jdbcTemplate.batchUpdate(stmts);
                }
            });
            log.debug("PanaceaSummaryGenerateTasklet execute returned size: " + ret.length);
            
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
    
    private String getSql(final Map<String, Object> jobParams) {
        String sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/generateSummary.sql");
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        
        final String[] params = new String[] { "cdm_schema", "ohdsi_schema", "results_schema", "cohortDefId", "studyId",
                "drugConceptId", "sourceId" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier, cohortDefId,
                this.pncStudy.getStudyId().toString(), drugConceptId, sourceId };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        return sql;
    }
}
