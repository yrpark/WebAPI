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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @param <T>
 */
//public class PanaceaPatientSequenceItemReader<T> implements ItemReader<T> {
//@Component("pncPatientSequenceItemReader")
//@Scope("step")
//@Scope(value = "step")
public class PanaceaPatientSequenceItemReader<PanaceaPatientSequenceCount> extends JdbcCursorItemReader<PanaceaPatientSequenceCount> {// implements StepExecutionListener {

    private static final Log log = LogFactory.getLog(PanaceaPatientSequenceItemReader.class);
    
    //    private JdbcTemplate jdbcTemplate;
    //    
    //    private TransactionTemplate transactionTemplate;
    //    
    //    private JobExecution jobExecution;
    //
    @Autowired
    private PanaceaService pncService;
    
    //    
    //    //private PanaceaStudy pncStudy;
    //
    
    //@Value("#{jobParameters['studyId']}")
    private Long studyId;
    
    /**
     * @param jdbcTemplate
     * @param transactionTemplate
     * @param pncService
     * @param pncStudy
     */
    //    public PanaceaPatientSequenceItemReader(final JdbcTemplate jdbcTemplate, final TransactionTemplate transactionTemplate,
    //        final PanaceaService pncService, final PanaceaStudy pncStudy) {
    //        super();
    //        this.jdbcTemplate = jdbcTemplate;
    //        this.transactionTemplate = transactionTemplate;
    //        this.pncService = pncService;
    //        //        this.pncStudy = pncStudy;
    //        
    //        setupReader();
    //    }
    
    public PanaceaPatientSequenceItemReader() {
        super();
        log.info("PanaceaPatientSequenceItemReader: in constructor");
    }
    
    /**
     * @return the studyId
     */
    public Long getStudyId() {
        return this.studyId;
    }
    
    public void setStudyId(final String studyId) {
        this.studyId = new Long(studyId);
    }
    
    public void setStudyId(final Long studyId) {
        this.studyId = studyId;
    }
    
    //    public void setupReader() {
    //        this.setDataSource(this.jdbcTemplate.getDataSource());
    //        this.setRowMapper(new PanaceaPatientSequenceCountMapper());
    //        
    //        // crucial if using mysql to ensure that results are streamed
    //        this.setFetchSize(Integer.MIN_VALUE);
    //        this.setVerifyCursorPosition(false);
    //        
    //        //final String sql = this.pncService.getPanaceaPatientSequenceCountSql(this.pncStudy.getStudyId());
    //        final String sql = this.pncService.getPanaceaPatientSequenceCountSql(this.studyId);
    //        
    //        this.setSql(sql);
    //        
    //        this.setFetchSize(50000);
    //        this.setSaveState(false);
    //        this.setVerifyCursorPosition(false);
    //        try {
    //            this.afterPropertiesSet();
    //        } catch (final Throwable t) {
    //            t.printStackTrace();
    //        }
    //        //        this.open(this.jobExecution.getExecutionContext());
    //        //        final ExecutionContext executionContext = new ExecutionContext();
    //        //        this.open(executionContext);
    //    }
    
    //    @Override
    //    public void afterPropertiesSet() throws Exception {
    //        setSql(sql);
    //        setRowMapper(rowMapper);
    //        setDataSource(dataSource);
    //        super.afterPropertiesSet();
    //    }
    
    /**
     * @return the pncService
     */
    public PanaceaService getPncService() {
        return this.pncService;
    }
    
    /**
     * @param pncService the pncService to set
     */
    public void setPncService(final PanaceaService pncService) {
        this.pncService = pncService;
    }
    
    //    @Override
    @BeforeStep
    public void beforeStep(final StepExecution stepexecution)
    
    {
        log.info("PanaceaPatientSequenceItemReader: in beforeStep");
        log.info("PanaceaPatientSequenceItemReader: before setting sql = " + this.getSql());
        
        final String studyIdFromJobParam = stepexecution.getJobParameters().getString("studyId");
        
        this.studyId = new Long(studyIdFromJobParam);
        
        //TODO -- testing sql
        this.setSql("select study_id from panacea_study");
        
//        final String realSql = this.pncService.getPanaceaPatientSequenceCountSql(this.studyId);
        
        log.info("PanaceaPatientSequenceItemReader: Getting studyId =" + this.studyId);
        log.info("PanaceaPatientSequenceItemReader: sql = " + this.getSql());
//        log.info("PanaceaPatientSequenceItemReader: real sql = " + realSql);
    }
    
    /* (non-Jsdoc)
     * @see org.springframework.batch.core.StepExecutionListener#afterStep(org.springframework.batch.core.StepExecution)
     */
    //    @Override
    public ExitStatus afterStep(final StepExecution stepExecution) {
        // TODO Auto-generated method stub
        return stepExecution.getExitStatus();
    }
}
