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

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.EntityManager;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.TerminateJobStepExceptionHandler;
import org.ohdsi.webapi.helper.ResourceHelper;
import org.ohdsi.webapi.job.JobExecutionResource;
import org.ohdsi.webapi.job.JobTemplate;
import org.ohdsi.webapi.panacea.mapper.PanaceaStageCombinationMapper;
import org.ohdsi.webapi.panacea.pojo.PanaceaPatientSequenceCount;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombination;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombinationMap;
import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.ohdsi.webapi.panacea.repository.PanaceaPatientSequenceCountRepository;
import org.ohdsi.webapi.panacea.repository.PanaceaStageCombinationMapRepository;
import org.ohdsi.webapi.panacea.repository.PanaceaStageCombinationRepository;
import org.ohdsi.webapi.panacea.repository.PanaceaStudyRepository;
import org.ohdsi.webapi.service.AbstractDaoService;
import org.ohdsi.webapi.service.VocabularyService;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.ohdsi.webapi.vocabulary.Concept;
import org.ohdsi.webapi.vocabulary.ConceptSetExpression;
import org.ohdsi.webapi.vocabulary.ConceptSetExpression.ConceptSetItem;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 */
@Path("/panacea")
@Component
public class PanaceaService extends AbstractDaoService {
    
    private static final Log log = LogFactory.getLog(PanaceaService.class);
    
    @Autowired
    private PanaceaStudyRepository panaceaStudyRepository;
    
    @Autowired
    private PanaceaStageCombinationRepository pncStageCombinationRepository;
    
    @Autowired
    private PanaceaStageCombinationMapRepository pncStageCombinationMapRepository;
    
    @Autowired
    private PanaceaPatientSequenceCountRepository pncPatientSequenceCountRepository;
    
    @Autowired
    private EntityManager em;
    
    @Autowired
    private JobTemplate jobTemplate;
    
    @Autowired
    private JobBuilderFactory jobBuilders;
    
    @Autowired
    private StepBuilderFactory stepBuilders;
    
    @Autowired
    private PanaceaJobConfiguration pncJobConfig;
    
    @Autowired
    private VocabularyService vocabService;
    
    /**
     * Get PanaceaStudy by id
     * 
     * @param studyId Long
     * @return PanaceaStudy
     */
    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public PanaceaStudy getPanaceaStudyWithId(@PathParam("id") final Long studyId) {
        
        final PanaceaStudy study = this.getPanaceaStudyRepository().getPanaceaStudyWithId(studyId);
        return study;
    }
    
    /**
     * Get all PanaceaStudy
     * 
     * @return PanaceaStudy
     */
    @GET
    @Path("/getAllStudy")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PanaceaStudy> getAllStudy() {
        final List<PanaceaStudy> studyList = new ArrayList<PanaceaStudy>();
        
        final Iterable<PanaceaStudy> allStudy = this.getPanaceaStudyRepository().findAll();
        
        if (allStudy != null) {
            for (final PanaceaStudy s : allStudy) {
                if (s != null) {
                    studyList.add(s);
                }
            }
        }
        
        return studyList;
    }
    
    /**
     * Save a study.
     * 
     * @param panaceaStudy PanaceaStudy
     * @return PanaceaStudy
     */
    @POST
    @Path("/savestudy")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PanaceaStudy saveStudy(final PanaceaStudy panaceaStudy) {
        final PanaceaStudy ps = new PanaceaStudy();
        ps.setStudyId(panaceaStudy.getStudyId());
        ps.setCohortDefId(panaceaStudy.getCohortDefId());
        ps.setConcepSetDef(panaceaStudy.getConcepSetDef());
        /**
         * The date is being set to actual date - 1 day (like 3/15/2015 set to 3/14/2015). I think
         * it's because it's set to mid-night. So I am subtract one day here for a quick fix.
         */
        ps.setEndDate(new Date(panaceaStudy.getEndDate().getTime() + (24 * 60 * 60 * 1000)));
        ps.setStartDate(new Date(panaceaStudy.getStartDate().getTime() + (24 * 60 * 60 * 1000)));
        ps.setStudyDesc(panaceaStudy.getStudyDesc());
        ps.setStudyDetail(panaceaStudy.getStudyDetail());
        ps.setStudyDuration(panaceaStudy.getStudyDuration());
        ps.setStudyName(panaceaStudy.getStudyName());
        ps.setSwitchWindow(panaceaStudy.getSwitchWindow());
        
        return this.getPanaceaStudyRepository().save(ps);
    }
    
    /**
     * Create new PanaceaStudy and save
     * 
     * @param newStudy PanaceaStudy
     * @return PanaceaStudy
     */
    @POST
    @Path("/newstudy")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PanaceaStudy createStudy(final PanaceaStudy newStudy) {
        final PanaceaStudy ps = new PanaceaStudy();
        ps.setCohortDefId(newStudy.getCohortDefId());
        ps.setConcepSetDef(newStudy.getConcepSetDef());
        ps.setEndDate(newStudy.getEndDate());
        ps.setStartDate(newStudy.getStartDate());
        ps.setStudyDesc(newStudy.getStudyDesc());
        ps.setStudyDetail(newStudy.getStudyDetail());
        ps.setStudyDuration(newStudy.getStudyDuration());
        ps.setStudyName(newStudy.getStudyName());
        ps.setSwitchWindow(newStudy.getSwitchWindow());
        
        return this.getPanaceaStudyRepository().save(ps);
    }
    
    /**
     * Create new empty PanaceaStudy
     * 
     * @return PanaceaStudy
     */
    @GET
    @Path("/getemptynewstudy")
    @Produces(MediaType.APPLICATION_JSON)
    public PanaceaStudy getNewEmptyStudy() {
        final PanaceaStudy ps = new PanaceaStudy();
        
        return ps;
    }
    
    /**
     * Get PanaceaStageCombination by id
     * 
     * @param pncStageCombId Long
     * @return PanaceaStageCombination
     */
    @GET
    @Path("/pncstudycombination/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public PanaceaStageCombination getPanaceaStageCombinationById(@PathParam("id") final Long pncStageCombId) {
        
        final PanaceaStageCombination pncStgCmb = this.getPncStageCombinationRepository().getPanaceaStageCombinationById(
            pncStageCombId);
        
        return pncStgCmb;
    }
    
    /**
     * Get PanaceaStageCombination by studyId
     * 
     * @param studyId Long
     * @return List of PanaceaStageCombination
     */
    @GET
    @Path("/pncstudycombinationforstudy/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PanaceaStageCombination> getPanaceaStageCombinationByStudyId(@PathParam("id") final Long studyId) {
        String sql = "select PNC_TX_STG_CMB_ID, STUDY_ID from @panacea_schema.pnc_tx_stage_combination where STUDY_ID = @studyId ";
        sql = SqlRender.renderSql(sql, new String[] { "panacea_schema", "studyId" }, new String[] { getOhdsiSchema(),
                studyId.toString() });
        sql = SqlTranslate.translateSql(sql, getSourceDialect(), getDialect());
        return this.getJdbcTemplate().query(sql, new PanaceaStageCombinationMapper());
    }
    
    /**
     * Get eagerly fetched PanaceaStageCombination by studyId (with
     * PanaceaStageCombination.combMapList fetched)
     * 
     * @param studyId Long
     * @return List of PanaceaStageCombination
     */
    @GET
    @Path("/pncstudycombinationwithmapforstudy/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PanaceaStageCombination> getPanaceaStageCombinationWithMapByStudyId(@PathParam("id") final Long studyId) {
        return this.getPncStageCombinationRepository().getAllStageCombination(studyId);
    }
    
    /**
     * Get PanaceaStageCombinationMap by id
     * 
     * @param pncStageCombMpId Long
     * @return PanaceaStageCombinationMap
     */
    @GET
    @Path("/pncstudycombinationmap/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public PanaceaStageCombinationMap getPanaceaStageCombinationMapById(@PathParam("id") final Long pncStageCombMpId) {
        
        final PanaceaStageCombinationMap pncStgCmbMp = this.getPncStageCombinationMapRepository()
                .getPanaceaStageCombinationMapById(pncStageCombMpId);
        return pncStgCmbMp;
    }
    
    public PanaceaPatientSequenceCount getPanaceaPatientSequenceCountById(final Long ppscId) {
        return this.pncPatientSequenceCountRepository.getPanaceaPatientSequenceCountById(ppscId);
    }
    
    /**
     * Get PanaceaStageCombination by studyId
     * 
     * @param studyId Long
     * @return List of PanaceaStageCombination
     */
    @GET
    @Path("/pncstudycombinationforstudy/{sourceKey}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PanaceaStageCombination> getPanaceaStageCombinationByStudyId(@PathParam("sourceKey") final String sourceKey,
                                                                             @PathParam("id") final Long studyId) {
        /**
         * with sourceKey as a parameter. I don't feel we need the multi-source part for Panacea.
         * Will need to discuss with Jon later.
         */
        final Source source = getSourceRepository().findBySourceKey(sourceKey);
        final String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
        String sql = "select PNC_TX_STG_CMB_ID, STUDY_ID from @panacea_schema.pnc_tx_stage_combination where STUDY_ID = @studyId ";
        sql = SqlRender.renderSql(sql, new String[] { "panacea_schema", "studyId" },
            new String[] { tableQualifier, studyId.toString() });
        sql = SqlTranslate.translateSql(sql, source.getSourceDialect(), source.getSourceDialect());
        return this.getSourceJdbcTemplate(source).query(sql, new PanaceaStageCombinationMapper());
    }
    
    //    /**
    //     * Create a new PanaceaStageCombination and save for a study
    //     * 
    //     * @param studyId Long
    //     * @return PanaceaStageCombination
    //     */
    //    @POST
    //    @Path("/newstudystagecombination")
    //    @Produces(MediaType.APPLICATION_JSON)
    //    @Consumes(MediaType.APPLICATION_JSON)
    //    public PanaceaStageCombination createStudyStageCombination(final Long studyId) {
    //        if (studyId != null) {
    //            final PanaceaStageCombination pncComb = new PanaceaStageCombination();
    //            pncComb.setStudyId(studyId);
    //            
    //            return this.pncStageCombinationRepository.save(pncComb);
    //        } else {
    //            log.error("Study ID is null in PanaceaService.createStudyStageCombination.");
    //            return null;
    //        }
    //    }
    
    public List<PanaceaStageCombination> savePanaceaStageCombinationById(final List<PanaceaStageCombination> pncStageCombinationList) {
        
        return (List<PanaceaStageCombination>) this.getPncStageCombinationRepository().save(pncStageCombinationList);
    }
    
    public String getPanaceaPatientSequenceCountSql(final Long studyId, final Integer sourceId) {
        final PanaceaStudy pncStudy = this.getPanaceaStudyWithId(studyId);
        
        String sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/getDrugCohortPatientCount.sql");
        
        final Source source = getSourceRepository().findOne(sourceId);
        final String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
        final String cdmTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
        
        final String cohortDefId = pncStudy.getCohortDefId().toString();
        final String[] params = new String[] { "cds_schema", "ohdsi_schema", "cohortDefId", "studyId", "drugConceptId",
                "sourceId" };
        //TODO -- for testing only!!!!!!!!!!!
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, cohortDefId, studyId.toString(),
                "1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315",
                (new Integer(source.getSourceId())).toString() };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, source.getSourceDialect(), source.getSourceDialect());
        
        return sql;
    }
    
    /**
     * Test file to file chunk job
     */
    //    public JobExecutionResource runTestJob() {
    //        
    //        final Job job = this.pncJobConfig.createMarkSheet(this.jobBuilders, this.stepBuilders);
    //        
    //        final JobParametersBuilder builder = new JobParametersBuilder();
    //        final JobParameters jobParameters = builder.toJobParameters();
    //        
    //        final JobExecutionResource jobExec = this.jobTemplate.launch(job, jobParameters);
    //        return jobExec;
    //    }
    
    /**
     * Test DB to file job
     */
    public void runTestPanaceaJob(final Long studyId, final Integer sourceId) {
        if (studyId != null) {
            final PanaceaStudy pncStudy = this.getPanaceaStudyWithId(studyId);
            if (pncStudy != null) {
                final Source source = getSourceRepository().findOne(sourceId);
                final String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
                final String cdmTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
                
                final JobParametersBuilder builder = new JobParametersBuilder();
                
                final String cohortDefId = pncStudy.getCohortDefId().toString();
                
                final String sql = this.getPanaceaPatientSequenceCountSql(studyId, sourceId);
                
                builder.addString("cds_schema", cdmTableQualifier);
                builder.addString("ohdsi_schema", resultsTableQualifier);
                builder.addString("cohortDefId", cohortDefId);
                builder.addString("studyId", studyId.toString());
                //TODO -- for testin only!!!
                builder.addString("drugConceptId", "1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315");
                builder.addString("sourceDialect", source.getSourceDialect());
                
                final JobParameters jobParameters = builder.toJobParameters();
                
                final Job job = this.pncJobConfig.createPanaceaJob(getSourceJdbcTemplate(source));
                
                try {
                    
                    /**
                     * Unit test doesn't work with ThreadPoolTaskExecutor. Had to wait for the job
                     * to execute and return. WebAPI works fine with app server. (In unit test:
                     * thread sleeping for sometime works. it used to be job launched and nothing
                     * happens. no error, no warning. set a break point after
                     * jobTemplate.launch(job, jobParameters) and wait worked. So sleep works too.)
                     */
                    final JobExecutionResource jobExec = this.jobTemplate.launch(job, jobParameters);
                    //                    try {
                    //                        Thread.sleep(20000);
                    //                    } catch (final InterruptedException ex) {
                    //                        log.error("sleeping thread 222222 goes wrong:");
                    //                        ex.printStackTrace();
                    //                        Thread.currentThread().interrupt();
                    //                    }
                } catch (final ItemStreamException e) {
                    e.printStackTrace();
                }
            }
        } else {//TODO
        }
    }
    
    /**
     * Test DB to file job
     */
    @GET
    @Path("/testpncjob")
    @Produces(MediaType.APPLICATION_JSON)
    public void testPncJob() {
        runTestPanaceaJob(new Long(18), new Integer(1));
    }
    
    /**
     * Test DB to file job
     */
    @GET
    @Path("/runPncTasklet/{sourceKey}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public void runPncTasklet(@PathParam("sourceKey") final String sourceKey, @PathParam("id") final Long studyId) {
        /**
         * test: localhost:8080/WebAPI/panacea/runPncTasklet/CCAE/18
         */
        runPanaceaTasklet(sourceKey, studyId);
    }
    
    public void runPanaceaTasklet(final String sourceKey, final Long studyId) {
        if ((studyId != null) && (sourceKey != null)) {
            final PanaceaStudy pncStudy = this.getPanaceaStudyWithId(studyId);
            if (pncStudy != null) {
                
                final Source source = getSourceRepository().findBySourceKey(sourceKey);
                if (source != null) {
                    final String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
                    final String cdmTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
                    
                    final JobParametersBuilder builder = new JobParametersBuilder();
                    
                    final String cohortDefId = pncStudy.getCohortDefId().toString();
                    
                    builder.addString("cdm_schema", cdmTableQualifier);
                    builder.addString("ohdsi_schema", resultsTableQualifier);
                    builder.addString("results_schema", resultsTableQualifier);
                    builder.addString("cohortDefId", cohortDefId);
                    builder.addString("studyId", studyId.toString());
                    builder.addString("switchWindow", pncStudy.getSwitchWindow().toString());
                    //TODO -- for testin only!!!
                    //builder.addString("drugConceptId",
                    //   "1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315,4304178");
                    
                    //this works locally for resolving the JSON: curl -X POST -H "Content-Type: application/json" -d '{"items" :[{"concept":{"CONCEPT_ID":72714,"CONCEPT_NAME":"Chronic polyarticular juvenile rheumatoid arthritis","STANDARD_CONCEPT":"S","INVALID_REASON":"V","CONCEPT_CODE":"1961000","DOMAIN_ID":"Condition","VOCABULARY_ID":"SNOMED","CONCEPT_CLASS_ID":"Clinical Finding","INVALID_REASON_CAPTION":"Valid","STANDARD_CONCEPT_CAPTION":"Standard"},"isExcluded":false,"includeDescendants":true,"includeMapped":true},{"concept":{"CONCEPT_ID":4253901,"CONCEPT_NAME":"Juvenile rheumatoid arthritis","STANDARD_CONCEPT":"S","INVALID_REASON":"V","CONCEPT_CODE":"410795001","DOMAIN_ID":"Condition","VOCABULARY_ID":"SNOMED","CONCEPT_CLASS_ID":"Clinical Finding","INVALID_REASON_CAPTION":"Valid","STANDARD_CONCEPT_CAPTION":"Standard"},"isExcluded":false,"includeDescendants":true,"includeMapped":true},{"concept":{"CONCEPT_ID":80809,"CONCEPT_NAME":"Rheumatoid arthritis","STANDARD_CONCEPT":"S","INVALID_REASON":"V","CONCEPT_CODE":"69896004","DOMAIN_ID":"Condition","VOCABULARY_ID":"SNOMED","CONCEPT_CLASS_ID":"Clinical Finding","INVALID_REASON_CAPTION":"Valid","STANDARD_CONCEPT_CAPTION":"Standard"},"isExcluded":false,"includeDescendants":true,"includeMapped":true}]}' http://localhost:8080/WebAPI/CCAE/vocabulary/resolveConceptSetExpression
                    final Map<Long, Concept> cMap = this.resolveConceptExpression(pncStudy.getConcepSetDef());
                    
                    final String drugConceptIdsStr = this.getConceptIdsString(cMap, "drug");
                    final String procedureConceptIdsStr = this.getConceptIdsString(cMap, "procedure");
                    final String allConceptIdsStr = StringUtils.isEmpty(procedureConceptIdsStr) ? drugConceptIdsStr
                            .toString() : drugConceptIdsStr.concat(", " + procedureConceptIdsStr);
                    
                    builder.addString("drugConceptId", drugConceptIdsStr);
                    builder.addString("procedureConceptId", procedureConceptIdsStr);
                    builder.addString("allConceptIdsStr", allConceptIdsStr);
                    
                    builder.addString("sourceDialect", source.getSourceDialect());
                    builder.addString("sourceId", new Integer(source.getSourceId()).toString());
                    
                    String drugEraStudyOptionalDateConstraint = "";
                    if (pncStudy.getStartDate() != null) {
                        drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
                                .concat("AND (era.DRUG_ERA_START_DATE > study.START_DATE OR era.DRUG_ERA_START_DATE = study.START_DATE) \n");
                    }
                    if (pncStudy.getEndDate() != null) {
                        drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
                                .concat("AND (era.DRUG_ERA_START_DATE < study.END_DATE OR era.DRUG_ERA_START_DATE = study.END_DATE) \n");
                    }
                    
                    String procedureStudyOptionalDateConstraint = "";
                    if (pncStudy.getStartDate() != null) {
                        procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
                                .concat("AND (proc.PROCEDURE_DATE > study.START_DATE OR proc.PROCEDURE_DATE = study.START_DATE) \n");
                    }
                    if (pncStudy.getEndDate() != null) {
                        procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
                                .concat("AND (proc.PROCEDURE_DATE < study.END_DATE OR proc.PROCEDURE_DATE = study.END_DATE) \n");
                    }
                    
                    builder.addString("drugEraStudyOptionalDateConstraint", drugEraStudyOptionalDateConstraint);
                    builder.addString("procedureStudyOptionalDateConstraint", procedureStudyOptionalDateConstraint);
                    
                    final JobParameters jobParameters = builder.toJobParameters();
                    
                    final PanaceaTasklet pncTasklet = new PanaceaTasklet(this.getSourceJdbcTemplate(source),
                            this.getTransactionTemplate(), this, pncStudy);
                    
                    final Step pncStep1 = this.stepBuilders.get("panaceaStudyStep1").tasklet(pncTasklet)
                            .exceptionHandler(new TerminateJobStepExceptionHandler()).build();
                    
                    //                    final PanaceaTasklet2 pncTasklet2 = new PanaceaTasklet2(this.getSourceJdbcTemplate(source),
                    //                            this.getTransactionTemplate(), this, pncStudy);
                    //                    
                    //                    final Step pncStep2 = this.stepBuilders.get("panaceaStudyStep2").tasklet(pncTasklet2)
                    //                            .exceptionHandler(new TerminateJobStepExceptionHandler()).build();
                    //                    
                    //                    final Job pncStudyJob = this.jobBuilders.get("panaceaStudy").start(pncStep1).next(pncStep2).build();
                    
                    //final Job pncStudyJob = this.jobBuilders.get("panaceaStudy").start(pncStep1).build();
                    
                    final PanaceaGetPersonIdsTasklet pncGetPersonIdsTasklet = new PanaceaGetPersonIdsTasklet(
                            this.getSourceJdbcTemplate(source), this.getTransactionTemplate());
                    
                    final Step pncGetPersonIdsTaskletStep = this.stepBuilders.get("pncGetPersonIdsTaskletStep")
                            .tasklet(pncGetPersonIdsTasklet).exceptionHandler(new TerminateJobStepExceptionHandler())
                            .build();
                    
                    final PanaceaPatientDrugComboTasklet pncPatientDrugComboTasklet = new PanaceaPatientDrugComboTasklet(
                            this.getSourceJdbcTemplate(source), this.getTransactionTemplate(), pncStudy,
                            this.pncStageCombinationRepository);
                    
                    final Step pncPatientDrugComboTaskletStep = this.stepBuilders.get("pncPatientDrugComboTaskletStep")
                            .tasklet(pncPatientDrugComboTasklet).exceptionHandler(new TerminateJobStepExceptionHandler())
                            .build();
                    
                    final PanaceaSummaryGenerateTasklet pncSummaryTasklet = new PanaceaSummaryGenerateTasklet(
                            this.getSourceJdbcTemplate(source), this.getTransactionTemplate(), pncStudy);
                    
                    final Step pncSummaryStep = this.stepBuilders.get("pncSummaryStep").tasklet(pncSummaryTasklet)
                            .exceptionHandler(new TerminateJobStepExceptionHandler()).build();
                    
                    final Job pncStudyJob = this.jobBuilders.get("panaceaStudy").start(pncStep1)
                            .next(pncGetPersonIdsTaskletStep).next(pncPatientDrugComboTaskletStep).next(pncSummaryStep)
                            .build();
                    
                    final JobExecutionResource jobExec = this.jobTemplate.launch(pncStudyJob, jobParameters);
                } else {
                    //TODO
                    log.error("");
                }
            }
        } else {
            //TODO
            log.error("");
        }
    }
    
    /**
     * @return the panaceaStudyRepository
     */
    public PanaceaStudyRepository getPanaceaStudyRepository() {
        return this.panaceaStudyRepository;
    }
    
    /**
     * @param panaceaStudyRepository the panaceaStudyRepository to set
     */
    public void setPanaceaStudyRepository(final PanaceaStudyRepository panaceaStudyRepository) {
        this.panaceaStudyRepository = panaceaStudyRepository;
    }
    
    /**
     * @return the pncStageCombinationRepository
     */
    public PanaceaStageCombinationRepository getPncStageCombinationRepository() {
        return this.pncStageCombinationRepository;
    }
    
    /**
     * @param pncStageCombinationRepository the pncStageCombinationRepository to set
     */
    public void setPncStageCombinationRepository(final PanaceaStageCombinationRepository pncStageCombinationRepository) {
        this.pncStageCombinationRepository = pncStageCombinationRepository;
    }
    
    /**
     * @return the pncStageCombinationMapRepository
     */
    public PanaceaStageCombinationMapRepository getPncStageCombinationMapRepository() {
        return this.pncStageCombinationMapRepository;
    }
    
    /**
     * @param pncStageCombinationMapRepository the pncStageCombinationMapRepository to set
     */
    public void setPncStageCombinationMapRepository(final PanaceaStageCombinationMapRepository pncStageCombinationMapRepository) {
        this.pncStageCombinationMapRepository = pncStageCombinationMapRepository;
    }
    
    /**
     * @return the em
     */
    public EntityManager getEm() {
        return this.em;
    }
    
    /**
     * @param em the em to set
     */
    public void setEm(final EntityManager em) {
        this.em = em;
    }
    
    /**
     * @return the jobTemplate
     */
    public JobTemplate getJobTemplate() {
        return this.jobTemplate;
    }
    
    /**
     * @param jobTemplate the jobTemplate to set
     */
    public void setJobTemplate(final JobTemplate jobTemplate) {
        this.jobTemplate = jobTemplate;
    }
    
    public Map<Long, Concept> resolveConceptExpression(final String expressionString) {
        if (!StringUtils.isEmpty(expressionString)) {
            final ObjectMapper mapper = new ObjectMapper();
            ConceptSetExpression expression;
            try {
                expression = mapper.readValue(expressionString, ConceptSetExpression.class);
                
                if (expression != null) {
                    final ConceptSetItem[] items = expression.items;
                    if ((items != null) && (items.length > 0)) {
                        final Map<Long, Concept> cMap = new HashMap<Long, Concept>();
                        for (final ConceptSetItem item : items) {
                            if ((item != null) && (item.concept != null)) {
                                cMap.put(item.concept.conceptId, item.concept);
                            }
                        }
                        
                        return cMap;
                    }
                }
            } catch (final JsonParseException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
            } catch (final JsonMappingException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
            } catch (final IOException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
            }
        }
        
        return null;
    }
    
    public String getConceptIdsString(final Map<Long, Concept> cMap, final String domainId) {
        
        if ((cMap != null) && (domainId != null)) {
            String conceptIdsStr = "";
            
            for (final Entry<Long, Concept> entry : cMap.entrySet()) {
                if ((entry.getKey() != null) && (entry.getValue() != null)) {
                    if (entry.getValue().domainId != null) {
                        if (entry.getValue().domainId.toLowerCase().equals(domainId.toLowerCase())) {
                            conceptIdsStr = StringUtils.isEmpty(conceptIdsStr) ? conceptIdsStr.concat(entry.getKey()
                                    .toString().toString()) : conceptIdsStr.concat("," + entry.getKey().toString());
                        }
                    }
                }
            }
            
            return conceptIdsStr;
        } else {
            //TODO - error logging...
            return null;
        }
    }    
}
