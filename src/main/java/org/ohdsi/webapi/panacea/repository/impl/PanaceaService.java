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

import java.util.List;

import javax.persistence.EntityManager;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.panacea.mapper.PanaceaStageCombinationMapper;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombination;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombinationMap;
import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.ohdsi.webapi.panacea.repository.PanaceaStageCombinationMapRepository;
import org.ohdsi.webapi.panacea.repository.PanaceaStageCombinationRepository;
import org.ohdsi.webapi.panacea.repository.PanaceaStudyRepository;
import org.ohdsi.webapi.service.AbstractDaoService;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
    private EntityManager em;
    
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
    
}
