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

import javax.persistence.EntityManager;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.ohdsi.webapi.panacea.repository.PanaceaStudyRepository;
import org.ohdsi.webapi.service.AbstractDaoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 */
@Path("/panacea")
@Component
public class PanaceaService extends AbstractDaoService {
    
    @Autowired
    private PanaceaStudyRepository panaceaStudyRepository;
    
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
}
