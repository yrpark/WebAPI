/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ohdsi.webapi.service;

import java.util.Date;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.ohdsi.webapi.cohortcomparison.ComparativeCohortAnalysis;
import org.ohdsi.webapi.cohortcomparison.ComparativeCohortAnalysisExecution;
import org.ohdsi.webapi.cohortcomparison.ComparativeCohortAnalysisInfo;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author Frank DeFalco <fdefalco@ohdsi.org>
 */

@Path("/comparativecohortanalysis/")
public class ComparativeCohortAnalysisService extends AbstractDaoService {

    @Autowired
    private CohortDefinitionService cohortDefinitionService;

    @Autowired
    private ConceptSetService conceptSetService;  
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Iterable<ComparativeCohortAnalysis> getComparativeCohortAnalyses() {
        return getComparativeCohortAnalysisRepository().findAll();
    }  
    
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public ComparativeCohortAnalysis saveComparativeCohortAnalysis(ComparativeCohortAnalysis comparativeCohortAnalysis) {
        Date d = new Date();
        if (comparativeCohortAnalysis.getCreated() == null) {
          comparativeCohortAnalysis.setCreated(d);
        }
        comparativeCohortAnalysis.setModified(d);
        comparativeCohortAnalysis = this.getComparativeCohortAnalysisRepository().save(comparativeCohortAnalysis);
        return comparativeCohortAnalysis;
    } 
    
    @GET
    @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public ComparativeCohortAnalysisInfo getComparativeCohortAnalysis(@PathParam("id") int id) {
        ComparativeCohortAnalysisInfo info = new ComparativeCohortAnalysisInfo();
        ComparativeCohortAnalysis analysis = this.getComparativeCohortAnalysisRepository().findOne(id);

        info.setId(analysis.getId());        
        info.setName(analysis.getName());        
        info.setCreated(analysis.getCreated());
        info.setModified(analysis.getModified());
        info.setUserId(analysis.getUserId());
        info.setTimeAtRisk(analysis.getTimeAtRisk());
        
        info.setComparatorId(analysis.getComparatorId());
        info.setComparatorCaption(cohortDefinitionService.getCohortDefinition(analysis.getComparatorId()).name);
        
        info.setTreatmentId(analysis.getTreatmentId());
        info.setTreatmentCaption(cohortDefinitionService.getCohortDefinition(analysis.getTreatmentId()).name);
        
        info.setOutcomeId(analysis.getOutcomeId());
        info.setOutcomeCaption(cohortDefinitionService.getCohortDefinition(analysis.getOutcomeId()).name);
        
        info.setExclusionId(analysis.getExclusionId());
        info.setExclusionCaption(conceptSetService.getConceptSet(analysis.getExclusionId()).getName());
                        
        return info;
    }  
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/executions")
    public Iterable<ComparativeCohortAnalysisExecution> getComparativeCohortAnalysisExecutions(@PathParam("id") int comparativeCohortAnalysisId){
      return getComparativeCohortAnalysisExecutionRepository().findAllByAnalysisId(comparativeCohortAnalysisId);
    }
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("execution/{eid}")
    public ComparativeCohortAnalysisExecution getComparativeCohortAnalysisExecution(@PathParam("eid") int executionId){
      return getComparativeCohortAnalysisExecutionRepository().findByExecutionId(executionId);
    }    
    
  @GET
  @Path("execution/{eid}/psmodel")
  @Produces(MediaType.APPLICATION_JSON)
  public PropensityScoreModelReport getPropensityScoreModelReport(@PathParam("eid") int executionId) {

    ComparativeCohortAnalysisExecution execution = getComparativeCohortAnalysisExecutionRepository().findByExecutionId(executionId);
    Source source = getSourceRepository().findOne(executionId)
    execution.get
    Source source = getSourceRepository().findBySourceKey(sourceKey);
    final String key = CohortResultsAnalysisRunner.CONDITION;
    List<HierarchicalConceptRecord> res = null;
    VisualizationData data = refresh ? null : this.visualizationDataRepository.findByCohortDefinitionIdAndSourceIdAndVisualizationKey(id, source.getSourceId(), key);

    if (refresh || data == null) {
      res = this.queryRunner.getConditionTreemap(this.getSourceJdbcTemplate(source), id, minCovariatePersonCountParam, minIntervalPersonCountParam, source, true);
    } else {
      try {
        res = mapper.readValue(data.getData(), new TypeReference<List<HierarchicalConceptRecord>>() {
        });
      } catch (Exception e) {
        log.error(e);
      }
    }

    return res;
  }    
}
