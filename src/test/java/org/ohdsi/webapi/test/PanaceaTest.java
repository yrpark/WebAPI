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
package org.ohdsi.webapi.test;

import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ohdsi.webapi.WebApi;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombination;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombinationMap;
import org.ohdsi.webapi.panacea.repository.impl.PanaceaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Document curl WS testing:
 * 
 * <pre>
 *  curl -X GET -H "Content-Type: application/json" http://localhost:8080/WebAPI/panacea/2
 *  
 *  curl -X POST -H "Content-Type: application/json" -d '{"studyName":"Test adding study","studyDesc":"test as proof of concept","concepSetDef":"{\"items\" :[{\"concept\":{\"CONCEPT_ID\":1301025,\"CONCEPT_NAME\":\"Enoxaparin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"67108\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"17\",\"DESCENDANT_RECORD_COUNT\":\"17\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1328165,\"CONCEPT_NAME\":\"Diltiazem\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"3443\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"473\",\"DESCENDANT_RECORD_COUNT\":\"984\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1771162,\"CONCEPT_NAME\":\"Cefazolin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"2180\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"86\",\"DESCENDANT_RECORD_COUNT\":\"172\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":19058274,\"CONCEPT_NAME\":\"Purified Protein Derivative of Tuberculin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"8948\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"0\",\"DESCENDANT_RECORD_COUNT\":\"0\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":918906,\"CONCEPT_NAME\":\"oxybutynin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"32675\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"117\",\"DESCENDANT_RECORD_COUNT\":\"241\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":923645,\"CONCEPT_NAME\":\"Omeprazole\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"7646\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"419\",\"DESCENDANT_RECORD_COUNT\":\"886\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":933724,\"CONCEPT_NAME\":\"Phenazopyridine\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"8120\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"28\",\"DESCENDANT_RECORD_COUNT\":\"56\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1310149,\"CONCEPT_NAME\":\"Warfarin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"11289\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"350\",\"DESCENDANT_RECORD_COUNT\":\"757\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1125315,\"CONCEPT_NAME\":\"Acetaminophen\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"161\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"1,258\",\"DESCENDANT_RECORD_COUNT\":\"2,821\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false}]}","cohortDefId":915,"studyDetail":"testing","switchWindow":30,"studyDuration":1095,"startDate":"2009-01-01","endDate":"2013-01-01"}' http://localhost:8080/WebAPI/panacea/newstudy
 *  
 *  curl -X POST -H "Content-Type: application/json" -d http://localhost:8080/WebAPI/panacea/newstudystagecombination
 * 
 * </pre>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = WebApi.class)
@Ignore
public class PanaceaTest extends TestCase {
    
    private static final Log log = LogFactory.getLog(PanaceaTest.class);
    
    @Autowired
    private PanaceaService pncService;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    @Test
    public void testGetCombination() {
        try {
            /**
             * Use browser test too:
             * http://localhost:8080/WebAPI/panacea/pncstudycombinationforstudy/RIV5/2
             */
            List<PanaceaStageCombination> pncStgCmbs = this.pncService.getPanaceaStageCombinationByStudyId("RIV5", new Long(
                    2));
            log.info("testGetCombination: " + pncStgCmbs.toString());
            
            /**
             * http://localhost:8080/WebAPI/panacea/pncstudycombinationforstudy/2
             */
            pncStgCmbs = this.pncService.getPanaceaStageCombinationByStudyId(new Long(2));
            log.info("testGetCombination without source: " + pncStgCmbs.toString());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testLoadCombinationWithMap() {
        try {
            /**
             * Use browser test too: http://localhost:8080/WebAPI/panacea/pncstudycombination/1
             */
            final PanaceaStageCombination pncStgCmb = this.pncService.getPanaceaStageCombinationById(new Long(2));
            log.info("testLoadCombinationWithMap: " + pncStgCmb.toString());
            
            /**
             * http://localhost:8080/WebAPI/panacea/pncstudycombinationmap/2
             */
            final PanaceaStageCombinationMap map = this.pncService.getPanaceaStageCombinationMapById(new Long(2));
            log.info("testLoadCombinationWithMap map: " + map.toString());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testGetAllCombForStudyWithMap() {
        try {
            /**
             * http://localhost:8080/WebAPI/panacea/pncstudycombinationwithmapforstudy/2
             */
            final List<PanaceaStageCombination> combList = this.pncService
                    .getPanaceaStageCombinationWithMapByStudyId(new Long(2));
            log.info("testGetAllCombForStudyWithMap: " + combList.toString());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
