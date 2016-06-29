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
package org.ohdsi.webapi.cohortcomparison;

import java.io.Serializable;

/**
 * @author Frank DeFalco <fdefalco@ohdsi.org>
 */
public class ComparativeCohortAnalysisInfo extends ComparativeCohortAnalysis implements Serializable {
  private String treatmentCaption;
  private String comparatorCaption;
  private String outcomeCaption;
  private String exclusionCaption;

  public String getTreatmentCaption() {
    return treatmentCaption;
  }

  public String getComparatorCaption() {
    return comparatorCaption;
  }

  public void setComparatorCaption(String comparatorCaption) {
    this.comparatorCaption = comparatorCaption;
  }

  public String getOutcomeCaption() {
    return outcomeCaption;
  }

  public void setOutcomeCaption(String outcomeCaption) {
    this.outcomeCaption = outcomeCaption;
  }

  public String getExclusionCaption() {
    return exclusionCaption;
  }

  public void setExclusionCaption(String exclusionsCaption) {
    this.exclusionCaption = exclusionsCaption;
  }

  public void setTreatmentCaption(String treatmentCaption) {
    this.treatmentCaption = treatmentCaption;
  }
  
}
