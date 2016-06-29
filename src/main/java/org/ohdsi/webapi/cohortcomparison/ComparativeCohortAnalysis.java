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
import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Frank DeFalco <fdefalco@ohdsi.org>
 */

@Entity(name = "ComparativeCohortAnalysis")
@Table(name="cca")
public class ComparativeCohortAnalysis implements Serializable {
  @Id
  @GeneratedValue
  @Column(name="cca_id")  
  private Integer id; 
  
  @Column(name="name")
  private String name;
  
  @Column(name="treatment_id")
  private Integer treatmentId;
  
  @Column(name="comparator_id")
  private Integer comparatorId;
  
  @Column(name="outcome_id")
  private Integer outcomeId;
  
  @Column(name="time_at_risk")
  private Integer timeAtRisk;
  
  @Column(name="exclusion_id")
  private Integer exclusionId;
  
  @Column(name="created")
  private Date created;
  
  @Column(name="modified")
  private Date modified;
  
  @Column(name="sec_user_id")
  private Integer userId = 0;
  
  @Column(name="locked")
  private Integer locked = 0;

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getTreatmentId() {
    return treatmentId;
  }

  public void setTreatmentId(Integer treatmentId) {
    this.treatmentId = treatmentId;
  }

  public Integer getComparatorId() {
    return comparatorId;
  }

  public void setComparatorId(Integer comparatorId) {
    this.comparatorId = comparatorId;
  }

  public Integer getOutcomeId() {
    return outcomeId;
  }

  public void setOutcomeId(Integer outcomeId) {
    this.outcomeId = outcomeId;
  }

  public Integer getTimeAtRisk() {
    return timeAtRisk;
  }

  public void setTimeAtRisk(Integer timeAtRisk) {
    this.timeAtRisk = timeAtRisk;
  }

  public Integer getExclusionId() {
    return exclusionId;
  }

  public void setExclusionId(Integer exclusionId) {
    this.exclusionId = exclusionId;
  }

  public Date getCreated() {
    return created;
  }

  public void setCreated(Date created) {
    this.created = created;
  }

  public Date getModified() {
    return modified;
  }

  public void setModified(Date modified) {
    this.modified = modified;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }
  
  public Integer getLocked() {
    return locked;
  }

  public void setLocked(Integer locked) {
    this.locked = locked;
  }  
}
