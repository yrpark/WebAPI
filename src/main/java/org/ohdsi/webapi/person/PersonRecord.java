/*
 * Copyright 2015 fdefalco.
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
package org.ohdsi.webapi.person;

import java.sql.Timestamp;

/**
 *
 * @author fdefalco
 */
public class PersonRecord {
	private String domain;
	private Long conceptId;
	private String conceptName;
	private Timestamp startDate;
	private Timestamp endDate;
	private String sourceConceptValue;
	private String sourceConceptName;
	private String displayValue;

	
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public Long getConceptId() {
		return conceptId;
	}
	public void setConceptId(Long conceptId) {
		this.conceptId = conceptId;
	}
	public String getConceptName() {
		return conceptName;
	}
	public void setConceptName(String conceptName) {
		this.conceptName = conceptName;
	}
	public Timestamp getStartDate() {
		return startDate;
	}
	public void setStartDate(Timestamp startDate) {
		this.startDate = startDate;
	}
	public Timestamp getEndDate() {
		return endDate;
	}
	public void setEndDate(Timestamp endDate) {
		this.endDate = endDate;
	}
	public String getSourceConceptValue() {
		return sourceConceptValue;
	}
	public void setSourceConceptValue(String sourceConceptValue) {
		this.sourceConceptValue = sourceConceptValue;
	}

	public String getSourceConceptName() {
		return sourceConceptName;
	}
	public void setSourceConceptName(String sourceConceptName) {
		this.sourceConceptName = sourceConceptName;
	}
	public String getDisplayValue() {
		return displayValue;
	}
	public void setDisplayValue(String displayValue) {
		this.displayValue = displayValue;
	}




}
