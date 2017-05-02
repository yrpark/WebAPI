package org.ohdsi.webapi.cohort;

import org.ohdsi.webapi.person.PersonDemographics;

public class PersonEntity extends CohortEntity {
    private PersonDemographics demographics;

    public PersonDemographics getDemographics() {
        return demographics;
    }

    public void setDemographics(PersonDemographics demographics) {
        this.demographics = demographics;
    }
}
