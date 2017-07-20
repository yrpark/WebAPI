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
package org.ohdsi.webapi.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.joda.time.LocalDate;
import org.joda.time.Years;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.helper.ResourceHelper;
import org.ohdsi.webapi.person.CohortPerson;
import org.ohdsi.webapi.person.ObservationPeriod;
import org.ohdsi.webapi.person.PersonDemographics;
import org.ohdsi.webapi.person.PersonProfile;
import org.ohdsi.webapi.person.PersonRecord;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

@Path("{sourceKey}/person/")
@Component
public class PersonService extends AbstractDaoService {


    @Autowired
    private VocabularyService vocabService;

    @Autowired
    private ConceptSetService conceptSetService;

    @Path("{personId}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public PersonProfile getPersonProfile(@PathParam("sourceKey") String sourceKey, @PathParam("personId") String personId) {
        final PersonProfile profile = new PersonProfile();

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
        String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);

        final PersonDemographics demographics = this.getPersonDemographics(sourceKey, personId, false);
        profile.gender = demographics.getGender();
        profile.yearOfBirth = demographics.getYearOfBirth();

        // get observation periods
        String sqlObservationPeriods = ResourceHelper.GetResourceAsString("/resources/person/sql/getObservationPeriods.sql");
        sqlObservationPeriods = SqlRender.renderSql(sqlObservationPeriods, new String[]{"personId", "tableQualifier"}, new String[]{personId, tableQualifier});
        sqlObservationPeriods = SqlTranslate.translateSql(sqlObservationPeriods, "sql server", source.getSourceDialect());

        getSourceJdbcTemplate(source).query(sqlObservationPeriods, new RowMapper<Void>() {
            @Override
            public Void mapRow(ResultSet resultSet, int arg1) throws SQLException {
                ObservationPeriod op = new ObservationPeriod();

                op.startDate = resultSet.getTimestamp("start_date");
                op.endDate = resultSet.getTimestamp("end_date");
                op.type = resultSet.getString("observation_period_type");
                op.id = resultSet.getInt("observation_period_id");

                profile.observationPeriods.add(op);
                return null;
            }
        });

        // get simplified records
        final ArrayList<PersonRecord> records = this.getPersonRecords(sourceKey, personId, false);
        profile.records = records;

        String sql_statement = ResourceHelper.GetResourceAsString("/resources/person/sql/getCohorts.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"subjectId", "tableQualifier"}, new String[]{personId, resultsTableQualifier});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

        getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
            @Override
            public Void mapRow(ResultSet resultSet, int arg1) throws SQLException {
                CohortPerson item = new CohortPerson();

                item.startDate = resultSet.getTimestamp("cohort_start_date");
                item.endDate = resultSet.getTimestamp("cohort_end_date");
                item.cohortDefinitionId = resultSet.getLong("cohort_definition_id");

                profile.cohorts.add(item);
                return null;
            }
        });

        return profile;
    }

    private void mapPatientDemographics(final ResultSet resultSet, final PersonDemographics demographics)
            throws SQLException {
        final Integer birthYear = resultSet.getInt("year_of_birth");
        final Integer birthMonth = resultSet.getInt("month_of_birth");
        final Integer birthDay = resultSet.getInt("day_of_birth");
        demographics.setYearOfBirth(birthYear);

        if (birthYear != null && birthMonth != null && birthDay != null) {
            final LocalDate birthDate = new LocalDate(demographics.getYearOfBirth(), birthMonth, birthDay);
            final LocalDate now = new LocalDate();
            final Years age = Years.yearsBetween(birthDate, now);
            demographics.setAge(age.getYears());
        } else {
            demographics.setAge(-1);
        }

        demographics.setGender(resultSet.getString("gender"));
        demographics.setRace(resultSet.getString("race"));
        demographics.setEthnicity(resultSet.getString("ethnicity"));

        demographics.setPersonId(resultSet.getInt("person_id"));
        demographics.setPersonSourceValue(resultSet.getString("person_source_value"));

    }

    @Path("{personKeys}/demographicsmap")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, PersonDemographics> getMultiplePersonDemographics(@PathParam("sourceKey") final String sourceKey,
                                                                         @PathParam(value = "personKeys") final String personKeys,
                                                                         @QueryParam(value = "usePersonSourceValue") @DefaultValue("false") final Boolean usePersonSourceValue) {
        final Map<String, PersonDemographics> demographicsMap = new HashMap<>();
        if (personKeys != null) {
            final Source source = getSourceRepository().findBySourceKey(sourceKey);
            final String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

            String sqlStatement = ResourceHelper.GetResourceAsString("/resources/person/sql/multiplePersonInfo.sql");
            sqlStatement = SqlRender.renderSql(sqlStatement, new String[]{"personIds", "tableQualifier", "usePersonSourceValue"},
                    new String[]{personKeys, tableQualifier, String.valueOf(usePersonSourceValue)});
            sqlStatement = SqlTranslate.translateSql(sqlStatement, "sql server", source.getSourceDialect());

            getSourceJdbcTemplate(source).query(sqlStatement, new RowMapper<Void>() {
                @Override
                public Void mapRow(final ResultSet resultSet, int arg1) throws SQLException {

                    final PersonDemographics demographics = new PersonDemographics();
                    mapPatientDemographics(resultSet, demographics);
                    final String key = String.valueOf(demographics.getPersonId());
                    demographicsMap.put(key, demographics);
                    return null;
                }
            });
        }

        return demographicsMap;
    }

    @Path("{personKey}/demographics")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public PersonDemographics getPersonDemographics(@PathParam("sourceKey") final String sourceKey,
                                                    @PathParam("personKey") final String personKey,
                                                    @QueryParam(value = "usePersonSourceValue") final Boolean usePersonSourceValue) {

        final PersonDemographics demographics = new PersonDemographics();

        final Source source = getSourceRepository().findBySourceKey(sourceKey);
        final String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String sqlStatement = ResourceHelper.GetResourceAsString("/resources/person/sql/personInfo.sql");
        sqlStatement = SqlRender.renderSql(sqlStatement, new String[]{"personId", "tableQualifier", "usePersonSourceValue"},
                new String[]{personKey, tableQualifier, String.valueOf(usePersonSourceValue)});
        sqlStatement = SqlTranslate.translateSql(sqlStatement, "sql server", source.getSourceDialect());


        getSourceJdbcTemplate(source).query(sqlStatement, new RowMapper<Void>() {
            @Override
            public Void mapRow(ResultSet resultSet, int arg1) throws SQLException {
                mapPatientDemographics(resultSet, demographics);
                return null;
            }
        });

        return demographics;
    }

    /**
     * @param sourceKey
     * @param personKey            the personId
     * @param usePersonSourceValue whether the person source value should be looked up, vs. the CDM person id
     */
    @Path("{personKey}/records")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public ArrayList<PersonRecord> getPersonRecords(@PathParam("sourceKey") String sourceKey,
                                                    @PathParam("personKey") String personKey,
                                                    @QueryParam(value = "usePersonSourceValue") final Boolean usePersonSourceValue) {

        final ArrayList<PersonRecord> records = new ArrayList<PersonRecord>();
        final Source source = getSourceRepository().findBySourceKey(sourceKey);
        final String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String personId = null;
        if (usePersonSourceValue) {
            final PersonDemographics demographics = getPersonDemographics(sourceKey, personKey, usePersonSourceValue);
            personId = String.valueOf(demographics.getPersonId());
        } else {
            personId = personKey;
        }

        String sqlStatement = ResourceHelper.GetResourceAsString("/resources/person/sql/getValueRecords.sql");
        sqlStatement = SqlRender.renderSql(sqlStatement, new String[]{"personId", "tableQualifier"},
                new String[]{personId, tableQualifier});
        sqlStatement = SqlTranslate.translateSql(sqlStatement, "sql server", source.getSourceDialect());

        getSourceJdbcTemplate(source).query(sqlStatement, new RowMapper<Void>() {
            @Override
            public Void mapRow(ResultSet resultSet, int arg1) throws SQLException {
                final PersonRecord record = new PersonRecord();

                record.setConceptId(resultSet.getLong("concept_id"));
                record.setConceptName(resultSet.getString("concept_name"));
                record.setDomain(resultSet.getString("domain"));
                record.setStartDate(resultSet.getTimestamp("start_date"));
                record.setEndDate(resultSet.getTimestamp("end_date"));
                record.setDisplayValue(resultSet.getString("display_value"));
                record.setSourceConceptName(resultSet.getString("source_concept_name"));
                record.setSourceConceptValue(resultSet.getString("source_concept_value"));

                records.add(record);
                return null;
            }
        });

        return records;
    }
}
