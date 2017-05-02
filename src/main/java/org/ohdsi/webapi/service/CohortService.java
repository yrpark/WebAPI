package org.ohdsi.webapi.service;

import org.joda.time.LocalDate;
import org.joda.time.Years;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.cohort.PersonEntity;
import org.ohdsi.webapi.cohort.CohortEntity;
import org.ohdsi.webapi.person.PersonDemographics;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service to read/write to the Cohort table
 */
@Path("{sourceKey}/cohort/")
@Component
public class CohortService extends AbstractDaoService {

	/**
	 * Retrieves all cohort entities for the given cohort definition id 
	 * from the COHORT table
	 * 
	 * @param id Cohort Definition id
	 * @return List of CohortEntity
	 */
	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public List<PersonEntity> getCohortListById(@PathParam("sourceKey") String sourceKey,
			@PathParam("id") final String cohortId) {

		final List<PersonEntity> cohortEntities = new ArrayList<PersonEntity>();
		final Source source = getSourceRepository().findBySourceKey(sourceKey);
		final String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
		final String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

		String sqlStatement = "select p.gender_concept_id, year_of_birth, month_of_birth, day_of_birth, cg.concept_name as gender, cr.concept_name as race, ce.concept_name as ethnicity, person_id, person_source_value, c.* from @resultsTableQualifier.cohort c " +
				"inner join @tableQualifier.person p on p.person_id = c.subject_id " +
				"join @tableQualifier.concept cg on p.gender_concept_id = cg.concept_id " +
				"left join @tableQualifier.concept cr on p.race_concept_id = cr.concept_id " +
				"left join @tableQualifier.concept ce on p.ethnicity_concept_id = ce.concept_id " +
				"where c.cohort_definition_id = @cohortId" ;
		sqlStatement = SqlRender.renderSql(sqlStatement, new String[]{"cohortId", "resultsTableQualifier", "tableQualifier"},
				new String[]{cohortId, resultsTableQualifier, tableQualifier});
		sqlStatement = SqlTranslate.translateSql(sqlStatement, "sql server", source.getSourceDialect());

		getSourceJdbcTemplate(source).query(sqlStatement, new RowMapper<Void>() {
			@Override
			public Void mapRow(ResultSet rs, int arg1) throws SQLException {
				final PersonEntity ce = new PersonEntity();
				ce.setCohortDefinitionId(rs.getLong("cohort_definition_id"));
				ce.setSubjectId(rs.getLong("subject_id"));
				ce.setCohortStartDate(rs.getDate("cohort_start_date"));
				ce.setCohortEndDate(rs.getDate("cohort_end_date"));

				final PersonDemographics demographics = new PersonDemographics();
				final Integer birthYear = rs.getInt("year_of_birth");
				final Integer birthMonth = rs.getInt("month_of_birth");
				final Integer birthDay = rs.getInt("day_of_birth");
				demographics.setYearOfBirth(birthYear);

				if (birthYear != null && birthMonth != null && birthDay != null) {
					final LocalDate birthDate = new LocalDate (demographics.getYearOfBirth(), birthMonth, birthDay);
					final LocalDate now = new LocalDate();
					final Years age = Years.yearsBetween(birthDate, now);
					demographics.setAge(age.getYears());
				} else {
					demographics.setAge(-1);
				}

				demographics.setGender(rs.getString("gender"));
				demographics.setRace(rs.getString("race"));
				demographics.setEthnicity(rs.getString("ethnicity"));

				demographics.setPersonId(rs.getInt("person_id"));
				demographics.setPersonSourceValue(rs.getString("person_source_value"));
				ce.setDemographics(demographics);

				cohortEntities.add(ce);
				return null;
			}
		});

		return cohortEntities;
	}
	
	/**
	 * Imports a List of CohortEntity into the COHORT table
	 * 
	 * @param cohort List of CohortEntity
	 * @return status
	 */
	@Deprecated
	@POST
	@Path("import")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public String saveCohortListToCDM(final List<CohortEntity> cohort) {
		// TODO needs to be refactored for sources

//		this.transactionTemplate.execute(new TransactionCallback<Void>() {
//            @Override
//            public Void doInTransaction(TransactionStatus status) {
//                int i = 0;
//                for (CohortEntity cohortEntity : cohort) {
//                	em.persist(cohortEntity);
//                    if (i % 5 == 0) { //5, same as the JDBC batch size
//                        //flush a batch of inserts and release memory:
//                        em.flush();
//                        em.clear();
//                    }
//                    i++;
//                }
//                return null;
//            }
//        });
       
		return "error";
	}

}
