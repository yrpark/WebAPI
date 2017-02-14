package org.ohdsi.webapi.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.cohort.CohortEntity;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

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
	public List<CohortEntity> getCohortListById(@PathParam("sourceKey") String sourceKey,
			@PathParam("id") final String cohortId) {

		final List<CohortEntity> cohortEntities = new ArrayList<CohortEntity>();
		final Source source = getSourceRepository().findBySourceKey(sourceKey);
		final String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);

		String sqlStatement = "select * from @resultsTableQualifier.cohort where cohort_definition_id = @cohortId" ;
		sqlStatement = SqlRender.renderSql(sqlStatement, new String[]{"cohortId", "resultsTableQualifier"}, 
				new String[]{cohortId, resultsTableQualifier});
		sqlStatement = SqlTranslate.translateSql(sqlStatement, "sql server", source.getSourceDialect());

		getSourceJdbcTemplate(source).query(sqlStatement, new RowMapper<Void>() {
			@Override
			public Void mapRow(ResultSet rs, int arg1) throws SQLException {
				final CohortEntity ce = new CohortEntity();
				ce.setCohortDefinitionId(rs.getLong("cohort_definition_id"));
				ce.setSubjectId(rs.getLong("subject_id"));
				ce.setCohortStartDate(rs.getDate("cohort_start_date"));
				ce.setCohortEndDate(rs.getDate("cohort_end_date"));
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
