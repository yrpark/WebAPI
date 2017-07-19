{DEFAULT @usePersonSourceValue = 'false'}
select p.gender_concept_id, year_of_birth, month_of_birth, day_of_birth, cg.concept_name as gender, cr.concept_name as race, ce.concept_name as ethnicity, person_id, person_source_value
from @tableQualifier.person p
join @tableQualifier.concept cg on p.gender_concept_id = cg.concept_id
left join @tableQualifier.concept cr on p.race_concept_id = cr.concept_id
left join @tableQualifier.concept ce on p.ethnicity_concept_id = ce.concept_id
where 
{@usePersonSourceValue == 'true'} ? {person_source_value in (@personIds)} : {person_id in (@personIds)}
