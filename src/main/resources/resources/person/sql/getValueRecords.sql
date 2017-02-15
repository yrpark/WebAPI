{DEFAULT @recordTypes = ''}


select distinct 'drug' as "domain", drug_concept_id concept_id, c.concept_name, 
drug_exposure_start_date start_date, drug_exposure_end_date end_date, 
trim(concat(case when d.quantity is null then '' else concat('#', d.quantity) end, 
case when d.refills is null then '' else concat( ' Refills: ', d.refills) end)) as display_value, 
s.concept_code as source_concept_value, s.concept_name as source_concept_name
from @tableQualifier.drug_exposure d
inner join @tableQualifier.concept c on d.drug_concept_id = c.concept_id
inner join @tableQualifier.concept s on d.drug_source_concept_id = s.concept_id
where person_id = @personId


union all

select distinct 'drugera' as "domain", drug_concept_id concept_id, concept_name, drug_era_start_date start_date, drug_era_end_date end_date , '' as display_value, '' as source_concept_value, '' as source_concept_name
from @tableQualifier.drug_era 
join @tableQualifier.concept c on c.concept_id = drug_era.drug_concept_id
where person_id = @personId  

union all

select distinct 'condition' as "domain", condition_concept_id concept_id, c.concept_name, condition_start_date start_date, condition_end_date end_date, 
'' as display_value, s.concept_code as source_concept_value, s.concept_name as source_concept_name
from @tableQualifier.condition_occurrence co
inner join @tableQualifier.concept c on co.condition_concept_id = c.concept_id
inner join @tableQualifier.concept s on co.condition_source_concept_id = s.concept_id
where person_id = @personId

union all

select distinct 'conditionera' as "domain", condition_concept_id concept_id, concept_name, condition_era_start_date start_date, condition_era_end_date end_date , '' as display_value, '' as source_concept_value, '' as source_concept_name
from @tableQualifier.condition_era
join @tableQualifier.concept c on c.concept_id = condition_era.condition_concept_id
where person_id = @personId  

union all

select distinct 'observation' as "domain", observation_concept_id concept_id, c.concept_name, observation_date start_date, observation_date end_date , 
'' as display_value, s.concept_code as source_concept_value, s.concept_name as source_concept_name
from @tableQualifier.observation o
inner join @tableQualifier.concept c on c.concept_id = o.observation_concept_id
inner join @tableQualifier.concept s on s.concept_id = o.observation_source_concept_id
where person_id = @personId  

union all

select distinct 'visit' as "domain", visit_concept_id concept_id, concept_name, visit_start_date start_date, visit_end_date end_date , '' as display_value, '' as source_concept_value, '' as source_concept_name
from @tableQualifier.visit_occurrence
join @tableQualifier.concept c on c.concept_id = visit_occurrence.visit_concept_id
where person_id = @personId 

union all

select distinct 'death' as "domain", death_type_concept_id concept_id, concept_name, death_date start_date, death_date end_date, '' as display_value, '' as source_concept_value, '' as source_concept_name
from @tableQualifier.death d
join @tableQualifier.concept c on d.death_type_concept_id = c.concept_id
where person_id = @personId

union all

select distinct 'measurement' as "domain", measurement_concept_id concept_id, c.concept_name, measurement_date start_date,
measurement_date end_date, 
concat(value_source_value, ' ', unit_source_value) as display_value,
s.concept_code as source_concept_value,
s.concept_name as source_concept_name
from @tableQualifier.measurement m
inner join @tableQualifier.concept c on m.measurement_concept_id = c.concept_id
inner join @tableQualifier.concept s on s.concept_id = m.measurement_source_concept_id
where person_id = @personId

union all

select distinct 'device' as "domain", device_concept_id concept_id, concept_name, device_exposure_start_date start_date, device_exposure_end_date end_date , '' as display_value, '' as source_concept_value, '' as source_concept_name
from @tableQualifier.device_exposure de
join @tableQualifier.concept c on de.device_concept_id = c.concept_id
where person_id = @personId


union all

select distinct 'procedure' as "domain", procedure_concept_id concept_id, c.concept_name, procedure_date start_date, procedure_date end_date , 
'' as display_value, s.concept_code as source_concept_value, s.concept_name as source_concept_name
from @tableQualifier.procedure_occurrence po
inner join @tableQualifier.concept c on po.procedure_concept_id = c.concept_id
inner join @tableQualifier.concept s on po.procedure_source_concept_id = s.concept_id
where person_id = @personId

union all

select distinct 'specimen' as "domain", specimen_concept_id concept_id, concept_name, specimen_date start_date, specimen_date end_date , '' as display_value, '' as source_concept_value, '' as source_concept_name
from @tableQualifier.specimen s
join @tableQualifier.concept c on s.specimen_concept_id = c.concept_id
where person_id = @personId


