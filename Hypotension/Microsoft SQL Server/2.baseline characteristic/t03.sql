----save : t03.csv
--Use of vasopressor before IV
--surgery before index IV
--index IV in ICU

select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
case when drug_concept_id_bf is not null then 'yes' else 'no' end as drug_bfiv,
a.drug_concept_id_bf, a.drug_source_value_bf,
case when b.procedure_occurrence_id is not null then 'yes' else 'no' end as surgery_bfiv,
case when c.visit_detail_source_value is not null then 'yes' else 'no' end as ICU,
c.visit_detail_source_value

from cdm_t2 a

left join cdm_2021.origin_procedure_occurrence b
on a.visit_occurrence_id = b.visit_occurrence_id
and b.procedure_concept_id in (select descendant_concept_id from cdm_voca_2021.concept_ancestor where ancestor_concept_id =4301351)
and a.visit_start_datetime < b.procedure_datetime
and b.procedure_datetime < a.drug_exposure_start_datetime

left join cdm_2021.origin_visit_detail c 
on a.visit_occurrence_id = c.visit_occurrence_id
and cast(c.visit_detail_start_datetime as date) < cast(a.drug_exposure_start_datetime as date)
and cast(a.drug_exposure_start_datetime as date) < cast(c.visit_detail_end_datetime as date)
and c.visit_detail_source_value like '%ICU%';