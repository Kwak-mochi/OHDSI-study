----save : t05.csv
--Comorbidities of interest 
--고혈압과 당뇨는 간호기록지에서 가지고 오는 부분 있음

with before_visit_diag as (
    select a.person_id, b.visit_occurrence_id, a.group_hypo, c.condition_source_value as condition_source_value_raw,
    case when length(c.condition_source_value) = 12 then substr(c.condition_source_value,8,2) else substr(c.condition_source_value,1,2) end as condition_source_value_2,
    case when length(c.condition_source_value) = 12 then substr(c.condition_source_value,8,3) else substr(c.condition_source_value,1,3) end as condition_source_value_3,
    case when length(c.condition_source_value) = 12 then substr(c.condition_source_value,8,4) else substr(c.condition_source_value,1,4) end as condition_source_value_4
    
    from cdm_t2 a
    
    left join cdm_2021.origin_visit_occurrence b
    on a.person_id = b.person_id
    and b.visit_start_datetime <= a.visit_start_datetime
    
    left join cdm_2021.origin_condition_occurrence c
    on b.visit_occurrence_id = c.visit_occurrence_id
    and (length(c.condition_source_value) = 12 or length(c.condition_source_value) < 6)
    and condition_source_value ~ '[a-zA-Z]'
)
select distinct a.person_id, a.visit_occurrence_id, a.group_hypo,
case when a.condition_source_value_3 in ('I10', 'I11', 'I12', 'I13', 'I15') or b.value_as_string like '%고혈압%' then 'yes' else 'no' end as Hypertension,
case when a.condition_source_value_3 in ('E10', 'E11', 'E12', 'E13', 'E14') or b.value_as_string like '%당뇨%' then 'yes' else 'no' end as Diabetes_mellitus,
case when a.condition_source_value_3 in ('I50') then 'yes' else 'no' end as Heart_failure,
case when a.condition_source_value_3 in ('N18') then 'yes' else 'no' end as Chronic_kidney_disease,
case when a.condition_source_value_3 in ('K70','K71','K72','K73','K74','K75','K76','K77') then 'yes' else 'no' end as Liver_disease,
case when a.condition_source_value_3 in ('I60','I61','I62','I63','I64','I65','I66','I67','I68','I69') or a.condition_source_value_4 ='G468' then 'yes' else 'no' end as Cerebrovascular,
case when a.condition_source_value_3 in ('C77','C78','C79','C80') then 'yes' else 'no' end as Metastatic_cancer,
case when a.condition_source_value_3 in ('C81','C82','C83','C84','C85','C88','C90','C91','C92','C93','C94','C95','C96','C97') then 'yes' else 'no' end as Hematologic_malignancy,
case when a.condition_source_value_2 in ('C0','C1','C6') or
a.condition_source_value_3 in ('C30','C31','C32','C33','C34','C37','C38','C39','C40','C41','C43','C44','C45','C46','C47','C48','C49',
                               'C50','C51','C52','C53','C54','C55','C56','C57','C58','C70','C71','C72','C73','C74','C75','C76') then 'yes' else 'no' end as solid_tumor

from before_visit_diag a
left join cdm_2021.origin_observation b 
on a.visit_occurrence_id=b.visit_occurrence_id
