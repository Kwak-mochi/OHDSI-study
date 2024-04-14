----save : t05.csv
--Comorbidities of interest 
--고혈압과 당뇨는 간호기록지에서 가지고 오는 부분 있음

with before_visit as (
    select a.person_id, b.visit_occurrence_id, a.group_hypo
    from cdm_t2 a
    left join cdm_2021.origin_visit_occurrence b
    on a.person_id = b.person_id
    and b.visit_start_datetime <= a.visit_start_datetime
),
diag as (
    select person_id, visit_occurrence_id,
    case when length(condition_source_value) = 12 then substr(condition_source_value,8,3) else substr(condition_source_value,1,4) end as condition_source_value 
    from cdm_2021.origin_condition_occurrence
    where (length(condition_source_value) = 12 or length(condition_source_value) < 6)
    and condition_source_value ~ '[a-zA-Z]'
),
htn as(
    select distinct a.visit_occurrence_id, case when condition_source_value is not null or value_as_string is not null then 'yes' end as htn
    FROM before_visit a
    left join diag b on a.visit_occurrence_id=b.visit_occurrence_id
    and substr(condition_source_value,1,3) in ('I10', 'I11', 'I12', 'I13', 'I15')
    left join cdm_2021.origin_observation c on a.visit_occurrence_id=c.visit_occurrence_id
    and value_as_string like '%고혈압%' --간호기록지
),
DM as (
    select distinct a.visit_occurrence_id, case when condition_source_value is not null or value_as_string is not null then 'yes' end as dm
    FROM before_visit a
    left join diag b on a.visit_occurrence_id=b.visit_occurrence_id
    and substr(condition_source_value,1,3) in ('E10', 'E11', 'E12', 'E13', 'E14')
    left join cdm_2021.origin_observation c on a.visit_occurrence_id=c.visit_occurrence_id
    and value_as_string like '%당뇨%'  --간호기록지
),
heart as (
    select distinct a.visit_occurrence_id, case when condition_source_value is not null then 'yes' end as heart
    FROM before_visit a
    left join diag b on a.visit_occurrence_id=b.visit_occurrence_id
    where substr(condition_source_value,1,3) in ('I50') 
),
ckd as (
    select distinct a.visit_occurrence_id, case when condition_source_value is not null then 'yes' end as ckd
    FROM before_visit a
    left join diag b on a.visit_occurrence_id=b.visit_occurrence_id
    where substr(condition_source_value,1,3) in ('N18')
),
liver as (
    select distinct a.visit_occurrence_id, case when condition_source_value is not null then 'yes' end as liver
    FROM before_visit a
    left join diag b on a.visit_occurrence_id=b.visit_occurrence_id
    where substr(condition_source_value,1,3) in ('K70','K71','K72','K73','K74','K75','K76','K77')
),
cere as (
    select distinct a.visit_occurrence_id, case when condition_source_value is not null then 'yes' end as cere
    FROM before_visit a
    left join diag b on a.visit_occurrence_id=b.visit_occurrence_id
    where substr(condition_source_value,1,3) in ('I60','I61','I62','I63','I64','I65','I66','I67','I68','I69')
    or substr(condition_source_value,1,4) ='G468'
),
meta as (
    select distinct a.visit_occurrence_id, case when condition_source_value is not null then 'yes' end as meta
    FROM before_visit a
    left join diag b on a.visit_occurrence_id=b.visit_occurrence_id
    where substr(condition_source_value,1,3) in ('C77','C78','C79','C80')   
),
hema as (
    select distinct a.visit_occurrence_id, case when condition_source_value is not null then 'yes' end as hema
    FROM before_visit a
    left join diag b on a.visit_occurrence_id=b.visit_occurrence_id
    where substr(condition_source_value,1,3) in ('C81','C82','C83','C84','C85','C88',
                                                 'C90','C91','C92','C93','C94','C95','C96','C97')
),    
solid as (
    select distinct a.visit_occurrence_id, case when condition_source_value is not null then 'yes' end as solid
    FROM before_visit a
    left join diag b on a.visit_occurrence_id=b.visit_occurrence_id
    where substr(condition_source_value,1,2) in ('C0','C1','C6')
    or substr(condition_source_value,1,3) in ('C30','C31','C32','C33','C34','C37','C38','C39',
                                              'C40','C41','C43','C44','C45','C46','C47','C48','C49',
                                              'C50','C51','C52','C53','C54','C55','C56','C57','C58',
                                              'C70','C71','C72','C73','C74','C75','C76')
)

select a.person_id, a.visit_occurrence_id, a.group_hypo,
case when htn.htn is null then 'no' else htn.htn end Hypertension,
case when dm.dm is null then 'no' else dm.dm end Diabetes_mellitus,
case when heart.heart is null then 'no' else heart.heart end Heart_failure,
case when ckd.ckd is null then 'no' else ckd.ckd end Chronic_kidney_disease,
case when liver.liver is null then 'no' else liver.liver end Liver_disease,
case when cere.cere is null then 'no' else cere.cere end Cerebrovascular,
case when meta.meta is null then 'no' else meta.meta end Metastatic_cancer,
case when hema.hema is null then 'no' else hema.hema end Hematologic_malignancy,
case when solid.solid is null then 'no' else solid.solid end solid_tumor
FROM cdm_t2 a
left outer join htn on a.visit_occurrence_id=htn.visit_occurrence_id
left outer join dm on a.visit_occurrence_id=dm.visit_occurrence_id
left outer join heart on a.visit_occurrence_id=heart.visit_occurrence_id
left outer join ckd on a.visit_occurrence_id=ckd.visit_occurrence_id
left outer join liver on a.visit_occurrence_id=liver.visit_occurrence_id
left outer join cere on a.visit_occurrence_id=cere.visit_occurrence_id
left outer join meta on a.visit_occurrence_id=meta.visit_occurrence_id
left outer join hema on a.visit_occurrence_id=hema.visit_occurrence_id
left outer join solid on a.visit_occurrence_id=solid.visit_occurrence_id

