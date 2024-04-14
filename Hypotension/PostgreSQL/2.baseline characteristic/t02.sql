----save : t02.csv
--cci
--save 후 python을 통해 cci score 계산
with before_visit as (
select a.person_id, b.visit_occurrence_id, a.group_hypo

from cdm_t2 a
    
left join cdm_2021.origin_visit_occurrence b
on a.person_id = b.person_id
and b.visit_start_datetime < a.visit_start_datetime
)

select a.group_hypo, a.person_id, a.visit_occurrence_id,
case when length(c.condition_source_value) = 12 then substr(c.condition_source_value,8,3) else substr(condition_source_value,1,3) end as condition_source_value 

from cdm_t2 a

left join before_visit b 
on a.person_id = b.person_id

left join cdm_2021.origin_condition_occurrence c
on b.visit_occurrence_id=c.visit_occurrence_id
where (length(c.condition_source_value) = 12 or length(c.condition_source_value) < 6)
and c.condition_source_value ~ '[a-zA-Z]'

