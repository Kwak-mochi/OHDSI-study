----save : t04.csv
--qSOFA, Sepsis여부 확인
--cdm_qsofa 이용
--cdm_antibiotics 이용

with pre_qsofa as (
    select a.group_hypo, a.person_id, a.drug_exposure_start_datetime, b.* 

    from cdm_t2 a

    join cdm_qsofa b
    on a.visit_occurrence_id=b.visit_occurrence_id
    and b.measurement_datetime - a.drug_exposure_start_datetime < '24:00:00'
    and a.drug_exposure_start_datetime - b.measurement_datetime < '24:00:00'
    and (sbp is not null or rr is not null or gcs is not null)
),
pre_antibio as (
    select b.*

    from cdm_t2 a

    join cdm_antibiotics b
    on a.visit_occurrence_id = b.visit_occurrence_id
    and a.drug_exposure_start_datetime - b.drug_exposure_start_datetime < '24:00:00'
    and b.drug_exposure_start_datetime - a.drug_exposure_start_datetime < '24:00:00'
)
select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
b.sbp_c, c.rr_c, d.gcs_c, b.sbp_c+c.rr_c+d.gcs_c as qSOFA,
case when e.visit_occurrence_id is not null then 'yes' else 'no' end as antibio,
case when f.condition_source_value is not null then 1 else 0 end as dig_sepsis,
case when f.condition_source_value is not null or (b.sbp_c+c.rr_c+d.gcs_c > 1 and e.visit_occurrence_id is not null) then 'yes' else 'no' end as sepsis
    
from cdm_t2 a

left join (select *, rank()over(partition by visit_occurrence_id order by extract(epoch from drug_exposure_start_datetime - measurement_datetime)) rank from pre_qsofa
           where sbp is not null) b
on a.visit_occurrence_id = b.visit_occurrence_id and b.rank=1
    
left join (select *, rank()over(partition by visit_occurrence_id order by extract(epoch from drug_exposure_start_datetime - measurement_datetime)) rank from pre_qsofa
           where rr is not null) c
on a.visit_occurrence_id = c.visit_occurrence_id and c.rank=1
           
left join (select *, rank()over(partition by visit_occurrence_id order by extract(epoch from drug_exposure_start_datetime - measurement_datetime)) rank from pre_qsofa
           where gcs is not null) d
on a.visit_occurrence_id = d.visit_occurrence_id and d.rank=1      

left join (select distinct visit_occurrence_id from pre_antibio) e
on a.visit_occurrence_id = e.visit_occurrence_id

left join cdm_2021.origin_condition_occurrence f
on a.visit_occurrence_id=f.visit_occurrence_id
and (f.condition_source_value like 'A40%' or f.condition_source_value like 'A41%')

