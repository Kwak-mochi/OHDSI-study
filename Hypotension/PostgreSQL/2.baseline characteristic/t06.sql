
----save : t06.csv
--vital sign
--cdm_vital 이용

with pre_vital as (
    select a.group_hypo, a.person_id, a.drug_exposure_start_datetime, b.*
    
    from cdm_t2 a
    left join public.cdm_vital b
    on a.visit_occurrence_id = b.visit_occurrence_id
    and b.measurement_datetime < a.drug_exposure_start_datetime
)

select a.group_hypo, a.person_id, a.visit_occurrence_id,
b.mbp, c.pr, d.rr, e.bt, f.spo2

from cdm_t2 a
left join (select *, rank()over(partition by visit_occurrence_id order by extract(epoch from drug_exposure_start_datetime - measurement_datetime)) rank from pre_vital 
           where mbp is not null) b
           on a.visit_occurrence_id = b.visit_occurrence_id and b.rank=1

left join (select *, rank()over(partition by visit_occurrence_id order by extract(epoch from drug_exposure_start_datetime - measurement_datetime)) rank from pre_vital 
           where pr is not null) c
           on a.visit_occurrence_id = c.visit_occurrence_id and c.rank=1

left join (select *, rank()over(partition by visit_occurrence_id order by extract(epoch from drug_exposure_start_datetime - measurement_datetime)) rank from pre_vital 
           where rr is not null) d
           on a.visit_occurrence_id = d.visit_occurrence_id and d.rank=1

left join (select *, rank()over(partition by visit_occurrence_id order by extract(epoch from drug_exposure_start_datetime - measurement_datetime)) rank from pre_vital 
           where bt is not null) e
           on a.visit_occurrence_id = e.visit_occurrence_id and e.rank=1

left join (select *, rank()over(partition by visit_occurrence_id order by extract(epoch from drug_exposure_start_datetime - measurement_datetime)) rank from pre_vital 
           where spo2 is not null) f
           on a.visit_occurrence_id = f.visit_occurrence_id and f.rank=1    


