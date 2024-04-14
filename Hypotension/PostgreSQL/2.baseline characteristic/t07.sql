----save : t07.csv
--lab(48h)
--cdm_lab 이용

with pre_lab_48 as (
    select a.group_hypo, a.person_id, b.*
    from cdm_t2 a
    left join public.cdm_LAB b
    on a.visit_occurrence_id = b.visit_occurrence_id
    and a.drug_exposure_start_datetime - b.measurement_datetime <'48:00:00'
    and b.measurement_datetime - a.drug_exposure_start_datetime <'48:00:00'
)
select a.group_hypo, a.person_id, a.visit_occurrence_id,
b.ph ph_48, c.pco2 pco2_48, d.po2 po2_48, e.hco3 hco3_48, f.crp crp_48,
g.lactate lactate_48, h.wbc wbc_48, i.hemoglobin hemoglobin_48, j.platelet platelet_48, k.creatinine creatinine_48,
o.bilirubin bilirubin_48, p.bun bun_48
from cdm_t2 a
left join (select b.*,rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a 
           join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.ph is not null) b on a.visit_occurrence_id = b.visit_occurrence_id and b.rank=1
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.pco2 is not null) c on a.visit_occurrence_id = c.visit_occurrence_id and c.rank=1
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.po2 is not null) d on a.visit_occurrence_id = d.visit_occurrence_id and d.rank=1    
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.hco3 is not null) e on a.visit_occurrence_id = e.visit_occurrence_id and e.rank=1
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.crp is not null) f on a.visit_occurrence_id = f.visit_occurrence_id and f.rank=1
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.lactate is not null) g on a.visit_occurrence_id = g.visit_occurrence_id and g.rank=1          
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.wbc is not null) h on a.visit_occurrence_id = h.visit_occurrence_id and h.rank=1
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.hemoglobin is not null) i on a.visit_occurrence_id = i.visit_occurrence_id and i.rank=1
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.platelet is not null) j on a.visit_occurrence_id = j.visit_occurrence_id and j.rank=1
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.creatinine is not null) k on a.visit_occurrence_id = k.visit_occurrence_id and k.rank=1
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.bilirubin is not null) o on a.visit_occurrence_id = o.visit_occurrence_id and o.rank=1
left join (select b.*, rank()over (partition by b.visit_occurrence_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.visit_occurrence_id = b.visit_occurrence_id
          where b.bilirubin is not null) p on a.visit_occurrence_id = p.visit_occurrence_id and p.rank=1



