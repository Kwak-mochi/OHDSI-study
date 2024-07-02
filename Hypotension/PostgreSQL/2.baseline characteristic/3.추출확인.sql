--이 단계는 추출된 Cohort로 부터 baseline characteristic talbe을 그리기 위한 단계입니다.

--cdm_vital, cdm_lab, cdm_antibiotics table이 우선 생성 되어있어야 합니다.
--이 단계에서 각 테이블결과는 따로 저장됩니다.
--t01.csv
--t02.csv → Python을 통해 t02_cci.csv를 생성
--t03.csv
--t04.csv
--t05.csv
--t06.csv
--t07.csv

--raw
select * from cdm_t2

---- save : t01.csv
--index year
--Age, years
--Sex, Men
--Height, cm : 3036277
--Weight, kg : 3013762
--BMI : R에서 계산

with measure as (
    select a.person_id, b.measurement_concept_id, b.value_as_number, 
    rank()over(partition by a.person_id, b.measurement_concept_id order by abs(extract(epoch from b.measurement_datetime-a.drug_exposure_start_datetime))) rank
    from cdm_t2 a
    join cdm_2021.origin_measurement b on a.person_id=b.person_id
    where b.measurement_concept_id in (3036277, 3013762)
) 

select distinct a.person_id, a.VISIT_OCCURRENCE_ID, a.GROUP_HYPO, EXTRACT(year from a.drug_exposure_start_datetime) index_year,
EXTRACT(year from a.drug_exposure_start_datetime)-b.year_of_birth age,
b.GENDER_SOURCE_VALUE sex, c.value_as_number height, d.value_as_number weight

from cdm_t2 a

left join cdm_2021.origin_person b 
on a.PERSON_ID = b.person_id

left join measure c 
on a.person_id = c.person_id 
and c.measurement_concept_id = 3036277 and c.rank = 1

left join measure d
on a.person_id = d.person_id
and d.measurement_concept_id = 3013762 and d.rank = 1



----save : t02.csv
--cci
--save 후 python을 통해 cci score 계산
with before_visit_condition as (
    select a.group_hypo, a.person_id, b.visit_occurrence_id,
    case when length(c.condition_source_value) = 12 then substr(c.condition_source_value,8,3) else substr(condition_source_value,1,3) end as condition_source_value
    
    from cdm_t2 a
    left join cdm_2021.origin_visit_occurrence b
    on a.person_id = b.person_id
    and b.visit_start_datetime < a.visit_start_datetime
    
    left join cdm_2021.origin_condition_occurrence c
    on b.visit_occurrence_id=c.visit_occurrence_id
    where (length(c.condition_source_value) = 12 or length(c.condition_source_value) < 6)
    and c.condition_source_value ~ '[a-zA-Z]'
)

select a.group_hypo, a.person_id, a.visit_occurrence_id, b.condition_source_value
 
from cdm_t2 a

left join before_visit_condition b 
on a.person_id = b.person_id







----save : t03.csv
--Use of vasopressor before IV
--surgery before index IV
--index IV in ICU
--ICU code

select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
case when a.drug_concept_id_bf is not null then 'yes' else 'no' end as drug_bfiv,
a.drug_concept_id_bf, a.drug_source_value_bf,
case when b.procedure_occurrence_id is not null then 'yes' else 'no' end as surgery_bfiv,
case when c.visit_detail_source_value is not null then 'yes' else 'no' end as ICU,
c.visit_detail_source_value

from cdm_t2 a

left join cdm_2021.origin_procedure_occurrence b
on a.visit_occurrence_id = b.visit_occurrence_id
and b.procedure_concept_id in (select descendant_concept_id from cdm_voca_2021.concept_ancestor where ancestor_concept_id =4301351)
and a.visit_start_datetime< b.procedure_datetime
and b.procedure_datetime < a.drug_exposure_start_datetime

left join cdm_2021.origin_visit_detail c 
on a.visit_occurrence_id = c.visit_occurrence_id
and to_date(to_char(c.visit_detail_start_datetime,'yyyy-mm-dd'),'yyyy-mm-dd') < to_date(to_char(a.drug_exposure_start_datetime,'yyyy-mm-dd'),'yyyy-mm-dd')
and to_date(to_char(a.drug_exposure_start_datetime,'yyyy-mm-dd'),'yyyy-mm-dd') < to_date(to_char(c.visit_detail_end_datetime,'yyyy-mm-dd'),'yyyy-mm-dd')
and c.visit_detail_source_value like '%ICU%'



----save : t04.csv


--sepsis 재정의
with bloodculture as ( --1.1 blood culture 시행 여부
    select distinct a.group_hypo, a.person_id, a.visit_occurrence_id, b.measurement_date,
    case when b.measurement_concept_id = 40770955 then 'yes' end as bloodculture,
    b.measurement_date - to_date(to_char(a.visit_start_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') as visit_blood_day
    
    from cdm_t2 a
    join cdm_2021.origin_measurement b
    on a.person_id = b.person_id
    and b.measurement_concept_id = 40770955  --blood culture
    and to_date(to_char(a.drug_exposure_start_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') - b.measurement_date < 3
    and to_date(to_char(a.drug_exposure_start_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') - b.measurement_date > -3
),
bloodculture_antibio as (
    select distinct a.group_hypo, a.person_id, a.visit_occurrence_id, b.drug_exposure_start_date, b.drug_exposure_end_date, b.drug_days, 
    b.drug_exposure_start_date - a.measurement_date as dayterm
    from bloodculture a
    join cdm_antibiotics b
    on a.person_id = b.person_id
    and b.drug_exposure_start_date - a.measurement_date > -3
    and b.drug_exposure_start_date - a.measurement_date < 6
),
daydb as (
    select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
    case when b.condition_source_value is not null then 'yes' end as CKD,
    case when c.dayterm = -2 then 'yes' end as d_minus2,
    case when d.dayterm = -1 or (c.dayterm = -2 and c.drug_days>0) then 'yes' end as d_minus1,
    case when e.dayterm = 0 or (c.dayterm = -2 and c.drug_days>1) or (d.dayterm = -1 and d.drug_days>0) then 'yes' end as d_0,
    case when f.dayterm = 1 or (c.dayterm = -2 and c.drug_days>2) or (d.dayterm = -1 and d.drug_days>1) or (e.dayterm = 0 and e.drug_days>0) then 'yes' end as d_1,
    case when g.dayterm = 2 or (c.dayterm = -2 and c.drug_days>3) or (d.dayterm = -1 and d.drug_days>2) or (e.dayterm = 0 and e.drug_days>1) or (f.dayterm = 1 and f.drug_days>0)then 'yes' end as d_2,
    case when h.dayterm = 3 or (c.dayterm = -2 and c.drug_days>4) or (d.dayterm = -1 and d.drug_days>3) or (e.dayterm = 0 and e.drug_days>2) or (f.dayterm = 1 and f.drug_days>1) or (g.dayterm = 2 and g.drug_days>0)then 'yes' end as d_3,
    case when i.dayterm = 4 or (c.dayterm = -2 and c.drug_days>5) or (d.dayterm = -1 and d.drug_days>4) or (e.dayterm = 0 and e.drug_days>3) or (f.dayterm = 1 and f.drug_days>2) or (g.dayterm = 2 and g.drug_days>1) or (h.dayterm = 3 and h.drug_days>0)then 'yes' end as d_4,
    case when j.dayterm = 5 or (c.dayterm = -2 and c.drug_days>6) or (d.dayterm = -1 and d.drug_days>5) or (e.dayterm = 0 and e.drug_days>4) or (f.dayterm = 1 and f.drug_days>3) or (g.dayterm = 2 and g.drug_days>2) or (h.dayterm = 3 and h.drug_days>1)or (i.dayterm = 4 and h.drug_days>0)then 'yes' end as d_5
    from bloodculture a
    left join cdm_2021.origin_condition_occurrence b 
    on a.visit_occurrence_id = b.visit_occurrence_id
    and b.condition_source_value ~ '[a-zA-Z]' 
    and((length(b.condition_source_value) = 12 and substr(b.condition_source_value,8,2) = 'N18') or (length(b.condition_source_value) < 6 and substr(b.condition_source_value,1,2) = 'N18'))
    
    left join bloodculture_antibio c on a.person_id = c.person_id and c.dayterm=-2
    left join bloodculture_antibio d on a.person_id = d.person_id and d.dayterm=-1
    left join bloodculture_antibio e on a.person_id = e.person_id and e.dayterm=0
    left join bloodculture_antibio f on a.person_id = f.person_id and f.dayterm=1
    left join bloodculture_antibio g on a.person_id = g.person_id and g.dayterm=2
    left join bloodculture_antibio h on a.person_id = h.person_id and h.dayterm=3
    left join bloodculture_antibio i on a.person_id = i.person_id and i.dayterm=4
    left join bloodculture_antibio j on a.person_id = j.person_id and j.dayterm=5
),
c1_2 as (--1.2 blood culture 시행 후 +-2일내에 항생제 사용 여부
    select group_hypo, person_id, visit_occurrence_id,
    case when (CKD is null and ( --신장질환 없는 경우, 연속4일
        (d_minus2 = 'yes' and d_minus1 = 'yes' and d_0 = 'yes' and d_1 = 'yes') or
        (d_minus1 = 'yes' and d_0 = 'yes' and d_1 = 'yes' and d_2 = 'yes') or
        (d_0 = 'yes' and d_1 = 'yes' and d_2 = 'yes' and d_3 = 'yes') or
        (d_1 = 'yes' and d_2 = 'yes' and d_3 = 'yes' and d_4 = 'yes') or
        (d_2 = 'yes' and d_3 = 'yes' and d_4 = 'yes' and d_5 = 'yes'))) or
    (CKD is not null and (  --신장질환 있는 경우, 퐁당퐁당
        (d_minus2 = 'yes' and d_minus1 is null and d_0 = 'yes') or
        (d_minus1 = 'yes' and d_0 is null and d_1 = 'yes') or
        (d_0 = 'yes' and d_1 is null and d_2 = 'yes') or
        (d_1 = 'yes' and d_2 is null and d_3 = 'yes') or
        (d_2 = 'yes' and d_3 is null and d_4 = 'yes') or
        (d_3 = 'yes' and d_4 is null and d_5 = 'yes'))
    )then 'yes' end as bloodculture_antibiotics
    from daydb
),
c2_1 as (--2.1 vasopressor 새로 시작했는지 여부
    select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
    case when b.drug_concept_id is not null and c.drug_concept_id is null then 'yes' end as vasopressor
    
    from bloodculture a
    join cdm_vaso b
    on a.person_id = b.person_id
    and a.measurement_date - b.drug_exposure_start_date < 3
    and a.measurement_date - b.drug_exposure_start_date > -3

    left join cdm_vaso c -- 하루전엔 맞았는지 확인
    on a.person_id = c.person_id
    and b.drug_exposure_start_date - 1 = c.drug_exposure_start_date
),
c2_2 as (--2.2 ventilation 여부
    select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
    case when b.procedure_concept_id is not null then 'yes' end as ventilation
    
    from bloodculture a
    join cdm_2021.procedure_occurrence b
    on a.person_id = b.person_id
    and a.measurement_date - b.procedure_date < 3
    and a.measurement_date - b.procedure_date > -3
    and b.procedure_concept_id in (4107670)
),
c2_3 as (--2.3 만성신장병제외, serum creatinine 2배 또는 eGFR 50%감소
    select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
    case when (a.visit_blood_day<3 and b_1.creatinine*2 <= c.creatinine) or (a.visit_blood_day>2 and b_2.creatinine*2 <= c.creatinine) then 'yes' end as double_creatinine,
    case when (a.visit_blood_day<3 and d_1.egfr*0.5 >= e.egfr) or (a.visit_blood_day>2 and d_2.egfr*0.5 >= e.egfr) then 'yes' end as reduce_egfr,
    case when (a.visit_blood_day<3 and b_1.creatinine*2 <= c.creatinine) or (a.visit_blood_day>2 and b_2.creatinine*2 <= c.creatinine) 
    or (a.visit_blood_day<3 and d_1.egfr*0.5 >= e.egfr) or (a.visit_blood_day>2 and d_2.egfr*0.5 >= e.egfr) then 'yes' end as acute_renal_failure,
    case when (((a.visit_blood_day<3 and b_1.creatinine*2 <= c.creatinine) or (a.visit_blood_day>2 and b_2.creatinine*2 <= c.creatinine)) and ((a.visit_blood_day<3 and d_1.egfr*0.5 >= e.egfr) or (a.visit_blood_day>2 and d_2.egfr*0.5 >= e.egfr))) then 2
    when (a.visit_blood_day<3 and b_1.creatinine*2 <= c.creatinine) or (a.visit_blood_day>2 and b_2.creatinine*2 <= c.creatinine) then 1
    when (a.visit_blood_day<3 and d_1.egfr*0.5 >= e.egfr) or (a.visit_blood_day>2 and d_2.egfr*0.5 >= e.egfr) then 1
    else 0 end as count_yes

    --만성신장병제외
    from (select a.* 
          from bloodculture a
          join cdm_2021.origin_condition_occurrence b 
          on a.visit_occurrence_id = b.visit_occurrence_id
          and b.condition_source_value ~ '[a-zA-Z]'
          and ((length(b.condition_source_value) = 12 and substr(b.condition_source_value,8,2) != 'N18') or
               (length(b.condition_source_value) < 6 and substr(condition_source_value,1,2) != 'N18'))
         ) a
    
    --baseline_1(입원일<3일) : Creatinine
    left join (select distinct person_id,visit_occurrence_id, creatinine, rank()over(partition by person_id order by creatinine) rank
               from cdm_lab where creatinine is not null) b_1 
    on a.visit_occurrence_id = b_1.visit_occurrence_id and b_1.rank=1 
    
    --baseline_2(입원일>2일) : Creatinine
    left join (select distinct a.person_id, b.creatinine, rank()over(partition by a.person_id order by b.creatinine) rank
               from bloodculture a
               join cdm_lab b
               on a.visit_occurrence_id = b.visit_occurrence_id
               and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') >= 0
               and b.creatinine is not null) b_2 
    on a.person_id = b_2.person_id and b_2.rank=1 
    
    --bloodculture : Creatinine
    left join (select distinct a.person_id, b.creatinine, rank()over(partition by a.person_id order by b.creatinine desc) rank
               from bloodculture a
               join cdm_lab b
               on a.person_id = b.person_id
               and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') < 3
               and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') > -3
               and b.creatinine is not null
         ) c
    on a.person_id = c.person_id and c.rank=1 
    
    --baseline_1(입원일<3일) : eGFR
    left join (select distinct person_id,visit_occurrence_id, VALUE_AS_NUMBER egfr, rank()over(partition by person_id order by VALUE_AS_NUMBER desc) rank
              from CDM_2021.origin_MEASUREMENT b where measurement_concept_id = 46236952 and VALUE_AS_NUMBER is not null) d_1
    on a.visit_occurrence_id = d_1.visit_occurrence_id and d_1.rank=1 

    --baseline_2(입원일>2일) : eGFR
    left join (select distinct a.person_id, b.VALUE_AS_NUMBER egfr, rank()over(partition by a.person_id order by b.VALUE_AS_NUMBER desc) rank
              from bloodculture a
               join CDM_2021.origin_MEASUREMENT b 
               on a.visit_occurrence_id = b.visit_occurrence_id 
               and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') >= 0
               and measurement_concept_id = 46236952 and VALUE_AS_NUMBER is not null) d_2
    on a.person_id = d_2.person_id and d_2.rank=1 
    
    --bloodculture : eGFR
    left join (select distinct a.person_id, b.VALUE_AS_NUMBER egfr, rank()over(partition by a.person_id order by b.VALUE_AS_NUMBER) rank
              from bloodculture a
              join CDM_2021.origin_MEASUREMENT b on a.person_id=b.person_id and measurement_concept_id = 46236952
              and a.measurement_date - b.measurement_date < 3
              and a.measurement_date - b.measurement_date > -3
               and b.VALUE_AS_NUMBER is not null
              ) e
    on a.person_id = e.person_id and e.rank=1 
),
c2_4 as ( --bilirubine 2이상 그리고 50%증가
    select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
    case when c.bilirubin >= 2 and ((a.visit_blood_day<3 and c.bilirubin >= b_1.bilirubin*1.5) or (a.visit_blood_day>2 and c.bilirubin >= b_2.bilirubin*1.5)) 
    then 'yes' end as hyperbilirubinemia
    
    from bloodculture a
    
    --baseline_1(입원일<3일) : bilirubin
    left join (select distinct person_id,visit_occurrence_id, bilirubin, rank()over(partition by person_id order by bilirubin) rank 
          from cdm_lab where bilirubin is not null) b_1
    on a.visit_occurrence_id = b_1.visit_occurrence_id and b_1.rank=1 

    --baseline_2(입원일>2일) : bilirubin
    left join (select distinct a.person_id, b.bilirubin, rank()over(partition by a.person_id order by b.bilirubin) rank 
          from bloodculture a
          join cdm_lab b
          on a.visit_occurrence_id = b.visit_occurrence_id
          and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') >= 0) b_2
    on a.person_id = b_2.person_id and b_2.rank=1 
    
    --bloodculture : bilirubin
    left join (select distinct a.person_id, b.bilirubin, rank()over(partition by a.person_id order by b.bilirubin desc) rank
          from bloodculture a
          join cdm_lab b
          on a.person_id = b.person_id
          and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') < 3
          and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') > -3
               and b.bilirubin is not null
         ) c
    on a.person_id = c.person_id and c.rank=1 
),
c2_5 as ( --thrombocytopenia
    select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
    case when c.platelet <100 and ((a.visit_blood_day<3 and c.platelet <= b_1.platelet*0.5) or (a.visit_blood_day>2 and c.platelet <= b_2.platelet*0.5))then 'yes' end as thrombocytopenia
    
    from bloodculture a
    
    --baseline_1(입원일<3일) : platelet
    left join (select distinct person_id,visit_occurrence_id, platelet, rank()over(partition by visit_occurrence_id order by platelet desc) rank
          from cdm_lab where platelet is not null) b_1
    on a.visit_occurrence_id = b_1.visit_occurrence_id and b_1.rank=1 
    
    --baseline_2(입원일>2일) : platelet
    left join (select distinct a.person_id, b.platelet, rank()over(partition by a.person_id order by b.platelet desc) rank 
          from bloodculture a
          join cdm_lab b
          on a.visit_occurrence_id = b.visit_occurrence_id
          and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') >= 0) b_2
    on a.person_id = b_2.person_id and b_2.rank=1 
    
    --bloodculture : platelet
    left join (select distinct a.person_id, b.platelet, rank()over(partition by a.person_id order by b.platelet) rank
          from bloodculture a
          join cdm_lab b
          on a.person_id = b.person_id
          and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') < 3
          and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') > -3
               and b.platelet is not null
         ) c
    on a.person_id = c.person_id and c.rank=1         
),
c2_6 as ( --serum lactate >= 2
    select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
    case when b.lactate >=2 then 'yes' end as lactate
    
    from bloodculture a
    left join (select distinct a.person_id, b.lactate, rank()over(partition by a.person_id order by b.lactate desc) rank
          from bloodculture a
          join cdm_lab b
          on a.person_id = b.person_id
          and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') < 3
          and a.measurement_date - to_date(to_char(b.measurement_datetime, 'YYYY-mm-dd'), 'YYYY-mm-dd') > -3
               and b.lactate is not null
         ) b
    on a.person_id = b.person_id and b.rank=1 
),
pre_antibio as (
    select b.*,
    case when b.drug_exposure_start_datetime is not null then 'yes' end as antibio
    from cdm_t2 a
    join cdm_antibiotics b
    on a.person_id = b.person_id
    and a.drug_exposure_start_datetime - b.drug_exposure_start_datetime < '24:00:00'
    and b.drug_exposure_start_datetime - a.drug_exposure_start_datetime < '24:00:00'
),
dig_sepsis as (
    select a.person_id, case when b.condition_source_value is not null then 'yes' end as dig_sepsis
    from cdm_t2 a
    join cdm_2021.origin_condition_occurrence b
    on a.visit_occurrence_id=b.visit_occurrence_id
    and (b.condition_source_value like 'A40%' or b.condition_source_value like 'A41%')
)
select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
b.bloodculture, c.bloodculture_antibiotics,
d.vasopressor, e.ventilation, f.double_creatinine, f.reduce_egfr, f.acute_renal_failure,
g.hyperbilirubinemia, h.thrombocytopenia, i.lactate,
case when b.bloodculture = 'yes' and c.bloodculture_antibiotics = 'yes' then 'yes' end as infection,
case when d.vasopressor = 'yes' or e.ventilation = 'yes' or f.acute_renal_failure ='yes' or g.hyperbilirubinemia = 'yes' or h.thrombocytopenia = 'yes' or i.lactate = 'yes'
then 'yes' end as organ_dysfunction,
case when b.bloodculture = 'yes' and c.bloodculture_antibiotics = 'yes' and (d.vasopressor = 'yes' or e.ventilation = 'yes' or f.acute_renal_failure ='yes' or g.hyperbilirubinemia = 'yes' or h.thrombocytopenia = 'yes' or i.lactate = 'yes')
then 'yes' end as new_sepsis, k.dig_sepsis,
case when k.dig_sepsis = 'yes' or ((b.bloodculture = 'yes' and c.bloodculture_antibiotics = 'yes') and (d.vasopressor = 'yes' or e.ventilation = 'yes' or f.acute_renal_failure ='yes' or g.hyperbilirubinemia = 'yes' or h.thrombocytopenia = 'yes' or i.lactate = 'yes'))
then 'yes' end as sepsis,
j.antibio
from cdm_t2 a

left join bloodculture b on a.person_id = b.person_id and b.bloodculture = 'yes'
left join c1_2 c on a.person_id = c.person_id and c.bloodculture_antibiotics = 'yes'
left join c2_1 d on a.person_id = d.person_id and d.vasopressor = 'yes'
left join c2_2 e on a.person_id = e.person_id and e.ventilation = 'yes'
left join (select *,rank()over(partition by person_id order by count_yes desc) rank_f from c2_3) f on a.person_id = f.person_id and f.acute_renal_failure = 'yes' and f.rank_f=1
left join c2_4 g on a.person_id = g.person_id and g.hyperbilirubinemia = 'yes'
left join c2_5 h on a.person_id = h.person_id and h.thrombocytopenia = 'yes'
left join c2_6 i on a.person_id = i.person_id and i.lactate = 'yes'
left join pre_antibio j on a.person_id = j.person_id
left join dig_sepsis k on a.person_id = k.person_id

----save : t05.csv
--Comorbidities of interest 
--고혈압과 당뇨는 간호기록지에서 가지고 오는 부분 있음

with before_visit_diag as (
    select a.person_id, b.visit_occurrence_id, a.group_hypo,
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
, htn_1 as (
    select distinct person_id, 'yes' as di from before_visit_diag
    where condition_source_value_3 in ('I10', 'I11', 'I12', 'I13', 'I15')
), htn_2 as (
    select distinct a.person_id, 'yes' as di from before_visit_diag a
    join cdm_2021.origin_observation b on a.visit_occurrence_id=b.visit_occurrence_id and b.value_as_string like '%고혈압%'
), htn as (
    select person_id, di from htn_1
    union
    select person_id, di from htn_2
), dm_1 as (
    select distinct person_id, 'yes' as di from before_visit_diag
    where condition_source_value_3 in ('E10', 'E11', 'E12', 'E13', 'E14')
), dm_2 as (
    select distinct a.person_id, 'yes' as di from before_visit_diag a
    join cdm_2021.origin_observation b on a.visit_occurrence_id=b.visit_occurrence_id and b.value_as_string like '%당뇨%'
), dm as (
    select person_id, di from dm_1
    union
    select person_id, di from dm_2
), hf as (
    select distinct person_id, 'yes' as di from before_visit_diag
    where condition_source_value_3 in ('I50')
), ckd as (
    select distinct person_id, 'yes' as di from before_visit_diag 
    where condition_source_value_3 in ('N18')
), ld as (
    select distinct person_id, 'yes' as di from before_visit_diag 
    where condition_source_value_3 in ('K70','K71','K72','K73','K74','K75','K76','K77')
), ce as (
    select distinct person_id, 'yes' as di from before_visit_diag 
    where condition_source_value_3 in ('I60','I61','I62','I63','I64','I65','I66','I67','I68','I69') or condition_source_value_4 ='G468'
), mc as (
    select distinct person_id, 'yes' as di from before_visit_diag 
    where condition_source_value_3 in ('C77','C78','C79','C80')
), hm as (
    select distinct person_id, 'yes' as di from before_visit_diag 
    where condition_source_value_3 in ('C81','C82','C83','C84','C85','C88','C90','C91','C92','C93','C94','C95','C96','C97')
), st as (
    select distinct person_id, 'yes' as di from before_visit_diag 
    where condition_source_value_2 in ('C0','C1','C6')
    or condition_source_value_3 in ('C30','C31','C32','C33','C34','C37','C38','C39','C40','C41','C43','C44','C45','C46','C47','C48','C49',
                               'C50','C51','C52','C53','C54','C55','C56','C57','C58','C70','C71','C72','C73','C74','C75','C76')
)
select distinct a.person_id, a.VISIT_OCCURRENCE_ID, a.GROUP_HYPO,
htn.di as Hypertension, dm.di as Diabetes_mellitus, hf.di as Heart_failure,
ckd.di as Chronic_kidney_disease, ld.di as Liver_disease, ce.di as Cerebrovascular,
mc.di as Metastatic_cancer, hm.di as Hematologic_malignancy, st.di as solid_tumor
from cdm_t2 a
left join htn on a.person_id=htn.person_id
left join dm on a.person_id=dm.person_id
left join hf on a.person_id=hf.person_id
left join ckd on a.person_id=ckd.person_id
left join ld on a.person_id=ld.person_id
left join ce on a.person_id=ce.person_id
left join mc on a.person_id=mc.person_id
left join hm on a.person_id=hm.person_id
left join st on a.person_id=st.person_id



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

select distinct a.group_hypo, a.person_id, a.visit_occurrence_id,
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




----save : t07.csv
--lab(48h)
--cdm_lab 이용

with pre_lab_48 as (
    select a.group_hypo, b.*
    from cdm_t2 a
    left join public.cdm_LAB b
    on a.person_id = b.person_id
    and a.drug_exposure_start_datetime - b.measurement_datetime <'48:00:00'
    and b.measurement_datetime - a.drug_exposure_start_datetime <'48:00:00'
)
select a.group_hypo, a.person_id, a.visit_occurrence_id,
b.ph ph_48, c.pco2 pco2_48, d.po2 po2_48, e.hco3 hco3_48, f.crp crp_48,
g.lactate lactate_48, h.wbc wbc_48, i.hemoglobin hemoglobin_48, j.platelet platelet_48, k.creatinine creatinine_48,
o.bilirubin bilirubin_48, p.bun bun_48
from cdm_t2 a
left join (select b.*,rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.ph is not null) b on a.person_id = b.person_id and b.rank=1
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.pco2 is not null) c on a.person_id = c.person_id and c.rank=1
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.po2 is not null) d on a.person_id = d.person_id and d.rank=1    
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.hco3 is not null) e on a.person_id = e.person_id and e.rank=1
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.crp is not null) f on a.person_id = f.person_id and f.rank=1
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.lactate is not null) g on a.person_id = g.person_id and g.rank=1          
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.wbc is not null) h on a.person_id = h.person_id and h.rank=1
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.hemoglobin is not null) i on a.person_id = i.person_id and i.rank=1
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.platelet is not null) j on a.person_id = j.person_id and j.rank=1
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.creatinine is not null) k on a.person_id = k.person_id and k.rank=1
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.bilirubin is not null) o on a.person_id = o.person_id and o.rank=1
left join (select b.*, rank()over (partition by b.person_id order by abs(extract(epoch from a.drug_exposure_start_datetime - b.measurement_datetime)))rank
           from cdm_t2 a join pre_lab_48 b on a.person_id = b.person_id
          where b.bilirubin is not null) p on a.person_id = p.person_id and p.rank=1



----save : t08.csv
--visit_to_drug (day)
--death
--drug_to_end (day)
--visit_to_end (day)

select a.group_hypo, a.person_id, a.visit_occurrence_id,
extract(day from a.drug_exposure_start_datetime - a.visit_start_datetime) as visit_to_drug,
case when to_char(a.visit_end_datetime, 'yyyy-mm-dd') = to_char(b.death_datetime, 'yyyy-mm-dd') then 'yes' else 'no' end dead_hosp,

case when to_char(a.visit_end_datetime, 'yyyy-mm-dd') = to_char(b.death_datetime, 'yyyy-mm-dd')
then extract(day from b.death_datetime-a.drug_exposure_start_datetime)
else extract(day from a.visit_end_Datetime-a.drug_exposure_start_datetime) end drug_to_end,

case when to_char(a.visit_end_datetime, 'yyyy-mm-dd') = to_char(b.death_datetime, 'yyyy-mm-dd')
then extract(day from b.death_datetime-a.visit_start_Datetime)
else extract(day from a.visit_end_Datetime-a.visit_start_Datetime) end visit_to_end

from cdm_t2 a
left join cdm_2021.origin_death b
on a.person_id = b.person_id