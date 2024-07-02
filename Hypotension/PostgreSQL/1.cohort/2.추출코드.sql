--원내입원 발열확인용
select distinct a.person_id
from CDM_2021.origin_MEASUREMENT b
join CDM_2021.origin_VISIT_occurrence a on a.visit_occurrence_id=b.visit_occurrence_id
where b.MEASUREMENT_CONCEPT_ID in ('3020891', '4302666') and  b.VALUE_AS_NUMBER>37.1 --Temperature
and b.measurement_datetime> '2011-12-31'
and a.visit_concept_id='9201'


create table cdm_t1 as (
    -- 내원환자, 성인, 발열 후 6시간내 index iv 사용 : id= 50034,row=185311
    with t_1 as (
        select distinct a.person_id, a.visit_occurrence_id,
        a.VISIT_START_DATETIME, a.VISIT_END_DATETIME, a.VISIT_START_DATE, a.VISIT_END_DATE,
        c.drug_exposure_start_datetime, c.drug_exposure_start_date, c.drug_concept_id, c.drug_source_value,
        rank()over(partition by a.person_id order by c.drug_exposure_start_datetime, c.drug_source_value) rank_index
        
        from CDM_2021.origin_VISIT_occurrence a 
        
        join cdm_2021.origin_person b
        on a.PERSON_ID = b.person_id

        join CDM_2021.origin_DRUG_EXPOSURE c
        on a.visit_occurrence_id = c.visit_occurrence_id 
        and c.ROUTE_CONCEPT_ID = '4171047' and c.stop_reason is null
        and c.drug_exposure_start_datetime > '2011-12-31'
        and (drug_concept_id in (
    		select descendant_concept_id from cdm_voca_2021.concept_ancestor 
    		where ancestor_concept_id in (1125315, 2100523)) --1125315 : acetaminophen, 2100523 : propacetamol hydrochloride
    		or drug_concept_id = 43291914) --성분명이 빠진 약물
        
        join (
            select person_id, visit_occurrence_id, measurement_datetime
            from CDM_2021.origin_MEASUREMENT
            where MEASUREMENT_CONCEPT_ID in ('3020891', '4302666') and  VALUE_AS_NUMBER>37.1
        ) d
        on a.person_id = d.person_id
        and d.measurement_datetime < c.drug_exposure_start_datetime
        and c.drug_exposure_start_datetime - d.measurement_datetime < '06:00:00'
        
        where a.visit_concept_id='9201'  --입원 --여기까지 55264명
        and EXTRACT(year from c.drug_exposure_start_datetime)-b.year_of_birth  > 19 --50034명
        and c.drug_exposure_start_date-a.VISIT_START_DATE <=7--<=7 --입원 후 7일내 index약물 --45445명
        and a.VISIT_END_DATE-c.drug_exposure_start_date<=90--재원기간 <= 90일 --45248명

    ) 
    -- 내원환자, 성인, 발열 후 6시간내 index iv 사용 중 첫번째 : id= 50034,row=50042, aap, ppa 동시처방 8명
    -- where person_id in (256534975705661, 350534868676358, 350535676686165, 351495372696065, 352485176686064, 352485474756163, 352495371745963, 352525571705956)
    , t_2 as (
        select a.*, b.sbp sbp_af, b.mbp mbp_af,
        rank()over(partition by a.person_id order by b.measurement_datetime, b.mbp desc) rank_bp
    
        from t_1 a
        join cdm_vital b
        on a.person_id = b.person_id
        and a.drug_exposure_start_datetime < b.measurement_datetime
        and b.measurement_datetime - a.drug_exposure_start_datetime < '06:00:00'
        and (b.mbp is not null or b.sbp is not null)
        
        where rank_index =1
    )
    -- id : 41939 row : 46147
    select distinct * from t_2
    where rank_bp =1
)

create table cdm_t2 as ( 
    with t_3 as (
        select distinct a.person_id, a.visit_occurrence_id,
        a.VISIT_START_DATETIME, a.VISIT_END_DATETIME,
        a.drug_exposure_start_datetime, a.drug_concept_id, a.drug_source_value,
        a.sbp_af, a.mbp_af, b.sbp_bf, b.mbp_bf,
        c.drug_concept_id_bf, c.drug_source_value_bf, c.quantity_bf,
        d.drug_concept_id_af, d.drug_source_value_af, d.quantity_af
    
        from cdm_t1 a
        
        --index약물 이전 6시간 이내 혈압측정 중 마지막 값
        left join (
            select distinct a.person_id, b.visit_occurrence_id, b.measurement_datetime measurement_datetime_bf,
            b.sbp sbp_bf, b.mbp mbp_bf,
            rank()over(partition by a.person_id order by b.measurement_datetime desc, b.mbp desc, b.sbp desc) rank
            from cdm_t1 a
            join cdm_vital b
            on a.person_id = b.person_id
            and b.measurement_datetime < a.drug_exposure_start_datetime
            and a.drug_exposure_start_datetime - b.measurement_datetime < '06:00:00'
            and (b.mbp is not null or b.sbp is not null)
        ) b
        on a.person_id = b.person_id
        and b.rank=1
        
        --index약물 이전 24시간 이내 승압제 iv 중 마지막
        left join (
            select distinct a.person_id, b.visit_occurrence_id,
            b.drug_concept_id drug_concept_id_bf, b.drug_source_value drug_source_value_bf, b.quantity quantity_bf,
            rank()over(partition by a.person_id order by b.drug_exposure_start_datetime desc, b.quantity desc, b.drug_concept_id) rank
            from cdm_t1 a
            join cdm_vaso b
            on a.person_id = b.person_id
            and b.drug_exposure_start_datetime < a.drug_exposure_start_datetime
            and a.drug_exposure_start_datetime - b.drug_exposure_start_datetime < '24:00:00'
        ) c
        on a.person_id = c.person_id
        and c.rank=1
        
        --index약물 이후 24시간 이내 승압제 iv 중 처음
         left join (
             select distinct a.person_id, b.visit_occurrence_id,
             b.drug_concept_id drug_concept_id_af, b.drug_source_value drug_source_value_af, b.quantity quantity_af,
             rank()over(partition by a.person_id order by b.drug_exposure_start_datetime, b.quantity desc, b.drug_concept_id) rank
          from cdm_t1 a
          join cdm_vas b
          on a.person_id = b.person_id
          and a.drug_exposure_start_datetime < b.drug_exposure_start_datetime
          and b.drug_exposure_start_datetime - a.drug_exposure_start_datetime < '24:00:00'
         ) d
    on a.person_id = d.person_id
    and d.rank=1
    )
    select 
    case when drug_concept_id_bf is null and drug_concept_id_af is not null then 'hypo'
    when substr(drug_source_value_bf,1,3)=substr(drug_source_value_af,1,3) and quantity_bf < quantity_af then 'hypo'
    when sbp_af < 90 then 'hypo'
    when mbp_af < 65 then 'hypo'
    when sbp_bf-sbp_af > 29 then 'hypo'
    else 'control' end as group_hypo, * 
    from t_3
    )