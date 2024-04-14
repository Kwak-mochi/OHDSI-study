--concept id
--3020891 : BT, body temperature
--9201 : 입원
--19112656 : AAP1I
--21035250 : AAPI
--43291914 : AAP5I
--2028218, 2028220 : PAAPI
--4171047 : IV

create table cdm_t1 as (
    select distinct aa.* from
    (
        select distinct a.person_id, a.visit_occurrence_id, a.VISIT_START_DATETIME, a.VISIT_END_DATETIME, a.VISIT_START_DATE, a.VISIT_END_DATE,
        b.measurement_datetime, c.drug_exposure_start_datetime, c.drug_exposure_start_date, c.drug_concept_id, c.drug_source_value,
        rank()over(partition by a.person_id order by c.drug_exposure_start_datetime, b.measurement_datetime) rank
        
        -- 입원
        from ( 
            select person_id, visit_occurrence_id, VISIT_START_DATETIME, VISIT_END_DATETIME,VISIT_START_DATE,VISIT_END_DATE
            from CDM_2021.origin_VISIT_occurrence
            where visit_concept_id='9201'
        ) a
        
         --발열이 있던 환자
        join (
            select person_id, visit_occurrence_id, measurement_datetime
            from CDM_2021.origin_MEASUREMENT
            where MEASUREMENT_CONCEPT_ID='3020891' and  VALUE_AS_NUMBER>37.1
        ) b
        on a.VISIT_OCCURRENCE_ID=b.VISIT_OCCURRENCE_ID
        and a.VISIT_START_DATETIME < b.measurement_datetime
        and b.measurement_datetime < a.VISIT_END_DATETIME
        
        --발열이후 6시간 이내 AAP, PAA를 IV투여가 된 환자
        join (
            select person_id, visit_occurrence_id, drug_exposure_start_datetime, drug_exposure_start_date, drug_concept_id, drug_source_value
            from CDM_2021.origin_DRUG_EXPOSURE
            where ROUTE_CONCEPT_ID = '4171047' and stop_reason is null
            and drug_exposure_start_datetime > '2011-12-31'
            and drug_concept_id in ('19112656', '21035250', '43291914', '2028218', '2028220')
        ) c
        on b.VISIT_OCCURRENCE_ID=c.VISIT_OCCURRENCE_ID
        and b.measurement_datetime < c.drug_exposure_start_datetime
        and c.drug_exposure_start_datetime - b.measurement_datetime < '06:00:00'
    ) aa
    
    --성인
    join cdm_2021.origin_person bb 
    on aa.PERSON_ID = bb.person_id
    and EXTRACT(year from aa.drug_exposure_start_datetime)-bb.year_of_birth  > 19
    
    --index약물 이후 6시간 이내 혈압측정
    join cdm_vital cc 
    on aa.VISIT_OCCURRENCE_ID = cc.VISIT_OCCURRENCE_ID
    and aa.drug_exposure_start_datetime < cc.measurement_datetime
    and cc.measurement_datetime - aa.drug_exposure_start_datetime < '06:00:00'
    and (cc.mbp is not null or cc.sbp is not null)

    where aa.rank=1 --환자당 첫번째 사건
    and aa.VISIT_END_DATE-aa.VISIT_START_DATE <=90   --재원기간 <= 90일
    and aa.drug_exposure_start_date-aa.VISIT_START_DATE <=7 --입원 후 7일내 index약물   
)




--concept id
--43290740, 19076658, 42918864 : Dobutamine
--1337860, 35606561, 42799673, 35606558, 36412218, 42918966, 1337860 : Dopamine
--44078900, 36419168, 36419192 : Epinephrine
--21058703, 1321363 : Norepinephrine
--1507835 : Vasopressin
create table cdm_t2 as ( 
    select 
    case when dd.drug_concept_id_bf is null and ee.drug_concept_id_af is not null then 'hypo'
    when substr(dd.drug_source_value_bf,1,3)=substr(ee.drug_source_value_af,1,3) and dd.quantity_bf < ee.quantity_af then 'hypo'
    when cc.sbp_af < 90 then 'hypo'
    when cc.mbp_af < 65 then 'hypo'
    when bb.sbp_bf-cc.sbp_af > 29 then 'hypo'
    else 'control' end as group_hypo,
    
    aa.person_id, aa.visit_occurrence_id,
    aa.visit_start_datetime, aa.visit_end_datetime, aa.measurement_datetime, aa.drug_exposure_start_datetime,
    aa.drug_concept_id, aa.drug_source_value,
    bb.measurement_datetime_bf, bb.sbp_bf, bb.dbp_bf, bb.mbp_bf,
    cc.measurement_datetime_af, cc.sbp_af, cc.dbp_af, cc.mbp_af,
    dd.drug_concept_id_bf, dd.drug_source_value_bf, dd.quantity_bf,
    ee.drug_concept_id_af, ee.drug_source_value_af, ee.quantity_af
    
    --환자당 첫번째 사건(약물)
    from (
        select distinct person_id, visit_occurrence_id, 
        visit_start_datetime, visit_end_datetime, measurement_datetime, drug_exposure_start_datetime,
        drug_concept_id, drug_source_value,
        rank()over(partition by person_id order by drug_source_value) rank 
        from cdm_t1
    ) aa
    
    --index약물 이전 6시간 이내 혈압측정 중 마지막 값
    left join (select distinct a.person_id, b.visit_occurrence_id, b.measurement_datetime measurement_datetime_bf,
          b.sbp sbp_bf, b.dbp dbp_bf, b.mbp mbp_bf,
          rank()over(partition by a.person_id order by b.measurement_datetime desc) rank
          from cdm_t1 a
          join cdm_vital b
         on a.VISIT_OCCURRENCE_ID = b.VISIT_OCCURRENCE_ID
         and b.measurement_datetime < a.drug_exposure_start_datetime
         and a.drug_exposure_start_datetime - b.measurement_datetime < '06:00:00'
         and (b.mbp is not null or b.sbp is not null)) bb
    on aa.person_id = bb.person_id
    and bb.rank=1
    
    --index약물 이후 6시간 이내 혈압측정 중 첫번째 값
    join (select distinct a.person_id, b.visit_occurrence_id, b.measurement_datetime measurement_datetime_af,
          b.sbp sbp_af, b.dbp dbp_af, b.mbp mbp_af,
          rank()over(partition by a.person_id order by b.measurement_datetime) rank
          from cdm_t1 a
          join cdm_vital b
         on a.VISIT_OCCURRENCE_ID = b.VISIT_OCCURRENCE_ID
         and a.drug_exposure_start_datetime < b.measurement_datetime
         and b.measurement_datetime - a.drug_exposure_start_datetime < '06:00:00'
         and (b.mbp is not null or b.sbp is not null)) cc
    on aa.person_id = cc.person_id
    and cc.rank=1
    
    --index약물 이전 24시간 이내 승압제 iv 중 마지막
    left join (select distinct a.person_id, b.visit_occurrence_id,
          b.drug_concept_id drug_concept_id_bf, b.drug_source_value drug_source_value_bf, b.quantity quantity_bf,
          rank()over(partition by a.person_id order by b.drug_exposure_start_datetime desc, b.quantity desc, b.drug_concept_id) rank
          from cdm_t1 a
          join CDM_2021.origin_DRUG_EXPOSURE b
          on a.VISIT_OCCURRENCE_ID = b.VISIT_OCCURRENCE_ID
          and b.drug_exposure_start_datetime < a.drug_exposure_start_datetime
          and a.drug_exposure_start_datetime - b.drug_exposure_start_datetime < '24:00:00'
          and b.drug_concept_id in(43290740, 19076658, 42918864, 1337860, 35606561, 42799673, 35606558, 36412218, 
                                 42918966, 1337860, 44078900, 36419168, 36419192, 21058703, 1321363, 1507835)) dd
    on aa.person_id = dd.person_id
    and dd.rank=1
    
    --index약물 이후 24시간 이내 승압제 iv 중 처음
    left join (select distinct a.person_id, b.visit_occurrence_id,
          b.drug_concept_id drug_concept_id_af, b.drug_source_value drug_source_value_af, b.quantity quantity_af,
          rank()over(partition by a.person_id order by b.drug_exposure_start_datetime, b.quantity desc, b.drug_concept_id) rank
          from cdm_t1 a
          join CDM_2021.origin_DRUG_EXPOSURE b
          on a.VISIT_OCCURRENCE_ID = b.VISIT_OCCURRENCE_ID
          and a.drug_exposure_start_datetime < b.drug_exposure_start_datetime
          and b.drug_exposure_start_datetime - a.drug_exposure_start_datetime < '24:00:00'
          and b.drug_concept_id in(43290740, 19076658, 42918864, 1337860, 35606561, 42799673, 35606558, 36412218, 
                                 42918966, 1337860, 44078900, 36419168, 36419192, 21058703, 1321363, 1507835)) ee
    on aa.person_id = ee.person_id
    and ee.rank=1

    where aa.rank=1
)


