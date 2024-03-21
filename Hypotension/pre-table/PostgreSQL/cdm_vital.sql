create table cdm_vital as (
    --SBP : 3004249, 4152194
    --dbp : 3012888, 4154790
    --mbp : 3027598, 4239021
    --Pulse rate : 3027018
    --Respiratory rate : 3024171
    --Temperature : 3020891
    --spo2 : 40762499
    select VISIT_OCCURRENCE_ID, measurement_datetime,
    sbp, dbp,
    case when mbp is null then (sbp+(dbp*2))/3 else mbp end mbp,
    pr, rr, bt, spo2
    from (
        select VISIT_OCCURRENCE_ID, measurement_datetime,
        AVG(case when measurement_concept_id in (3004249, 4152194) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<400 then VALUE_AS_NUMBER else null end)::numeric as sbp, 
        AVG(case when measurement_concept_id in (3012888, 4154790) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<300 then VALUE_AS_NUMBER else null end)::numeric as dbp, 
        AVG(case when measurement_concept_id in (3027598, 4239021) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<300 then VALUE_AS_NUMBER else null end)::numeric as mbp,
        AVG(case when measurement_concept_id in (3027018) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<300 then VALUE_AS_NUMBER else null end)::numeric as pr, 
        AVG(case when measurement_concept_id in (3024171) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<70 then VALUE_AS_NUMBER else null end)::numeric as rr, 
        AVG(case when measurement_concept_id in (3020891) and VALUE_AS_NUMBER>10 and VALUE_AS_NUMBER<50 then VALUE_AS_NUMBER else null end)::numeric as bt,
        AVG(case when measurement_concept_id in (40762499) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<101 then VALUE_AS_NUMBER else null end)::numeric as spo2 
        from CDM_2021.origin_MEASUREMENT
        where measurement_concept_id in (3004249, 4152194, 3012888, 4154790, 3027598, 4239021,3027018,3024171,3020891,40762499)
        group by VISIT_OCCURRENCE_ID, measurement_datetime
        ) a
)