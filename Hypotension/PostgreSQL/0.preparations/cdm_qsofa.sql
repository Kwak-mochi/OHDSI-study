create table cdm_qsofa as (
    select VISIT_OCCURRENCE_ID, measurement_datetime, sbp, rr, eye, verbal, motor, eye+verbal+motor as gcs,
    case when sbp < 90 then 1 else 0 end as sbp_c,
    case when rr >= 22 then 1 else 0 end as rr_c,
    case when eye+verbal+motor <= 13 then 1 else 0 end as gcs_c
    from (
        select VISIT_OCCURRENCE_ID, measurement_datetime,
        AVG(case when measurement_concept_id in (3004249, 4152194) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<400 then VALUE_AS_NUMBER else null end)::numeric as sbp, 
        AVG(case when measurement_concept_id in (3024171) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<70 then VALUE_AS_NUMBER else null end)::numeric as rr, 
        AVG(case when measurement_concept_id in (3016335) and value_source_value ~ '^[0-9]+$' and value_source_value::numeric <= 4 then value_source_value::numeric  else null end)as eye,
        AVG(case when measurement_concept_id in (3009094) and value_source_value ~ '^[0-9]+$' and value_source_value::numeric <= 5 then value_source_value::numeric  else null end)as verbal,
        AVG(case when measurement_concept_id in (3008223) and value_source_value ~ '^[0-9]+$' and value_source_value::numeric <= 6 then value_source_value::numeric  else null end)as motor
        from CDM_2021.origin_MEASUREMENT
        where measurement_concept_id in (3004249, 4152194, 3024171, 3016335, 3009094, 3008223)
        group by VISIT_OCCURRENCE_ID, measurement_datetime
        ) a
)