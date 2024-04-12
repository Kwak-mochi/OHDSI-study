create table cdm_lab as (
    select VISIT_OCCURRENCE_ID, measurement_datetime,
    ph, pCO2, pO2,HCO3, CRP, Lactate, WBC, Hemoglobin, Platelet, Creatinine, Bilirubin, BUN
    from (
        select VISIT_OCCURRENCE_ID, measurement_datetime,
        AVG(case when measurement_concept_id in (3019977, 3012544, 3019977) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<10 then VALUE_AS_NUMBER else null end)::numeric as ph, 
        AVG(case when measurement_concept_id in (3027946, 3021447, 3027946) then VALUE_AS_NUMBER else null end)::numeric as pCO2, 
        AVG(case when measurement_concept_id in (3027801, 3024354, 3027801) then VALUE_AS_NUMBER else null end)::numeric as pO2,
        AVG(case when measurement_concept_id in (3008152, 3027273, 3008152) then VALUE_AS_NUMBER else null end)::numeric as HCO3, 
        AVG(case when measurement_concept_id in (3020460, 3010156, 3010156, 3020460) then VALUE_AS_NUMBER else null end)::numeric as CRP, 
        AVG(case when measurement_concept_id in (3020410, 3047181) then VALUE_AS_NUMBER else null end)::numeric as Lactate,
        AVG(case when measurement_concept_id in (3000905) then VALUE_AS_NUMBER else null end)::numeric as WBC, 
        AVG(case when measurement_concept_id in (3000963) then VALUE_AS_NUMBER else null end)::numeric as Hemoglobin,
        AVG(case when measurement_concept_id in (3024929) then VALUE_AS_NUMBER else null end)::numeric as Platelet,
        AVG(case when measurement_concept_id in (3016723) then VALUE_AS_NUMBER else null end)::numeric as Creatinine,
        AVG(case when measurement_concept_id in (3024128) then VALUE_AS_NUMBER else null end)::numeric as Bilirubin,
        AVG(case when measurement_concept_id in (3013682) then VALUE_AS_NUMBER else null end)::numeric as BUN
        from CDM_2021.origin_MEASUREMENT
        where measurement_concept_id in (3019977, 3012544, 3019977, 3027946, 3021447, 3027946, 3027801, 3024354, 3027801, 3008152,
                                         3027273, 3008152, 3020460, 3010156, 3010156, 3020460, 3020410, 3047181, 3000905, 3000963,
                                        3024929, 3016723, 3024128, 3013682)
        group by VISIT_OCCURRENCE_ID, measurement_datetime
        ) a
)
