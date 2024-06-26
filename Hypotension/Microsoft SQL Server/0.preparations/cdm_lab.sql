CREATE TABLE cdm_lab AS
SELECT
    VISIT_OCCURRENCE_ID,
    measurement_datetime,
    AVG(CASE WHEN measurement_concept_id IN (3019977, 3012544, 3019977) AND VALUE_AS_NUMBER > 0 AND VALUE_AS_NUMBER < 10 THEN VALUE_AS_NUMBER ELSE NULL END) AS ph,
    AVG(CASE WHEN measurement_concept_id IN (3027946, 3021447, 3027946) THEN VALUE_AS_NUMBER ELSE NULL END) AS pCO2,
    AVG(CASE WHEN measurement_concept_id IN (3027801, 3024354, 3027801) THEN VALUE_AS_NUMBER ELSE NULL END) AS pO2,
    AVG(CASE WHEN measurement_concept_id IN (3008152, 3027273, 3008152) THEN VALUE_AS_NUMBER ELSE NULL END) AS HCO3,
    AVG(CASE WHEN measurement_concept_id IN (3020460, 3010156, 3010156, 3020460) THEN VALUE_AS_NUMBER ELSE NULL END) AS CRP,
    AVG(CASE WHEN measurement_concept_id IN (3020410, 3047181) THEN VALUE_AS_NUMBER ELSE NULL END) AS Lactate,
    AVG(CASE WHEN measurement_concept_id IN (3000905) THEN VALUE_AS_NUMBER ELSE NULL END) AS WBC,
    AVG(CASE WHEN measurement_concept_id IN (3000963) THEN VALUE_AS_NUMBER ELSE NULL END) AS Hemoglobin,
    AVG(CASE WHEN measurement_concept_id IN (3024929) THEN VALUE_AS_NUMBER ELSE NULL END) AS Platelet,
    AVG(CASE WHEN measurement_concept_id IN (3016723) THEN VALUE_AS_NUMBER ELSE NULL END) AS Creatinine,
    AVG(CASE WHEN measurement_concept_id IN (3024128) THEN VALUE_AS_NUMBER ELSE NULL END) AS Bilirubin,
    AVG(CASE WHEN measurement_concept_id IN (3013682) THEN VALUE_AS_NUMBER ELSE NULL END) AS BUN
FROM
    (
        SELECT
            VISIT_OCCURRENCE_ID,
            measurement_datetime,
            VALUE_AS_NUMBER,
            measurement_concept_id
        FROM
            CDM_2021.origin_MEASUREMENT
        WHERE
            measurement_concept_id IN (3019977, 3012544, 3019977, 3027946, 3021447, 3027946, 3027801, 3024354, 3027801, 3008152,
                                       3027273, 3008152, 3020460, 3010156, 3010156, 3020460, 3020410, 3047181, 3000905, 3000963,
                                       3024929, 3016723, 3024128, 3013682)
    ) a
GROUP BY
    VISIT_OCCURRENCE_ID,
    measurement_datetime;