CREATE TABLE cdm_qsofa AS
SELECT
    VISIT_OCCURRENCE_ID,
    measurement_datetime,
    sbp,
    rr,
    eye,
    verbal,
    motor,
    eye + verbal + motor AS gcs,
    CASE WHEN sbp < 90 THEN 1 ELSE 0 END AS sbp_c,
    CASE WHEN rr >= 22 THEN 1 ELSE 0 END AS rr_c,
    CASE WHEN eye + verbal + motor <= 13 THEN 1 ELSE 0 END AS gcs_c
FROM
    (
        SELECT
            VISIT_OCCURRENCE_ID,
            measurement_datetime,
            AVG(CASE WHEN measurement_concept_id IN (3004249, 4152194) AND VALUE_AS_NUMBER > 0 AND VALUE_AS_NUMBER < 400 THEN VALUE_AS_NUMBER ELSE NULL END) AS sbp,
            AVG(CASE WHEN measurement_concept_id IN (3024171) AND VALUE_AS_NUMBER > 0 AND VALUE_AS_NUMBER < 70 THEN VALUE_AS_NUMBER ELSE NULL END) AS rr,
            AVG(CASE WHEN measurement_concept_id IN (3016335) AND value_source_value LIKE '[0-9]%' AND CAST(value_source_value AS numeric) <= 4 THEN CAST(value_source_value AS numeric) ELSE NULL END) AS eye,
            AVG(CASE WHEN measurement_concept_id IN (3009094) AND value_source_value LIKE '[0-9]%' AND CAST(value_source_value AS numeric) <= 5 THEN CAST(value_source_value AS numeric) ELSE NULL END) AS verbal,
            AVG(CASE WHEN measurement_concept_id IN (3008223) AND value_source_value LIKE '[0-9]%' AND CAST(value_source_value AS numeric) <= 6 THEN CAST(value_source_value AS numeric) ELSE NULL END) AS motor
        FROM
            CDM_2021.origin_MEASUREMENT
        WHERE
            measurement_concept_id IN (3004249, 4152194, 3024171, 3016335, 3009094, 3008223)
        GROUP BY
            VISIT_OCCURRENCE_ID,
            measurement_datetime
    ) a;