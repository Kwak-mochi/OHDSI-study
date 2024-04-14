----save : t04.csv
--qSOFA, Sepsis여부 확인
--cdm_qsofa 이용
--cdm_antibiotics 이용

WITH pre_qsofa AS (
    SELECT
        a.group_hypo,
        a.person_id,
        a.drug_exposure_start_datetime,
        b.* 
    FROM
        cdm_t2 a
        JOIN cdm_qsofa b ON a.visit_occurrence_id = b.visit_occurrence_id
        AND ABS(DATEDIFF(SECOND, b.measurement_datetime, a.drug_exposure_start_datetime)) < 86400
        AND ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime)) < 86400
        AND (b.sbp IS NOT NULL OR b.rr IS NOT NULL OR b.gcs IS NOT NULL)
),
pre_antibio AS (
    SELECT
        b.*
    FROM
        cdm_t2 a
        JOIN cdm_antibiotics b ON a.visit_occurrence_id = b.visit_occurrence_id
        AND ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.drug_exposure_start_datetime)) < 86400
        AND ABS(DATEDIFF(SECOND, b.drug_exposure_start_datetime, a.drug_exposure_start_datetime)) < 86400
)
SELECT DISTINCT
    a.group_hypo,
    a.person_id,
    a.visit_occurrence_id,
    b.sbp_c,
    c.rr_c,
    d.gcs_c,
    b.sbp_c + c.rr_c + d.gcs_c AS qSOFA,
    CASE
        WHEN e.visit_occurrence_id IS NOT NULL THEN 'yes'
        ELSE 'no'
    END AS antibio,
    CASE
        WHEN f.condition_source_value IS NOT NULL THEN 1
        ELSE 0
    END AS dig_sepsis,
    CASE
        WHEN f.condition_source_value IS NOT NULL OR (b.sbp_c + c.rr_c + d.gcs_c > 1 AND e.visit_occurrence_id IS NOT NULL) THEN 'yes'
        ELSE 'no'
    END AS sepsis
FROM
    cdm_t2 a
    LEFT JOIN (
        SELECT
            *,
            RANK() OVER (PARTITION BY visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, drug_exposure_start_datetime, measurement_datetime))) AS rank
        FROM
            pre_qsofa
        WHERE
            sbp IS NOT NULL
    ) b ON a.visit_occurrence_id = b.visit_occurrence_id
    AND b.rank = 1
    LEFT JOIN (
        SELECT
            *,
            RANK() OVER (PARTITION BY visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, drug_exposure_start_datetime, measurement_datetime))) AS rank
        FROM
            pre_qsofa
        WHERE
            rr IS NOT NULL
    ) c ON a.visit_occurrence_id = c.visit_occurrence_id
    AND c.rank = 1
    LEFT JOIN (
        SELECT
            *,
            RANK() OVER (PARTITION BY visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, drug_exposure_start_datetime, measurement_datetime))) AS rank
        FROM
            pre_qsofa
        WHERE
            gcs IS NOT NULL
    ) d ON a.visit_occurrence_id = d.visit_occurrence_id
    AND d.rank = 1
LEFT JOIN (
    SELECT DISTINCT
        visit_occurrence_id
    FROM
        pre_antibio
) e ON a.visit_occurrence_id = e.visit_occurrence_id
LEFT JOIN cdm_2021.origin_condition_occurrence f ON a.visit_occurrence_id = f.visit_occurrence_id
AND (f.condition_source_value LIKE 'A40%' OR f.condition_source_value LIKE 'A41%');