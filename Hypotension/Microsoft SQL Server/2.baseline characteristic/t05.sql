----save : t05.csv
--Comorbidities of interest 
--고혈압과 당뇨는 간호기록지에서 가지고 오는 부분 있음

WITH before_visit_diag AS (
    SELECT
        a.person_id,
        b.visit_occurrence_id,
        a.group_hypo,
        CASE
            WHEN LEN(c.condition_source_value) = 12 THEN SUBSTRING(c.condition_source_value, 8, 2)
            ELSE SUBSTRING(c.condition_source_value, 1, 2)
        END AS condition_source_value_2,
        CASE
            WHEN LEN(c.condition_source_value) = 12 THEN SUBSTRING(c.condition_source_value, 8, 3)
            ELSE SUBSTRING(c.condition_source_value, 1, 3)
        END AS condition_source_value_3,
        CASE
            WHEN LEN(c.condition_source_value) = 12 THEN SUBSTRING(c.condition_source_value, 8, 4)
            ELSE SUBSTRING(c.condition_source_value, 1, 4)
        END AS condition_source_value_4
    FROM
        cdm_t2 a
        LEFT JOIN cdm_2021.origin_visit_occurrence b ON a.person_id = b.person_id AND b.visit_start_datetime <= a.visit_start_datetime
        LEFT JOIN cdm_2021.origin_condition_occurrence c ON b.visit_occurrence_id = c.visit_occurrence_id AND (LEN(c.condition_source_value) = 12 OR LEN(c.condition_source_value) < 6) AND c.condition_source_value LIKE '%[a-zA-Z]%'
),
htn_1 AS (
    SELECT DISTINCT
        person_id,
        'yes' AS di
    FROM
        before_visit_diag
    WHERE
        condition_source_value_3 IN ('I10', 'I11', 'I12', 'I13', 'I15')
),
htn_2 AS (
    SELECT DISTINCT
        a.person_id,
        'yes' AS di
    FROM
        before_visit_diag a
        JOIN cdm_2021.origin_observation b ON a.visit_occurrence_id = b.visit_occurrence_id AND b.value_as_string LIKE '%고혈압%'
),
htn AS (
    SELECT
        person_id,
        di
    FROM
        htn_1
    UNION
    SELECT
        person_id,
        di
    FROM
        htn_2
)

SELECT DISTINCT
    a.person_id,
    a.visit_occurrence_id,
    a.group_hypo,
    htn.di AS Hypertension,
    dm.di AS Diabetes_mellitus,
    hf.di AS Heart_failure,
    ckd.di AS Chronic_kidney_disease,
    ld.di AS Liver_disease,
    ce.di AS Cerebrovascular,
    mc.di AS Metastatic_cancer,
    hm.di AS Hematologic_malignancy,
    st.di AS solid_tumor
FROM
    cdm_t2 a
    LEFT JOIN htn ON a.person_id = htn.person_id
    LEFT JOIN dm ON a.person_id = dm.person_id
    LEFT JOIN hf ON a.person_id = hf.person_id
    LEFT JOIN ckd ON a.person_id = ckd.person_id
    LEFT JOIN ld ON a.person_id = ld.person_id
    LEFT JOIN ce ON a.person_id = ce.person_id
    LEFT JOIN mc ON a.person_id = mc.person_id
    LEFT JOIN hm ON a.person_id = hm.person_id
    LEFT JOIN st ON a.person_id = st.person_id;