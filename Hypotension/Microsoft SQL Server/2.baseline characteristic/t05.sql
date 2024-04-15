----save : t05.csv
--Comorbidities of interest 
--고혈압과 당뇨는 간호기록지에서 가지고 오는 부분 있음

WITH before_visit_diag AS (
    SELECT
        a.person_id,
        b.visit_occurrence_id,
        a.group_hypo,
        c.condition_source_value AS condition_source_value_raw,
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
        LEFT JOIN cdm_2021.origin_visit_occurrence b ON a.person_id = b.person_id
            AND b.visit_start_datetime <= a.visit_start_datetime
        LEFT JOIN cdm_2021.origin_condition_occurrence c ON b.visit_occurrence_id = c.visit_occurrence_id
            AND (LEN(c.condition_source_value) = 12 OR LEN(c.condition_source_value) < 6)
            AND c.condition_source_value LIKE '%[a-zA-Z]%'
)
SELECT DISTINCT
    a.person_id,
    a.visit_occurrence_id,
    a.group_hypo,
    CASE
        WHEN a.condition_source_value_3 IN ('I10', 'I11', 'I12', 'I13', 'I15')
            OR b.value_as_string LIKE '%고혈압%' THEN 'yes'
        ELSE 'no'
    END AS Hypertension,
    CASE
        WHEN a.condition_source_value_3 IN ('E10', 'E11', 'E12', 'E13', 'E14')
            OR b.value_as_string LIKE '%당뇨%' THEN 'yes'
        ELSE 'no'
    END AS Diabetes_mellitus,
    CASE
        WHEN a.condition_source_value_3 IN ('I50') THEN 'yes'
        ELSE 'no'
    END AS Heart_failure,
    CASE
        WHEN a.condition_source_value_3 IN ('N18') THEN 'yes'
        ELSE 'no'
    END AS Chronic_kidney_disease,
    CASE
        WHEN a.condition_source_value_3 IN ('K70', 'K71', 'K72', 'K73', 'K74', 'K75', 'K76', 'K77') THEN 'yes'
        ELSE 'no'
    END AS Liver_disease,
    CASE
        WHEN a.condition_source_value_3 IN ('I60', 'I61', 'I62', 'I63', 'I64', 'I65', 'I66', 'I67', 'I68', 'I69')
            OR a.condition_source_value_4 = 'G468' THEN 'yes'
        ELSE 'no'
    END AS Cerebrovascular,
    CASE
        WHEN a.condition_source_value_3 IN ('C77', 'C78', 'C79', 'C80') THEN 'yes'
        ELSE 'no'
    END AS Metastatic_cancer,
    CASE
        WHEN a.condition_source_value_3 IN ('C81', 'C82', 'C83', 'C84', 'C85', 'C88', 'C90', 'C91', 'C92', 'C93', 'C94', 'C95', 'C96', 'C97') THEN 'yes'
        ELSE 'no'
    END AS Hematologic_malignancy,
    CASE
        WHEN a.condition_source_value_2 IN ('C0', 'C1', 'C6')
            OR a.condition_source_value_3 IN ('C30', 'C31', 'C32', 'C33', 'C34', 'C37', 'C38', 'C39', 'C40', 'C41', 'C43', 'C44', 'C45', 'C46', 'C47', 'C48', 'C49',
                                               'C50', 'C51', 'C52', 'C53', 'C54', 'C55', 'C56', 'C57', 'C58', 'C70', 'C71', 'C72', 'C73', 'C74', 'C75', 'C76') THEN 'yes'
        ELSE 'no'
    END AS solid_tumor
FROM
    before_visit_diag a
    LEFT JOIN cdm_2021.origin_observation b ON a.visit_occurrence_id = b.visit_occurrence_id;