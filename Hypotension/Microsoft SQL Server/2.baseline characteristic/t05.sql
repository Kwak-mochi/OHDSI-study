----save : t05.csv
--Comorbidities of interest 
--고혈압과 당뇨는 간호기록지에서 가지고 오는 부분 있음

WITH before_visit AS (
    SELECT
        a.person_id,
        b.visit_occurrence_id,
        a.group_hypo
    FROM
        cdm_t2 a
        LEFT JOIN cdm_2021.origin_visit_occurrence b ON a.person_id = b.person_id
        AND b.visit_start_datetime <= a.visit_start_datetime
),
diag AS (
    SELECT
        person_id,
        visit_occurrence_id,
        CASE
            WHEN LEN(condition_source_value) = 12 THEN SUBSTRING(condition_source_value, 8, 3)
            ELSE SUBSTRING(condition_source_value, 1, 4)
        END AS condition_source_value
    FROM
        cdm_2021.origin_condition_occurrence
    WHERE
        (LEN(condition_source_value) = 12 OR LEN(condition_source_value) < 6)
        AND condition_source_value LIKE '%[a-zA-Z]%'
),
htn AS (
    SELECT DISTINCT
        a.visit_occurrence_id,
        CASE
            WHEN condition_source_value IS NOT NULL OR value_as_string IS NOT NULL THEN 'yes'
        END AS htn
    FROM
        before_visit a
        LEFT JOIN diag b ON a.visit_occurrence_id = b.visit_occurrence_id
        AND SUBSTRING(condition_source_value, 1, 3) IN ('I10', 'I11', 'I12', 'I13', 'I15')
        LEFT JOIN cdm_2021.origin_observation c ON a.visit_occurrence_id = c.visit_occurrence_id
        AND value_as_string LIKE '%고혈압%'
),
DM AS (
    SELECT DISTINCT
        a.visit_occurrence_id,
        CASE
            WHEN condition_source_value IS NOT NULL OR value_as_string IS NOT NULL THEN 'yes'
        END AS dm
    FROM
        before_visit a
        LEFT JOIN diag b ON a.visit_occurrence_id = b.visit_occurrence_id
        AND SUBSTRING(condition_source_value, 1, 3) IN ('E10', 'E11', 'E12', 'E13', 'E14')
        LEFT JOIN cdm_2021.origin_observation c ON a.visit_occurrence_id = c.visit_occurrence_id
        AND value_as_string LIKE '%당뇨%'
),
heart AS (
    SELECT DISTINCT
        a.visit_occurrence_id,
        CASE
            WHEN condition_source_value IS NOT NULL THEN 'yes'
        END AS heart
    FROM
        before_visit a
        LEFT JOIN diag b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            SUBSTRING(condition_source_value, 1, 3) IN ('I50')
),
ckd AS (
    SELECT DISTINCT
        a.visit_occurrence_id,
        CASE
            WHEN condition_source_value IS NOT NULL THEN 'yes'
        END AS ckd
    FROM
        before_visit a
        LEFT JOIN diag b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            SUBSTRING(condition_source_value, 1, 3) IN ('N18')
),
liver AS (
    SELECT DISTINCT
        a.visit_occurrence_id,
        CASE
            WHEN condition_source_value IS NOT NULL THEN 'yes'
        END AS liver
    FROM
        before_visit a
        LEFT JOIN diag b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            SUBSTRING(condition_source_value, 1, 3) IN ('K70', 'K71', 'K72', 'K73', 'K74', 'K75', 'K76', 'K77')
),
cere AS (
    SELECT DISTINCT
        a.visit_occurrence_id,
        CASE
            WHEN condition_source_value IS NOT NULL THEN 'yes'
        END AS cere
    FROM
        before_visit a
        LEFT JOIN diag b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            SUBSTRING(condition_source_value, 1, 3) IN ('I60', 'I61', 'I62', 'I63', 'I64', 'I65', 'I66', 'I67', 'I68', 'I69')
            OR SUBSTRING(condition_source_value, 1, 4) = 'G468'
),
meta AS (
    SELECT DISTINCT
        a.visit_occurrence_id,
        CASE
            WHEN condition_source_value IS NOT NULL THEN 'yes'
        END AS meta
    FROM
        before_visit a
        LEFT JOIN diag b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            SUBSTRING(condition_source_value, 1, 3) IN ('C77', 'C78', 'C79', 'C80')
),
hema AS (
    SELECT DISTINCT
        a.visit_occurrence_id,
        CASE
            WHEN condition_source_value IS NOT NULL THEN 'yes'
        END AS hema
    FROM
        before_visit a
        LEFT JOIN diag b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            SUBSTRING(condition_source_value, 1, 3) IN ('C81', 'C82', 'C83', 'C84', 'C85', 'C88', 'C90', 'C91', 'C92', 'C93', 'C94', 'C95', 'C96', 'C97')
),
solid AS (
    SELECT DISTINCT
        a.visit_occurrence_id,
        CASE
            WHEN condition_source_value IS NOT NULL THEN 'yes'
        END AS solid
    FROM
        before_visit a
        LEFT JOIN diag b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            SUBSTRING(condition_source_value, 1, 2) IN ('C0', 'C1', 'C6')
            OR SUBSTRING(condition_source_value, 1, 3) IN ('C30', 'C31', 'C32', 'C33', 'C34', 'C37', 'C38', 'C39', 'C40', 'C41', 'C43', 'C44', 'C45', 'C46', 'C47', 'C48', 'C49', 'C50', 'C51', 'C52', 'C53', 'C54', 'C55', 'C56', 'C57', 'C58', 'C70', 'C71', 'C72', 'C73', 'C74', 'C75', 'C76')
)

SELECT
    a.person_id,
    a.visit_occurrence_id,
    a.group_hypo,
    CASE
        WHEN htn.htn IS NULL THEN 'no'
        ELSE htn.htn
    END AS Hypertension,
    CASE
        WHEN dm.dm IS NULL THEN 'no'
        ELSE dm.dm
    END AS Diabetes_mellitus,
    CASE
        WHEN heart.heart IS NULL THEN 'no'
        ELSE heart.heart
    END AS Heart_failure,
    CASE
        WHEN ckd.ckd IS NULL THEN 'no'
        ELSE ckd.ckd
    END AS Chronic_kidney_disease,
    CASE
        WHEN liver.liver IS NULL THEN 'no'
        ELSE liver.liver
    END AS Liver_disease,
    CASE
        WHEN cere.cere IS NULL THEN 'no'
        ELSE cere.cere
    END AS Cerebrovascular,
    CASE
        WHEN meta.meta IS NULL THEN 'no'
        ELSE meta.meta
    END AS Metastatic_cancer,
    CASE
        WHEN hema.hema IS NULL THEN 'no'
        ELSE hema.hema
    END AS Hematologic_malignancy,
    CASE
        WHEN solid.solid IS NULL THEN 'no'
        ELSE solid.solid
    END AS solid_tumor
FROM
    cdm_t2 a
    LEFT OUTER JOIN htn ON a.visit_occurrence_id = htn.visit_occurrence_id
    LEFT OUTER JOIN dm ON a.visit_occurrence_id = dm.visit_occurrence_id
    LEFT OUTER JOIN heart ON a.visit_occurrence_id = heart.visit_occurrence_id
    LEFT OUTER JOIN ckd ON a.visit_occurrence_id = ckd.visit_occurrence_id
    LEFT OUTER JOIN liver ON a.visit_occurrence_id = liver.visit_occurrence_id
    LEFT OUTER JOIN cere ON a.visit_occurrence_id = cere.visit_occurrence_id
    LEFT OUTER JOIN meta ON a.visit_occurrence_id = meta.visit_occurrence_id
    LEFT OUTER JOIN hema ON a.visit_occurrence_id = hema.visit_occurrence_id
    LEFT OUTER JOIN solid ON a.visit_occurrence_id = solid.visit_occurrence_id;