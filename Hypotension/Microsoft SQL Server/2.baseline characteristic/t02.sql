----save : t02.csv
--cci
--save 후 python을 통해 cci score 계산

WITH before_visit AS (
    SELECT
        a.person_id,
        b.visit_occurrence_id,
        a.group_hypo
    FROM
        cdm_t2 a
        LEFT JOIN cdm_2021.origin_visit_occurrence b ON a.person_id = b.person_id
        AND b.visit_start_datetime < a.visit_start_datetime
)

SELECT
    a.group_hypo,
    a.person_id,
    a.visit_occurrence_id,
    CASE
        WHEN LEN(c.condition_source_value) = 12 THEN SUBSTRING(c.condition_source_value, 8, 3)
        ELSE SUBSTRING(c.condition_source_value, 1, 3)
    END AS condition_source_value
FROM
    cdm_t2 a
    LEFT JOIN before_visit b ON a.person_id = b.person_id
    LEFT JOIN cdm_2021.origin_condition_occurrence c ON b.visit_occurrence_id = c.visit_occurrence_id
WHERE
    (LEN(c.condition_source_value) = 12 OR LEN(c.condition_source_value) < 6)
    AND c.condition_source_value LIKE '%[a-zA-Z]%';