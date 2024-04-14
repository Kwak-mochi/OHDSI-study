----save : t02.csv
--cci
--save 후 python을 통해 cci score 계산

WITH before_visit_condition AS (
    SELECT
        a.group_hypo,
        a.person_id,
        b.visit_occurrence_id,
        CASE
            WHEN LENGTH(c.condition_source_value) = 12 THEN SUBSTRING(c.condition_source_value, 8, 3)
            ELSE SUBSTRING(c.condition_source_value, 1, 4)
        END AS condition_source_value
    FROM
        cdm_t2 a
        LEFT JOIN cdm_2021.origin_visit_occurrence b ON a.person_id = b.person_id AND b.visit_start_datetime < a.visit_start_datetime
        LEFT JOIN cdm_2021.origin_condition_occurrence c ON b.visit_occurrence_id = c.visit_occurrence_id
    WHERE
        (LENGTH(c.condition_source_value) = 12 OR LENGTH(c.condition_source_value) < 6)
        AND c.condition_source_value ~ '[a-zA-Z]'
)

SELECT
    a.group_hypo,
    a.person_id,
    a.visit_occurrence_id,
    b.condition_source_value
FROM
    cdm_t2 a
    LEFT JOIN before_visit_condition b ON a.person_id = b.person_id;