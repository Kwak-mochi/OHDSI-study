---- save : t01.csv
--index year
--Age, years
--Sex, Men
--Height, cm : 3036277
--Weight, kg : 3013762
--BMI : R에서 계산


WITH measure AS (
    SELECT
        a.VISIT_OCCURRENCE_ID,
        b.measurement_concept_id,
        b.value_as_number,
        RANK() OVER (PARTITION BY a.VISIT_OCCURRENCE_ID, b.measurement_concept_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) AS rank
    FROM
        cdm_t2 a
        JOIN cdm_2021.origin_measurement b ON a.VISIT_OCCURRENCE_ID = b.VISIT_OCCURRENCE_ID
    WHERE
        b.measurement_concept_id IN (3036277, 3013762)
)

SELECT
    a.person_id,
    a.VISIT_OCCURRENCE_ID,
    a.GROUP_HYPO,
    YEAR(a.drug_exposure_start_datetime) AS index_year,
    YEAR(a.drug_exposure_start_datetime) - b.year_of_birth AS age,
    b.GENDER_SOURCE_VALUE AS sex,
    c.value_as_number AS height,
    d.value_as_number AS weight
FROM
    cdm_t2 a
    LEFT JOIN cdm_2021.origin_person b ON a.PERSON_ID = b.person_id
    LEFT JOIN measure c ON a.VISIT_OCCURRENCE_ID = c.VISIT_OCCURRENCE_ID AND c.measurement_concept_id = 3036277 AND c.rank = 1
    LEFT JOIN measure d ON a.VISIT_OCCURRENCE_ID = d.VISIT_OCCURRENCE_ID AND d.measurement_concept_id = 3013762 AND d.rank = 1;