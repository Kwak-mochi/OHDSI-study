
----save : t08.csv
--visit_to_drug (day)
--death
--drug_to_end (day)
--visit_to_end (day)

SELECT
    a.group_hypo,
    a.person_id,
    a.visit_occurrence_id,
    EXTRACT(day FROM a.drug_exposure_start_datetime - a.visit_start_datetime) AS visit_to_drug,
    CASE
        WHEN to_char(a.visit_end_datetime, 'yyyy-mm-dd') = to_char(b.death_datetime, 'yyyy-mm-dd') THEN 'yes'
        ELSE 'no'
    END AS dead_hosp,
    CASE
        WHEN to_char(a.visit_end_datetime, 'yyyy-mm-dd') = to_char(b.death_datetime, 'yyyy-mm-dd') THEN EXTRACT(day FROM b.death_datetime - a.drug_exposure_start_datetime)
        ELSE EXTRACT(day FROM a.visit_end_datetime - a.drug_exposure_start_datetime)
    END AS drug_to_end,
    CASE
        WHEN to_char(a.visit_end_datetime, 'yyyy-mm-dd') = to_char(b.death_datetime, 'yyyy-mm-dd') THEN EXTRACT(day FROM b.death_datetime - a.visit_start_datetime)
        ELSE EXTRACT(day FROM a.visit_end_datetime - a.visit_start_datetime)
    END AS visit_to_end
FROM
    cdm_t2 a
    LEFT JOIN cdm_2021.origin_death b ON a.person_id = b.person_id;