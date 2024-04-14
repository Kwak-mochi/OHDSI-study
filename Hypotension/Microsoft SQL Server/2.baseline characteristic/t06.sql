
----save : t06.csv
--vital sign
--cdm_vital 이용

WITH pre_vital AS (
    SELECT
        a.group_hypo,
        a.person_id,
        a.drug_exposure_start_datetime,
        b.*    
    FROM
        cdm_t2 a
        LEFT JOIN public.cdm_vital b ON a.visit_occurrence_id = b.visit_occurrence_id
        AND b.measurement_datetime < a.drug_exposure_start_datetime
)

SELECT
    a.group_hypo,
    a.person_id,
    a.visit_occurrence_id,
    b.mbp,
    c.pr,
    d.rr,
    e.bt,
    f.spo2
FROM
    cdm_t2 a
    LEFT JOIN (
        SELECT
            *,
            RANK() OVER (PARTITION BY visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, drug_exposure_start_datetime, measurement_datetime))) rank
        FROM
            pre_vital
        WHERE
            mbp IS NOT NULL
    ) b ON a.visit_occurrence_id = b.visit_occurrence_id
    AND b.rank = 1
    LEFT JOIN (
        SELECT
            *,
            RANK() OVER (PARTITION BY visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, drug_exposure_start_datetime, measurement_datetime))) rank
        FROM
            pre_vital
        WHERE
            pr IS NOT NULL
    ) c ON a.visit_occurrence_id = c.visit_occurrence_id
    AND c.rank = 1
    LEFT JOIN (
        SELECT
            *,
            RANK() OVER (PARTITION BY visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, drug_exposure_start_datetime, measurement_datetime))) rank
        FROM
            pre_vital
        WHERE
            rr IS NOT NULL
    ) d ON a.visit_occurrence_id = d.visit_occurrence_id
    AND d.rank = 1
    LEFT JOIN (
        SELECT
            *,
            RANK() OVER (PARTITION BY visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, drug_exposure_start_datetime, measurement_datetime))) rank
        FROM
            pre_vital
        WHERE
            bt IS NOT NULL
    ) e ON a.visit_occurrence_id = e.visit_occurrence_id
    AND e.rank = 1
    LEFT JOIN (
        SELECT
            *,
            RANK() OVER (PARTITION BY visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, drug_exposure_start_datetime, measurement_datetime))) rank
        FROM
            pre_vital
        WHERE
            spo2 IS NOT NULL
    ) f ON a.visit_occurrence_id = f.visit_occurrence_id
    AND f.rank = 1;