----save : t07.csv
--lab(48h)
--cdm_lab 이용

WITH pre_lab_48 AS (
    SELECT
        a.group_hypo,
        a.person_id,
        b.*
    FROM
        cdm_t2 a
        LEFT JOIN public.cdm_LAB b ON a.visit_occurrence_id = b.visit_occurrence_id
        AND a.drug_exposure_start_datetime - b.measurement_datetime < '48:00:00'
        AND b.measurement_datetime - a.drug_exposure_start_datetime < '48:00:00'
)

SELECT
    a.group_hypo,
    a.person_id,
    a.visit_occurrence_id,
    b.ph AS ph_48,
    c.pco2 AS pco2_48,
    d.po2 AS po2_48,
    e.hco3 AS hco3_48,
    f.crp AS crp_48,
    g.lactate AS lactate_48,
    h.wbc AS wbc_48,
    i.hemoglobin AS hemoglobin_48,
    j.platelet AS platelet_48,
    k.creatinine AS creatinine_48,
    o.bilirubin AS bilirubin_48,
    p.bun AS bun_48
FROM
    cdm_t2 a
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.ph IS NOT NULL
    ) b ON a.visit_occurrence_id = b.visit_occurrence_id
    AND b.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.pco2 IS NOT NULL
    ) c ON a.visit_occurrence_id = c.visit_occurrence_id
    AND c.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.po2 IS NOT NULL
    ) d ON a.visit_occurrence_id = d.visit_occurrence_id
    AND d.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.hco3 IS NOT NULL
    ) e ON a.visit_occurrence_id = e.visit_occurrence_id
    AND e.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.crp IS NOT NULL
    ) f ON a.visit_occurrence_id = f.visit_occurrence_id
    AND f.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.lactate IS NOT NULL
    ) g ON a.visit_occurrence_id = g.visit_occurrence_id
    AND g.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.wbc IS NOT NULL
    ) h ON a.visit_occurrence_id = h.visit_occurrence_id
    AND h.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.hemoglobin IS NOT NULL
    ) i ON a.visit_occurrence_id = i.visit_occurrence_id
    AND i.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.platelet IS NOT NULL
    ) j ON a.visit_occurrence_id = j.visit_occurrence_id
    AND j.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.creatinine IS NOT NULL
    ) k ON a.visit_occurrence_id = k.visit_occurrence_id
    AND k.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.bilirubin IS NOT NULL
    ) o ON a.visit_occurrence_id = o.visit_occurrence_id
    AND o.rank = 1
    LEFT JOIN (
        SELECT
            b.*,
            RANK() OVER (PARTITION BY b.visit_occurrence_id ORDER BY ABS(DATEDIFF(SECOND, a.drug_exposure_start_datetime, b.measurement_datetime))) rank
        FROM
            cdm_t2 a
            JOIN pre_lab_48 b ON a.visit_occurrence_id = b.visit_occurrence_id
        WHERE
            b.bun IS NOT NULL
    ) p ON a.visit_occurrence_id = p.visit_occurrence_id
    AND p.rank = 1;