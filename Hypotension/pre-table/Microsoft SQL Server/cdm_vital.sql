CREATE TABLE cdm_vital AS
SELECT VISIT_OCCURRENCE_ID,
       measurement_datetime,
       sbp,
       dbp,
       CASE
           WHEN mbp IS NULL THEN (sbp + (dbp * 2)) / 3
           ELSE mbp
       END AS mbp,
       pr,
       rr,
       bt,
       spo2
FROM
    (SELECT VISIT_OCCURRENCE_ID,
            measurement_datetime,
            AVG(CASE
                    WHEN measurement_concept_id IN (3004249, 4152194) AND VALUE_AS_NUMBER > 0 AND VALUE_AS_NUMBER < 400 THEN VALUE_AS_NUMBER
                    ELSE NULL
                END) AS sbp,
            AVG(CASE
                    WHEN measurement_concept_id IN (3012888, 4154790) AND VALUE_AS_NUMBER > 0 AND VALUE_AS_NUMBER < 300 THEN VALUE_AS_NUMBER
                    ELSE NULL
                END) AS dbp,
            AVG(CASE
                    WHEN measurement_concept_id IN (3027598, 4239021) AND VALUE_AS_NUMBER > 0 AND VALUE_AS_NUMBER < 300 THEN VALUE_AS_NUMBER
                    ELSE NULL
                END) AS mbp,
            AVG(CASE
                    WHEN measurement_concept_id IN (3027018) AND VALUE_AS_NUMBER > 0 AND VALUE_AS_NUMBER < 300 THEN VALUE_AS_NUMBER
                    ELSE NULL
                END) AS pr,
            AVG(CASE
                    WHEN measurement_concept_id IN (3024171) AND VALUE_AS_NUMBER > 0 AND VALUE_AS_NUMBER < 70 THEN VALUE_AS_NUMBER
                    ELSE NULL
                END) AS rr,
            AVG(CASE
                    WHEN measurement_concept_id IN (3020891) AND VALUE_AS_NUMBER > 10 AND VALUE_AS_NUMBER < 50 THEN VALUE_AS_NUMBER
                    ELSE NULL
                END) AS bt,
            AVG(CASE
                    WHEN measurement_concept_id IN (40762499) AND VALUE_AS_NUMBER > 0 AND VALUE_AS_NUMBER < 101 THEN VALUE_AS_NUMBER
                    ELSE NULL
                END) AS spo2
     FROM CDM_2021.origin_MEASUREMENT
     WHERE measurement_concept_id IN (3004249, 4152194, 3012888, 4154790, 3027598, 4239021, 3027018, 3024171, 3020891, 40762499)
     GROUP BY VISIT_OCCURRENCE_ID, measurement_datetime) a;