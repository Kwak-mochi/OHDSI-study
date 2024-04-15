--concept id
--3020891 : BT, body temperature
--9201 : 입원
--19112656 : AAP1I
--21035250 : AAPI
--43291914 : AAP5I
--2028218, 2028220 : PAAPI
--4171047 : IV

CREATE TABLE cdm_t1 AS
SELECT DISTINCT
    aa.*
FROM
    (
        SELECT DISTINCT
            a.person_id,
            a.visit_occurrence_id,
            a.VISIT_START_DATETIME,
            a.VISIT_END_DATETIME,
            a.VISIT_START_DATE,
            a.VISIT_END_DATE,
            b.measurement_datetime,
            c.drug_exposure_start_datetime,
            c.drug_exposure_start_date,
            c.drug_concept_id,
            c.drug_source_value,
            RANK() OVER (PARTITION BY a.person_id ORDER BY c.drug_exposure_start_datetime, b.measurement_datetime) AS rank
        FROM
            (
                SELECT
                    person_id,
                    visit_occurrence_id,
                    VISIT_START_DATETIME,
                    VISIT_END_DATETIME,
                    VISIT_START_DATE,
                    VISIT_END_DATE
                FROM
                    CDM_2021.origin_VISIT_occurrence
                WHERE
                    visit_concept_id = '9201'
            ) a
            JOIN (
                SELECT
                    person_id,
                    visit_occurrence_id,
                    measurement_datetime
                FROM
                    CDM_2021.origin_MEASUREMENT
                WHERE
                    MEASUREMENT_CONCEPT_ID = '3020891'
                    AND VALUE_AS_NUMBER > 37.1
            ) b ON a.VISIT_OCCURRENCE_ID = b.VISIT_OCCURRENCE_ID
            AND a.VISIT_START_DATETIME < b.measurement_datetime
            AND b.measurement_datetime < a.VISIT_END_DATETIME
            JOIN (
                SELECT
                    person_id,
                    visit_occurrence_id,
                    drug_exposure_start_datetime,
                    drug_exposure_start_date,
                    drug_concept_id,
                    drug_source_value
                FROM
                    CDM_2021.origin_DRUG_EXPOSURE
                WHERE
                    ROUTE_CONCEPT_ID = '4171047'
                    AND stop_reason IS NULL
                    AND drug_exposure_start_datetime > '2011-12-31'
                    AND drug_concept_id IN ('19112656', '21035250', '43291914', '2028218', '2028220')
            ) c ON b.VISIT_OCCURRENCE_ID = c.VISIT_OCCURRENCE_ID
            AND b.measurement_datetime < c.drug_exposure_start_datetime
            AND c.drug_exposure_start_datetime - b.measurement_datetime < '06:00:00'
    ) aa
    JOIN cdm_2021.origin_person bb ON aa.PERSON_ID = bb.person_id
    AND YEAR(aa.drug_exposure_start_datetime) - bb.year_of_birth > 19
    JOIN cdm_vital cc ON aa.VISIT_OCCURRENCE_ID = cc.VISIT_OCCURRENCE_ID
    AND aa.drug_exposure_start_datetime < cc.measurement_datetime
    AND cc.measurement_datetime - aa.drug_exposure_start_datetime < '06:00:00'
    AND (cc.mbp IS NOT NULL OR cc.sbp IS NOT NULL)
WHERE
    aa.rank = 1
    AND DATEDIFF(day, aa.VISIT_START_DATE, aa.VISIT_END_DATE) <= 90
    AND DATEDIFF(day, aa.VISIT_START_DATE, aa.drug_exposure_start_date) <= 7;

-- cdm_t2 테이블 생성
--concept id
--43290740, 19076658, 42918864 : Dobutamine
--1337860, 35606561, 42799673, 35606558, 36412218, 42918966, 1337860 : Dopamine
--44078900, 36419168, 36419192 : Epinephrine
--21058703, 1321363 : Norepinephrine
--1507835 : Vasopressin

CREATE TABLE cdm_t2 AS
SELECT
    CASE
        WHEN dd.drug_concept_id_bf IS NULL AND ee.drug_concept_id_af IS NOT NULL THEN 'hypo'
        WHEN SUBSTRING(dd.drug_source_value_bf, 1, 3) = SUBSTRING(ee.drug_source_value_af, 1, 3) AND dd.quantity_bf < ee.quantity_af THEN 'hypo'
        WHEN cc.sbp_af < 90 THEN 'hypo'
        WHEN cc.mbp_af < 65 THEN 'hypo'
        WHEN bb.sbp_bf - cc.sbp_af > 29 THEN 'hypo'
        ELSE 'control'
    END AS group_hypo,
    aa.person_id,
    aa.visit_occurrence_id,
    aa.visit_start_datetime,
    aa.visit_end_datetime,
    aa.measurement_datetime,
    aa.drug_exposure_start_datetime,
    aa.drug_concept_id,
    aa.drug_source_value,
    bb.measurement_datetime_bf,
    bb.sbp_bf,
    bb.dbp_bf,
    bb.mbp_bf,
    cc.measurement_datetime_af,
    cc.sbp_af,
    cc.dbp_af,
    cc.mbp_af,
    dd.drug_concept_id_bf,
    dd.drug_source_value_bf,
    dd.quantity_bf,
    ee.drug_concept_id_af,
    ee.drug_source_value_af,
    ee.quantity_af
FROM
    (SELECT DISTINCT
            person_id,
            visit_occurrence_id,
            visit_start_datetime,
            visit_end_datetime,
            measurement_datetime,
            drug_exposure_start_datetime,
            drug_concept_id,
            drug_source_value,
            RANK() OVER (PARTITION BY person_id ORDER BY drug_source_value) AS rank
     FROM cdm_t1) aa
          LEFT JOIN (SELECT DISTINCT
                            a.person_id,
                            b.visit_occurrence_id,
                            b.measurement_datetime AS measurement_datetime_bf,
                            b.sbp AS sbp_bf,
                            b.dbp AS dbp_bf,
                            b.mbp AS mbp_bf,
                            RANK() OVER (PARTITION BY a.person_id ORDER BY b.measurement_datetime DESC) AS rank
                     FROM cdm_t1 a
                              JOIN cdm_vital b ON a.VISIT_OCCURRENCE_ID = b.VISIT_OCCURRENCE_ID
                                                 AND b.measurement_datetime < a.drug_exposure_start_datetime
                                                 AND a.drug_exposure_start_datetime - b.measurement_datetime < '06:00:00'
                                                 AND (b.mbp IS NOT NULL OR b.sbp IS NOT NULL)) bb ON aa.person_id = bb.person_id
                                                                                                         AND bb.rank = 1
          JOIN (SELECT DISTINCT
                            a.person_id,
                            b.visit_occurrence_id,
                            b.measurement_datetime AS measurement_datetime_af,
                            b.sbp AS sbp_af,
                            b.dbp AS dbp_af,
                            b.mbp AS mbp_af,
                            RANK() OVER (PARTITION BY a.person_id ORDER BY b.measurement_datetime) AS rank
                     FROM cdm_t1 a
                              JOIN cdm_vital b ON a.VISIT_OCCURRENCE_ID = b.VISIT_OCCURRENCE_ID
                                                 AND a.drug_exposure_start_datetime < b.measurement_datetime
                                                 AND b.measurement_datetime - a.drug_exposure_start_datetime < '06:00:00'
                                                 AND (b.mbp IS NOT NULL OR b.sbp IS NOT NULL)) cc ON aa.person_id = cc.person_id
                                                                                                         AND cc.rank = 1
          LEFT JOIN (SELECT DISTINCT
                            a.person_id,
                            b.visit_occurrence_id,
                            b.drug_concept_id AS drug_concept_id_bf,
                            b.drug_source_value AS drug_source_value_bf,
                            b.quantity AS quantity_bf,
                            RANK() OVER (PARTITION BY a.person_id ORDER BY b.drug_exposure_start_datetime DESC, b.quantity DESC, b.drug_concept_id) AS rank

--save : raw.csv
select * from cdm_t2;