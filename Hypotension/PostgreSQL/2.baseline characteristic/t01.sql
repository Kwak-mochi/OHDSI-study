---- save : t01.csv
--index year
--Age, years
--Sex, Men
--Height, cm : 3036277
--Weight, kg : 3013762
--BMI : R에서 계산

with measure as (
    select a.VISIT_OCCURRENCE_ID, b.measurement_concept_id, b.value_as_number, 
    rank()over(partition by a.VISIT_OCCURRENCE_ID, b.measurement_concept_id order by abs(extract(epoch from b.measurement_datetime-a.drug_exposure_start_datetime))) rank
    from cdm_t2 a
    join cdm_2021.origin_measurement b on a.VISIT_OCCURRENCE_ID=b.VISIT_OCCURRENCE_ID
    where b.measurement_concept_id in (3036277, 3013762)
) 

select a.person_id, a.VISIT_OCCURRENCE_ID, a.GROUP_HYPO, EXTRACT(year from a.drug_exposure_start_datetime) index_year,
EXTRACT(year from a.drug_exposure_start_datetime)-b.year_of_birth age,
b.GENDER_SOURCE_VALUE sex, c.value_as_number height, d.value_as_number weight

from cdm_t2 a

left join cdm_2021.origin_person b 
on a.PERSON_ID = b.person_id

left join measure c 
on a.VISIT_OCCURRENCE_ID = c.VISIT_OCCURRENCE_ID 
and c.measurement_concept_id = 3036277 and c.rank = 1

left join measure d
on a.VISIT_OCCURRENCE_ID = d.VISIT_OCCURRENCE_ID 
and d.measurement_concept_id = 3013762 and d.rank = 1