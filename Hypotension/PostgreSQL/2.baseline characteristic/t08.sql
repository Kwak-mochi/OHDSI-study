
----save : t08.csv
--visit_to_drug (day)
--death
--drug_to_end (day)
--visit_to_end (day)

select a.group_hypo, a.person_id, a.visit_occurrence_id,
extract(day from a.drug_exposure_start_datetime - a.visit_start_datetime) as visit_to_drug,
case when to_char(a.visit_end_datetime, 'yyyy-mm-dd') = to_char(b.death_datetime, 'yyyy-mm-dd') then 'yes' else 'no' end dead_hosp,

case when to_char(a.visit_end_datetime, 'yyyy-mm-dd') = to_char(b.death_datetime, 'yyyy-mm-dd')
then extract(day from b.death_datetime-a.drug_exposure_start_datetime)
else extract(day from a.visit_end_Datetime-a.drug_exposure_start_datetime) end drug_to_end,

case when to_char(a.visit_end_datetime, 'yyyy-mm-dd') = to_char(b.death_datetime, 'yyyy-mm-dd')
then extract(day from b.death_datetime-a.visit_start_Datetime)
else extract(day from a.visit_end_Datetime-a.visit_start_Datetime) end visit_to_end

from cdm_t2 a
left join cdm_2021.origin_death b
on a.person_id = b.person_id