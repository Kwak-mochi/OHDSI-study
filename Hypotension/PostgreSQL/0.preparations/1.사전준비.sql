--다음 코드는 postgreSQL로 작성되었습니다.
--pgAdmin 4 (v6.10)


-- 생성코드
create table cdm_vital as (
    select person_id, VISIT_OCCURRENCE_ID, measurement_datetime,
    sbp, dbp,
    case when mbp is null then (sbp+(dbp*2))/3 else mbp end mbp,
    pr, rr, bt, spo2
    from (
        select person_id, VISIT_OCCURRENCE_ID, measurement_datetime,
        AVG(case when measurement_concept_id in (3004249, 4152194) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<400 then VALUE_AS_NUMBER else null end)::numeric as sbp, 
        AVG(case when measurement_concept_id in (3012888, 4154790) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<300 then VALUE_AS_NUMBER else null end)::numeric as dbp, 
        AVG(case when measurement_concept_id in (3027598, 4239021) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<300 then VALUE_AS_NUMBER else null end)::numeric as mbp,
        AVG(case when measurement_concept_id in (3027018, 4239408) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<300 then VALUE_AS_NUMBER else null end)::numeric as pr, 
        AVG(case when measurement_concept_id in (3024171, 4313591) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<70 then VALUE_AS_NUMBER else null end)::numeric as rr, 
        AVG(case when measurement_concept_id in (3020891, 4302666) and VALUE_AS_NUMBER>10 and VALUE_AS_NUMBER<50 then VALUE_AS_NUMBER else null end)::numeric as bt,
        AVG(case when measurement_concept_id in (40762499, 4011919) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<101 then VALUE_AS_NUMBER else null end)::numeric as spo2 
        from CDM_2021.origin_MEASUREMENT
        where measurement_concept_id in (3004249, 4152194, 3012888, 4154790, 3027598, 4239021, 3027018, 4239408, 3024171, 4313591, 3020891, 4302666, 40762499, 4011919)
        group by person_id, VISIT_OCCURRENCE_ID, measurement_datetime
        ) a
)


create table cdm_lab as (
    select person_id, VISIT_OCCURRENCE_ID, measurement_datetime,
    ph, pCO2, pO2,HCO3, CRP, Lactate, WBC, Hemoglobin, Platelet, Creatinine, Bilirubin, BUN
    from (
        select person_id, VISIT_OCCURRENCE_ID, measurement_datetime,
        AVG(case when measurement_concept_id in (3019977, 3012544, 3019977) and VALUE_AS_NUMBER>0 and VALUE_AS_NUMBER<10 then VALUE_AS_NUMBER else null end)::numeric as ph, 
        AVG(case when measurement_concept_id in (3027946, 3021447, 3027946) then VALUE_AS_NUMBER else null end)::numeric as pCO2, 
        AVG(case when measurement_concept_id in (3027801, 3024354, 3027801) then VALUE_AS_NUMBER else null end)::numeric as pO2,
        AVG(case when measurement_concept_id in (3008152, 3027273, 3008152) then VALUE_AS_NUMBER else null end)::numeric as HCO3, 
        AVG(case when measurement_concept_id in (3020460, 3010156, 3010156, 3020460) then VALUE_AS_NUMBER else null end)::numeric as CRP, 
        AVG(case when measurement_concept_id in (3020410, 3047181) then VALUE_AS_NUMBER else null end)::numeric as Lactate,
        AVG(case when measurement_concept_id in (3000905) then VALUE_AS_NUMBER else null end)::numeric as WBC, 
        AVG(case when measurement_concept_id in (3000963) then VALUE_AS_NUMBER else null end)::numeric as Hemoglobin,
        AVG(case when measurement_concept_id in (3024929) then VALUE_AS_NUMBER else null end)::numeric as Platelet,
        AVG(case when measurement_concept_id in (3016723) then VALUE_AS_NUMBER else null end)::numeric as Creatinine,
        AVG(case when measurement_concept_id in (3024128) then VALUE_AS_NUMBER else null end)::numeric as Bilirubin,
        AVG(case when measurement_concept_id in (3013682) then VALUE_AS_NUMBER else null end)::numeric as BUN
        from CDM_2021.origin_MEASUREMENT
        where measurement_concept_id in (3019977, 3012544, 3019977, 3027946, 3021447, 3027946, 3027801, 3024354, 3027801, 3008152,
                                         3027273, 3008152, 3020460, 3010156, 3010156, 3020460, 3020410, 3047181, 3000905, 3000963,
                                        3024929, 3016723, 3024128, 3013682)
        group by person_id, VISIT_OCCURRENCE_ID, measurement_datetime
        ) a
)


create table cdm_vaso as (
with vaso_db as (
    select distinct * from cdm_voca_2021.concept
    where domain_id = 'Drug' and concept_class_id = 'Ingredient' and
    (lower(concept_name) like 'epinephrine' or lower(concept_name) like 'norepinephrine' or lower(concept_name) like 'dopamine' or 
     lower(concept_name) like 'vasopressin (USP)' or lower(concept_name) like 'dobutamine'))

    select person_id, visit_occurrence_id,drug_exposure_id, drug_exposure_start_datetime, drug_exposure_end_datetime,
    drug_exposure_start_date, drug_exposure_end_date,drug_concept_id, drug_source_value,route_concept_id, route_source_value, quantity
    from cdm_2021.origin_drug_exposure
    where drug_concept_id in (select descendant_concept_id from cdm_voca_2021.concept_ancestor where ancestor_concept_id in (select concept_id from vaso_db))
    and route_concept_id = 4171047 --iv
)


create table cdm_antibiotics as (
with antibio_db as (
    select distinct * from cdm_voca_2021.concept
    where domain_id = 'Drug' and concept_class_id = 'Ingredient' and
    (lower(concept_name) like 'abacavir' or
lower(concept_name) like 'acyclovir' or
lower(concept_name) like 'albendazole' or
lower(concept_name) like 'amikacin' or
lower(concept_name) like 'amoxicillin' or
lower(concept_name) like 'ampho b liposomal' or
lower(concept_name) like 'amphotericin b' or
lower(concept_name) like 'ampicillin' or
lower(concept_name) like 'anidulafungin' or
lower(concept_name) like 'azithromycin' or
lower(concept_name) like 'aztreonam' or
lower(concept_name) like 'baloxavir' or
lower(concept_name) like 'bedaquiline' or
lower(concept_name) like 'benzathine pc' or
lower(concept_name) like 'caspofungin' or
lower(concept_name) like 'cefaclor' or
lower(concept_name) like 'cefadroxil' or
lower(concept_name) like 'cefamandole' or
lower(concept_name) like 'cefazolin' or
lower(concept_name) like 'cefdinir' or
lower(concept_name) like 'cefditoren' or
lower(concept_name) like 'cefepime' or
lower(concept_name) like 'cefixime' or
lower(concept_name) like 'cefmetazole' or
lower(concept_name) like 'cefonicid' or
lower(concept_name) like 'cefoperazone' or
lower(concept_name) like 'cefotaxime' or
lower(concept_name) like 'cefotetan' or
lower(concept_name) like 'cefoxitin' or
lower(concept_name) like 'cefpodoxime' or
lower(concept_name) like 'cefprozil' or
lower(concept_name) like 'ceftaroline' or
lower(concept_name) like 'ceftazidime' or
lower(concept_name) like 'ceftibuten' or
lower(concept_name) like 'ceftizoxime' or
lower(concept_name) like 'ceftriaxone' or
lower(concept_name) like 'cefuroxime' or
lower(concept_name) like 'cephalex' or
lower(concept_name) like 'cephalexin' or
lower(concept_name) like 'cephalothin' or
lower(concept_name) like 'cephapirin' or
lower(concept_name) like 'cephradine' or
lower(concept_name) like 'chloramphenicol' or
lower(concept_name) like 'cidofovir' or
lower(concept_name) like 'cinoxacin' or
lower(concept_name) like 'ciprofloxacin' or
lower(concept_name) like 'clarithromycin' or
lower(concept_name) like 'clindamycin' or
lower(concept_name) like 'clofazimine' or
lower(concept_name) like 'cloxacillin' or
lower(concept_name) like 'colistimethate' or
lower(concept_name) like 'colistin' or
lower(concept_name) like 'combivir' or
lower(concept_name) like 'cycloserine' or
lower(concept_name) like 'dalbavancin' or
lower(concept_name) like 'daptomycin' or
lower(concept_name) like 'delamanid' or
lower(concept_name) like 'dicloxacillin' or
lower(concept_name) like 'dolutegravir' or
lower(concept_name) like 'doravirine' or
lower(concept_name) like 'doripenem' or
lower(concept_name) like 'doxycycline' or
lower(concept_name) like 'ertapenem' or
lower(concept_name) like 'ethambutol' or
lower(concept_name) like 'etravirine' or
lower(concept_name) like 'evofloxacin' or
lower(concept_name) like 'famciclovir' or
lower(concept_name) like 'fidaxomicin' or
lower(concept_name) like 'fluconazole' or
lower(concept_name) like 'foscarnet' or
lower(concept_name) like 'fosfomycin' or
lower(concept_name) like 'ganciclovir' or
lower(concept_name) like 'gatifloxacin' or
lower(concept_name) like 'gemifloxacin' or
lower(concept_name) like 'gentamicin' or
lower(concept_name) like 'isavuconazole' or
lower(concept_name) like 'isavuconazonium' or
lower(concept_name) like 'isoniazid' or
lower(concept_name) like 'itraconazole' or
lower(concept_name) like 'kanamycin' or
lower(concept_name) like 'letermovir' or
lower(concept_name) like 'levofloxacin' or
lower(concept_name) like 'lincomycin' or
lower(concept_name) like 'linezolid' or
lower(concept_name) like 'meropenem' or
lower(concept_name) like 'methicillin' or
lower(concept_name) like 'metronidazole' or
lower(concept_name) like 'mezlocillin' or
lower(concept_name) like 'micafungin' or
lower(concept_name) like 'minocycline' or
lower(concept_name) like 'mipenem' or
lower(concept_name) like 'molnupiravir' or
lower(concept_name) like 'moxifloxacin' or
lower(concept_name) like 'nafcillin' or
lower(concept_name) like 'nitrofurantoin' or
lower(concept_name) like 'norfloxacin' or
lower(concept_name) like 'nystatin' or
lower(concept_name) like 'ofloxacin' or
lower(concept_name) like 'oritavancin' or
lower(concept_name) like 'oseltamivir' or
lower(concept_name) like 'oxacillin' or
lower(concept_name) like 'pas' or
lower(concept_name) like 'penicillin' or
lower(concept_name) like 'pentamidine' or
lower(concept_name) like 'peramivir' or
lower(concept_name) like 'piperacillin' or
lower(concept_name) like 'pivampicillin' or
lower(concept_name) like 'polymyxin B' or
lower(concept_name) like 'posaconazole' or
lower(concept_name) like 'praziquantel' or
lower(concept_name) like 'pretomanid' or
lower(concept_name) like 'primaquine' or
lower(concept_name) like 'prothionamide' or
lower(concept_name) like 'pyrazinamide' or
lower(concept_name) like 'raltegravir' or
lower(concept_name) like 'remdesivir' or
lower(concept_name) like 'rifabutin' or
lower(concept_name) like 'rifampicin' or
lower(concept_name) like 'rifampin' or
lower(concept_name) like 'rifaximin' or
lower(concept_name) like 'roxithromycin' or
lower(concept_name) like 'streptomycin' or
lower(concept_name) like 'sulfadiazine' or
lower(concept_name) like 'sulfadiazine trimethoprim' or
lower(concept_name) like 'sulfamethoxazole' or
lower(concept_name) like 'sulfisoxazole' or
lower(concept_name) like 'tecovirimat' or
lower(concept_name) like 'tedizolid' or
lower(concept_name) like 'teicoplanin' or
lower(concept_name) like 'telavancin' or
lower(concept_name) like 'telithromycin' or
lower(concept_name) like 'tenofovir alafenamide' or
lower(concept_name) like 'terbinafine' or
lower(concept_name) like 'tetracycline' or
lower(concept_name) like 'ticarcillin' or
lower(concept_name) like 'tigecycline' or
lower(concept_name) like 'tobramycin' or
lower(concept_name) like 'trimethoprim' or
lower(concept_name) like 'valacyclovir' or
lower(concept_name) like 'valganciclovir' or
lower(concept_name) like 'vancomycin' or
lower(concept_name) like 'voriconazole' or
lower(concept_name) like 'amoxicillin hydrate' or
lower(concept_name) like 'ampicillin sodium' or
lower(concept_name) like 'artesunate' or
lower(concept_name) like 'atazanavir' or
lower(concept_name) like 'atovaquone' or
lower(concept_name) like 'ceftolozane' or
lower(concept_name) like 'darunavir' or
lower(concept_name) like 'emtricitabine' or
lower(concept_name) like 'imipenem' or
lower(concept_name) like 'lopinavir' or
lower(concept_name) like 'nirmatrelvir' or
lower(concept_name) like 'quinupristin' or
lower(concept_name) like 'tixagevimab' or
lower(concept_name) like 'lamivudine' or
lower(concept_name) like 'potassium clavulanate' or
lower(concept_name) like 'clavulanate' or
lower(concept_name) like 'sulbactam sodium' or
lower(concept_name) like 'sulbactam' or
lower(concept_name) like 'pyronaridine' or
lower(concept_name) like 'cobicistat' or
lower(concept_name) like 'proguanil' or
lower(concept_name) like 'avibactam' or
lower(concept_name) like 'tazobactam' or
lower(concept_name) like 'tenofovir disoproxil fumarate' or
lower(concept_name) like 'cilastatin' or
lower(concept_name) like 'ritonavir' or
lower(concept_name) like 'vaborbactam' or
lower(concept_name) like 'dalfopristin' or
lower(concept_name) like 'cilgavimab' or
lower(concept_name) like 'bictegravir' or
lower(concept_name) like 'tenofovir')
)
    select person_id, visit_occurrence_id,drug_exposure_id, drug_exposure_start_datetime, drug_exposure_end_datetime,
    drug_exposure_start_date, drug_exposure_end_date,
    drug_exposure_end_date-drug_exposure_start_date as drug_days, drug_source_value, route_source_value, quantity
    from cdm_2021.origin_drug_exposure
    where drug_concept_id in (select descendant_concept_id from cdm_voca_2021.concept_ancestor where ancestor_concept_id in (select concept_id from antibio_db))
)



