CREATE TABLE cdm_antibiotics AS
SELECT
    person_id,
    visit_occurrence_id,
    drug_exposure_id,
    drug_exposure_start_datetime,
    drug_exposure_end_datetime,
    drug_source_value,
    route_source_value,
    quantity
FROM
    cdm_2021.origin_drug_exposure
WHERE
    drug_concept_id IN (1736997, 19006904, 19080857, 1703691, 21169993, 19002897, 40220937, 1717298, 35604135, 19082973,
                        2051215, 42966044, 42966048, 42966050, 42966052, 43201362, 43135328, 1713545, 19073187, 1713544,
                        40105046, 42951954, 42951957, 42951958, 40131076, 40131076, 35603389, 44180191, 19073777, 21149333,
                        43012519, 44105004, 46275519, 42924540, 42924590, 19074847, 1769598, 42972207, 42924604, 46287354,
                        42943315, 44107631, 1798477, 997934, 19109384, 19085270, 43269450, 40837466, 40837464, 1797556,
                        19075380, 1797513, 19088843, 19074932, 19074935, 1750723, 1750724, 1750562, 21123288, 19058072,
                        1778228, 46287418, 1710449, 46287328, 36884923, 46287335, 36894724, 35605994, 46275704, 46275711,
                        46275859, 42944728, 46287363, 46287336, 21061050, 45892974, 43560387, 793583, 46233710, 44110269,
                        35604225, 1703093, 40222616, 35606488, 1758538, 35606301, 1703632, 1703605, 1755077, 1754996,
                        1755073, 21039304, 19132594, 19077574, 45892419, 19124327, 19078728, 19078729, 35604068, 19081585,
                        1728416, 1742255, 19082484, 19085063, 19085063, 42953630, 1736892, 36896808, 40164950, 19081057,
                        1708904, 40166165, 35605999, 35606006, 19081680, 1707403, 1716905, 1716908, 35605334, 1799141,
                        19127259, 1799160, 40241045, 1726228, 42924566, 41146966, 21147798, 46275444, 46275427, 21164206,
                        35606450, 42955259, 41240556, 42973225, 19100017, 1750486, 19019699, 1763295, 1763296, 19089490,
                        1763297, 1735948, 1712891, 19122186, 19063879, 40081388, 40081388, 35605552, 964015, 19107132,
                        1836978, 45775747, 19122306, 36407417, 21172842, 21025554, 44104083, 40221357, 40752945, 1707751,
                        1714278, 19098164, 1703066, 1717708, 42939506, 36057816, 37499275, 19022896);