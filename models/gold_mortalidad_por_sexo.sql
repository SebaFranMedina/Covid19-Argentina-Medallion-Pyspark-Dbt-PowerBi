select
    sexo,
    count(*)                                                    as total_casos,
    sum(case when fallecido then 1 else 0 end)                  as total_fallecidos,
    round(sum(case when fallecido then 1 else 0 end) / count(*) * 100, 2) as tasa_letalidad,
    sum(case when requirio_uti then 1 else 0 end)                as total_uti,
    round(sum(case when requirio_uti then 1 else 0 end) / count(*) * 100, 2) as tasa_uti,
    sum(case when requirio_arm then 1 else 0 end)                as total_arm,
    round(sum(case when requirio_arm then 1 else 0 end) / count(*) * 100, 2) as tasa_arm
from {{ ref('fact_casos') }}
group by sexo