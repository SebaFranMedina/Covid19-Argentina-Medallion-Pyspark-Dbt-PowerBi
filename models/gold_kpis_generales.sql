select
    count(*)                                                as total_casos,
    sum(case when fallecido then 1 else 0 end)              as total_fallecidos,
    round(sum(case when fallecido then 1 else 0 end) / count(*) * 100, 2) as tasa_letalidad,
    sum(case when requirio_uti then 1 else 0 end)            as total_uti,
    sum(case when requirio_arm then 1 else 0 end)            as total_arm,
    count(distinct provincia_id)                             as provincias_afectadas
from {{ ref('fact_casos') }}