select
    requirio_arm,
    count(*)                                                    as total_casos,
    sum(case when fallecido then 1 else 0 end)                  as total_fallecidos,
    round(sum(case when fallecido then 1 else 0 end) / count(*) * 100, 2) as tasa_letalidad
from {{ ref('fact_casos') }}
where requirio_arm is not null
group by requirio_arm