select
    fecha_inicio_sintomas as fecha,
    count(*)                                       as casos,
    sum(case when fallecido then 1 else 0 end)      as fallecidos
from {{ ref('fact_casos') }}
where fecha_inicio_sintomas is not null
group by fecha_inicio_sintomas
order by fecha