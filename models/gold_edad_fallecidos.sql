select
    edad_anios,
    count(*) as total_fallecidos
from {{ ref('fact_casos') }}
where fallecido = true and edad_anios is not null
group by edad_anios
order by edad_anios