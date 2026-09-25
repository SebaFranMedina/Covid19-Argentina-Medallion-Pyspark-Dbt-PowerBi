select
    sexo,
    count(*) as total_fallecidos
from {{ ref('fact_casos') }}
where fallecido = true
group by sexo