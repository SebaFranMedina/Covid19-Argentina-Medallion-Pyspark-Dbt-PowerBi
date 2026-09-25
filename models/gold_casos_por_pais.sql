select
    coalesce(pais, 'Sin dato') as pais,
    count(*)                   as total_casos
from {{ ref('fact_casos') }}
group by pais
order by total_casos desc