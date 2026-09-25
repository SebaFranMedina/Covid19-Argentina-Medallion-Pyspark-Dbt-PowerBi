select
    case
        when edad_anios is null then 'Sin dato'
        else concat(cast(floor(edad_anios / 10) * 10 as string), '-', cast(floor(edad_anios / 10) * 10 + 9 as string))
    end as rango_edad,
    count(*)                                           as total_casos,
    sum(case when fallecido then 1 else 0 end)          as total_fallecidos,
    round(sum(case when fallecido then 1 else 0 end) / count(*) * 100, 2) as tasa_letalidad,
    sum(case when requirio_uti then 1 else 0 end)       as total_uti
from {{ ref('fact_casos') }}
group by rango_edad
order by rango_edad