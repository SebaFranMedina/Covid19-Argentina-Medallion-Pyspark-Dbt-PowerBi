with fact as (
    select * from {{ ref('fact_casos') }}
),
dim as (
    select * from {{ ref('dim_provincia') }}
)

select
    d.provincia,
    count(*)                                                    as total_casos,
    sum(case when f.fallecido then 1 else 0 end)                as total_fallecidos,
    round(sum(case when f.fallecido then 1 else 0 end) / count(*) * 100, 2) as tasa_letalidad,
    sum(case when f.requirio_uti then 1 else 0 end)              as total_uti,
    round(avg(f.dias_a_diagnostico), 2)                          as promedio_dias_diagnostico
from fact f
join dim d on f.provincia_id = d.provincia_id
group by d.provincia
order by total_casos desc