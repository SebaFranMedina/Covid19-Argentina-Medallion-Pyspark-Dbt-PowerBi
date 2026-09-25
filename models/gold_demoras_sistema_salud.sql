with fact as (
    select * from {{ ref('fact_casos') }}
),
dim as (
    select * from {{ ref('dim_provincia') }}
)

select
    d.provincia,
    round(avg(f.dias_a_notificacion), 2)   as promedio_dias_notificacion,
    round(avg(f.dias_a_diagnostico), 2)    as promedio_dias_diagnostico,
    round(avg(f.dias_a_internacion), 2)    as promedio_dias_internacion
from fact f
join dim d on f.provincia_id = d.provincia_id
group by d.provincia
order by promedio_dias_diagnostico desc