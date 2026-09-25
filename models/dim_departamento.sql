with silver as (
    select * from {{ ref('silver_casos') }}
)

select
    departamento,
    residencia_provincia_id as provincia_id,
    first(departamento_id_original) as departamento_id_original
from silver
where departamento is not null
group by departamento, residencia_provincia_id