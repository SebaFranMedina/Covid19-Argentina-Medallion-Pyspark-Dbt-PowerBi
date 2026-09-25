with silver as (
    select * from {{ ref('silver_casos') }}
)

select distinct
    residencia_provincia_id as provincia_id,
    provincia
from silver
where residencia_provincia_id is not null