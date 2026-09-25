with silver as (
    select * from {{ ref('silver_casos') }}
),

dim_dep as (
    select * from {{ ref('dim_departamento') }}
)

select
    s.id_caso,
    s.sexo,
    s.edad_anios,
    s.pais,
    dp.provincia_id,
    dd.departamento,
    s.fecha_inicio_sintomas,
    s.fecha_apertura,
    s.fecha_internacion,
    s.requirio_uti,
    s.fecha_cui_intensivo,
    s.fallecido,
    s.fecha_fallecimiento,
    s.requirio_arm,
    s.origen_financiamiento,
    s.clasificacion,
    s.fecha_diagnostico,
    datediff(s.fecha_apertura, s.fecha_inicio_sintomas) as dias_a_notificacion,
    datediff(s.fecha_diagnostico, s.fecha_inicio_sintomas) as dias_a_diagnostico,
    datediff(s.fecha_internacion, s.fecha_inicio_sintomas) as dias_a_internacion,
    datediff(s.fecha_cui_intensivo, s.fecha_inicio_sintomas) as dias_a_uti,
    datediff(s.fecha_fallecimiento, s.fecha_inicio_sintomas) as dias_a_fallecimiento
from silver s
left join dim_dep dd
    on s.departamento = dd.departamento
    and s.residencia_provincia_id = dd.provincia_id
left join {{ ref('dim_provincia') }} dp
    on s.residencia_provincia_id = dp.provincia_id