with bronze as (
    select * from {{ source('covid', 'bronze_casos') }}
),

typed as (
    select
        id_evento_caso                                     as id_caso,
        sexo,
        cast(edad as int)                                  as edad_original,
        `edad_años_meses`,
        residencia_pais_nombre                             as pais,
        residencia_provincia_nombre                         as provincia,
        residencia_departamento_nombre                       as departamento,
        cast(fecha_inicio_sintomas as date)                 as fecha_inicio_sintomas,
        cast(fecha_apertura as date)                        as fecha_apertura,
        cast(fecha_internacion as date)                     as fecha_internacion,
        case when cuidado_intensivo = 'SI' then true
             when cuidado_intensivo = 'NO' then false end   as requirio_uti,
        cast(fecha_cui_intensivo as date)                   as fecha_cui_intensivo,
        case when fallecido = 'SI' then true
             when fallecido = 'NO' then false end           as fallecido,
        cast(fecha_fallecimiento as date)                   as fecha_fallecimiento,
        case when asistencia_respiratoria_mecanica = 'SI' then true
             when asistencia_respiratoria_mecanica = 'NO' then false end as requirio_arm,
        case when origen_financiamiento = '*sin dato*' then null
             else origen_financiamiento end                 as origen_financiamiento,
        clasificacion                                        as clasificacion_detalle,
        clasificacion_resumen                                as clasificacion,
        cast(fecha_diagnostico as date)                     as fecha_diagnostico,
        residencia_provincia_id,
        residencia_departamento_id                          as departamento_id_original
    from bronze
),

with_edad as (
    select
        *,
        case
            when `edad_años_meses` = 'Meses' then 0
            when `edad_años_meses` = 'Años' and edad_original between 0 and 120 then edad_original
            else null
        end as edad_anios
    from typed
),

deduped as (
    select *,
        row_number() over (
            partition by id_caso
            order by case when origen_financiamiento = 'Público' then 0 else 1 end
        ) as rn
    from with_edad
)

select
    id_caso, sexo, edad_anios, pais, provincia, departamento,
    fecha_inicio_sintomas, fecha_apertura, fecha_internacion,
    requirio_uti, fecha_cui_intensivo, fallecido, fecha_fallecimiento,
    requirio_arm, origen_financiamiento, clasificacion_detalle, clasificacion,
    fecha_diagnostico, residencia_provincia_id, departamento_id_original
from deduped
where rn = 1