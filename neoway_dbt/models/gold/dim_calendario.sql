{{ config(materialized='table') }}

with
  limite as (
    select date '1990-01-01' as dt_min
         , ((current_date + interval 365 day)::date) as dt_max
  ),
  dia as (
    select unnest(generate_series(dt_min, dt_max, interval 1 day))::date as dt
      from limite
  ),
  end_cte as (
    select dt                                                 as dt_calendario
         , year(dt)                                           as ano
         , month(dt)                                          as mes
         , day(dt)                                            as dia
         , cast(strftime(dt, '%w') as int)                    as dia_semana_num
         , case cast(strftime(dt, '%w') as int)
           when 0 then 'domingo'
           when 1 then 'segunda'
           when 2 then 'terca'
           when 3 then 'quarta'
           when 4 then 'quinta'
           when 5 then 'sexta'
           when 6 then 'sabado'
            end                                               as dia_semana_nome
         , cast(strftime(dt, '%W') as int)                    as semana_ano
         , strftime(dt, '%Y-%m-01')::date                     as primeiro_dia_mes
         , case when cast(strftime(dt, '%w') as int) in (0,6)
                then 1
                else 0
            end                                               as fim_semana
      from dia
  )
select *
  from end_cte
