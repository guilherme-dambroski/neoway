{{ config(materialized='table') }}

with
  empresas_nivel_atividade as (
    select *
      from {{ ref('bz_empresas_nivel_atividade') }}
  ),
  end_cte as (
    select lpad(regexp_replace(cnpj, '[^0-9]', ''), 14, '0') as cnpj
         , nivel_atividade
      from empresas_nivel_atividade
  )
select *
  from end_cte
