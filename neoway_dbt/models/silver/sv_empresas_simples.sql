{{ config(materialized='table') }}

with
  bz_empresas_simples as (
    select *
      from {{ ref('bz_empresas_simples') }}
  ),
  end_cte as (
    select lpad(regexp_replace(cnpj, '[^0-9]', ''), 14, '0')  as cnpj
         , optante_simples
         , optante_simei
      from bz_empresas_simples
  )
select *
  from end_cte
