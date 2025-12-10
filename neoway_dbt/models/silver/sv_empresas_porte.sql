{{ config(materialized='table') }}

with
  bz_empresas_porte as (
    select *
      from {{ ref('bz_empresas_porte') }}
  ),
  end_cte as (
    select lpad(regexp_replace(cnpj, '[^0-9]', ''), 14, '0')  as cnpj
         , empresa_porte
      from bz_empresas_porte
  )
select *
  from end_cte
