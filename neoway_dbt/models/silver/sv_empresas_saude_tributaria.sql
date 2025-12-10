{{ config(materialized='table') }}

with
  bz_empresas_saude_tributaria as (
    select *
      from {{ ref('bz_empresas_saude_tributaria') }}
  ),
  end_cte as (
    select lpad(regexp_replace(cnpj, '[^0-9]', ''), 14, '0')  as cnpj
         , saude_tributaria
      from bz_empresas_saude_tributaria
  )
select *
  from end_cte
