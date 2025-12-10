{{ config(materialized='table') }}

with
  bz_empresas_bolsa as (
    select *
      from {{ ref('bz_empresas_bolsa') }}
  ),
  end_cte as (
    select lower(cd_acao_rdz) as cd_acao_rdz
         , cd_acao
         , nm_empresa
         , setor_economico
         , subsetor
         , segmento
         , segmento_b3
         , nm_segmento_b3
         , tx_cnpj
         , vl_cnpj
         , right(lpad(regexp_replace(coalesce(tx_cnpj, vl_cnpj, ''), '[^0-9]', '', 'g'), 14, '0'), 14) as cnpj
      from bz_empresas_bolsa
  )
select *
  from end_cte
