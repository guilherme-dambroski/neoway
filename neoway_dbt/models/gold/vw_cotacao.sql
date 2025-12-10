{{ config(materialized='view') }}

with
  fact_cotacao as (
    select *
      from {{ ref('fact_cotacao') }}
  ),
  dim_empresa as (
    select *
      from {{ ref('dim_empresa') }}
  ),
  dim_calendario as (
    select *
      from {{ ref('dim_calendario') }}
  ),
  end_cte as (
    select fc.dt_pregao
         , fc.cd_acao
         , fc.vl_abertura      as preco_abertura
         , fc.vl_maximo        as preco_maximo
         , fc.vl_minimo        as preco_minimo
         , fc.vl_medio         as preco_medio
         , fc.vl_fechamento    as preco_fechamento
         , fc.vl_volume        as volume_financeiro
         , fc.vl_ttl_neg       as negocios
         , fc.qt_tit_neg       as quantidade_negociada
         , fc.cnpj
         , dc.ano
         , dc.mes
         , dc.semana_ano
         , dc.dia_semana_nome  as dia_semana
         , de.nm_empresa       as empresa
         , de.segmento_b3
         , de.empresa_porte
         , de.endereco_uf      as uf
         , de.endereco_regiao  as regiao
      from fact_cotacao fc
      left
      join dim_calendario dc
        on fc.dt_pregao = dc.dt_calendario
      left
      join dim_empresa de
        on fc.cnpj = de.cnpj
  )
select *
     , current_date as dt_update
  from end_cte
