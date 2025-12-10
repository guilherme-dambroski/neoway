{{ config(materialized='incremental', unique_key=['dt_pregao','cd_acao_rdz']) }}

with
  sv_cotacoes_bolsa as (
    select *
      from {{ ref('sv_cotacoes_bolsa') }}
    {% if is_incremental() %}
      where dt_pregao >= (select coalesce(max(dt_pregao), '1900-01-01') from {{ this }})
    {% endif %}
  ),
  sv_empresas_bolsa as (
    select *
      from {{ ref('sv_empresas_bolsa') }}
  ),
  end_cte as (
    select cb.dt_pregao
         , cb.cd_acao_rdz
         , cb.cd_acao
         , cb.cd_isin
         , cb.nm_empresa_rdz
         , cb.cd_bdi
         , cb.tp_merc
         , cb.especi
         , cb.vl_abertura
         , cb.vl_maximo
         , cb.vl_minimo
         , cb.vl_medio
         , cb.vl_fechamento
         , cb.vl_volume
         , cb.vl_ttl_neg
         , cb.qt_tit_neg
         , cb.vl_mlh_oft_compra
         , cb.vl_mlh_oft_venda
         , cb.in_opc
         , cb.dt_vnct_opc
         , cb.vl_exec_opc
         , cb.ft_cotacao
         , cb.vl_exec_moeda_corrente
         , eb.cnpj
      from sv_cotacoes_bolsa cb
      left
      join sv_empresas_bolsa eb
        on cb.cd_acao_rdz = eb.cd_acao_rdz
  )
select *
  from end_cte