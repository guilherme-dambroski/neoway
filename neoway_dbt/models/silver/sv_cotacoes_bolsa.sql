{{ config(materialized='table') }}

with
  bz_cotacoes_bolsa as (
    select *
      from {{ ref('bz_cotacoes_bolsa') }}
  ),
  end_cte as (
    select try_strptime(dt_pregao, '%Y%m%d')::date        as dt_pregao
         , cd_acao_rdz                                    as cd_acao_rdz
         , cd_isin
         , nm_empresa_rdz
         , cd_acao
         , cd_bdi
         , tp_merc
         , especi
         , replace(vl_abertura, ',', '.')::double         as vl_abertura
         , replace(vl_maximo, ',', '.')::double           as vl_maximo
         , replace(vl_minimo, ',', '.')::double           as vl_minimo
         , replace(vl_medio, ',', '.')::double            as vl_medio
         , replace(vl_fechamento, ',', '.')::double       as vl_fechamento
         , replace(vl_ttl_neg, ',', '.')::double          as vl_ttl_neg
         , try_cast(qt_tit_neg as bigint)                 as qt_tit_neg
         , replace(vl_volume, ',', '.')::double           as vl_volume
         , replace(vl_mlh_oft_compra, ',', '.')::double   as vl_mlh_oft_compra
         , replace(vl_mlh_oft_venda, ',', '.')::double    as vl_mlh_oft_venda
         , in_opc
         , try_strptime(dt_vnct_opc, '%Y%m%d')::date      as dt_vnct_opc
         , replace(vl_exec_opc, ',', '.')::double         as vl_exec_opc
         , try_cast(replace(ft_cotacao, ',', '.') as int) as ft_cotacao
         , replace(vl_exec_moeda_corrente, ',', '.')::double as vl_exec_moeda_corrente
      from bz_cotacoes_bolsa
  )
select *
  from end_cte
