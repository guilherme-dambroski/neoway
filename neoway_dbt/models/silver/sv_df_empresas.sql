{{ config(materialized='table') }}

with
  bz_df_empresas as (
    select *
      from {{ ref('bz_df_empresas') }}
  ),
  end_cte as (
    select right(lpad(regexp_replace(coalesce(cnpj, ''), '[^0-9]', '', 'g'), 14, '0'), 14) as cnpj
         , coalesce(
             try_strptime(dt_abertura, '%Y-%m-%d')::date,
             try_strptime(dt_abertura, '%d/%m/%Y')::date
           )                                          as dt_abertura
         , cd_cnae_principal
         , de_cnae_principal
         , de_ramo_atividade
         , de_setor
         , endereco_cep
         , endereco_municipio
         , endereco_uf
         , endereco_regiao
         , endereco_mesorregiao
         , situacao_cadastral
      from bz_df_empresas
  )
select *
  from end_cte
