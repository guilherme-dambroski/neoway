{{ config(materialized='incremental', unique_key='cnpj') }}

with
  sv_empresas_bolsa as (
    select *
      from {{ ref('sv_empresas_bolsa') }}
     where cnpj is not null
       and cnpj <> '00000000000000'
  ),
  sv_df_empresas as (
    select *
      from {{ ref('sv_df_empresas') }}
  ),
  sv_empresas_nivel_atividade as (
    select *
      from {{ ref('sv_empresas_nivel_atividade') }}
  ),
  sv_empresas_porte as (
    select *
      from {{ ref('sv_empresas_porte') }}
  ),
  sv_empresas_saude_tributaria as (
    select *
      from {{ ref('sv_empresas_saude_tributaria') }}
  ),
  sv_empresas_simples as (
    select *
      from {{ ref('sv_empresas_simples') }}
  ),
  end_cte as (
    select eb.cnpj
         , eb.cd_acao_rdz
         , eb.cd_acao
         , eb.nm_empresa
         , eb.setor_economico
         , eb.subsetor
         , eb.segmento
         , eb.segmento_b3
         , eb.nm_segmento_b3
         , eb.tx_cnpj
         , eb.vl_cnpj
         , e.dt_abertura
         , e.cd_cnae_principal
         , e.de_cnae_principal
         , e.de_ramo_atividade
         , e.de_setor
         , e.endereco_cep
         , e.endereco_municipio
         , e.endereco_uf
         , e.endereco_regiao
         , e.endereco_mesorregiao
         , e.situacao_cadastral
         , nv.nivel_atividade
         , pr.empresa_porte
         , st.saude_tributaria
         , sp.optante_simples
         , sp.optante_simei
      from sv_empresas_bolsa eb
      left
      join sv_df_empresas e
        on eb.cnpj = e.cnpj
      left
      join sv_empresas_nivel_atividade nv
        on eb.cnpj = nv.cnpj
      left
      join sv_empresas_porte pr
        on eb.cnpj = pr.cnpj
      left
      join sv_empresas_saude_tributaria st
        on eb.cnpj = st.cnpj
      left
      join sv_empresas_simples sp
        on eb.cnpj = sp.cnpj
  )
select *
  from end_cte

