{{ config(materialized='table') }}

{{ bronze_seed('empresas_nivel_atividade') }}
