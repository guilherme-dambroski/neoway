{{ config(materialized='table') }}

{{ bronze_seed('empresas_saude_tributaria') }}
