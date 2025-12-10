{{ config(materialized='table') }}

{{ bronze_seed('empresas_porte') }}
