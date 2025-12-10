{{ config(materialized='table') }}

{{ bronze_seed('df_empresas') }}
