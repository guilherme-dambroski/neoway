{{ config(materialized='table') }}

{{ bronze_seed('cotacoes_bolsa') }}