{% macro bronze_seed(seed_name) %}
with end_cte as (
  select *
    from {{ ref(seed_name) }}
)
select *
  from end_cte
{% endmacro %}
