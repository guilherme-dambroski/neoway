/*
Validações de Qualidade - Pipeline dbt Neoway
Guilherme Dambroski - 2025-12-10

Conecte ao dev.duckdb no DBeaver e execute as queries.
*/

-- BRONZE: Validar carregamento dos CSVs
select 'empresas_bolsa' as tabela, count(*) as registros from main_bronze.empresas_bolsa
union all select 'df_empresas', count(*) from main_bronze.df_empresas
union all select 'empresas_nivel_atividade', count(*) from main_bronze.empresas_nivel_atividade
union all select 'empresas_porte', count(*) from main_bronze.empresas_porte
union all select 'empresas_saude_tributaria', count(*) from main_bronze.empresas_saude_tributaria
union all select 'empresas_simples', count(*) from main_bronze.empresas_simples
union all select 'cotacoes_b3', count(*) from main_bronze.cotacoes_b3
order by 1;

-- BRONZE: CNPJs zerados nas seeds
select 'empresas_bolsa' as tabela, count(*) as cnpj_zerado
  from main_bronze.empresas_bolsa where cnpj = 00000000000000
union all select 'df_empresas', count(*) from main_bronze.df_empresas where cnpj = 00000000000000
union all select 'empresas_nivel_atividade', count(*) from main_bronze.empresas_nivel_atividade where cnpj = 00000000000000
union all select 'empresas_porte', count(*) from main_bronze.empresas_porte where cnpj = 00000000000000
union all select 'empresas_saude_tributaria', count(*) from main_bronze.empresas_saude_tributaria where cnpj = 00000000000000
union all select 'empresas_simples', count(*) from main_bronze.empresas_simples where cnpj = 00000000000000
order by 2 desc;

-- SILVER: Comparar Bronze vs Silver (deve ser igual)
with
  bronze as (
    select 'empresas_bolsa' as tabela, count(*) as cnt from main_bronze.empresas_bolsa
    union all select 'df_empresas', count(*) from main_bronze.df_empresas
    union all select 'cotacoes_b3', count(*) from main_bronze.cotacoes_b3
  ),
  silver as (
    select 'empresas_bolsa' as tabela, count(*) as cnt from main_silver.sv_empresas_bolsa
    union all select 'df_empresas', count(*) from main_silver.sv_df_empresas
    union all select 'cotacoes_b3', count(*) from main_silver.sv_cotacoes_b3
  )
select b.tabela, b.cnt as bronze, s.cnt as silver, (s.cnt - b.cnt) as diff
  from bronze b join silver s on b.tabela = s.tabela
 order by b.tabela;

-- GOLD: Contagem geral
select 'dim_empresa' as tabela, count(*) as registros from main_gold.dim_empresa
union all select 'dim_calendario', count(*) from main_gold.dim_calendario
union all select 'fact_cotacao', count(*) from main_gold.fact_cotacao
union all select 'vw_cotacao', count(*) from main_gold.vw_cotacao;

-- GOLD: CNPJ zerado (deve ser 0 na dim_empresa após correção)
select 'silver (origem)' as camada, count(*) as cnpj_zerado
  from main_silver.sv_empresas_bolsa where cnpj = 00000000000000
union all
select 'gold (dim_empresa)', count(*)
  from main_gold.dim_empresa where cnpj = 00000000000000
union all
select 'gold (fact_cotacao)', count(*)
  from main_gold.fact_cotacao where cnpj = 00000000000000;

-- GOLD: Duplicatas na dim_empresa (deve retornar 0 linhas)
select cnpj, count(*) as duplicatas
  from main_gold.dim_empresa
 group by cnpj
having count(*) > 1
 order by duplicatas desc;

-- GOLD: Inflação de linhas (fato vs view)
with contagens as (
  select (select count(*) from main_gold.fact_cotacao) as fato,
         (select count(*) from main_gold.vw_cotacao) as view
)
select fato, view, (view - fato) as inflacao,
       round((view - fato) * 100.0 / fato, 2) as percentual
  from contagens;

-- GOLD: Integridade referencial (fato > dim_empresa)
select count(*) as cotacoes_sem_empresa
  from main_gold.fact_cotacao fc
  left join main_gold.dim_empresa de on de.cnpj = fc.cnpj
 where de.cnpj is null;

-- GOLD: Integridade referencial (fato > dim_calendario)
select count(*) as cotacoes_sem_data
  from main_gold.fact_cotacao fc
  left join main_gold.dim_calendario dc on dc.dt_calendario = fc.dt_pregao
 where dc.dt_calendario is null;

-- GOLD: Grão da fact_cotacao (deve retornar 0 linhas)
select cd_acao, dt_pregao, count(*) as duplicatas
  from main_gold.fact_cotacao
 group by cd_acao, dt_pregao
having count(*) > 1
 order by duplicatas desc
 limit 10;

-- GOLD: Completude da dim_empresa
select count(*) as total,
       sum(case when nm_empresa is null then 1 else 0 end) as sem_nome,
       sum(case when cnpj is null then 1 else 0 end) as sem_cnpj,
       sum(case when dt_abertura is null then 1 else 0 end) as sem_data_abertura
  from main_gold.dim_empresa;

-- RESUMO: Linhagem completa
select 'Bronze' as camada, count(*) as total_registros
  from (select * from main_bronze.empresas_bolsa union all select * from main_bronze.df_empresas)
union all
select 'Silver', count(*)
  from (select * from main_silver.sv_empresas_bolsa union all select * from main_silver.sv_df_empresas)
union all
select 'Gold', count(*) from main_gold.dim_empresa;
