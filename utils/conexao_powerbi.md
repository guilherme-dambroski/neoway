# Conexão Power BI com DuckDB

## Opção Recomendada: Arquivo Parquet

### Configuração Automática

O pipeline gera automaticamente o arquivo `vw_cotacao.parquet` após cada execução.

**Localização:** `D:\Code\Random\Testes Dev\Neoway\neoway\neoway_dbt\vw_cotacao.parquet`

### Conectar no Power BI

1. Abra Power BI Desktop
2. Obter Dados > Parquet
3. Navegue até: `D:\Code\Random\Testes Dev\Neoway\neoway\neoway_dbt\vw_cotacao.parquet`
4. Carregar

**Vantagens:**
- Conector nativo do Power BI
- Rápido (formato colunar otimizado)
- Sem dependências externas
- Atualização automática (após DAG executar)

### Atualização dos Dados

**Automática:** DAG executa diariamente às 6h e gera novo Parquet
**Manual:** Execute a DAG no Airflow e clique em "Atualizar" no Power BI

---

## Opção Alternativa: ODBC

### Instalação

1. Baixe: https://github.com/duckdb/duckdb-odbc/releases
2. Instale o driver 64-bit
3. Configure DSN:
   - Nome: `neoway_prod`
   - Database: `D:\Code\Random\Testes Dev\Neoway\neoway\neoway_dbt\prod.duckdb`
   - Username: (deixe vazio)
   - Password: (deixe vazio)

### Conectar no Power BI

1. Obter Dados > ODBC
2. DSN: `neoway_prod`
3. SQL: `SELECT * FROM main_gold.vw_cotacao`

**Desvantagem:** Requer instalação de driver adicional

---

## Recomendação

Use **Parquet** - é mais simples, nativo e performático.
