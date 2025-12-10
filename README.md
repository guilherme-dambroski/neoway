# Neoway - Pipeline de Dados

Pipeline de dados com dbt, DuckDB e Airflow para processamento de cotações da B3.

## Arquitetura

**Stack:**
- dbt: Transformação de dados (Bronze > Silver > Gold)
- DuckDB: Engine OLAP local
- Airflow: Orquestração
- Docker: Containerização

**Fluxo:**
1. `dbt run` - Processa dados em `dev.duckdb`
2. `dbt test` - 17 testes de qualidade
3. Se testes OK, copia `dev.duckdb` para `prod.duckdb`
4. PowerBI consome `prod.duckdb`

## Execução

```bash
cd neoway_airflow
docker-compose up -d --build
```

Acesse: `http://localhost:8080` (admin/admin)

Ative a DAG `neoway_dbt_orchestration`

## Estrutura

```
/neoway_dbt          # Modelos SQL, seeds, testes
/neoway_airflow      # DAGs e infraestrutura
/valid               # Validações SQL e investigação
```

## Testes de Qualidade

**Automatizados (dbt test):**
- 17 testes executados a cada run
- 16 PASS, 1 WARN (1232 cotações com CNPJ zerado)
- Pipeline só publica se testes críticos passarem

**Manuais:**
- `valid/validacoes_dbeaver.sql`: 12 queries SQL
- Conecte ao `dev.duckdb` no DBeaver

## Problema Resolvido

**Erro:** Teste `unique_dim_empresa_cnpj` falhando
- CNPJ zerado duplicado 66x na dim_empresa
- Inflação na view final (80k linhas extras)

**Solução:** Filtro em `dim_empresa.sql`
```sql
where cnpj is not null and cnpj <> '00000000000000'
```

**Resultado:** 16/16 testes passando, inflação eliminada

Detalhes em `valid/investigacao_cnpj_zerado.md`

## Ajustes na View Final

Durante a revisão da `vw_cotacao`, alguns campos foram removidos e outros receberam nomes mais claros. A decisão foi simples e prática:

### 1. Remoção de colunas que não ajudavam na análise
Alguns campos foram retirados porque não contribuíam para responder ao case, eram redundantes ou estavam completamente vazios nas bases originais.

**Exemplos práticos dos campos removidos:**  
- **nm_segmento_b3** – veio 100% nulo nas bases, não serve para cortes nem filtros.  
- **vl_mlh_oft_compra** e **vl_mlh_oft_venda** – dados de ofertas no book, não fazem parte de nenhum KPI solicitado.  
- **tx_cnpj** e **vl_cnpj** – duplicações do mesmo identificador que já existe limpo como `cnpj`.  
- **prazot, moeda_ref, especi, tp_reg, tp_merc** – metadados da B3 que não afetam nenhum indicador do case.  
- **dt_vnct_opc, vl_exec_opc, in_opc** – campos de opções; o case é exclusivamente sobre ações.  
- **created_at, updated_at, __index_level_0__** – metadados do arquivo, sem utilidade analítica.  
- **endereco_municipio, endereco_mesorregiao, endereco_cep** – granularidade desnecessária; UF + região já resolvem tudo que o case pede.

### 2. Renomeação para tornar o uso mais direto
Campos como `vl_fechamento`, `vl_medio` e `vl_volume` foram renomeados para versões mais intuitivas, como `preco_fechamento`, `preco_medio` e `volume_financeiro`. Isso deixa a visualização mais limpa e evita perda de tempo interpretando abreviações.
O objetivo foi deixar a view pronta para consumo e fácil de entender, sem excesso de colunas e sem nomes obscuros.

## Tecnologias

- Airflow 2.9.1
- dbt 1.8.7
- DuckDB 1.0.0
- Docker
- Python 3.12

---

Guilherme Dambroski - 2025
