# Data Lakehouse E-commerce — Databricks Asset Bundle

Projeto acadêmico de Data Lakehouse (arquitetura Medallion) construído inteiramente com
**Lakeflow Declarative Pipelines** (DLT) em SQL, **Unity Catalog** (governança, máscara,
row filter) e empacotado como **Databricks Asset Bundle**, rodando em computação
**serverless** (Databricks Free Edition).

## Arquitetura

```
                 ┌──────────────┐
 gerador_dados → │   Volume     │  bronze.landing (JSON/CSV)
                 │  (landing)   │
                 └──────┬───────┘
                        │ Auto Loader (STREAM read_files)
                        ▼
                 ┌──────────────┐
                 │   BRONZE     │  streaming tables, schema fixo, _rescued_data
                 └──────┬───────┘
                        │ expectations + CDC (AUTO CDC INTO)
                        ▼
                 ┌──────────────┐
                 │   SILVER     │  clientes (SCD2), pedidos (SCD1) + quarentena, produtos (SCD1)
                 └──────┬───────┘
                        │ + referencia.categorias (dependência externa)
                        ▼
                 ┌──────────────┐
                 │    GOLD      │  MVs (liquid clustering), dim_clientes mascarada,
                 └──────────────┘  streaming table com watermark
```

- **Bronze**: 3 streaming tables (`clientes_raw`, `pedidos_raw`, `produtos_raw`) via Auto
  Loader (`STREAM read_files`), com `schemaHints`, coluna de rescued data e expectation WARN.
- **Silver**: streaming tables de eventos validados (expectations `DROP ROW`/`FAIL UPDATE`) +
  `CREATE FLOW ... AUTO CDC INTO` para `clientes` (SCD Type 2), `pedidos` (SCD Type 1, com
  quarentena `pedidos_quarentena`) e `produtos` (SCD Type 1 com DELETE).
- **Gold**: materialized views com `CLUSTER BY` (liquid clustering) para métricas de negócio,
  `dim_clientes` com colunas `MASK`, e uma streaming table (`pedidos_por_hora`) com
  `WATERMARK`.
- **Governança (Unity Catalog)**: funções de máscara (e-mail, CPF, telefone) liberadas para
  os grupos `ecom_engenharia`/`ecom_pii_leitores`; isolamento silver↔gold (analistas só leem
  gold); grupos de conta `ecom_engenharia`, `ecom_analistas`, `ecom_pii_leitores`.
- **Operação**: manutenção Delta (time travel, RESTORE, OPTIMIZE ZORDER, REORG, VACUUM),
  métricas de qualidade a partir do event log do pipeline, demo de Auto Loader standalone com
  watermark + `dropDuplicatesWithinWatermark`, e validação de governança.

## Estrutura do projeto

```
databricks.yml              # bundle, variables, targets — inclui resources/*.yml
resources/
  pipeline_ecommerce.yml    # definição do pipeline Lakeflow
  job_setup_e_carga.yml     # job principal (setup → dados → pipeline → operação)
  job_ingestao.yml          # job disparado por file_arrival no volume de landing (PAUSED)
  job_manutencao.yml        # job agendado (cron, PAUSED) para manutenção noturna
src/
  setup/                    # catálogo, governança, gerador de dados sintéticos
  pipeline/                 # bronze.sql, silver.sql, gold.sql (libraries do pipeline)
  operacao/                 # manutenção Delta, métricas, checkpoint demo, validação
docs/
  workflow/                 # JSONs de referência/documentais (não usados pelo bundle)
EVIDENCIAS.md                # consultas de evidência coletadas após a execução
```

## Como rodar

Pré-requisitos: Databricks CLI ≥ 0.230 autenticada via OAuth (`databricks auth login`),
Terraform disponível para o `databricks bundle` (veja nota abaixo).

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run setup_e_carga
```

> **Nota sobre o Terraform da CLI**: neste ambiente, o download automático do Terraform pela
> CLI falhou por uma chave GPG expirada do lado do HashiCorp. Contornei apontando
> `DATABRICKS_TF_EXEC_PATH` para um binário do Terraform já presente localmente (instalado
> junto com a extensão Databricks do VS Code). Se você encontrar o mesmo erro, aponte essa
> variável de ambiente para qualquer binário `terraform` ≥ 1.5 disponível na máquina.

Os grupos `ecom_engenharia`, `ecom_analistas` e `ecom_pii_leitores` precisam existir como
**grupos de conta** (não de workspace) antes do deploy — crie-os em *Settings > Identity
and access > Groups* e adicione seu usuário a `ecom_engenharia`.

## Decisões e adaptações feitas durante a implementação

Trabalho iterativo: a cada erro do `databricks bundle run`, o arquivo responsável foi
corrigido, sem remover a intenção original do requisito. Principais adaptações:

1. **Grupos do Unity Catalog precisam ser de conta, não de workspace.** `databricks groups
   create` cria grupos de workspace, que o `GRANT` do Unity Catalog não reconhece
   (`PRINCIPAL_DOES_NOT_EXIST`). Os grupos foram recriados como grupos de conta pela UI.
2. **`CHECK` constraint não é aceito inline em `CREATE TABLE`.** Movido para
   `ALTER TABLE ... ADD CONSTRAINT` em `referencia.categorias`.
3. **`ROW FILTER` não é aceito em `MATERIALIZED VIEW`** neste motor do Lakeflow (erro de
   sintaxe testado em produção). Mantido apenas `MASK` na MV `gold.dim_clientes`; o filtro por
   UF foi implementado por fora, na view dinâmica `gold.vw_clientes_regional`
   (`src/operacao/governanca_validacao.sql`), que reaplica máscara + `governanca.filtro_uf`.
4. **Funções `MASK` precisam de nome totalmente qualificado** (`${catalogo}.governanca...`)
   dentro do pipeline — o catálogo padrão de resolução da MV não era o do pipeline.
5. **`WATERMARK` vem logo após o `FROM STREAM(...)`**, antes do `WHERE` (não depois).
6. **Streaming não lê de uma tabela que sofre `MERGE`** (`silver.pedidos`, alvo do CDC). A
   streaming table `gold.pedidos_por_hora` passou a ler de `silver.pedidos_eventos_validos`
   (streaming table só de append, um evento por linha), preservando o watermark.
7. **`__START_AT`/`__END_AT` do CDC seguem o tipo da coluna do `SEQUENCE BY`.** Como
   `clientes` usa `SEQUENCE BY evento_seq` (BIGINT), a coluna correspondente em
   `gold.dim_clientes` é `vigente_desde_evento_seq BIGINT`, não `TIMESTAMP`.
8. **`table_changes` e `RESTORE ... TO VERSION AS OF` exigem valor literal**, não subquery.
   Resolvido calculando a versão em uma célula Python e interpolando o literal na consulta.
9. **`event_log` não é configurável via `databricks.yml`** nesta versão da CLI (campo
   desconhecido). Em vez de publicar o event log numa tabela, as consultas usam a função
   `event_log(TABLE(...))`, que resolve o pipeline dono da tabela automaticamente.
10. **CREATE CATALOG é permitido nesta Free Edition** — testado antes de decidir a estratégia;
    não foi necessário usar o catálogo `workspace` como contingência.

## Grupos e isolamento de dados

| Grupo               | Acesso                                                          |
|---------------------|------------------------------------------------------------------|
| `ecom_engenharia`   | leitura/escrita em bronze, silver, gold + volume de landing      |
| `ecom_pii_leitores` | leitura de gold (com PII real, sem máscara)                      |
| `ecom_analistas`    | leitura de gold (com PII mascarada e apenas UFs SP/RJ/MG/GO/DF)  |

## Links

- Pipeline: `ecommerce_medallion_dlt` — Unity Catalog → Data Engineering → Pipelines
- Jobs: `ecommerce_lakehouse_setup_e_carga`, `ecommerce_lakehouse_ingestao` (PAUSED),
  `ecommerce_lakehouse_manutencao` (PAUSED)
- Evidências completas: [EVIDENCIAS.md](EVIDENCIAS.md)
