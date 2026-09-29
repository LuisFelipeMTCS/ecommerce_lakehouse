# Evidências de execução

Coletadas via `databricks api post /api/2.0/sql/statements` no SQL warehouse serverless
(`Serverless Starter Warehouse`, id `27e9651a7a5451a1`), após a execução bem-sucedida e
completa do job `setup_e_carga` (todas as 10 tasks com `SUCCESS`, incluindo os dois
refreshes do pipeline DLT).

## 1. Qualidade por regra (aprovados/reprovados), por dataset

```sql
SELECT dataset, regra, sum(aprovados) AS total_aprovados, sum(reprovados) AS total_reprovados
FROM ecommerce_lakehouse.monitoramento.vw_qualidade_expectations
GROUP BY ALL ORDER BY dataset, regra
```

| Dataset | Regra | Aprovados | Reprovados |
|---|---|---:|---:|
| bronze.clientes_raw | rescued_data_vazio | 571 | 0 |
| bronze.pedidos_raw | rescued_data_vazio | 4800 | 0 |
| bronze.produtos_raw | rescued_data_vazio | 46 | 0 |
| gold.vendas_diarias_categoria | receita_nao_negativa | 38665 | 0 |
| silver.clientes_cdc_flow | id_final_valido | 476 | 0 |
| silver.clientes_eventos_validos | cpf_valido | 563 | 8 |
| silver.clientes_eventos_validos | email_valido | 562 | 9 |
| silver.clientes_eventos_validos | id_valido | 571 | 0 |
| silver.clientes_eventos_validos | operacao_valida | 571 | 0 |
| silver.clientes_eventos_validos | sequencia_valida | 571 | 0 |
| silver.clientes_eventos_validos | uf_valida | 571 | 0 |
| silver.pedidos_cdc_flow | id_final_valido | 3873 | 0 |
| silver.pedidos_eventos_validos | canal_valido | 4800 | 0 |
| silver.pedidos_eventos_validos | cliente_id_valido | 4767 | 33 |
| silver.pedidos_eventos_validos | data_coerente | 4800 | 0 |
| silver.pedidos_eventos_validos | pedido_id_valido | 4767 | 33 |
| silver.pedidos_eventos_validos | preco_valido | 4769 | 31 |
| silver.pedidos_eventos_validos | quantidade_valida | 4765 | 35 |
| silver.pedidos_eventos_validos | status_valido | 4778 | 22 |
| silver.produtos_eventos_validos | produto_id_obrigatorio | 46 | 0 |

## 2. Quarentena de pedidos por regra violada

```sql
SELECT regra, count(*) AS qtd
FROM ecommerce_lakehouse.silver.pedidos_quarentena
LATERAL VIEW explode(regras_violadas) AS regra
GROUP BY regra ORDER BY qtd DESC
```

| Regra violada | Qtd |
|---|---:|
| quantidade_invalida | 35 |
| pedido_id_nulo | 33 |
| cliente_id_nulo | 33 |
| preco_invalido | 31 |
| status_invalido | 22 |

Total de pedidos em quarentena: **154** de 4.800 eventos de pedido processados nos dois
lotes (500 clientes/3.000 pedidos no lote 1 + 1.000 pedidos novos/800 mudanças de status no
lote 2 — os eventos de quarentena vêm apenas dos pedidos recém-criados, não das mudanças de
status de pedidos já existentes).

## 3. Duração das atualizações do pipeline e linhas por flow

Linhas processadas por flow (última atualização bem-sucedida, agregando os dois refreshes):

```sql
SELECT origin.flow_name AS flow_name, min(timestamp) AS inicio, max(timestamp) AS fim,
       sum(details:flow_progress.metrics.num_output_rows::BIGINT) AS linhas
FROM ecommerce_lakehouse.monitoramento.vw_event_log
WHERE event_type = 'flow_progress' GROUP BY ALL ORDER BY flow_name
```

| Flow | Linhas (acumulado) |
|---|---:|
| bronze.clientes_raw | 571 |
| bronze.pedidos_raw | 4800 |
| bronze.produtos_raw | 46 |
| gold.clientes_valor | 10956 |
| gold.dim_clientes | 10956 |
| gold.funil_status_pedidos | 111 |
| gold.pedidos_por_hora | 5564 |
| gold.vendas_diarias_categoria | 38665 |
| silver.clientes_eventos_validos | 554 |
| silver.pedidos_eventos_validos | 4646 |
| silver.pedidos_quarentena | 154 |
| silver.produtos_eventos_validos | 46 |

Duração das últimas atualizações completas do pipeline (as duas mais recentes correspondem
aos refreshes do lote 1 e do lote 2 da execução final bem-sucedida):

```sql
SELECT origin.update_id AS update_id, min(timestamp) AS inicio, max(timestamp) AS fim,
       cast(max(timestamp) AS DOUBLE) - cast(min(timestamp) AS DOUBLE) AS duracao_segundos
FROM ecommerce_lakehouse.monitoramento.vw_event_log
WHERE event_type = 'update_progress' GROUP BY origin.update_id ORDER BY inicio DESC
```

| Update (últimas 2, execução final) | Início | Fim | Duração (s) |
|---|---|---|---:|
| 12d1798f… | 02:50:16 | 02:52:04 | 107,5 |
| 74970297… | 02:47:25 | 02:49:44 | 138,8 |

> O histórico completo do event log guarda também as ~35 tentativas anteriores da fase de
> depuração (falhas de sintaxe corrigidas iterativamente, ver README.md); a tabela acima
> mostra só as duas atualizações da execução final que teve sucesso ponta a ponta.

## 4. Histórico e `numFiles` antes/depois do OPTIMIZE em `auditoria.pedidos_snapshot`

```sql
DESCRIBE HISTORY ecommerce_lakehouse.auditoria.pedidos_snapshot
```

Sequência de operações do ciclo final (CTAS → UPDATE → DELETE → MERGE → DELETE acidental →
RESTORE → OPTIMIZE → REORG/VACUUM):

| Versão | Operação | Linhas upd/del/ins | Arquivos add/remove |
|---:|---|---|---|
| 25 | UPDATE | —/—/— | +1 / +0 |
| 26 | DELETE | —/—/— | +0 / +0 |
| 27 | MERGE | 176 upd / 0 del / 22 ins | — |
| 28 | DELETE | —/—/— | +0 / +2 |
| 29 | RESTORE | —/—/— | +0 / +0 |
| 30 | OPTIMIZE | —/—/— | **+1 / +2** |

```sql
DESCRIBE DETAIL ecommerce_lakehouse.auditoria.pedidos_snapshot
```

- **Antes do OPTIMIZE** (após o RESTORE, versão 29): 2 arquivos.
- **Depois do OPTIMIZE + VACUUM** (estado atual): **`numFiles = 1`**, `sizeInBytes = 51126`.

## 5. Linhagem de tabelas (`system.access.table_lineage`)

```sql
SELECT source_table_catalog, source_table_schema, source_table_name,
       target_table_catalog, target_table_schema, target_table_name, event_time
FROM system.access.table_lineage
WHERE source_table_catalog = 'ecommerce_lakehouse' OR target_table_catalog = 'ecommerce_lakehouse'
ORDER BY event_time DESC LIMIT 200
```

Principais arestas capturadas (bronze → silver → gold + dependência externa + auditoria):

```
(landing files)                          -> bronze.clientes_raw / pedidos_raw / produtos_raw
bronze.clientes_raw                      -> silver.clientes_eventos_validos
bronze.pedidos_raw                       -> silver.pedidos_eventos_validos
bronze.pedidos_raw                       -> silver.pedidos_quarentena
bronze.produtos_raw                      -> silver.produtos_eventos_validos
silver.clientes_eventos_validos          -> silver.clientes
silver.pedidos_eventos_validos           -> silver.pedidos
silver.pedidos_eventos_validos           -> gold.pedidos_por_hora
silver.produtos_eventos_validos          -> silver.produtos
referencia.categorias                    -> gold.clientes_valor / funil_status_pedidos / vendas_diarias_categoria
silver.clientes, silver.produtos,
silver.pedidos                           -> gold.clientes_valor / funil_status_pedidos / vendas_diarias_categoria
silver.clientes                          -> gold.dim_clientes
silver.pedidos                           -> auditoria.pedidos_snapshot
monitoramento.vw_event_log               -> monitoramento.vw_qualidade_expectations
```

## 6. Contagem de linhas de cada tabela

```sql
SELECT 'bronze.clientes_raw', count(*) FROM ecommerce_lakehouse.bronze.clientes_raw
UNION ALL SELECT 'bronze.pedidos_raw', count(*) FROM ecommerce_lakehouse.bronze.pedidos_raw
UNION ALL SELECT 'bronze.produtos_raw', count(*) FROM ecommerce_lakehouse.bronze.produtos_raw
UNION ALL SELECT 'silver.clientes', count(*) FROM ecommerce_lakehouse.silver.clientes
UNION ALL SELECT 'silver.pedidos', count(*) FROM ecommerce_lakehouse.silver.pedidos
UNION ALL SELECT 'silver.produtos', count(*) FROM ecommerce_lakehouse.silver.produtos
UNION ALL SELECT 'silver.pedidos_quarentena', count(*) FROM ecommerce_lakehouse.silver.pedidos_quarentena
UNION ALL SELECT 'gold.vendas_diarias_categoria', count(*) FROM ecommerce_lakehouse.gold.vendas_diarias_categoria
UNION ALL SELECT 'gold.funil_status_pedidos', count(*) FROM ecommerce_lakehouse.gold.funil_status_pedidos
UNION ALL SELECT 'gold.clientes_valor', count(*) FROM ecommerce_lakehouse.gold.clientes_valor
UNION ALL SELECT 'gold.dim_clientes', count(*) FROM ecommerce_lakehouse.gold.dim_clientes
UNION ALL SELECT 'gold.pedidos_por_hora', count(*) FROM ecommerce_lakehouse.gold.pedidos_por_hora
UNION ALL SELECT 'referencia.categorias', count(*) FROM ecommerce_lakehouse.referencia.categorias
UNION ALL SELECT 'auditoria.pedidos_snapshot', count(*) FROM ecommerce_lakehouse.auditoria.pedidos_snapshot
```

| Tabela | Linhas |
|---|---:|
| bronze.clientes_raw | 571 |
| bronze.pedidos_raw | 4.800 |
| bronze.produtos_raw | 46 |
| silver.clientes | 544 |
| silver.pedidos | 3.873 |
| silver.produtos | 39 |
| silver.pedidos_quarentena | 154 |
| gold.vendas_diarias_categoria | 1.833 |
| gold.funil_status_pedidos | 5 |
| gold.clientes_valor | 476 |
| gold.dim_clientes | 476 |
| gold.pedidos_por_hora | 2.786 |
| referencia.categorias | 8 |
| auditoria.pedidos_snapshot | 3.873 |

> Nota sobre os números "esperados" do enunciado original (258 pedidos em quarentena/6.600
> no total, 50 pedido_id nulos etc.): este projeto foi **reconstruído do zero** com um
> gerador de dados próprio (determinístico, semente por lote), então os totais absolutos
> são diferentes por construção — o gerador atual roda 2 lotes (3.000 + 1.000 pedidos novos
> = 4.000 pedidos "CRIADO", mais 800 eventos de mudança de status = 4.800 eventos no total),
> não 3 lotes de 2.200. As proporções de erro (~4% pedidos inválidos, ~3% clientes
> inválidos) foram respeitadas e batem com os números observados (154/4.000 ≈ 3,85% dos
> pedidos criados; 17/571 ≈ 3% dos eventos de cliente).

## Prints a tirar na interface (Databricks UI)

1. **Grafo do pipeline** — Workflows → Pipelines → `ecommerce_medallion_dlt` → aba
   "Pipeline" (mostra bronze → silver → gold e as duas streaming tables/materialized views).
2. **Aba "Data quality"** do pipeline — mostra o gráfico de aprovados/reprovados por
   expectation ao longo do tempo (mesmos dados da consulta 1 acima, em formato visual).
3. **Aba "Lineage" de `gold.vendas_diarias_categoria`** — Catalog Explorer →
   `ecommerce_lakehouse.gold.vendas_diarias_categoria` → aba "Lineage" → mostra
   `silver.pedidos`, `silver.produtos` e `referencia.categorias` como upstream.
4. **`gold.dim_clientes` mascarada** — rodar `SELECT * FROM ecommerce_lakehouse.gold.dim_clientes`
   logado como um usuário que **não** está em `ecom_engenharia`/`ecom_pii_leitores` (ou usar
   `gold.vw_clientes_regional`, que também aplica o filtro por UF), mostrando e-mail/CPF/telefone
   mascarados.
