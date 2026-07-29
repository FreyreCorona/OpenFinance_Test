## Status
Aceito

## Contexto
O antifraude estava recebendo eventos duplicados. A causa era reprocessamento por rebalance aliado a auto-commit com intervalos grandes.

## Decisão
Desabilitar `auto.commit` e usar `commitSync()` manual apenas após processar e registrar o `transaction_id` no Redis. O handler usa `MarkMessage` mais cedo para evitar reprocessamento em crash, mas o offset só avança no próximo rebalance.

## Consequências
- Duplicatas eliminadas mesmo com rebalanceamentos
- Menos throughput que auto-commit (síncrono)
- Store Redis vira fonte de verdade para dedup