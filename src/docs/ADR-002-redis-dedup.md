## Status
Aceito

## Contexto
O consumidor precisa garantir que cada evento seja processado apenas uma vez, mesmo com rebalanceamentos e reprocessamento de mensagens.

## Decisão
Usar Redis com `SETNX` e TTL como store centralizada. A interface `Store` permite trocar a implementação sem alterar o handler.

## Consequências
- Store compartilhada entre réplicas do consumer
- TTL evita acúmulo de chaves órfãs
- Dependência externa (Redis) adicionada ao pipeline