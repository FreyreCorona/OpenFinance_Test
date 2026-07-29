## Status
Aceito

## Contexto
Duplicatas podem ser geradas quando o produtor reintenta o envio após timeout de rede.

## Decisão
Habilitar `enable.idempotence=true` no produtor Sarama, que força `RequiredAcks=WaitForAll` e `MaxOpenRequests=1`, garantindo que mensagens duplicadas sejam descartadas pelo broker.

## Consequências
- Elimina duplicatas na origem
- Reduz throughput máximo (1 requisição por vez)
- Configuração automática pelo Sarama ao ativar idempotência