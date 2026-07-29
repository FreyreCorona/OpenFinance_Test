## Status
Aceito

## Contexto
Múltiplas réplicas do consumer no mesmo node causam contenção de recursos (CPU, rede) e rebalanceios mais frequentes quando o node cai.

## Decisão
Usar `preferredDuringSchedulingIgnoredDuringExecution` com `topologyKey: kubernetes.io/hostname` para distribuir réplicas entre nodes diferentes.

## Consequências
- Melhor distribuição de carga
- Rebalanceios menos frequentes
- Soft regra — não bloqueia deploy se houver poucos nodes