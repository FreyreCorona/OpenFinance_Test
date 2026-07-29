## Status
Aceito

## Contexto
Precisávamos de um broker Kafka funcional para o pipeline de eventos. A abordagem tradicional usa Zookeeper como coordenador, mas adiciona complexidade operacional (um serviço extra para gerenciar).

## Decisão
Usar KRaft mode, que elimina a dependência de Zookeeper. O próprio Kafka gerencia o consenso via Raft.

## Consequências
- Menos componentes para gerenciar no cluster
- Setup mais simples no Kubernetes
