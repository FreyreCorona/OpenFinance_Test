# Teste Tecnico - Backend Go

**Cliente:** Einier Freyre  
**Data:** Julio 2026

## Conteúdo do repositório

| Arquivo | Descrição |
|---|---|
| `Respostas.md` | Respostas do teste técnico |
| `src/cmd/consumer/` | Consumidor idempotente em Go |
| `src/cmd/producer/` | Produtor assíncrono de eventos |
| `src/internal/` | Pacotes internos (idempotência, modelo, Kafka) |
| `src/k8s/` | Manifestos Kubernetes |
| `src/Dockerfile.producer` | Dockerfile do produtor |
| `src/Dockerfile.consumer` | Dockerfile do consumidor |

## Como reproduzir o cenário localmente

```bash
# 1. Iniciar minikube
minikube start --cpus=4 --memory=8192

# 2. Namespace e infraestrutura
kubectl apply -f src/k8s/00-namespace.yaml
kubectl apply -f src/k8s/01-redis.yaml
kubectl apply -f src/k8s/02-kafka.yaml

# 3. Criar tópico
kubectl exec kafka-0 -n openfinance -- /opt/kafka/bin/kafka-topics.sh \
  --create --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# 4. Deploy do producer e consumer
kubectl apply -f src/k8s/03-producer.yaml
kubectl apply -f src/k8s/04-consumer.yaml

# 6. Verificar
kubectl logs -n openfinance -l app=producer --tail=5
kubectl logs -n openfinance -l app=consumer --tail=5
```
