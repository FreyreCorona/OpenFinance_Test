1-Investigação do incidente (antes da solução). Diante do quadro acima, descreva passo a passo COMO você investigaria — sem pular para a correção. Liste suas hipóteses iniciais (ordene por probabilidade), o que olharia primeiro (quais logs, métricas, traces, comandos ou dashboards) e como cada observação confirmaria ou descartaria uma hipótese. Qual seria seu primeiro sinal de que está no caminho certo?

# Resposta 1
## Hipóteses
1. Mudança recente — deploy, configmap, feature flag ou atualização de biblioteca alterou o comportamento do consumer, produtor ou infraestrutura.
2. Hot partition / poucas partições — O tópico tem partições insuficientes para 3x o tráfego, ou uma partição específica está saturada.
3. HPA limitado + rebalanceios frequentes — O Horizontal Pod Autoscaler não está escalando adequadamente (maxReplicas baixo, recurso insuficiente, métrica indisponível) OU o consumer está sofrendo rebalanceios constantes por timeout/GC/config.
4. Downstream lento causando backpressure — Antifraude, extrato ou conciliação estão lentos, travando o commit de offset do consumer.
5. Falha no broker Kafka — Disco cheio, líder de partição indisponível, ou problema de rede no cluster Kafka.

## Pasos para a confirmacão/descarte
1. Contexto e mudanças recentes. Executo ```kubectl config current-context``` para confirmar o cluster. Depois ```kubectl get events --sort-by=.metadata.creationTimestamp | tail -50```. Se vejo ```FailedGetResourceMetric``` ou ```ScalingLimited```, reforça H3. Se vejo ```CrashLoopBackOff```, investigo. Então ```kubectl rollout history deployment/<consumer>``` — rollout recente reforça H1; sem rollout há dias descarta H1 parcialmente. ```kubectl describe deployment <consumer>``` — ```replicas/maxReplicas``` baixos reforça H3. ```kubectl describe configmap <kafka-config>``` — parâmetros alterados (```max.poll.interval.ms, session.timeout.ms, enable.auto.commit```) reforça H1.
2. Infraestrutura do Kafka. Uso ```kafka-consumer-groups --bootstrap-server <broker> --group <group> --describe``` para ver lag por partição. Lag homogêneo em todas → H3/H4. Lag em uma partição específica → H2. Partição sem líder → H5. Complemento com ```kubectl get pods -n kafka``` e logs dos brokers. Broker caído ou disco cheio → H5.
3. Saúde do consumer e do cluster. ```kubectl describe hpa <consumer>``` — ```currentReplicas``` vs ```maxReplicas```, condições de escala. ```kubectl top pods``` — CPU/mem dos consumers. Logs do consumer com ```kubectl logs <pod> --tail=100 | grep -iE "rebalance|commit failed|timeout|gc"```. Rebalanceios frequentes ou resources no limite → H3. Commit falhando com HPA ok → H4. ```kubectl top nodes``` — cluster sem capacidade → H3.
4. Saúde do downstream. Consulto métricas de latência p99 e error rate do antifraude e extracto. Downstream lento (p99 elevado) ou com erros → H4. Saudável → descarta H4.

### Primeiro sinal de que estou no caminho certo: 
Se no passo 1 encontro um rollout recente com alteração de configuração crítica (ex: max.poll.interval.ms reduzido) ou um evento ScalingLimited, sei que estou na direção certa — uma causa identificável e com ação clara.
