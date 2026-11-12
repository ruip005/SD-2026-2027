# COMINDIR02 · Particionamento e recuperação com Publish/Subscribe

Três projetos que simulam três máquinas (pacote `rep02`):

| Projeto | Classes novas | Classes herdadas/fornecidas |
|---|---|---|
| `rep02-produtor` | `Producer` | `SensorRecord`, `RecordFile`, `IpfsPubSub`, `Publisher`, `Wire`, `Partitioner` |
| `rep02-lider` | `PartitionLeader` | idem (`RecordFile` com `range`) |
| `rep02-replica` | `PartitionReplica`, `Recuperador`, `TcpRecuperador`, `PartitionCheck` | idem |

Documentação (contexto, fluxo, perguntas de defesa): página do Notion do grupo e o guia visual.
A arquitetura do professor está em `Arquitetura COMINDIR02.html`.

## Estado do código: completo (20 out 2026)

Tudo implementado, incluindo o núcleo:

* `PartitionLeader.onIn`: valida o `IN`, confirma a partição, dá o próximo `seq` e **escreve no ficheiro
  antes de publicar** o `REG` (numeração e escrita no mesmo bloco `synchronized`; a publicação fica fora dele).
* `PartitionLeader.processarPedido`: responde a `PEDIDO de ate` com as linhas do intervalo e `FIM`,
  ou com `ERRO` se o pedido for inválido.
* `PartitionReplica.Particao.aplicar`, `escreverEmSequencia` e `recuperar`: regra da réplica
  (duplicado / escreve / retém), escrita em cascata dos retidos e recuperação por TCP quando há
  mais de 10 retidos, sem recuperação dentro da recuperação.

Verificado: **95 testes, 95 passam**; Javadoc com `-Xdoclint:all -private` sem avisos; ensaio real com o
Kubo em `ensaio-kubo/` (`PartitionCheck` deu `RESULTADO: OK` nas 3 fases).

## Antes de correr (o que tem de existir primeiro)

* **Java 17 ou mais recente** (`java -version`). Foi testado com Java 21.
* Para os testes: `junit-4.13.2.jar` e `hamcrest-core-1.3.jar` (no IntelliJ: adicionar como bibliotecas).
  O JUnit 4 depende do hamcrest: sem ele dá `NoClassDefFoundError: org/hamcrest/...`.
* Para o sistema real: **Kubo (IPFS) com PubSub ativo** e o daemon a correr. Sem o daemon o produtor e os
  líderes falham logo a publicar/subscrever (`Connection refused` em `127.0.0.1:5001`).
* Portos **7000, 7001 e 7002** livres na máquina dos líderes (recuperação TCP).

## Correr os testes (sem IPFS)

```
cd rep02-lider        # ou rep02-replica / rep02-produtor
mkdir -p out
javac -cp <junit>:<hamcrest> -d out src/rep02/*.java test/rep02/*Test.java
java  -cp out:<junit>:<hamcrest> org.junit.runner.JUnitCore rep02.LeaderTest rep02.WireTest rep02.PartitionerTest rep02.RecordFileRangeTest rep02.ServidorRecuperacaoTest rep02.IpfsPubSubTest
```

Em `rep02-replica`: `rep02.ReplicaTest rep02.TcpRecuperadorTest rep02.PartitionCheckTest rep02.RecuperacaoTcpTest`. Em `rep02-produtor`: `rep02.ProducerTest`.

No Windows o separador do classpath é `;` em vez de `:`.

Resultado esperado: `OK (10 tests)` no produtor, `OK (46 tests)` no líder e `OK (39 tests)` na réplica.
`ServidorRecuperacaoTest` demora cerca de 5 s (espera pelo tempo limite de propósito).

| Classe de teste | O que prova |
|---|---|
| `ProducerTest` | partição e tópico certos, rajadas, validação de sensor e temperatura |
| `PartitionerTest`, `WireTest`, `RecordFileRangeTest` | função de partição, formato das mensagens, `range` |
| `IpfsPubSubTest` | cliente do Kubo contra um Kubo falso (HTTP do JDK): multibase, `pub`, `sub`, erros |
| `LeaderTest` | `onIn` (numeração, validação, escrever antes de publicar, concorrência) e `processarPedido` |
| `ServidorRecuperacaoTest` | servidor TCP real: vários pedidos por ligação, cliente mudo só atrasa 5 s |
| `ReplicaTest` | regra da réplica (duplicado, retido, cascata) e recuperação (limite, intervalo, falhas) |
| `TcpRecuperadorTest` | cliente TCP: `FIM`, `ERRO`, ligação cortada, recusada, tempo esgotado |
| `RecuperacaoTcpTest` | TCP real: líder em baixo e volta, réplica nova sincroniza, líder mudo, duas réplicas com perdas |
| `PartitionCheckTest` | verificador: partição errada, lacunas, repetições, réplicas, linhas corrompidas |

## Correr o sistema (precisa do Kubo com PubSub ativo)

```
ipfs init && ipfs config --json Pubsub.Enabled true && ipfs daemon        # API em 127.0.0.1:5001
# compilar as classes de cada projeto para out/ (javac -d out src/rep02/*.java)
# ordem de arranque: réplicas, líderes, produtor
java -cp out rep02.PartitionReplica 1 0,1      # réplica 1: partições 0 e 1
java -cp out rep02.PartitionReplica 2 1,2
java -cp out rep02.PartitionReplica 3 2,0
java -cp out rep02.PartitionLeader 0 3         # líder da partição 0 (recuperação em :7000)
java -cp out rep02.PartitionLeader 1 3
java -cp out rep02.PartitionLeader 2 3
java -cp out rep02.Producer 3                  # comandos: rajada 200 10 | rajada-quente 200 10 | sair
java -cp out rep02.PartitionCheck 3 .          # no fim, na pasta dos ficheiros
```

Simulação de perda (só nas réplicas): `java -Dperda=0.2 ...` e `-Ddescartar=42`.

## O que pode correr mal

* **Ordem de arranque**: o PubSub não guarda histórico. Uma réplica arrancada depois do líder perde o que
  já foi publicado; só o apanha por TCP quando chegarem registos novos suficientes (mais de 10 retidos).
* **Ficheiros de ensaios anteriores**: apagar `lider-p*.txt` e `replica-*-p*.txt` antes de cada ensaio.
  O líder continua a numeração a partir do ficheiro que encontrar, de propósito.
* **"Porto 700k ocupado"**: ficou outro líder com o mesmo `k` a correr. Terminem-no primeiro.
* **Duas máquinas**: arranquem o líder com o IP real como 3.º argumento (`PartitionLeader 0 3 192.168.1.20`),
  senão as réplicas tentam recuperar de `127.0.0.1`. A firewall tem de deixar passar 7000–7002, e os dois
  daemons do Kubo têm de se ver (`ipfs swarm peers`), senão as mensagens PubSub não atravessam.
* **Acentos na consola**: `java -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8 ...` (ou `chcp 65001` no Windows).
* **Perda da cauda**: se as últimas mensagens se perderem e não vier mais nada, a réplica fica como prefixo
  do líder. O `PartitionCheck` aceita prefixos e diz quantos faltam. É uma limitação da arquitetura,
  não um erro (ver `ensaio-kubo/LEIA-ME.md`, fase 2).

Outras limitações conhecidas (para a defesa): perda entre produtor e líder não é detetada, o líder é ponto
único de falha da sua partição, um sensor muito ativo sobrecarrega uma partição (chave quente), mudar N obriga
a redistribuir os dados, não há autenticação e o servidor de recuperação atende uma ligação de cada vez.

## Javadoc

```
javadoc -Xdoclint:all -private -d docs -encoding UTF-8 -sourcepath src rep02
```
