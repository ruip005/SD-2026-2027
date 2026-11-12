# COMINDIR04 — Heartbeat, envio em lote e detetor de falhas

Enunciado: [Sprint COMINDIR04, issue 17](https://github.com/cunhaestgv/SD-2026-2027/issues/17).
Base: COMINDIR03, copiada para quatro projetos independentes, todos no pacote `rep04`.

| Projeto | Entrada principal | Responsabilidade |
| --- | --- | --- |
| `rep04-produtor` | `Producer <N> [api]` | Publica leituras; acompanha os `LOTE` (só o cabeçalho) com um detetor por partição |
| `rep04-lider` | `PartitionLeader <k> <N> [endereço] [api]` | Atribui seq, L e V; acumula em `pendentes`; publica um `LOTE` a cada Δ; serve recuperação TCP |
| `rep04-replica` | `PartitionReplica <id> <k1,k2> [portoApi]` | Aplica os lotes por seq; recupera por TCP com `ultimoSeq`; detetor por partição |
| `rep04-processador` | `AlarmProcessor <N> [api]` | Trata os registos de cada lote pela ordem; alarmes a partir de 28 °C; detetor por partição |

Ferramentas de análise (projeto `rep04-replica`): `HeartbeatCheck <N> <pasta> [falhas.txt]`,
`CausalCheck <N> <pasta>` e `PartitionCheck <N> <pasta>`.

Propriedades de sistema, iguais em **todos** os processos de um ensaio:
`-Drep04.periodo=<ms>` (Δ, por omissão 5000) e `-Drep04.fator=<f>` (`TIMEOUT` = f × Δ, por omissão 3).

## Compilar

Requer JDK 11 ou superior. Não usa bibliotecas externas. A partir da raiz do repositório, em PowerShell:

```powershell
$taskRoot = (Resolve-Path Sprints/E1-COMINDIR04).Path
foreach ($p in 'lider','produtor','replica','processador') {
    $src = @(Get-ChildItem -LiteralPath "$taskRoot/rep04-$p/src/rep04" -Filter '*.java' | ForEach-Object FullName)
    & javac --release 11 -encoding UTF-8 -d "$taskRoot/rep04-$p/out" @src
}
```

## Executar com Kubo

Daemon Kubo com PubSub ativo (`ipfs config --json Pubsub.Enabled true`, depois `ipfs daemon`).
Todos os processos na mesma máquina e na mesma pasta de ensaio (uma pasta nova por ensaio):

```powershell
$taskRoot = (Resolve-Path Sprints/E1-COMINDIR04).Path
New-Item -ItemType Directory -Force "$taskRoot/ensaios-locais/8a" | Out-Null
Set-Location "$taskRoot/ensaios-locais/8a"
```

Um comando por terminal, por esta ordem (N=3): réplicas, líderes, processador, produtor.

```powershell
java -cp "$taskRoot/rep04-replica/out" rep04.PartitionReplica 1 0,1
java -cp "$taskRoot/rep04-replica/out" rep04.PartitionReplica 2 1,2
java -cp "$taskRoot/rep04-replica/out" rep04.PartitionReplica 3 2,0
java -cp "$taskRoot/rep04-lider/out" rep04.PartitionLeader 0 3
java -cp "$taskRoot/rep04-lider/out" rep04.PartitionLeader 1 3
java -cp "$taskRoot/rep04-lider/out" rep04.PartitionLeader 2 3
java -cp "$taskRoot/rep04-processador/out" rep04.AlarmProcessor 3
java -cp "$taskRoot/rep04-produtor/out" rep04.Producer 3
```

Para o ensaio 8(e), acrescentar `-Drep04.periodo=1000` (ou 10000) a seguir a `java` em todos os processos.
O líder imprime o seu `pid` no arranque.

### Provocar falhas no Windows

Terminar o líder de p1 sem lhe dar oportunidade de reagir, anotando antes o instante em `falhas.txt`
(equivalente a `date +%s%3N >> falhas.txt; kill -9 <pid>`):

```powershell
Add-Content falhas.txt ("p1;" + [DateTimeOffset]::Now.ToUnixTimeMilliseconds()); Stop-Process -Id <pid> -Force
```

Suspender e retomar o líder de p2 (equivalente a `kill -STOP` / `kill -CONT`): o PowerShell não tem
comando próprio. Pode usar-se o Monitor de Recursos (`resmon`, separador CPU, botão direito no processo
`java.exe` com esse pid → *Suspend Process* / *Resume Process*) ou o `pssuspend` das Sysinternals
(`pssuspend <pid>` e `pssuspend -r <pid>`).

## Analisar

No fim de cada ensaio, com os processos parados:

```powershell
java -cp "$taskRoot/rep04-replica/out" rep04.HeartbeatCheck 3 . falhas.txt
java -cp "$taskRoot/rep04-replica/out" rep04.CausalCheck 3 .
```

`HeartbeatCheck` imprime o relatório e grava-o em `relatorio-heartbeat.txt` na pasta analisada.
Sem `falhas.txt`, a secção do tempo de deteção é omitida.

Notas sobre as medições:

- O `instante` dos registos é gravado ao segundo (`LocalDateTime.now().withNano(0)`, herdado da
  COMINDIR03). A latência de lote calculada tem, por isso, um excesso entre 0 e 1 s em cada valor.
- O produtor só lê o cabeçalho dos lotes: os seus `RECEBIDO` têm `seq=?` e não entram na latência.
- Só a recuperação desencadeada pelo `ultimoSeq` é registada como `RECUPERACAO` no `monitor-*.txt`;
  a recuperação por excesso de retidos (COMINDIR02) continua a ser assinalada apenas na consola.
- A comparação de instantes de processos diferentes só é válida com todos na mesma máquina.
