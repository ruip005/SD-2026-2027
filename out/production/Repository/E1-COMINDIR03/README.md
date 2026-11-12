# COMINDIR03 — Relógios de Lamport e vetoriais

Enunciado: [Sprint COMINDIR03, issue 16](https://github.com/cunhaestgv/SD-2026-2027/issues/16).
Base: COMINDIR02, copiada para quatro projetos independentes, todos no pacote `rep03`.

| Projeto | Entrada principal | Responsabilidade |
| --- | --- | --- |
| `rep03-produtor` | `Producer <N> [api]` | Publica leituras com L=0 e vetor zero |
| `rep03-lider` | `PartitionLeader <k> <N> [endereço] [api]` | Atribui seq, L e V; persiste; serve recuperação TCP |
| `rep03-replica` | `PartitionReplica <id> <k1,k2> [portoApi]` | Replica por seq, preservando os carimbos |
| `rep03-processador` | `AlarmProcessor <N> [api]` | Observa as partições e publica alarmes a partir de 28 °C |

## Compilar e testar

Requer JDK 11 ou superior. Não usa bibliotecas externas. A partir da raiz do repositório:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File Sprints/E1-COMINDIR03/verificar.ps1
```

O script compila cada projeto separadamente com `--release 11` e executa testes de integração sem Kubo.
Cada execução cria uma pasta própria em `ensaios-locais/`, ignorada pelo Git.
A opção `ExecutionPolicy` aplica-se apenas ao processo que executa este script.

Os testes verificam as quatro relações vetoriais, cópias defensivas, serialização, exemplo da Figura 1,
acumulação causal e duplicados no processador, concorrência no líder, reinício, rejeição de N incompatível,
falha de escrita, recuperação das réplicas e transferência de carimbos numa ligação TCP real.
Também verificam a deteção de violações de Lamport, linhas inválidas e ficheiros em falta.
Estes testes não substituem os ensaios com PubSub do enunciado.

## Executar com Kubo

É necessário um daemon Kubo acessível, com PubSub ativo. Na primeira utilização executa `ipfs init`. Para a configuração local habitual:

```powershell
ipfs config --json Pubsub.Enabled true
ipfs daemon
```

Após compilar, abre terminais separados, todos na mesma pasta de ensaio. Em cada terminal, partindo da
raiz do repositório:

```powershell
$taskRoot = (Resolve-Path Sprints/E1-COMINDIR03).Path
New-Item -ItemType Directory -Force "$taskRoot/ensaios-locais/manual" | Out-Null
Set-Location "$taskRoot/ensaios-locais/manual"
```

Executa um comando por terminal, por esta ordem (N=3):

```powershell
java -cp "$taskRoot/rep03-replica/out" rep03.PartitionReplica 1 0,1
java -cp "$taskRoot/rep03-replica/out" rep03.PartitionReplica 2 1,2
java -cp "$taskRoot/rep03-replica/out" rep03.PartitionReplica 3 2,0
# Depois, os três líderes:
java -cp "$taskRoot/rep03-lider/out" rep03.PartitionLeader 0 3
java -cp "$taskRoot/rep03-lider/out" rep03.PartitionLeader 1 3
java -cp "$taskRoot/rep03-lider/out" rep03.PartitionLeader 2 3
# Depois, o processador; por fim, o produtor:
java -cp "$taskRoot/rep03-processador/out" rep03.AlarmProcessor 3
java -cp "$taskRoot/rep03-produtor/out" rep03.Producer 3
```

O produtor aceita `<sensor> <temperatura>`, `rajada 300 10` e `sair`.
No final, com os processos parados para estabilizar os ficheiros:

```powershell
java -cp "$taskRoot/rep03-replica/out" rep03.CausalCheck 3 .
java -cp "$taskRoot/rep03-replica/out" rep03.PartitionCheck 3 .
```

`CausalCheck` imprime o relatório e escreve `vista-lamport.txt` na pasta analisada.
Cada linha da vista tem `partição;seq;sensor;temperatura;instante;L;V`.
Cada par entre partições é contado uma vez, com `a` na partição menor e `b` na maior.
O código de saída é 1 se houver violações ou dados inválidos. Ficheiros de líderes ausentes são assinalados;
uma partição sem eventos pode ser representada por um ficheiro vazio.

## Decisões de implementação

- O líder prepara os novos relógios, verifica `V[k] == seq` e faz `append` sob o mesmo monitor.
  Só confirma o novo estado após a escrita; publica o `REG` fora do monitor.
- No reinício, `RecordFile.last()` fornece seq, L e V. Um ficheiro com N incompatível é rejeitado.
- `SensorRecord` copia o vetor tanto no construtor como no getter. Registos antigos não mudam quando
  o relógio do líder avança.
- O processador acumula máximos, sem incrementar relógios. Um conjunto de seq por partição elimina
  duplicados mesmo quando chegam fora de ordem. Os conjuntos crescem com o histórico observado.
- A captura de L e da cópia de V ocorre sob o monitor do processador. A mensagem fica imutável antes
  da publicação HTTP, permitindo continuar a receber outras partições durante essa publicação.
- As réplicas continuam a aplicar exclusivamente a regra por seq da COMINDIR02; não impõem entrega causal.
- As classes comuns são copiadas entre projetos conforme o enunciado. Alterações futuras devem manter
  essas cópias consistentes.

## Ensaios e limitações

Os ensaios 7(a–e) e 8, as três rajadas com alarmes, as medições e a reflexão da secção 4.3 foram
executados com Kubo real. Ver o [relatório completo](ensaios/20261025-221951/RELATORIO.md),
os [resultados agregados](ensaios/20261025-221951/resumo.json) e as provas nas pastas de cada cenário.
O relatório distingue os casos em que o analisador detetou a falha daqueles em que os carimbos disponíveis
não permitem detetá-la. As falhas foram compiladas em cópias isoladas; o código de produção conserva as regras corretas.

Para repetir, com JDK, Python 3 e o Kubo ativo, sem outros programas rep03 em execução:

```powershell
python Sprints/E1-COMINDIR03/executar-ensaios.py
# O comando imprime a pasta nova criada. Usar esse caminho no comando seguinte:
python Sprints/E1-COMINDIR03/relatorio-ensaios.py Sprints/E1-COMINDIR03/ensaios/<pasta-impressa>
```

O ensaiador usa os portos 7000–7002 e termina os processos Java que arrancou; mantém o daemon Kubo.
Cada execução guarda os resultados numa pasta nova. Os builds com falhas ficam em `ensaios-locais/`,
ignorados pelo Git; os patches das variantes são guardados com as provas.
A propriedade opcional `-Drep03.traceWrites=escritas.csv` regista intervalos de append para demonstrar
ordem de escrita dentro do mesmo JVM. Não altera o formato dos registos nem introduz entrega causal.

Para reproduzir a Figura 1, introduz `S02 29.3` e espera que o alarme chegue ao líder p2 antes de
introduzir `S01 21.0` e `S04 22.5`. A ordem de entrega no PubSub pode acrescentar dependências se
forem introduzidas outras leituras antes do alarme.
Usa uma pasta nova para cada ensaio; apenas o teste de reinício deve conservar os ficheiros.

Mantêm-se as limitações da ficha: dimensão fixa N, perdas antes da atribuição de seq não detetáveis,
PubSub sem histórico e ausência de entrega causal nas réplicas. A vista por (L,k) ordena também eventos
concorrentes e não prova que uma leitura tenha provocado um alarme.
Os instantes nos registos são atribuídos pelo líder e preservados nas réplicas; para observar a ordem
real de escrita entre partições numa réplica é necessária observação adicional, não basta ordenar esses instantes.
