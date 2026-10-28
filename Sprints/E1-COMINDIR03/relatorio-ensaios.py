"""Gera o relatório da COMINDIR03 a partir das provas reais de uma execução."""
from pathlib import Path
import json, statistics, sys

# Lê as provas de uma execução, valida os resultados, calcula as médias e gera o relatório e os ficheiros JSON.
def main():
    base=Path(sys.argv[1]).resolve()
    summary={p.parent.name:json.loads(p.read_text(encoding='utf-8')) for p in sorted(base.glob('*/metricas.json'))}
    trials=[summary[f'7c-com-processador-{i}'] for i in [1,2,3]]
    for label in ['7a-figura','7b-sem-processador','7c-com-processador-1','7c-com-processador-2','7c-com-processador-3','7d-reinicio','7e-entrega-sem-ordem-causal']:
        assert summary[label]['exit']==0 and summary[label]['partitioncheck_exit']==0, label
    assert all(r['originais']==300 and r['alarmes_publicados']==r['alarmes_gravados'] for r in trials)
    assert summary['8a-sem-merge']['exit']==0 and summary['8a-sem-merge']['causais']==0
    assert summary['8b-lamport-sem-mensagem']['exit']==1
    assert summary['8c-reinicio-zero']['exit']==0
    assert summary['8d-snapshot-sem-copy']['exit']==1 and summary['8d-controlo-com-copy']['exit']==0
    assert summary['8e-N-incompativel']['exit']==0 and summary['8e-N-incompativel']['originais']==0

    keys=['alarmes_publicados','causais','concorrentes','concorrentes_L_distinto','proporcao','REG_sem_carimbos_media_bytes','REG_com_carimbos_media_bytes','vetor_media_bytes']
    means={key:statistics.mean(r[key] for r in trials) for key in keys}
    means['registos']=[statistics.mean(r['registos'][k] for r in trials) for k in range(3)]
    means['proporcao_ponderada']=sum(r['concorrentes_L_distinto'] for r in trials)/sum(r['concorrentes'] for r in trials)
    means['sobrecusto_carimbos_bytes']=means['REG_com_carimbos_media_bytes']-means['REG_sem_carimbos_media_bytes']
    (base/'resumo.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (base/'medias.json').write_text(json.dumps(means,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    baseline=summary['7b-sem-processador']
    proof=json.loads((base/'7e-entrega-sem-ordem-causal/ordem-escrita.json').read_text(encoding='utf-8'))
    restart=json.loads((base/'7d-reinicio/reinicio.json').read_text(encoding='utf-8'))
    fmt=lambda x:f'{x:.2f}'.replace('.',',')
    trial_rows='\n'.join('| '+str(i)+' | '+' / '.join(map(str,r['registos']))+' | '+str(r['alarmes_publicados'])+' | '+str(r['causais'])+' | '+str(r['concorrentes'])+' | '+str(r['concorrentes_L_distinto'])+' | '+fmt(100*r['proporcao'])+'% |' for i,r in enumerate(trials,1))
    size_rows='\n'.join('| '+str(i)+' | '+fmt(r['REG_sem_carimbos_media_bytes'])+' | '+fmt(r['REG_com_carimbos_media_bytes'])+' | '+fmt(r['vetor_media_bytes'])+' |' for i,r in enumerate(trials,1))
    comparison='superior às três execuções com alarmes' if baseline['proporcao']>max(r['proporcao'] for r in trials) else 'a comparar com os valores das três execuções com alarmes'
    supplement=''
    if 'suplementar-reinicio-antes-publicacao' in summary:
        comparison='superior às três execuções com alarmes' if baseline['proporcao']>max(r['proporcao'] for r in trials) else 'a comparar com os valores das três execuções com alarmes'
    supplement='''
Foi conservado um ensaio adicional em `suplementar-reinicio-antes-publicacao/`: o líder foi terminado
logo após o `append` do primeiro alarme, antes de esse `REG` chegar às réplicas. Após reiniciar,
as réplicas receberam seq=2 e ficaram à espera de seq=1. Como só havia um retido, não se atingiu
o limiar de mais de 10 que desencadeia recuperação. O ensaio principal 7(d) foi repetido depois
de confirmar a entrega do primeiro alarme às duas réplicas. Este caso adicional documenta a janela
entre persistência e publicação e a limitação da recuperação por limiar; não é uma violação dos relógios.
'''
    text=f'''# COMINDIR03 — Ensaios e reflexão

Ensaio `{base.name}`. Resultados obtidos por execução real, com Kubo 0.43.1 e OpenJDK 21.0.9,
compilação compatível com Java 11, numa máquina Windows. N=3; réplicas 1=(p0,p1), 2=(p1,p2), 3=(p2,p0).
Um único daemon Kubo serve processos Java separados. Este ensaio valida a comunicação pela API PubSub local;
não mede propagação entre computadores ou daemons diferentes.

## Procedimento e provas

O script [executar-ensaios.py](../../executar-ensaios.py) arranca réplicas, líderes, processador e produtor
por essa ordem. Aguarda as subscrições, envia comandos ao produtor real e termina os processos que criou
quando os ficheiros estabilizam. O Kubo continua disponível. Cada cenário usa ficheiros novos; o reinício
preserva o ficheiro do líder. As temperaturas das rajadas são geradas pelo `Random` do produtor, sem seed fixa;
outras execuções terão outros valores e interleavings.

Cada pasta contém comandos enviados, PID/comando de arranque, stdout/stderr, ficheiros dos líderes e réplicas,
`causalcheck.txt`, `partitioncheck.txt`, `vista-lamport.txt` e `metricas.json`. O agregado está em
[resumo.json](resumo.json) e as médias em [medias.json](medias.json). Os ficheiros de ambiente registam
as versões e hashes dos fontes corretos. Os ficheiros vazios representam partições sem qualquer append;
não representam leituras inventadas. Nas primeiras execuções, a preparação dos vazios para o `PartitionCheck`
foi feita após parar os processos, conservando os diagnósticos iniciais; ver `preparacao-ficheiros-vazios.json`.

A contagem usa apenas os ficheiros dos líderes: cada evento entra uma vez. Os pares de partições diferentes
são contados uma vez, com `a` na partição menor. O analisador Java e uma segunda contagem em Python concordam.
As medições de bytes referem-se ao payload `REG` em UTF-8, sem multipart, HTTP ou IPFS. Para a comparação,
retiram-se apenas os campos L e V da mesma mensagem, conservando os restantes dados.

## 7(a) — Figura 1

[Provas](7a-figura/causalcheck.txt). Comandos `S02 29.3`, espera pelo alarme, `S01 21.0`, `S04 22.5`.

| Evento | Registo | L | V |
| --- | --- | --- | --- |
| a | p1#1, S02 | 1 | (0,1,0) |
| b | p0#1, S01 | 1 | (1,0,0) |
| c | p2#1, ALARM-S02 | 2 | (0,1,1) |
| d | p0#2, S04 | 2 | (2,0,0) |

Os carimbos coincidem com a figura. Entre partições: 1 par causal (a→c), 4 concorrentes,
dos quais 2 com L distinto (50%). Há ainda b→d dentro de p0, fora da contagem entre partições.
O `CausalCheck` não encontrou violações e o `PartitionCheck` confirmou as duas réplicas de cada partição.

## 7(b) — Rajada sem processador

[Provas](7b-sem-processador/causalcheck.txt). Comando `rajada 300 10`.

Foram gravadas 300 leituras: {' / '.join(map(str,baseline['registos']))} em p0/p1/p2.
Resultado: {baseline['causais']} pares causais entre partições e {baseline['concorrentes']} concorrentes.
Destes, {baseline['concorrentes_L_distinto']} têm L distinto ({fmt(100*baseline['proporcao'])}%).
Sem processador não há transporte de conhecimento causal de um líder para outro; os vetores de partições
diferentes são incomparáveis. A ordem local de cada líder mantém-se.

## 7(c) — Três rajadas com processador

Cada execução usa `rajada 300 10` numa pasta nova e gravou as 300 leituras originais.
Todos os alarmes assinalados como publicados foram encontrados nos ficheiros dos líderes.
Os dois verificadores terminaram com sucesso nas três execuções.

| Execução | Registos p0 / p1 / p2 | Alarmes | Pares causais | Concorrentes | Concorrentes com L distinto | Proporção |
| --- | --- | --- | --- | --- | --- | --- |
{trial_rows}

| Execução | REG sem carimbos, bytes | REG com carimbos, bytes | Vetor textual, bytes |
| --- | --- | --- | --- |
{size_rows}

### 9 — Tabela de comparação: média das três execuções

| Métrica | Valor médio |
| --- | --- |
| Registos por partição (p0 / p1 / p2) | {' / '.join(fmt(x) for x in means['registos'])} |
| Alarmes publicados | {fmt(means['alarmes_publicados'])} |
| Pares entre partições causalmente relacionados | {fmt(means['causais'])} |
| Pares entre partições concorrentes | {fmt(means['concorrentes'])} |
| Concorrentes com L distinto, quantidade | {fmt(means['concorrentes_L_distinto'])} |
| Concorrentes com L distinto, proporção média | {fmt(100*means['proporcao'])}% |
| Tamanho médio REG sem carimbos | {fmt(means['REG_sem_carimbos_media_bytes'])} bytes |
| Tamanho médio REG com carimbos | {fmt(means['REG_com_carimbos_media_bytes'])} bytes |

O aumento médio dos dois carimbos é de {fmt(means['sobrecusto_carimbos_bytes'])} bytes por REG.
O vetor textual ocupa em média {fmt(means['vetor_media_bytes'])} bytes. A proporção da tabela é a média
aritmética das três proporções; se agregarmos todos os pares, a proporção ponderada é
{fmt(100*means['proporcao_ponderada'])}%.

## 7(d) — Reinício do líder p2

[Provas](7d-reinicio/reinicio.json). Antes de parar: seq={restart['antes']['seq']}, L={restart['antes']['L']},
V={tuple(restart['antes']['V'])}. O primeiro alarme já estava nas duas réplicas quando o líder foi parado.
Depois de reiniciar e introduzir `S03 22.0`: seq={restart['depois']['seq']}, L={restart['depois']['L']},
V={tuple(restart['depois']['V'])}. A inicialização pela última linha preservou o conhecimento sobre p1
mesmo recebendo uma nova leitura do produtor com carimbos zero. Os verificadores terminaram com sucesso.
{supplement}
## 7(e) — Alarme escrito antes da origem

[Provas](7e-entrega-sem-ordem-causal/ordem-escrita.json). Apenas a réplica 1 arrancou com `-Ddescartar=1`.
Foi enviada `S01 29.3`: o REG p0#1 foi descartado nessa réplica, mas o processador recebeu-o e publicou
ALARM-S01, cujo líder é p1. A réplica escreveu p1#1. Seguiram-se 12 leituras frias de S01;
ao ultrapassar 10 retidos em p0, a recuperação TCP trouxe p0#1 e permitiu escrever a sequência.

A propriedade opcional `-Drep03.traceWrites=...` regista intervalos reais de append num ficheiro separado,
sem alterar as linhas de dados nem consultar os relógios. Os intervalos usam `System.nanoTime()` no mesmo JVM;
a hora UTC é auxiliar. Não são comparados relógios monotónicos de processos diferentes.

| Append na réplica 1 | Início monotónico (ns) | Fim monotónico (ns) |
| --- | --- | --- |
| p1#1, ALARM-S01 | {proof['alarme']['start']} | {proof['alarme']['end']} |
| p0#1, S01, origem | {proof['origem']['start']} | {proof['origem']['end']} |

O fim do append do alarme precedeu o início do append da origem em pelo menos
**{fmt(proof['intervalo_minimo_ms'])} ms**. Não foi apenas uma ordem de impressão dos logs.
No final, a recuperação deixou as réplicas iguais aos líderes e ambos os verificadores terminaram com sucesso.
A perda foi introduzida deliberadamente; este resultado não mede a frequência espontânea da inversão.

## 8 — Falhas introduzidas isoladamente

As versões com falhas foram compiladas em `ensaios-locais/variantes/`, sem substituir os fontes corretos.
Os patches exatos estão em [variantes](variantes/). Cada cenário arranca de novo, sem o estado da falha anterior.

| Falha | Lado onde surgiu | CausalCheck detetou? | Conclusão medida |
| --- | --- | --- | --- |
| Líder sem merge de V recebido | Líder p2 grava o alarme com V=(0,0,1), perdendo p1 | Não: reportou 1 par concorrente e zero causais | O verificador confia nos vetores; precisa de prova independente da origem para detetar uma dependência apagada |
| Líder ignora L recebido | Líder p2 grava L=1 e V=(0,1,1), após p1#1 com L=1 | Sim: violação p1#1→p2#1 | A condição de relógio exige L estritamente crescente ao longo da causalidade |
| Reinício com L e V a zero, conservando seq | Líder p2 rejeita o próximo IN porque V[k] difere de seq | Não: os ficheiros anteriores continuam válidos | A guarda local impede gravar o estado incoerente; o analisador não deteta a falta de progresso |
| Vetor partilhado sem copy, serialização adiada | Outra thread do processador altera V entre captura e publicação | Sim: violação p0#2→p2#1, ambos com L=2 | É necessário capturar L e V como um estado coerente |
| Produtor N=4, restantes N=3 | Líder p1 rejeita vetor com 4 componentes; S04 vai para p3 sem subscritor | Não: nenhum evento novo foi gravado | Validação de dimensão protege o líder; perda antes da atribuição de seq não aparece nos relógios |

### Precisão do ensaio de copy

Na implementação correta, `Wire.in(...)` produz uma String dentro do bloco sincronizado. Remover apenas
o `copy()` dessa expressão não deixaria uma referência mutável na mensagem: a serialização imediata sob
o monitor já a protege. Para ensaiar a falha pedida, a variante captura L e a referência de V sob o monitor
e adia a serialização até outra thread processar p0#1 e p0#2. Uma barreira controlada reproduz essa intercalação;
esta alteração do momento da serialização faz parte explícita do patch experimental.

Capturou L=1 e V=(0,1,0); após a outra thread, publicou L=1 e V=(2,1,0).
O líder p2 gravou L=2 e V=(2,1,1), que afirma dependência de p0#2 com L=2: o analisador assinalou a violação.
O controlo `8d-controlo-com-copy` repete o mesmo escalonamento mudando a captura para `vector.copy()`:
publicou V=(0,1,0), resultou em L=2 e V=(0,1,1), e passou no analisador.
Nenhuma barreira ou serialização adiada ficou no código de produção.

## 10 — Reflexão crítica

### Lamport e concorrência

Nos ensaios com alarmes, {fmt(100*means['proporcao'])}% dos pares concorrentes tiveram L distinto, em média.
A ordenação por L impõe-lhes uma ordem apesar de não haver relação causal. A vista por (L,k) ordena também
os empates por partição; logo todos os eventos ficam comparáveis, mas estar antes nessa vista não prova
que uma leitura provocou um alarme. O exemplo b,c demonstra-o: L(b)=1 < L(c)=2, mas V(b) e V(c) são incomparáveis.

Na rajada sem alarmes, a proporção foi {fmt(100*baseline['proporcao'])}%, {comparison}.
São observações destas amostras:
as rajadas usam leituras aleatórias e escalonamentos diferentes, pelo que não demonstram uma lei monotónica.
A frequência de alarmes altera as dependências, os contadores e o conjunto dos pares que ainda são concorrentes.

### Dimensão do vetor e custo

Para N=3, `0,1,1` ocupa 5 bytes em UTF-8, mas o custo não é fixo: contadores com mais dígitos tornam o vetor
maior. A média medida foi {fmt(means['vetor_media_bytes'])} bytes. Em geral, o texto ocupa a soma dos dígitos
das N componentes mais N−1 vírgulas: cresce linearmente em N para componentes com a mesma largura,
e também cresce com os dígitos dos contadores. O payload acrescenta ainda o L e dois separadores `;`.

O vetor tem uma posição por líder/partição porque são esses os processos que geram os eventos contados.
Réplicas e produtores não acrescentam posições nem fazem tick. Alterar N de 3 para 4 muda o esquema e
pode mudar a função de partição: os vetores antigos têm uma dimensão incompatível. Seria necessário definir
identidades estáveis, versionamento/migração e a transição da distribuição de dados; essa mudança está fora
do âmbito de dimensão fixa da ficha.

### Relógios e números de sequência

V[k]=seq porque só o líder k incrementa a sua componente, uma vez por registo sequenciado. O valor local
é redundante em termos de informação, mas `seq` continua a ser usado explicitamente pelo protocolo de
ordenação, deteção de lacunas/duplicados e intervalos TCP da COMINDIR02. O vetor acrescenta o conhecimento
sobre eventos das outras partições; dois números de sequência isolados não fornecem essa comparação causal.
As réplicas preservam os carimbos, mas não os atualizam nem usam para decidir a entrega.

### Entrega causal

A inversão medida em 7(e) mostra que conhecer a causalidade não impõe a ordem de aplicação nas réplicas.
Antes de escrever um registo de p1, a réplica 1 teria de confirmar a sequência seguinte de p1 e, para p0,
que já aplicou pelo menos V_registo[0] eventos. Caso contrário teria de reter o registo até cumprir essa
condição. Só pode verificar diretamente as partições que guarda; sem p2 não conhece o prefixo aplicado
dessa partição nem consegue garantir a satisfação de dependências sobre p2. Uma entrega causal completa
exigiria informação/mecanismos adicionais. Não foi implementada: está expressamente fora do âmbito.

### Troço produtor → líder

O produtor envia sempre L=0 e V=(0,0,0). Uma leitura perdida antes de ser recebida pelo líder não recebe
seq, não incrementa L/V e não deixa lacuna nos ficheiros. O ensaio N=4 ilustra a limitação: o produtor
publicou, mas os ficheiros ficaram vazios e o `CausalCheck` não teve eventos inválidos para assinalar.
Para detetar e recuperar essas perdas seria necessário, por exemplo, identificar leituras no produtor,
confirmar a receção e repetir envios com eliminação de duplicados; isso não faz parte do protocolo pedido.

### Fundamentos e limites da verificação

A relação aconteceu antes inclui a ordem local, envio antes da receção e transitividade. Entre os eventos
da figura, a→c vem da observação pelo processador e b→d da ordem do mesmo líder. c é concorrente com b e d,
pois os vetores têm componentes maiores num sentido e menores no outro. Se o processador tivesse observado
b antes de emitir o alarme de a, c teria V=(1,1,1): a memória acumulada inclui também observações que não
foram o disparo direto do alarme. A ordem por (L,k) respeita a causalidade porque esta implica L estritamente
menor; o desempate só escolhe uma ordem entre eventos sem essa restrição.

Seq, atualização dos relógios e append estão na mesma secção sincronizada do líder, evitando números
repetidos e registos com carimbos de outro evento. A captura do processador é igualmente protegida.
O estado do líder é retomado pela última linha persistida. Estes mecanismos não resolvem a dimensão variável,
a perda antes do líder, a ausência de entrega causal nem a recuperação de uma cauda perdida sem novos eventos.
O `CausalCheck` verifica a consistência dos carimbos disponíveis; os ensaios sem merge e N incompatível
mostram por que um resultado OK, por si só, não prova ausência de perdas ou completude da causalidade.
'''
    (base/'RELATORIO.md').write_text(text,encoding='utf-8')
    print(base/'RELATORIO.md')
if __name__=='__main__': main()
