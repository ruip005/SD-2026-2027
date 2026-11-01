package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Ponto de entrada das leituras. Lê comandos da consola e publica mensagens {@code IN} no tópico
 * de entrada da partição do sensor.
 *
 * <p>Invocação: {@code Producer <N> [api]}, com {@code N} o número de partições e {@code api} a
 * URL base do Kubo (por omissão {@code http://127.0.0.1:5001/api/v0}).</p>
 *
 * <p>Comandos: {@code <sensor> <temperatura>}; {@code rajada <n> <s>} (n leituras repartidas pelos
 * sensores S01..S&lt;s&gt;, 15 a 30 °C); {@code rajada-quente <n> <s>} (80 % das leituras do sensor
 * S01); {@code sair}.</p>
 *
 * <p>O produtor não numera nem guarda o que envia: uma perda neste troço não é detetável nem
 * recuperável.</p>
 *
 * <p>Subscreve também os tópicos {@code rep04-p<k>} de todas as partições, interpretando apenas o
 * cabeçalho dos {@code LOTE}, e mantém um {@link HeartbeatMonitor} por partição (eventos em
 * {@code monitor-produtor.txt}). Antes de enviar um {@code IN} para um líder suspeito, avisa e
 * envia na mesma.</p>
 *
 * <h2>Fluxo</h2>
 * <ol>
 *   <li>Lê uma linha da consola (<strong>bloqueia</strong> em {@code Scanner.nextLine()}).</li>
 *   <li>Valida o sensor e a temperatura ({@link #validar(String, double)}).</li>
 *   <li>Calcula a partição {@code k = floorMod(hashCode(sensor), N)}.</li>
 *   <li>Publica {@code IN;<sensor>;<temp>;0;<vetor-zero>} no tópico {@code rep04-in-p<k>} (pedido HTTP ao Kubo,
 *       <strong>bloqueia</strong> até o Kubo responder).</li>
 * </ol>
 *
 * <h2>O que pode correr mal</h2>
 * <ul>
 *   <li>Kubo desligado ou sem PubSub ativo: a publicação falha; o produtor avisa e continua.</li>
 *   <li>Nenhum líder subscrito no momento: o Kubo aceita a mensagem e ela perde-se em silêncio
 *       (o PubSub não guarda mensagens).</li>
 *   <li>Sensor com {@code ;} partiria o formato da mensagem: é rejeitado antes de publicar.</li>
 *   <li>N diferente do usado pelos líderes: as leituras vão para o tópico errado e o líder
 *       responde {@code PARTIÇÃO ERRADA}.</li>
 * </ul>
 */
public class Producer {
    /** Função de partição. */
    private final Partitioner partitioner;
    /** Destino das mensagens {@code IN}. */
    private final Publisher publisher;
    /** Um detetor por partição, indexado por {@code k}. */
    private final List<HeartbeatMonitor> monitores = new ArrayList<>();
    /** Destino dos eventos {@code RECEBIDO} e dos eventos dos detetores. */
    private final Consumer<String> log;

    /**
     * Cria o produtor.
     *
     * @param partitioner função de partição
     * @param publisher   destino das mensagens {@code IN}
     * @param log         destino dos eventos dos detetores
     */
    // Recebe a função de partição e o publicador, permitindo usar transporte real ou simulado nos testes, e cria um detetor por partição.
    public Producer(Partitioner partitioner, Publisher publisher, Consumer<String> log) {
        this.partitioner = partitioner;
        this.publisher = publisher;
        this.log = log;
        for (int k = 0; k < partitioner.count(); k++) {
            monitores.add(new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log));
        }
    }

    /**
     * Devolve os detetores, um por partição.
     *
     * @return lista indexada por {@code k}
     */
    // Devolve os detetores, para a thread de verificação chamar check() em todos.
    public List<HeartbeatMonitor> getMonitores() {
        return monitores;
    }

    /**
     * Trata um {@code LOTE} da partição {@code k}, lendo apenas o cabeçalho.
     *
     * @param k   partição do tópico onde chegou
     * @param msg mensagem {@code LOTE}
     */
    // Renova o detetor de k com inc e hb do cabeçalho e assinala RECEBIDO; as linhas de registo não são lidas.
    public void onLote(int k, String msg) {
        Batch b;
        try {
            b = Batch.decodeHeader(msg);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO no produtor: " + e.getMessage());
            return;
        }
        // n vem do cabeçalho (7.º campo), já validado por decodeHeader.
        int count = Integer.parseInt(msg.strip().split("\r?\n", 2)[0].split(";")[6]);
        monitores.get(k).onHeartbeat(b.getInc(), b.getHb());
        // Só o cabeçalho é interpretado: o intervalo de seq não é conhecido pelo produtor.
        log.accept("RECEBIDO;p" + k + ";inc=" + b.getInc() + ";hb=" + b.getHb() + ";ultimoSeq="
                + b.getLastSeq() + ";n=" + count + ";seq=" + (count == 0 ? "-" : "?"));
    }

    /**
     * Publica uma leitura no tópico de entrada da partição do sensor.
     *
     * @param sensor identificador do sensor
     * @param temp   temperatura
     * @return a partição de destino
     * @throws IllegalArgumentException se o sensor ou a temperatura forem inválidos (nada é publicado)
     * @throws Exception se a publicação falhar
     */
    // Valida a leitura, calcula o destino, avisa se o líder for suspeito e envia IN com L=0 e vetor zero.
    public int publicar(String sensor, double temp) throws Exception {
        validar(sensor, temp);
        int k = partitioner.of(sensor);
        if (monitores.get(k).state() == HeartbeatMonitor.State.SUSPEITO) {
            // Só se assinala: o produtor não retém nem reenvia leituras.
            System.out.println("AVISO: líder p" + k + " suspeito; leitura " + sensor + " pode perder-se");
        }
        publisher.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0, new VectorClock(partitioner.count())));
        return k;
    }

    /** Ficheiro de eventos do detetor ({@code monitor-produtor.txt}). */
    private static final Path MONITOR_FILE = Path.of("monitor-produtor.txt");

    // Junta System.currentTimeMillis() ao evento; synchronized porque o chamam as threads das partições e a de verificação.
    static synchronized void registar(String evento) {
        String linha = System.currentTimeMillis() + ";" + evento;
        System.out.println(linha);
        try {
            Files.writeString(MONITOR_FILE, linha + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("ERRO a escrever " + MONITOR_FILE + ": " + e.getMessage());
        }
    }

    /**
     * Valida uma leitura antes de a publicar.
     *
     * <p>O identificador do sensor viaja dentro de mensagens separadas por {@code ;} e é gravado em
     * linhas {@code seq;sensor;temp;instante}; um {@code ;} ou espaços no nome partiriam esse
     * formato em todos os processos a jusante. Temperaturas {@code NaN} ou infinitas também não
     * fazem sentido para um sensor.</p>
     *
     * @param sensor identificador do sensor
     * @param temp   temperatura
     * @throws IllegalArgumentException se o sensor for nulo, vazio, tiver {@code ;} ou espaços, ou
     *                                  se a temperatura não for um número finito
     */
    // Rejeita sensores incompatíveis com o formato textual e temperaturas que não sejam números finitos.
    public static void validar(String sensor, double temp) {
        if (sensor == null || sensor.isBlank() || sensor.contains(";") || !sensor.equals(sensor.trim())
                || sensor.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("sensor inválido: '" + sensor + "'");
        }
        if (!Double.isFinite(temp)) {
            throw new IllegalArgumentException("temperatura inválida: " + temp);
        }
    }

    /**
     * Publica uma rajada de leituras à máxima cadência.
     *
     * @param n      número de leituras
     * @param s      número de sensores (S01..S&lt;s&gt;)
     * @param quente se verdadeiro, 80 % das leituras são do sensor S01 (chave muito frequente)
     * @param rnd    gerador aleatório (injetado para os testes serem reprodutíveis)
     * @return número de leituras publicadas por partição
     * @throws Exception se a publicação falhar
     */
    // Gera n leituras aleatórias para s sensores e conta os envios por partição; o modo quente concentra leituras em S01.
    public int[] rajada(int n, int s, boolean quente, Random rnd) throws Exception {
        int[] porParticao = new int[partitioner.count()];
        for (int i = 0; i < n; i++) {
            int idx = (quente && rnd.nextDouble() < 0.8) ? 1 : 1 + rnd.nextInt(s);
            String sensor = String.format("S%02d", idx);
            double temp = Math.round((15 + rnd.nextDouble() * 15) * 10) / 10.0;
            porParticao[publicar(sensor, temp)]++;
        }
        return porParticao;
    }

    /**
     * Arranca o produtor interativo.
     *
     * @param args {@code <N> [api]}
     * @throws Exception se o Kubo não estiver acessível
     */
    // Arranca a consola do produtor e interpreta comandos de leitura, rajada e saída.
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: Producer <N> [api]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub ipfs = new IpfsPubSub(api);
        Producer p = new Producer(new Partitioner(n), ipfs, Producer::registar);
        ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "verificacao");
            t.setDaemon(true);
            return t;
        });
        // Uma exceção que saísse da tarefa cancelaria as verificações seguintes sem aviso.
        verificacao.scheduleAtFixedRate(() -> {
            try {
                for (HeartbeatMonitor m : p.getMonitores()) m.check();
            } catch (RuntimeException e) {
                System.out.println("ERRO na verificação: " + e);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        for (int k = 0; k < n; k++) {
            final int partition = k;
            Thread t = new Thread(() -> {
                // Se a ligação ao Kubo cair, volta a subscrever de 2 em 2 segundos.
                while (true) {
                    try {
                        ipfs.subscribe(Wire.topicData(partition), msg -> p.onLote(partition, msg));
                    } catch (InterruptedException e) {
                        return;
                    } catch (Exception e) {
                        System.out.println("Subscrição p" + partition + " falhou: " + e.getMessage());
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }, "lotes-p" + k);
            t.setDaemon(true); // o produtor termina com "sair"
            t.start();
        }
        Scanner sc = new Scanner(System.in);
        System.out.println("Comandos: <sensor> <temp> | rajada <n> <s> | rajada-quente <n> <s> | sair");
        while (sc.hasNextLine()) {
            String[] t = sc.nextLine().trim().split("\\s+");
            try {
                if (t[0].equals("sair")) {
                    break;
                } else if (t[0].startsWith("rajada") && t.length == 3) {
                    long t0 = System.currentTimeMillis();
                    int[] c = p.rajada(Integer.parseInt(t[1]), Integer.parseInt(t[2]),
                            t[0].equals("rajada-quente"), new Random());
                    System.out.println("primeira..última publicação: " + t0 + ".." + System.currentTimeMillis());
                    for (int k = 0; k < c.length; k++) {
                        System.out.println("partição " + k + ": " + c[k] + " leituras");
                    }
                } else if (t.length == 2) {
                    System.out.println("publicado na partição " + p.publicar(t[0], Double.parseDouble(t[1])));
                } else {
                    System.out.println("Comando inválido");
                }
            } catch (NumberFormatException e) {
                System.out.println("Número inválido: " + e.getMessage());
            } catch (IllegalArgumentException e) {
                System.out.println("REJEITADO: " + e.getMessage());
            } catch (java.io.IOException e) {
                // Kubo desligado, sem PubSub ou a responder com erro: avisa e continua a aceitar comandos
                System.out.println("PUBLICAÇÃO FALHOU (o Kubo está a correr com PubSub ativo?): " + e.getMessage());
            }
        }
    }
}
