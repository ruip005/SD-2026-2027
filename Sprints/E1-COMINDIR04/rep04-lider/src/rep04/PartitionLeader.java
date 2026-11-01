package rep04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Líder de uma partição {@code k}: define a ordem dos registos dessa partição.
 *
 * <p>Tem três atividades concorrentes: a <em>atividade de entrada</em> (thread dedicada, bloqueada
 * em {@code subscribe(topicIn(k))}), o <em>heartbeat</em> (thread do {@link ScheduledExecutorService},
 * que corre {@link #enviarLote()} a cada {@link Wire#PERIODO_HB_MS}) e o <em>servidor de
 * recuperação</em> (thread principal, bloqueada em {@code accept()}). O estado partilhado é
 * {@code ultimoSeq}, L, V, {@code pendentes}, {@code hb} e o ficheiro {@code lider-p<k>.txt}.</p>
 *
 * <p>Invocação: {@code PartitionLeader <k> <N> [endereço] [api]}. O endereço anunciado é por
 * omissão {@code 127.0.0.1} e o porto de recuperação é {@code 7000 + k}.</p>
 *
 * <h2>Fluxo</h2>
 * <ol>
 *   <li>Arranque: seq, L e V são recuperados de {@code last()}. Um líder reiniciado continua a
 *       numeração onde ficou, sem repetir números.</li>
 *   <li>Entrada (thread {@code entrada-p<k>}): {@code IN} chega pelo tópico {@code rep04-in-p<k>};
 *       {@link #onIn(String)} valida, numera, <strong>escreve</strong> e acrescenta o registo a
 *       {@code pendentes}; já não publica nada.</li>
 *   <li>Heartbeat (thread {@code heartbeat-p<k>}): no arranque e depois a cada Δ,
 *       {@link #enviarLote()} fecha o lote e publica um {@code LOTE} em {@code rep04-p<k>}, mesmo
 *       que vazio.</li>
 *   <li>Recuperação (thread principal): {@code accept()} no porto {@code 7000 + k}; cada ligação
 *       é atendida por {@link #atender(Socket)}, que responde a {@code PEDIDO <de> <até>} com os
 *       registos e {@code FIM} ({@link #processarPedido(String)}).</li>
 * </ol>
 *
 * <h2>Ações bloqueantes e porquê</h2>
 * <ul>
 *   <li>{@code subscribe}: a resposta HTTP do Kubo nunca termina, por isso tem thread própria.</li>
 *   <li>{@code accept}: espera por uma réplica que peça recuperação; é o trabalho da thread
 *       principal, por isso pode bloquear sem prejudicar a entrada.</li>
 *   <li>{@code readLine} em {@code atender}: limitado a {@link Wire#T_TCP_MS} ms, para uma réplica
 *       que liga e não diz nada não prender o servidor para sempre.</li>
 * </ul>
 *
 * <h2>O que pode correr mal</h2>
 * <ul>
 *   <li>Porto {@code 7000 + k} ocupado (outro líder com o mesmo {@code k}, ou um ensaio anterior
 *       ainda a correr): {@code BindException} no arranque.</li>
 *   <li>Ligações atendidas uma de cada vez: uma réplica lenta atrasa as outras até
 *       {@link Wire#T_TCP_MS} ms. É uma simplificação consciente (uma thread por ligação, como no
 *       TCP01, resolveria, mas obrigaria a mais cuidado com o estado partilhado).</li>
 *   <li>Publicação de um lote falhada: os registos ficam no ficheiro e não são republicados; as
 *       réplicas vão buscá-los por TCP quando o {@code ultimoSeq} do lote seguinte os denunciar.</li>
 *   <li>O líder é ponto único de falha da partição: se cair, não há eleição de novo líder.</li>
 *   <li>Anunciar {@code 127.0.0.1} com réplicas noutra máquina: elas não conseguem recuperar.</li>
 * </ul>
 */
public class PartitionLeader {
    /** Partição deste líder. */
    private final int k;
    /** Função de partição (define N e valida a partição dos registos). */
    private final Partitioner partitioner;
    /** Ficheiro {@code lider-p<k>.txt}, fonte da recuperação. */
    private final RecordFile file;
    /** Ficheiro {@code hb-p<k>.txt}, com uma linha por lote publicado. */
    private final Path hbFile;
    /** Endereço anunciado nas mensagens {@code LOTE}. */
    private final String host;
    /** Porto de recuperação anunciado ({@code 7000 + k}). */
    private final int porto;
    /** Destino das mensagens {@code LOTE}. */
    private final Publisher publisher;
    /** Incarnação: instante de arranque deste líder, em milissegundos. */
    private final long inc;
    /** Trinco que protege seq, L, V, a escrita no ficheiro, {@code pendentes} e {@code hb}. */
    private final Object lock = new Object();
    /** Último número de sequência atribuído e escrito. */
    private long ultimoSeq;
    private long lamport;
    private VectorClock vector;
    /** Registos já numerados e escritos que ainda não seguiram num lote. */
    private List<SensorRecord> pendentes = new ArrayList<>();
    /** Número do último heartbeat desta incarnação (o primeiro lote leva hb=1). */
    private long hb = 0;

    /**
     * Cria o líder e retoma a sequência a partir do ficheiro.
     *
     * @param k           partição deste líder
     * @param partitioner função de partição (define N)
     * @param file        ficheiro {@code lider-p<k>.txt}
     * @param hbFile      ficheiro {@code hb-p<k>.txt}
     * @param host        endereço anunciado nas mensagens {@code LOTE}
     * @param porto       porto de recuperação anunciado ({@code 7000 + k})
     * @param publisher   destino das mensagens {@code LOTE}
     * @throws IOException se não for possível ler o último {@code seq} do ficheiro
     */
    // Define a incarnação, recupera seq, L e V do último registo persistido e verifica a compatibilidade com a partição e N.
    public PartitionLeader(int k, Partitioner partitioner, RecordFile file, Path hbFile, String host,
                           int porto, Publisher publisher) throws IOException {
        this.k = k;
        this.partitioner = partitioner;
        this.file = file;
        this.hbFile = hbFile;
        this.host = host;
        this.porto = porto;
        this.publisher = publisher;
        this.inc = System.currentTimeMillis();
        if (k < 0 || k >= partitioner.count()) throw new IllegalArgumentException("Partição inválida");
        SensorRecord last = file.last();
        this.ultimoSeq = last == null ? 0 : last.getSeq();
        this.lamport = last == null ? 0 : last.getLamport();
        this.vector = last == null ? new VectorClock(partitioner.count()) : last.getVector();
        if (vector.size() != partitioner.count() || vector.get(k) != ultimoSeq)
            throw new IllegalArgumentException("Estado persistido incompatível com a partição/N");
    }

    /**
     * Devolve o último número de sequência atribuído.
     *
     * @return o último número de sequência atribuído e escrito
     */
    // Consulta sob o mesmo lock o último seq confirmado pelo líder.
    public long getUltimoSeq() {
        synchronized (lock) {
            return ultimoSeq;
        }
    }

    /**
     * Devolve a incarnação deste líder.
     *
     * @return o instante de arranque, em milissegundos
     */
    // Devolve o identificador da incarnação, que não muda durante a vida do processo.
    public long getInc() {
        return inc;
    }

    /**
     * Trata uma mensagem {@code IN}: valida-a, numera-a, escreve-a e deixa-a pendente para o
     * próximo lote.
     *
     * @param msg mensagem recebida no tópico de entrada
     * @return {@code true} se o registo foi aceite (escrito em ficheiro); {@code false} se rejeitado
     */
    // Valida IN e, sob o lock, prepara relógios e seq, verifica V[k]==seq, escreve e junta a pendentes.
    public boolean onIn(String msg) {
        if (!"IN".equals(Wire.type(msg))) {
            System.out.println("REJEITADO: " + msg);
            return false;
        }
        String[] f = Wire.body(msg).split(";", -1);
        if (f.length != 4) {
            System.out.println("REJEITADO: " + msg);
            return false;
        }
        double temp;
        long receivedL;
        VectorClock receivedV;
        try {
            temp = Double.parseDouble(f[1]);
            receivedL = Long.parseLong(f[2]);
            receivedV = VectorClock.parse(f[3], partitioner.count());
            if (!Double.isFinite(temp) || receivedL < 0 || f[0].isBlank()
                    || f[0].chars().anyMatch(Character::isWhitespace))
                throw new IllegalArgumentException("Leitura inválida");
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + msg);
            return false;
        }
        if (partitioner.of(f[0]) != k) {
            System.out.println("PARTIÇÃO ERRADA: " + f[0] + " não pertence a p" + k);
            return false;
        }
        synchronized (lock) {
            try {
                // Prepara o novo estado sem alterar o estado confirmado se append falhar.
                long nextL = Math.incrementExact(Math.max(lamport, receivedL));
                VectorClock nextV = vector.copy();
                nextV.merge(receivedV);
                nextV.tick(k);
                long nextSeq = Math.incrementExact(ultimoSeq);
                if (nextV.get(k) != nextSeq) {
                    System.out.println("REJEITADO: V[k] incompatível com seq");
                    return false;
                }
                SensorRecord r = SensorRecord.now(nextSeq, f[0], temp, nextL, nextV);
                file.append(r);
                ultimoSeq = nextSeq;
                lamport = nextL;
                vector = nextV;
                // No mesmo monitor em que enviarLote() troca a lista e lê ultimoSeq: um registo
                // com seq <= ultimoSeq de um lote está nesse lote ou num anterior, nunca no seguinte.
                pendentes.add(r);
                System.out.println("EVENTO: seq=" + ultimoSeq + " L=" + lamport + " V=" + vector);
            } catch (IOException | ArithmeticException e) {
                System.out.println("ERRO a escrever: " + e.getMessage());
                return false;
            }
        }
        return true;
    }

    /**
     * Fecha o lote atual e publica-o como heartbeat, mesmo que não tenha registos.
     *
     * <p>Sob o mesmo trinco de {@link #onIn(String)}: troca {@code pendentes} por uma lista nova,
     * lê {@code ultimoSeq} e incrementa {@code hb}. Fora dele: codifica, publica, escreve a linha
     * em {@code hb-p<k>.txt} e assinala {@code HEARTBEAT}. Não altera L nem V.</p>
     */
    // Corre na thread do ScheduledExecutorService; trata as falhas de publicação e de escrita sem as propagar.
    public void enviarLote() {
        List<SensorRecord> lote;
        long seqFecho;
        long hbLote;
        synchronized (lock) {
            lote = pendentes;
            pendentes = new ArrayList<>();
            seqFecho = ultimoSeq;
            hbLote = ++hb;
        }
        // Fora do trinco: a publicação é um pedido HTTP ao Kubo sem tempo limite e não pode
        // impedir a thread de entrada de numerar e escrever as leituras que vão chegando.
        String msg = new Batch(host, porto, inc, hbLote, seqFecho, lote).encode();
        int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
        long instante = System.currentTimeMillis();
        try {
            publisher.publish(Wire.topicData(k), msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("PUBLICAÇÃO INTERROMPIDA (hb=" + hbLote + ")");
            return;
        } catch (Exception e) {
            // Não se republica: os registos ficam no ficheiro e o ultimoSeq do lote seguinte
            // leva os participantes a pedi-los por TCP.
            System.out.println("PUBLICAÇÃO FALHOU (hb=" + hbLote + ", " + lote.size()
                    + " registos ficam recuperáveis): " + e.getMessage());
            return;
        }
        String linha = inc + ";" + hbLote + ";" + instante + ";" + seqFecho + ";" + lote.size() + ";" + bytes;
        try {
            Files.writeString(hbFile, linha + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("ERRO a escrever " + hbFile + ": " + e.getMessage());
        }
        System.out.println("HEARTBEAT: hb=" + hbLote + " n=" + lote.size() + " ultimoSeq=" + seqFecho);
    }

    /**
     * Responde a uma linha de pedido de recuperação.
     *
     * @param linha pedido recebido, esperado no formato {@code PEDIDO <de> <até>}
     * @return as linhas da resposta: zero ou mais registos seguidos de {@code FIM}, ou uma única
     *         linha {@code ERRO <motivo>}
     */
    // Valida PEDIDO <de> <até> e devolve as linhas persistidas desse intervalo, terminando com FIM.
    public List<String> processarPedido(String linha) {
        String[] t = linha == null ? new String[0] : linha.trim().split("\\s+");
        if (t.length != 3 || !t[0].equals("PEDIDO")) {
            return List.of("ERRO pedido mal formado");
        }
        long de;
        long ate;
        try {
            de = Long.parseLong(t[1]);
            ate = Long.parseLong(t[2]);
        } catch (NumberFormatException e) {
            return List.of("ERRO pedido mal formado");
        }
        if (de < 1 || de > ate) {
            return List.of("ERRO intervalo inválido");
        }
        long limite = Math.min(ate, getUltimoSeq());
        List<String> out = new ArrayList<>();
        try {
            for (SensorRecord r : file.range(de, limite)) {
                out.add(r.toLine());
            }
        } catch (IOException e) {
            return List.of("ERRO interno");
        }
        out.add("FIM");
        System.out.println("RECUPERAÇÃO: " + de + ".." + limite + " (" + (out.size() - 1) + " registos)");
        return out;
    }

    /**
     * Atende uma ligação de recuperação: lê pedidos linha a linha e responde a cada um.
     * Bloqueia em {@code readLine()} no máximo {@link Wire#T_TCP_MS}.
     *
     * @param s socket da ligação (é fechado no fim)
     */
    // Atende uma ligação TCP de recuperação, lendo pedidos e enviando respostas com tempo limite de leitura.
    public void atender(Socket s) {
        try (Socket sock = s;
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(sock.getOutputStream(), true, StandardCharsets.UTF_8)) {
            sock.setSoTimeout(Wire.T_TCP_MS);
            String linha;
            while ((linha = in.readLine()) != null) {
                for (String l : processarPedido(linha)) {
                    out.println(l);
                }
            }
        } catch (IOException e) {
            System.out.println("Ligação de recuperação terminada: " + e.getMessage());
        }
    }

    /**
     * Servidor de recuperação: aceita ligações e atende-as <strong>sequencialmente</strong>.
     *
     * @param ss socket de escuta
     * @throws IOException se o {@code accept()} falhar
     */
    // Aceita ligações TCP e atende cada uma sequencialmente até o servidor fechar.
    public void servir(ServerSocket ss) throws IOException {
        while (!ss.isClosed()) {
            atender(ss.accept());
        }
    }

    /**
     * Arranca o líder.
     *
     * @param args {@code <k> <N> [endereço] [api]}
     * @throws Exception se o Kubo ou o porto de recuperação não estiverem disponíveis
     */
    // Configura o líder, reserva o porto TCP e arranca a subscrição de IN numa thread separada.
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        if (n < 1 || k < 0 || k >= n) {
            // sem esta verificação um líder "p5" com N = 3 nunca receberia nada e ninguém daria por isso
            System.err.println("k tem de estar entre 0 e N-1 (k=" + k + ", N=" + n + ")");
            return;
        }
        String host = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub ipfs = new IpfsPubSub(api);
        PartitionLeader l = new PartitionLeader(k, new Partitioner(n), new RecordFile("lider-p" + k + ".txt"),
                Path.of("hb-p" + k + ".txt"), host, Wire.PORTO_BASE + k, ipfs);
        // 1.º reservar o porto de recuperação, e só depois começar a aceitar leituras: se já houver
        // um líder p<k> a correr, este termina ANTES de numerar e escrever no mesmo ficheiro
        // (dois líderes da mesma partição criariam números de sequência repetidos).
        ServerSocket ss;
        try {
            ss = new ServerSocket(Wire.PORTO_BASE + k);
        } catch (java.net.BindException e) {
            System.err.println("Porto " + (Wire.PORTO_BASE + k) + " ocupado: já há um líder p" + k
                    + " a correr (ou outro programa nesse porto)? " + e.getMessage());
            return;
        }
        // O pid serve para provocar a falha nos ensaios (terminar ou suspender este processo).
        System.out.println("Líder p" + k + " pronto (pid=" + ProcessHandle.current().pid() + ", ultimoSeq="
                + l.getUltimoSeq() + ", inc=" + l.getInc() + ", Δ=" + Wire.PERIODO_HB_MS
                + " ms, recuperação em :" + (Wire.PORTO_BASE + k) + ")");
        ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-p" + k);
            t.setDaemon(true); // não impede o processo de terminar
            return t;
        });
        // Primeiro lote (hb=1) já no arranque e depois a cada Δ. Uma exceção que saísse da tarefa
        // cancelaria as execuções seguintes sem aviso, e o líder deixaria de dar sinal de vida.
        heartbeat.scheduleAtFixedRate(() -> {
            try {
                l.enviarLote();
            } catch (RuntimeException e) {
                System.out.println("ERRO no envio do lote: " + e);
            }
        }, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
        Thread entrada = new Thread(() -> {
            // se o Kubo reiniciar a subscrição cai: volta a subscrever. As leituras publicadas
            // entretanto perdem-se (o troço produtor -> líder não é numerado, logo não é detetável).
            while (true) {
                try {
                    ipfs.subscribe(Wire.topicIn(k), l::onIn);
                    System.err.println("Subscrição terminou; a voltar a subscrever");
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    System.err.println("Subscrição falhou (" + e.getMessage() + "); nova tentativa em 2 s");
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "entrada-p" + k);
        entrada.setDaemon(true); // não impede o processo de terminar
        entrada.start();
        try (ServerSocket s = ss) {
            l.servir(s); // thread principal: bloqueia em accept() até o processo terminar
        }
    }
}
