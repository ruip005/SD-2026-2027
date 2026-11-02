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
 * Líder de uma partição {@code k}: define a ordem dos registos dessa partição. Threads: entrada (IN),
 * heartbeat ({@link #enviarLote()} a cada Δ) e principal (servidor TCP de recuperação).
 */
public class PartitionLeader {
    private final int k;
    private final Partitioner partitioner;
    private final RecordFile file;
    private final Path hbFile;
    private final String host;
    private final int porto;
    private final Publisher publisher;
    /** Incarnação: instante de arranque deste líder, em milissegundos. */
    private final long inc;
    /** Trinco que protege seq, L, V, a escrita no ficheiro, pendentes e hb. */
    private final Object lock = new Object();
    private long ultimoSeq;
    private long lamport;
    private VectorClock vector;
    /** Registos já numerados e escritos que ainda não seguiram num lote. */
    private List<SensorRecord> pendentes = new ArrayList<>();
    /** Número do último heartbeat desta incarnação (o primeiro lote leva hb=1). */
    private long hb = 0;

    // Nova incarnação; seq, L e V continuam a partir do último registo gravado.
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

    public long getUltimoSeq() {
        synchronized (lock) {
            return ultimoSeq;
        }
    }

    public long getInc() {
        return inc;
    }

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

    // Thread de heartbeat: fecha o lote e publica-o, mesmo vazio. Não altera L nem V.
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

    public void servir(ServerSocket ss) throws IOException {
        while (!ss.isClosed()) {
            atender(ss.accept());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        if (n < 1 || k < 0 || k >= n) {
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
            t.setDaemon(true);
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
        entrada.setDaemon(true);
        entrada.start();
        try (ServerSocket s = ss) {
            l.servir(s); // thread principal: bloqueia em accept() até o processo terminar
        }
    }
}
