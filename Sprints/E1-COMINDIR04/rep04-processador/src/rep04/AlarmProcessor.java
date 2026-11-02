package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Acumula conhecimento causal sem gerar eventos locais de relógio. Recebe os {@code LOTE} de todas as
 * partições, trata cada registo pela ordem com as regras da COMINDIR03 e mantém um {@link HeartbeatMonitor}
 * por partição (eventos em {@code monitor-processador.txt}).
 */
public final class AlarmProcessor {
    private final Partitioner partitioner;
    private final Publisher publisher;
    private final Object lock = new Object();
    private final List<Set<Long>> vistos = new ArrayList<>();
    private long lamport;
    private final VectorClock vector;
    private final List<HeartbeatMonitor> monitores = new ArrayList<>();
    private final Consumer<String> log;

    public AlarmProcessor(int n, Publisher publisher, Consumer<String> log) {
        partitioner = new Partitioner(n);
        this.publisher = publisher;
        this.log = log;
        vector = new VectorClock(n);
        for (int k = 0; k < n; k++) {
            vistos.add(new HashSet<>());
            monitores.add(new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log));
        }
    }

    public List<HeartbeatMonitor> getMonitores() {
        return monitores;
    }

    // Valida o LOTE, renova o detetor de k, assinala RECEBIDO e trata os registos um a um, pela ordem do lote.
    public int onLote(int k, String msg) {
        Batch b;
        try {
            if (k < 0 || k >= partitioner.count()) throw new IllegalArgumentException("Partição inválida");
            b = Batch.decode(msg, partitioner.count());
        } catch (IllegalArgumentException e) {
            System.err.println("REJEITADO no processador: " + e.getMessage());
            return -1;
        }
        monitores.get(k).onHeartbeat(b.getInc(), b.getHb());
        List<SensorRecord> rs = b.getRecords();
        String intervalo = rs.isEmpty() ? "-" : rs.get(0).getSeq() + ".." + rs.get(rs.size() - 1).getSeq();
        log.accept("RECEBIDO;p" + k + ";inc=" + b.getInc() + ";hb=" + b.getHb() + ";ultimoSeq="
                + b.getLastSeq() + ";n=" + rs.size() + ";seq=" + intervalo);
        int aceites = 0;
        for (SensorRecord r : rs) {
            if (onRecord(k, r)) aceites++;
        }
        return aceites;
    }

    // Ignora alarmes e duplicados, acumula Lp/Vp e publica um alarme quando a temperatura atinge o limiar.
    public boolean onRecord(int k, SensorRecord r) {
        if (partitioner.of(r.getSensorId()) != k || r.getVector().get(k) != r.getSeq()) {
            System.err.println("REJEITADO no processador: Partição/seq incompatível com vetor");
            return false;
        }
        if (r.getSensorId().startsWith("ALARM-")) return false;

        String alarm = null;
        String sensor = null;
        int target = -1;
        synchronized (lock) {
            // Um conjunto por partição também trata duplicados após reordenação.
            if (!vistos.get(k).add(r.getSeq())) return false;
            lamport = Math.max(lamport, r.getLamport());
            vector.merge(r.getVector());
            if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
                sensor = "ALARM-" + r.getSensorId();
                target = partitioner.of(sensor);
                // Captura um snapshot coerente; a publicação não retém o lock durante HTTP.
                alarm = Wire.in(sensor, r.getTemperature(), lamport, vector.copy());
            }
        }
        if (alarm != null) {
            if (monitores.get(target).state() == HeartbeatMonitor.State.SUSPEITO) {
                // Só se assinala: o processador não retém nem reenvia alarmes.
                System.out.println("AVISO: líder p" + target + " suspeito; alarme " + sensor + " pode perder-se");
            }
            try {
                publisher.publish(Wire.topicIn(target), alarm);
                System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " → p" + target);
            } catch (Exception e) {
                // PubSub best-effort: não existe recuperação no troço processador → líder.
                System.err.println("PUBLICAÇÃO DE ALARME FALHOU: " + e.getMessage());
            }
        }
        return true;
    }

    private static final Path MONITOR_FILE = Path.of("monitor-processador.txt");

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

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: AlarmProcessor <N> [api]  [-Drep04.periodo=ms] [-Drep04.fator=f]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        IpfsPubSub ipfs = new IpfsPubSub(args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0");
        AlarmProcessor processor = new AlarmProcessor(n, ipfs, AlarmProcessor::registar);
        ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "verificacao");
            t.setDaemon(true);
            return t;
        });
        // Uma exceção que saísse da tarefa cancelaria as verificações seguintes sem aviso.
        verificacao.scheduleAtFixedRate(() -> {
            try {
                for (HeartbeatMonitor m : processor.getMonitores()) m.check();
            } catch (RuntimeException e) {
                System.err.println("ERRO na verificação: " + e);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        System.out.println("Processador pronto (N=" + n + ", Δ=" + Wire.PERIODO_HB_MS + " ms, TIMEOUT="
                + Wire.PERIODO_HB_MS * Wire.FATOR_TIMEOUT + " ms)");
        List<Thread> threads = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            final int partition = k;
            Thread t = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        ipfs.subscribe(Wire.topicData(partition), msg -> processor.onLote(partition, msg));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        System.err.println("Subscrição p" + partition + " falhou: " + e.getMessage());
                    }
                    try { Thread.sleep(2000); }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "alarmes-p" + k);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) t.join();
    }
}
