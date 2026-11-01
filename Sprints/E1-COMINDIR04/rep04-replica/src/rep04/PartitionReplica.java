package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Réplica {@code n}: mantém uma ou mais partições, cada uma num ficheiro próprio ({@code
 * replica-<id>-p<k>.txt}), com uma thread de subscrição por partição e um {@link HeartbeatMonitor} por
 * partição.
 */
public class PartitionReplica {

    private PartitionReplica() {
    }

    /**
     * Estado e regra de aplicação de <strong>uma</strong> partição numa réplica. Cada partição tem o seu
     * estado, por isso partições diferentes não partilham nada.
     */
    public static class Particao {
        private final int k;
        private final RecordFile file;
        private final Recuperador recuperador;
        private final HeartbeatMonitor monitor;
        private final Consumer<String> log;
        /** Registos que chegaram adiantados, ordenados por seq. */
        private final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        private long ultimo;
        private String hostLider;
        private int portoLider = -1;

        public Particao(int k, RecordFile file, Recuperador recuperador, HeartbeatMonitor monitor,
                        Consumer<String> log) throws IOException {
            this.k = k;
            this.file = file;
            this.recuperador = recuperador;
            this.monitor = monitor;
            this.log = log;
            this.ultimo = file.lastSeq();
        }

        public synchronized long getUltimo() {
            return ultimo;
        }

        public synchronized int getRetidos() {
            return retidos.size();
        }

        // Renova o detetor, aplica os registos do lote e pede por TCP o intervalo ultimo+1..ultimoSeq se faltar algum.
        public synchronized void onLote(String msg) {
            Batch b;
            try {
                b = Batch.decode(msg, 0);
            } catch (IllegalArgumentException e) {
                System.out.println("REJEITADO: " + msg.replace('\n', '|'));
                return;
            }
            monitor.onHeartbeat(b.getInc(), b.getHb());
            List<SensorRecord> rs = b.getRecords();
            String intervalo = rs.isEmpty() ? "-" : rs.get(0).getSeq() + ".." + rs.get(rs.size() - 1).getSeq();
            log.accept("RECEBIDO;p" + k + ";inc=" + b.getInc() + ";hb=" + b.getHb() + ";ultimoSeq="
                    + b.getLastSeq() + ";n=" + rs.size() + ";seq=" + intervalo);
            hostLider = b.getAddress();
            portoLider = b.getPort();
            try {
                for (SensorRecord r : rs) {
                    aplicar(r);
                }
                recuperarAte(b.getLastSeq());
            } catch (IllegalStateException e) {
                // falha de escrita no disco: o registo não conta como escrito (ultimo não avança),
                // pelo que volta a ser pedido no lote seguinte, através do ultimoSeq
                System.out.println("ERRO DE ESCRITA p" + k + ": " + e.getMessage());
            }
        }

        // Compara o último aplicado com o ultimoSeq do lote; se for menor, pede o intervalo em falta e assinala RECUPERACAO.
        private void recuperarAte(long ultimoSeq) {
            if (ultimo >= ultimoSeq || hostLider == null) {
                return;
            }
            long de = ultimo + 1;
            System.out.println("RECUPERAÇÃO p" + k + ": " + de + ".." + ultimoSeq + " pedida a " + hostLider + ":" + portoLider);
            try {
                recuperador.pedir(hostLider, portoLider, de, ultimoSeq,
                        l -> aplicarSemRecuperar(SensorRecord.fromLine(l)));
                log.accept("RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ultimoSeq);
            } catch (IOException | IllegalArgumentException e) {
                System.out.println("RECUPERAÇÃO FALHOU p" + k + ": " + e.getMessage());
            }
            escreverEmSequencia();
        }

        // Aplica ou retém o registo por seq e pede recuperação quando o número de retidos excede o limite.
        public synchronized void aplicar(SensorRecord r) {
            if (!aplicarSemRecuperar(r)) {
                return;
            }
            if (retidos.size() > Wire.LIMITE_RETIDOS) {
                recuperar();
            }
        }

        // Ignora duplicados, escreve o próximo seq ou retém um registo adiantado, sem iniciar outra recuperação.
        private boolean aplicarSemRecuperar(SensorRecord r) {
            long seq = r.getSeq();
            if (seq <= ultimo || retidos.containsKey(seq)) {
                System.out.println("DUPLICADO p" + k + ": " + seq);
                return false;
            }
            if (seq == ultimo + 1) {
                escrever(r);
                escreverEmSequencia();
            } else {
                retidos.put(seq, r);
                System.out.println("EM ESPERA p" + k + ": " + seq + " (falta " + (ultimo + 1) + ")");
            }
            return true;
        }

        // Persiste o registo recebido com os seus carimbos e só depois avança o último seq aplicado.
        private void escrever(SensorRecord r) {
            try {
                file.append(r);
                ultimo = r.getSeq();
            } catch (IOException e) {
                throw new IllegalStateException("não foi possível escrever " + r.toLine(), e);
            }
        }

        public synchronized void escreverEmSequencia() {
            while (!retidos.isEmpty() && retidos.firstKey() == ultimo + 1) {
                escrever(retidos.pollFirstEntry().getValue());
            }
        }

        // Pede por TCP o intervalo em falta antes do primeiro retido e aplica cada linha devolvida pelo líder.
        public synchronized void recuperar() {
            if (retidos.isEmpty() || hostLider == null) {
                return;
            }
            long de = ultimo + 1;
            long ate = retidos.firstKey() - 1;
            System.out.println("RECUPERAÇÃO p" + k + ": " + de + ".." + ate + " pedida a " + hostLider + ":" + portoLider);
            try {
                recuperador.pedir(hostLider, portoLider, de, ate,
                        l -> aplicarSemRecuperar(SensorRecord.fromLine(l)));
                System.out.println("RECUPERADO p" + k + ": " + de + ".." + ate);
            } catch (IOException | IllegalArgumentException e) {
                System.out.println("RECUPERAÇÃO FALHOU p" + k + ": " + e.getMessage());
            }
            escreverEmSequencia();
        }
    }

    private static final AtomicBoolean DESCARTE_FEITO = new AtomicBoolean(false);
    private static final Random RND = new Random();

    // Simula perda aleatória ou descarte do lote com um dado seq na receção PubSub; entrega os restantes à partição.
    static boolean simular(Particao p, String msg, double perda, long descartarSeq) {
        synchronized (RND) {
            boolean descarta = false;
            if (descartarSeq >= 0 && !DESCARTE_FEITO.get()) {
                try {
                    boolean contem = Batch.decode(msg, 0).getRecords().stream()
                            .anyMatch(r -> r.getSeq() == descartarSeq);
                    if (contem && DESCARTE_FEITO.compareAndSet(false, true)) {
                        descarta = true;
                    }
                } catch (IllegalArgumentException e) {
                }
            }
            if (!descarta && perda > 0 && RND.nextDouble() < perda) {
                descarta = true;
            }
            if (descarta) {
                System.out.println("DESCARTADO (simulação) p: " + msg.replace('\n', '|'));
                return true;
            }
        }
        p.onLote(msg);
        return false;
    }

    private static Path monitorFile;

    // Junta System.currentTimeMillis() ao evento; synchronized porque o chamam as threads das partições e a de verificação.
    static synchronized void registar(String evento) {
        String linha = System.currentTimeMillis() + ";" + evento;
        System.out.println(linha);
        if (monitorFile == null) {
            return;
        }
        try {
            Files.writeString(monitorFile, linha + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("ERRO a escrever " + monitorFile + ": " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: PartitionReplica <id> <k1,k2> [portoApi]  [-Dperda=p] [-Ddescartar=seq]"
                    + " [-Drep04.periodo=ms] [-Drep04.fator=f]");
            return;
        }
        int id = Integer.parseInt(args[0]);
        String api = "http://127.0.0.1:" + (args.length > 2 ? args[2] : "5001") + "/api/v0";
        double perda = Double.parseDouble(System.getProperty("perda", "0"));
        long descartar = Long.parseLong(System.getProperty("descartar", "-1"));
        monitorFile = Path.of("monitor-replica" + id + ".txt");
        IpfsPubSub ipfs = new IpfsPubSub(api);
        List<Thread> threads = new ArrayList<>();
        List<HeartbeatMonitor> monitores = new ArrayList<>();
        List<Particao> particoes = new ArrayList<>();
        for (String ks : args[1].split(",")) {
            int k = Integer.parseInt(ks.trim());
            HeartbeatMonitor m = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    PartitionReplica::registar);
            monitores.add(m);
            particoes.add(new Particao(k, new RecordFile("replica-" + id + "-p" + k + ".txt"),
                    new TcpRecuperador(), m, PartitionReplica::registar));
        }
        ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "verificacao");
            t.setDaemon(true);
            return t;
        });
        // Uma exceção que saísse da tarefa cancelaria as verificações seguintes sem aviso.
        verificacao.scheduleAtFixedRate(() -> {
            try {
                for (HeartbeatMonitor m : monitores) {
                    m.check();
                }
            } catch (RuntimeException e) {
                System.out.println("ERRO na verificação: " + e);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        for (Particao p : particoes) {
            int k = p.k;
            System.out.println("Réplica " + id + " p" + k + " pronta (ultimo=" + p.getUltimo() + ", Δ="
                    + Wire.PERIODO_HB_MS + " ms, TIMEOUT=" + Wire.PERIODO_HB_MS * Wire.FATOR_TIMEOUT + " ms)");
            Thread t = new Thread(() -> {
                // subscribe bloqueia enquanto o Kubo mantiver a resposta aberta; se a ligação cair
                // (Kubo reiniciado), volta a subscrever. O que for publicado entretanto perde-se e
                // será recuperado por TCP através do ultimoSeq do lote seguinte.
                while (true) {
                    try {
                        ipfs.subscribe(Wire.topicData(k), msg -> simular(p, msg, perda, descartar));
                        System.err.println("Subscrição p" + k + " terminou; a voltar a subscrever");
                    } catch (InterruptedException e) {
                        return;
                    } catch (Exception e) {
                        System.err.println("Subscrição p" + k + " falhou (" + e.getMessage() + "); nova tentativa em 2 s");
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }, "sub-p" + k);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
    }
}
