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
 * Réplica {@code n}: mantém uma ou mais partições, cada uma num ficheiro próprio
 * ({@code replica-<id>-p<k>.txt}), com uma thread de subscrição por partição e um
 * {@link HeartbeatMonitor} por partição.
 *
 * <p>Invocação: {@code PartitionReplica <id> <k1,k2> [portoApi]}, com as propriedades de simulação
 * {@code -Dperda=p} (probabilidade de descartar um {@code LOTE}) e {@code -Ddescartar=seq} (descarta,
 * uma única vez, o {@code LOTE} que contém esse {@code seq}), aplicadas exclusivamente à receção por
 * PubSub.</p>
 *
 * <h2>Fluxo (por partição)</h2>
 * <ol>
 *   <li>Ao arrancar, lê o último {@code seq} do seu ficheiro ({@link RecordFile#lastSeq()}): é
 *       assim que uma réplica reiniciada sabe onde ficou.</li>
 *   <li>Subscreve {@code rep04-p<k>} numa thread própria (<strong>bloqueia</strong> à espera de
 *       cada linha da resposta HTTP do Kubo).</li>
 *   <li>Cada {@code LOTE} recebido passa por {@link Particao#onLote(String)}: renova o detetor,
 *       assinala {@code RECEBIDO}, guarda o endereço do líder anunciado e aplica cada registo com a
 *       regra de {@link Particao#aplicar(SensorRecord)}.</li>
 *   <li>Se houver mais de {@link Wire#LIMITE_RETIDOS} registos retidos, pede os que faltam ao líder
 *       por TCP ({@link Recuperador}); a ligação e cada leitura bloqueiam no máximo
 *       {@link Wire#T_TCP_MS} ms.</li>
 *   <li>Se, depois de aplicar o lote, o último {@code seq} aplicado for inferior ao
 *       {@code ultimoSeq} do cabeçalho, pede o intervalo em falta por TCP e assinala
 *       {@code RECUPERACAO}.</li>
 * </ol>
 *
 * <p>Uma thread de verificação chama {@link HeartbeatMonitor#check()} em todos os monitores a cada
 * {@link Wire#VERIFICACAO_MS}. Os eventos do detetor vão para a consola e para
 * {@code monitor-replica<id>.txt}, precedidos de {@code System.currentTimeMillis()}. Suspeitar do
 * líder não muda a forma como os registos são aplicados.</p>
 *
 * <h2>Ações bloqueantes e porquê</h2>
 * <ul>
 *   <li>{@code subscribe}: a API do Kubo entrega as mensagens numa resposta HTTP que nunca acaba;
 *       por isso há uma thread por partição, para uma partição não parar as outras.</li>
 *   <li>{@code connect} e {@code readLine} na recuperação: correm na thread da partição e têm
 *       tempo limite, para um líder em baixo ou mudo não congelar a réplica para sempre. Durante a
 *       recuperação, as mensagens dessa partição ficam à espera na resposta HTTP (não se perdem).</li>
 *   <li>{@code join} no {@code main}: a thread principal só espera pelas subscrições.</li>
 * </ul>
 *
 * <h2>O que pode correr mal</h2>
 * <ul>
 *   <li>Kubo reiniciado ou desligado: a subscrição cai. O {@code main} volta a subscrever de 2 em 2
 *       segundos; o que foi publicado entretanto perde-se e é recuperado pela regra dos retidos.</li>
 *   <li>Perda do último lote de uma série: o {@code ultimoSeq} do lote seguinte, mesmo vazio,
 *       denuncia a falta; só não é recuperado se o líder deixar de enviar lotes.</li>
 *   <li>Líder em baixo: a recuperação falha ({@code RECUPERAÇÃO FALHOU}), o estado mantém-se e volta
 *       a tentar no lote seguinte.</li>
 *   <li>{@code LOTE} falsificado: a réplica confia no endereço anunciado; qualquer processo pode
 *       desviar os pedidos de recuperação (não há autenticação).</li>
 *   <li>Endereço {@code 127.0.0.1} anunciado por um líder noutra máquina: a recuperação falha com
 *       ligação recusada. Arrancar o líder com o IP real.</li>
 * </ul>
 */
public class PartitionReplica {

    /** Classe não instanciável: só tem métodos estáticos e a classe interna {@link Particao}. */
    // Impede instanciar a classe exterior; o estado de cada partição fica na classe interna Particao.
    private PartitionReplica() {
    }

    /**
     * Estado e regra de aplicação de <strong>uma</strong> partição numa réplica. Cada partição tem
     * o seu estado, por isso partições diferentes não partilham nada.
     *
     * <p><strong>Porquê um {@link TreeMap} para os retidos</strong> (e não uma {@code List} ou um
     * {@code HashMap}): a regra precisa de três operações rápidas: saber se um {@code seq} já está
     * retido (deteção de duplicados), obter o <em>menor</em> {@code seq} retido ({@code firstKey})
     * e retirá-lo ({@code pollFirstEntry}). O {@code TreeMap} mantém as chaves ordenadas e faz as três
     * em O(log n). Um {@code HashMap} não tem ordem (teríamos de procurar o mínimo, O(n)); uma
     * {@code List} obrigaria a ordenar ou a percorrer para encontrar duplicados.</p>
     *
     * <p>Os métodos são {@code synchronized} porque a mesma partição pode ser tocada pela thread de
     * subscrição e pelos testes; dentro de uma réplica real só a thread da partição a usa.</p>
     */
    public static class Particao {
        /** Partição. */
        private final int k;
        /** Ficheiro {@code replica-<id>-p<k>.txt}. */
        private final RecordFile file;
        /** Canal de recuperação junto do líder. */
        private final Recuperador recuperador;
        /** Detetor de falhas do líder desta partição. */
        private final HeartbeatMonitor monitor;
        /** Destino dos eventos {@code RECEBIDO} e {@code RECUPERACAO} ({@code monitor-replica<id>.txt}). */
        private final Consumer<String> log;
        /** Registos que chegaram adiantados, ordenados por {@code seq}. */
        private final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        /** Último {@code seq} escrito no ficheiro. */
        private long ultimo;
        /** Endereço do líder, aprendido em cada {@code LOTE}. */
        private String hostLider;
        /** Porto de recuperação do líder, aprendido em cada {@code LOTE}; -1 se ainda desconhecido. */
        private int portoLider = -1;

        /**
         * Cria o estado de uma partição e retoma a sequência a partir do ficheiro.
         *
         * @param k           partição
         * @param file        ficheiro {@code replica-<id>-p<k>.txt}
         * @param recuperador canal de recuperação junto do líder
         * @param monitor     detetor de falhas do líder de {@code k}
         * @param log         destino dos eventos da partição
         * @throws IOException se não for possível ler o último {@code seq} do ficheiro
         */
        // Inicializa uma partição da réplica, retomando o último seq escrito no respetivo ficheiro.
        public Particao(int k, RecordFile file, Recuperador recuperador, HeartbeatMonitor monitor,
                        Consumer<String> log) throws IOException {
            this.k = k;
            this.file = file;
            this.recuperador = recuperador;
            this.monitor = monitor;
            this.log = log;
            this.ultimo = file.lastSeq();
        }

        /**
         * Devolve o último {@code seq} escrito.
         *
         * @return o último {@code seq} escrito no ficheiro desta partição
         */
        // Consulta com sincronização o último seq aplicado nesta partição.
        public synchronized long getUltimo() {
            return ultimo;
        }

        /**
         * Devolve quantos registos estão retidos.
         *
         * @return o número de registos retidos em memória à espera de um anterior
         */
        // Consulta com sincronização quantos registos aguardam a chegada de anteriores.
        public synchronized int getRetidos() {
            return retidos.size();
        }

        /**
         * Trata uma mensagem {@code LOTE} recebida por PubSub: renova o detetor, assinala
         * {@code RECEBIDO}, atualiza o endereço do líder, aplica os registos pela ordem e, no fim,
         * recupera por TCP o que o {@code ultimoSeq} mostrar que falta.
         *
         * <p>Os registos de um lote de incarnação antiga também são aplicados: o detetor ignora-o como
         * heartbeat, mas o {@code seq} continua entre incarnações e os repetidos são duplicados.</p>
         *
         * @param msg mensagem {@code LOTE} completa
         */
        // Renova o detetor, aplica os registos do lote e pede por TCP o intervalo ultimo+1..ultimoSeq se faltar algum.
        public synchronized void onLote(String msg) {
            Batch b;
            try {
                b = Batch.decode(msg, 0);
            } catch (IllegalArgumentException e) {
                // inclui NumberFormatException: tipo errado, campos a menos, n diferente das linhas
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

        /**
         * Pede ao líder os registos de {@code ultimo + 1} até {@code ultimoSeq}, se faltar algum, e
         * aplica-os com a mesma regra, sem desencadear a recuperação por retidos.
         *
         * @param ultimoSeq o {@code ultimoSeq} do cabeçalho do lote acabado de aplicar
         */
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

        /**
         * Aplica um registo segundo a regra da réplica (secção 7 da arquitetura):
         * <ul>
         *   <li>{@code seq <= ultimo} ou já retido: duplicado, é ignorado ({@code DUPLICADO});</li>
         *   <li>{@code seq == ultimo + 1}: é escrito e, a seguir, os retidos que ficam em sequência;</li>
         *   <li>{@code seq > ultimo + 1}: fica retido ({@code EM ESPERA}); se os retidos passarem
         *       {@link Wire#LIMITE_RETIDOS}, é pedida a recuperação ao líder.</li>
         * </ul>
         * Os registos obtidos na recuperação seguem a mesma regra, mas não desencadeiam outra
         * recuperação dentro desta (ver {@link #recuperar()}).
         *
         * @param r registo recebido, por PubSub ou por recuperação
         */
        // Aplica ou retém o registo por seq e pede recuperação quando o número de retidos excede o limite.
        public synchronized void aplicar(SensorRecord r) {
            if (!aplicarSemRecuperar(r)) {
                return;
            }
            if (retidos.size() > Wire.LIMITE_RETIDOS) {
                recuperar();
            }
        }

        /**
         * Aplica a regra da réplica sem verificar o limite de retidos. É o passo usado por
         * {@link #aplicar(SensorRecord)} e, durante a recuperação, para cada linha recebida do líder,
         * o que impede uma recuperação dentro de outra.
         *
         * @param r registo a aplicar
         * @return {@code false} se o registo era duplicado; {@code true} se foi escrito ou retido
         */
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

        /**
         * Escreve o registo no ficheiro e avança {@code ultimo}. Se a escrita falhar, {@code ultimo}
         * não avança, pelo que o registo volta a ser obtido mais tarde.
         *
         * @param r registo a escrever
         * @throws IllegalStateException se o ficheiro não puder ser escrito
         */
        // Persiste o registo recebido com os seus carimbos e só depois avança o último seq aplicado.
        private void escrever(SensorRecord r) {
            try {
                file.append(r);
                ultimo = r.getSeq();
            } catch (IOException e) {
                throw new IllegalStateException("não foi possível escrever " + r.toLine(), e);
            }
        }

        /** Escreve os registos retidos que passaram a estar em sequência com o último escrito. */
        // Escreve os retidos consecutivos a partir de ultimo+1, parando quando falta o seguinte.
        public synchronized void escreverEmSequencia() {
            while (!retidos.isEmpty() && retidos.firstKey() == ultimo + 1) {
                escrever(retidos.pollFirstEntry().getValue());
            }
        }

        /**
         * Pede ao líder o intervalo em falta ({@code ultimo + 1} até ao primeiro retido menos 1) e
         * aplica cada registo recebido segundo a mesma regra, sem voltar a desencadear recuperação.
         * Se falhar, mantém o que já foi escrito e os retidos; é tentada de novo no próximo registo.
         */
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

    /** Indica se o descarte único pedido por {@code -Ddescartar} já foi feito. */
    private static final AtomicBoolean DESCARTE_FEITO = new AtomicBoolean(false);
    /** Gerador usado na simulação de perda. */
    private static final Random RND = new Random();

    /**
     * Aplica a simulação de perda (apenas na receção por PubSub) e, se a mensagem não for
     * descartada, entrega-a à partição. Descartar um {@code LOTE} perde também o heartbeat.
     *
     * @param p    partição de destino
     * @param msg  mensagem {@code LOTE}
     * @param perda probabilidade de descarte em {@code [0,1]}
     * @param descartarSeq {@code seq} cujo lote se descarta uma única vez, ou {@code -1} para nenhum
     * @return {@code true} se a mensagem foi descartada pela simulação
     */
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
                    // mensagem mal formada: segue para onLote, que a rejeita
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

    /** Ficheiro de eventos do detetor desta réplica ({@code monitor-replica<id>.txt}). */
    private static Path monitorFile;

    /**
     * Regista um evento do detetor na consola e no ficheiro de monitorização.
     *
     * @param evento texto {@code <evento>;p<k>;<detalhe>}
     */
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

    /**
     * Arranca a réplica.
     *
     * @param args {@code <id> <k1,k2> [portoApi]}
     * @throws Exception se o Kubo não estiver acessível
     */
    // Cria o estado e o detetor das partições atribuídas, arranca a verificação periódica e uma thread de subscrição por partição.
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
