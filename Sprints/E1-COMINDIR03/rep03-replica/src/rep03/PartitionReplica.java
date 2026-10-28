package rep03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Réplica {@code n}: mantém uma ou mais partições, cada uma num ficheiro próprio
 * ({@code replica-<id>-p<k>.txt}), com uma thread de subscrição por partição.
 *
 * <p>Invocação: {@code PartitionReplica <id> <k1,k2> [portoApi]}, com as propriedades de simulação
 * {@code -Dperda=p} (probabilidade de descartar um {@code REG}) e {@code -Ddescartar=seq} (descarta,
 * uma única vez, o {@code REG} com esse {@code seq}), aplicadas exclusivamente à receção por PubSub.</p>
 *
 * <h2>Fluxo (por partição)</h2>
 * <ol>
 *   <li>Ao arrancar, lê o último {@code seq} do seu ficheiro ({@link RecordFile#lastSeq()}): é
 *       assim que uma réplica reiniciada sabe onde ficou.</li>
 *   <li>Subscreve {@code rep03-p<k>} numa thread própria (<strong>bloqueia</strong> à espera de
 *       cada linha da resposta HTTP do Kubo).</li>
 *   <li>Cada {@code REG} recebido passa por {@link Particao#onReg(String)}: guarda o endereço do
 *       líder anunciado e aplica a regra de {@link Particao#aplicar(SensorRecord)}.</li>
 *   <li>Se houver mais de {@link Wire#LIMITE_RETIDOS} registos retidos, pede os que faltam ao líder
 *       por TCP ({@link Recuperador}); a ligação e cada leitura bloqueiam no máximo
 *       {@link Wire#T_TCP_MS} ms.</li>
 * </ol>
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
 *   <li>Perda do último registo de uma série: nunca chegam 10 posteriores, logo não é recuperado
 *       (limitação documentada pelo docente).</li>
 *   <li>Líder em baixo: a recuperação falha ({@code RECUPERAÇÃO FALHOU}), o estado mantém-se e volta
 *       a tentar no registo seguinte.</li>
 *   <li>{@code REG} falsificado: a réplica confia no endereço anunciado; qualquer processo pode
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
        /** Registos que chegaram adiantados, ordenados por {@code seq}. */
        private final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        /** Último {@code seq} escrito no ficheiro. */
        private long ultimo;
        /** Endereço do líder, aprendido em cada {@code REG}. */
        private String hostLider;
        /** Porto de recuperação do líder, aprendido em cada {@code REG}; -1 se ainda desconhecido. */
        private int portoLider = -1;

        /**
         * Cria o estado de uma partição e retoma a sequência a partir do ficheiro.
         *
         * @param k           partição
         * @param file        ficheiro {@code replica-<id>-p<k>.txt}
         * @param recuperador canal de recuperação junto do líder
         * @throws IOException se não for possível ler o último {@code seq} do ficheiro
         */
        // Inicializa uma partição da réplica, retomando o último seq escrito no respetivo ficheiro.
        public Particao(int k, RecordFile file, Recuperador recuperador) throws IOException {
            this.k = k;
            this.file = file;
            this.recuperador = recuperador;
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
         * Trata uma mensagem {@code REG} recebida por PubSub: atualiza o endereço do líder com o
         * que ela anuncia e aplica o registo.
         *
         * @param msg mensagem {@code REG} completa
         */
        // Extrai o registo e o endereço de recuperação do líder e entrega o registo à regra de aplicação.
        public synchronized void onReg(String msg) {
            try {
                if (!"REG".equals(Wire.type(msg))) {
                    throw new IllegalArgumentException("tipo");
                }
                String[] p = Wire.regParts(msg);
                SensorRecord r = SensorRecord.fromLine(p[2]);
                hostLider = p[0];
                portoLider = Integer.parseInt(p[1]);
                aplicar(r);
            } catch (IllegalArgumentException e) {
                // inclui NumberFormatException: tipo errado, campos a menos, porto ou seq não numéricos
                System.out.println("REJEITADO: " + msg);
            } catch (IllegalStateException e) {
                // falha de escrita no disco: o registo não conta como escrito (ultimo não avança),
                // pelo que volta a ser pedido mais tarde pela regra dos retidos
                System.out.println("ERRO DE ESCRITA p" + k + ": " + e.getMessage());
            }
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
     * descartada, entrega-a à partição.
     *
     * @param p    partição de destino
     * @param msg  mensagem {@code REG}
     * @param perda probabilidade de descarte em {@code [0,1]}
     * @param descartarSeq {@code seq} a descartar uma única vez, ou {@code -1} para nenhum
     * @return {@code true} se a mensagem foi descartada pela simulação
     */
    // Simula perda aleatória ou descarte de um seq na receção PubSub; entrega à partição as mensagens não descartadas.
    static boolean simular(Particao p, String msg, double perda, long descartarSeq) {
        synchronized (RND) {
            boolean descarta = false;
            if (descartarSeq >= 0 && "REG".equals(Wire.type(msg)) && !DESCARTE_FEITO.get()) {
                try {
                    long seq = SensorRecord.fromLine(Wire.regParts(msg)[2]).getSeq();
                    if (seq == descartarSeq && DESCARTE_FEITO.compareAndSet(false, true)) {
                        descarta = true;
                    }
                } catch (IllegalArgumentException e) {
                    // mensagem mal formada: segue para onReg, que a rejeita
                }
            }
            if (!descarta && perda > 0 && RND.nextDouble() < perda) {
                descarta = true;
            }
            if (descarta) {
                System.out.println("DESCARTADO (simulação) p: " + msg);
                return true;
            }
        }
        p.onReg(msg);
        return false;
    }

    /**
     * Arranca a réplica.
     *
     * @param args {@code <id> <k1,k2> [portoApi]}
     * @throws Exception se o Kubo não estiver acessível
     */
    // Cria o estado das partições atribuídas à réplica e arranca uma thread de subscrição para cada uma.
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: PartitionReplica <id> <k1,k2> [portoApi]  [-Dperda=p] [-Ddescartar=seq]");
            return;
        }
        int id = Integer.parseInt(args[0]);
        String api = "http://127.0.0.1:" + (args.length > 2 ? args[2] : "5001") + "/api/v0";
        double perda = Double.parseDouble(System.getProperty("perda", "0"));
        long descartar = Long.parseLong(System.getProperty("descartar", "-1"));
        IpfsPubSub ipfs = new IpfsPubSub(api);
        List<Thread> threads = new ArrayList<>();
        for (String ks : args[1].split(",")) {
            int k = Integer.parseInt(ks.trim());
            Particao p = new Particao(k, new RecordFile("replica-" + id + "-p" + k + ".txt"), new TcpRecuperador());
            System.out.println("Réplica " + id + " p" + k + " pronta (ultimo=" + p.getUltimo() + ")");
            Thread t = new Thread(() -> {
                // subscribe bloqueia enquanto o Kubo mantiver a resposta aberta; se a ligação cair
                // (Kubo reiniciado), volta a subscrever. O que for publicado entretanto perde-se e
                // será recuperado por TCP quando houver mais de LIMITE_RETIDOS retidos.
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
