package raft04;

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
 * Ponto de entrada das leituras. Lê comandos da consola e publica mensagens {@code IN} no tópico de entrada
 * da partição do sensor.
 */
public class Producer {
    private final Partitioner partitioner;
    private final Publisher publisher;
    private final List<HeartbeatMonitor> monitores = new ArrayList<>();
    private final Consumer<String> log;

    public Producer(Partitioner partitioner, Publisher publisher, Consumer<String> log) {
        this.partitioner = partitioner;
        this.publisher = publisher;
        this.log = log;
        for (int k = 0; k < partitioner.count(); k++) {
            monitores.add(new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log));
        }
    }

    public List<HeartbeatMonitor> getMonitores() {
        return monitores;
    }

    // Renova o detetor de k com termo, líder e hb do cabeçalho e assinala RECEBIDO; as linhas de registo não são lidas.
    public void onLote(int k, String msg) {
        Batch b;
        int count;
        try {
            b = Batch.decodeHeader(msg);
            count = Integer.parseInt(msg.strip().split("\\r?\\n", 2)[0].split(";")[8]);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO no produtor: " + e.getMessage());
            return;
        }
        monitores.get(k).onHeartbeat(b.getTerm(), b.getLeader(), b.getHb());
        // Só o cabeçalho é interpretado: o intervalo de seq não é conhecido pelo produtor.
        log.accept("RECEBIDO;p" + k + ";termo=" + b.getTerm() + ";lider=n" + b.getLeader() + ";hb=" + b.getHb()
                + ";ultimoSeq=" + b.getLastSeq() + ";ultimoTermo=" + b.getLastTerm() + ";n=" + count
                + ";seq=" + (count == 0 ? "-" : "?"));
    }

    // Valida a leitura, calcula o destino, avisa se o líder for suspeito e envia IN com L=0 e vetor zero.
    public int publicar(String sensor, double temp) throws Exception {
        validar(sensor, temp);
        int k = partitioner.of(sensor);
        if (monitores.get(k).state() == HeartbeatMonitor.State.SUSPEITO) {
            // Só se assinala: o produtor não retém nem reenvia leituras.
            System.out.println("AVISO: partição p" + k + " sem líder conhecido; leitura " + sensor + " pode perder-se");
        }
        publisher.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0, new VectorClock(partitioner.count())));
        registarLeitura(k, sensor, temp);
        return k;
    }

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

    private static final Path READINGS_FILE = Path.of("produtor.txt");

    // Uma linha por IN enviado: instante;p<k>;sensor;temperatura (usada pelo RaftCheck para contar leituras perdidas).
    static synchronized void registarLeitura(int k, String sensor, double temp) {
        try {
            Files.writeString(READINGS_FILE, System.currentTimeMillis() + ";p" + k + ";" + sensor + ";" + temp
                    + System.lineSeparator(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("ERRO a escrever " + READINGS_FILE + ": " + e.getMessage());
        }
    }

    public static void validar(String sensor, double temp) {
        if (sensor == null || sensor.isBlank() || sensor.contains(";") || !sensor.equals(sensor.trim())
                || sensor.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("sensor inválido: '" + sensor + "'");
        }
        if (!Double.isFinite(temp)) {
            throw new IllegalArgumentException("temperatura inválida: " + temp);
        }
    }

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
            t.setDaemon(true);
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
            } catch (IOException e) {
                // Kubo desligado, sem PubSub ou a responder com erro: avisa e continua a aceitar comandos
                System.out.println("PUBLICAÇÃO FALHOU (o Kubo está a correr com PubSub ativo?): " + e.getMessage());
            }
        }
    }
}
