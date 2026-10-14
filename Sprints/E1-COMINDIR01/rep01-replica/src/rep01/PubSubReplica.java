package rep01;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class PubSubReplica {

    private static final String TOPIC = "rep01-temperaturas";
    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";

        String porto = args.length > 1 ? args[1] : "5001";
        String api = "http://127.0.0.1:" + porto + "/api/v0";

        // Permite também fornecer o URL completo da API como segundo argumento.
        if (args.length > 1 && args[1].startsWith("http")) {
            api = args[1];
        }

        RecordFile file = new RecordFile("replica-" + id + ".txt");

        try {
            IpfsPubSub ipfs = new IpfsPubSub(
                    args.length > 1 ? api : DEFAULT_API
            );

            long last = file.lastSeq();
            long[] metricas = new long[5];
            // [0] recebidos, [1] escritos, [2] retidos,
            // [3] primeira receção, [4] última receção

            Map<Long, SensorRecord> pendingRecords = new TreeMap<>();

            System.out.println("Réplica PubSub " + id + " à escuta.");
            System.out.println("API IPFS: " + api);
            System.out.println("Tópico: " + TOPIC);

            final long[] ultimaSequencia = {last};

            ipfs.subscribe(TOPIC, mensagem -> {
                long agora = System.currentTimeMillis();

                if (metricas[3] == 0) {
                    metricas[3] = agora;
                }
                metricas[4] = agora;

                SensorRecord r;

                try {
                    r = SensorRecord.fromLine(mensagem);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + mensagem);
                    mostrarMetricas(
                            metricas[0], metricas[1], metricas[2],
                            metricas[3], metricas[4]
                    );
                    return;
                }

                metricas[0]++;
                long seq = r.getSeq();

                // 1. Ignorar registos duplicados ou antigos.
                if (seq <= ultimaSequencia[0]
                        || pendingRecords.containsKey(seq)) {
                    System.out.println("DUPLICADO: " + seq);
                    mostrarMetricas(
                            metricas[0], metricas[1], metricas[2],
                            metricas[3], metricas[4]
                    );
                    return;
                }

                // 2. Reter registos que chegam adiantados.
                if (seq > ultimaSequencia[0] + 1) {
                    pendingRecords.put(seq, r);
                    metricas[2]++;

                    System.out.println(
                            "EM ESPERA: " + seq
                                    + " (falta o registo "
                                    + (ultimaSequencia[0] + 1) + ")"
                    );

                    mostrarMetricas(
                            metricas[0], metricas[1], metricas[2],
                            metricas[3], metricas[4]
                    );
                    return;
                }

                // 3. Aplicar o próximo registo esperado.
                try {
                    file.append(r);
                    metricas[1]++;
                    ultimaSequencia[0] = r.getSeq();

                    System.out.println("Aplicado: " + r.toLine());

                    // 4. Aplicar os registos consecutivos retidos.
                    while (true) {
                        long next = ultimaSequencia[0] + 1;
                        SensorRecord pending = pendingRecords.get(next);

                        if (pending == null) {
                            break;
                        }

                        file.append(pending);
                        pendingRecords.remove(next);
                        metricas[1]++;
                        metricas[2]--;
                        ultimaSequencia[0] = pending.getSeq();

                        System.out.println(
                                "Aplicado: " + pending.toLine()
                        );
                    }

                } catch (IOException e) {
                    System.err.println(
                            "Erro ao escrever registo: " + e.getMessage()
                    );
                    return;
                }

                mostrarMetricas(
                        metricas[0], metricas[1], metricas[2],
                        metricas[3], metricas[4]
                );
            });

        } catch (IOException e) {
            System.err.println("Erro de comunicação IPFS: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Subscrição interrompida.");
        }
    }

    static void mostrarMetricas(
            long recebidos,
            long escritos,
            long retidos,
            long primeiraRececao,
            long ultimaRececao) {

        System.out.println("--- Métricas da réplica ---");
        System.out.println("Recebidos: " + recebidos);
        System.out.println("Escritos: " + escritos);
        System.out.println("Retidos: " + retidos);

        if (primeiraRececao != 0) {
            System.out.println(
                    "Primeira receção: " + primeiraRececao + " ms"
            );
            System.out.println(
                    "Última receção: " + ultimaRececao + " ms"
            );
            System.out.println(
                    "Intervalo de receção: "
                            + (ultimaRececao - primeiraRececao) + " ms"
            );
        }
    }
}
