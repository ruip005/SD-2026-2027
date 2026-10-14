package rep01;

import java.util.TreeMap;
import java.util.Map;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);                                // passa a receber o que é enviado ao grupo
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);
            byte[] datagramBuffer = new byte[1000];                 //recebe os bytes da rede
            Map<Long, SensorRecord> pendingRecords = new TreeMap<>();                  //guarda os registos em orndem
            long last = file.lastSeq();
            long recebidos = 0;
            long escritos = 0;
            long retidos = 0;
            long primeiraRececao = 0;
            long ultimaRececao = 0;
            while (true) {
                DatagramPacket p = new DatagramPacket(datagramBuffer, datagramBuffer.length);
                socket.receive(p);                                  // bloqueia até chegar um datagrama
                long agora = System.currentTimeMillis();
                if (primeiraRececao == 0) {
                    primeiraRececao = agora;
                }
                ultimaRececao = agora;
                String line = new String(
                        p.getData(), 0, p.getLength(), StandardCharsets.UTF_8
                );
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                    recebidos++;
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                    //Mostrar metricas atualizadas
                    mostrarMetricas(
                            recebidos, escritos, retidos, primeiraRececao, ultimaRececao
                    );
                    continue;
                }

                long seq = r.getSeq();

                // 1. Ignorar registos duplicados
                if (seq <= last || pendingRecords.containsKey(seq)) {
                    System.out.println("DUPLICADO: " + seq);
                    //Mostrar metricas atualizadas
                    mostrarMetricas(
                            recebidos, escritos, retidos, primeiraRececao, ultimaRececao
                    );
                    continue;
                }

                // 2. Reter registos que chegam adiantados
                if (seq > last + 1) {
                    pendingRecords.put(seq, r);
                    retidos++;
                    System.out.println(
                            "EM ESPERA: " + seq + " (falta o registo " + (last + 1) + ")"
                    );
                    //Mostrar metricas atualizadas
                    mostrarMetricas(
                            recebidos, escritos, retidos, primeiraRececao, ultimaRececao
                    );
                    continue;
                }

                // O registo recebido é o próximo esperado.
                file.append(r);
                escritos++;
                last = r.getSeq();
                System.out.println("Aplicado: " + r.toLine());

                // Aplicar os registos consecutivos que estejam no buffer.
                while (true) {
                    long next = last + 1;
                    SensorRecord pending = pendingRecords.remove(next);
                    if (pending == null) {
                        break;
                    }
                    file.append(pending);
                    escritos++;
                    retidos--;
                    last = pending.getSeq();
                    System.out.println("Aplicado: " + pending.toLine());
                }
                //Mostrar metricas atualizadas
                mostrarMetricas(
                        recebidos, escritos, retidos, primeiraRececao, ultimaRececao
                );
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
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
            System.out.println("Primeira receção: " + primeiraRececao + " ms");
            System.out.println("Última receção: " + ultimaRececao + " ms");
            System.out.println("Intervalo de receção: " + (ultimaRececao - primeiraRececao) + " ms");
        }
    }
}