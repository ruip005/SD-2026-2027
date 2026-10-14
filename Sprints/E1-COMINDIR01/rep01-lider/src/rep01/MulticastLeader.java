package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.Random;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner sc = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1);                                // o datagrama não sai da rede local
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();                              // continua a numeração após reinício
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'sair' para terminar");
            Random random = new Random();

            /*// TESTE TEMPORÁRIO: enviar os registos pela ordem 2, 3, 1 tenho que comentar o while(true)
            SensorRecord r1 = SensorRecord.now(1, "sensor1", 20.0);
            SensorRecord r2 = SensorRecord.now(2, "sensor2", 21.0);
            SensorRecord r3 = SensorRecord.now(3, "sensor3", 22.0);

            // Guardar no ficheiro do líder pela ordem correta
            file.append(r1);
            file.append(r2);
            file.append(r3);

            // Enviar pela ordem 2, 3, 1
            SensorRecord[] ordemEnvio = {r2, r3, r1};

            for (SensorRecord r : ordemEnvio) {
                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(m, m.length, group, PORT));
                System.out.println("Enviado: " + r.toLine());
            }*/

            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;                       // bloqueia à espera do utilizador
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                if (line.startsWith("rajada ")) {
                    String[] cmd = line.split("\\s+");
                        if (cmd.length != 2) {
                            System.out.println("Formato: rajada <n>");
                            continue;
                        }
                        int n;
                        try {
                            n = Integer.parseInt(cmd[1]);
                            if (n <= 0) {
                                System.out.println("n tem de ser positivo.");
                                continue;
                            }
                        } catch (NumberFormatException e) {
                            System.out.println("Número inválido: " + cmd[1]);
                            continue;
                        }
                        long primeiroEnvio = 0;
                        long ultimoEnvio = 0;

                        for (int i = 0; i < n; i++) {
                            double temp = 15.0 + random.nextDouble() * 15.0;

                            SensorRecord r = SensorRecord.now(++seq, "SIM", temp);
                            file.append(r);

                            byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);

                            long instante = System.currentTimeMillis();

                            socket.send(new DatagramPacket(m, m.length, group, PORT));

                            if (i == 0) {
                                primeiroEnvio = instante;
                            }

                            ultimoEnvio = System.currentTimeMillis();
                        }

                        long duracao = ultimoEnvio - primeiroEnvio;

                        System.out.println("Rajada enviada: " + n + " registos");
                        System.out.println("Primeiro envio: " + primeiroEnvio + " ms");
                        System.out.println("Último envio: " + ultimoEnvio + " ms");
                        System.out.println("Duração da rajada: " + duracao + " ms");
                        continue;
                }

                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }

                SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                file.append(r);                                             // 1) escreve localmente
                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(m, m.length, group, PORT));  // 2) propaga para o grupo
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}