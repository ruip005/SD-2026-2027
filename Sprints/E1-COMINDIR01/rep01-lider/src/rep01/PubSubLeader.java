package rep01;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

// & "C:\Users\admin\Downloads\kubo_v0.43.1_windows-amd64\kubo\ipfs.exe" pubsub sub rep01-temperaturas
// echo "teste" | & "C:\Users\admin\Downloads\kubo_v0.43.1_windows-amd64\kubo\ipfs.exe" pubsub pub rep01-temperaturas

public class PubSubLeader {

    private static final String TOPIC = "rep01-temperaturas";
    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;

        try (Scanner sc = new Scanner(System.in)) {
            IpfsPubSub ipfs = new IpfsPubSub(api);
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();
            Random random = new Random();

            System.out.println("Líder PubSub pronto.");
            System.out.println("API IPFS: " + api);
            System.out.println("Tópico: " + TOPIC);
            System.out.println("Formato: <sensor> <temperatura> | 'sair' para terminar");

            while (true) {
                System.out.print("> ");

                if (!sc.hasNextLine()) {
                    break;
                }

                String line = sc.nextLine().trim();

                if (line.equalsIgnoreCase("sair")) {
                    break;
                }

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

                        long instante = System.currentTimeMillis();

                        ipfs.publish(TOPIC, r.toLine());

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
                file.append(r);
                ipfs.publish(TOPIC, r.toLine());

                System.out.println("Registado e publicado: " + r.toLine());
            }

        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Publicação interrompida.");
        }
    }
}
