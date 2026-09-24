import java.net.*;
import java.io.*;

public class UDPClient {

    private static final String PALAVRA_SAIDA = "sair";
    private static final String PALAVRA_MODO  = "modo";

    public static void main(String args[]) {
        DatagramSocket aSocket = null;

        try {
            aSocket = new DatagramSocket();
            aSocket.setSoTimeout(3000);

            InetAddress aHost = InetAddress.getByName("localhost");
            int serverPort = 6789;

            BufferedReader teclado =
                    new BufferedReader(new InputStreamReader(System.in));

            byte[] buffer = new byte[1000];

            int proximoN = 1;
            boolean automatico = escolherModo(teclado);

            System.out.println("Escreva \"" + PALAVRA_SAIDA + "\" para terminar"
                    + " ou \"" + PALAVRA_MODO + "\" para trocar de modo.");

            while (true) {
                System.out.print(automatico ? "[auto] > " : "[manual] > ");
                String linha = teclado.readLine();

                if (linha == null || linha.equalsIgnoreCase(PALAVRA_SAIDA)) {
                    System.out.println("A terminar o cliente.");
                    break;
                }
                if (linha.equalsIgnoreCase(PALAVRA_MODO)) {
                    automatico = escolherModo(teclado);
                    continue;
                }
                if (linha.isEmpty()) {
                    continue;
                }

                int N;
                if (automatico) {
                    N = proximoN;
                } else {
                    N = lerNumero(teclado);
                    if (N == Integer.MIN_VALUE) {
                        System.out.println("Numero invalido, mensagem nao enviada.");
                        continue;
                    }
                }

                String mensagem = N + "," + linha;

                byte[] m = mensagem.getBytes();
                DatagramPacket request =
                        new DatagramPacket(m, m.length, aHost, serverPort);
                aSocket.send(request);
                System.out.println("enviado: " + mensagem);

                DatagramPacket reply =
                        new DatagramPacket(buffer, buffer.length);

                try {
                    aSocket.receive(reply);
                    String resposta = new String(reply.getData(),
                            0,
                            reply.getLength());

                    if (resposta.startsWith("waitingfor,")) {
                        String esperada = resposta.substring("waitingfor,".length());
                        System.out.println("  !! FORA DE ORDEM"
                                + " - o servidor esta a espera da mensagem "
                                + esperada);
                    } else {
                        System.out.println("  echo: " + resposta);
                        if (automatico) {
                            proximoN++;
                        }
                    }

                } catch (SocketTimeoutException e) {
                    System.out.println("  sem resposta do servidor (timeout).");
                }
            }

        } catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (aSocket != null) aSocket.close();
        }
    }

    private static boolean escolherModo(BufferedReader teclado)
            throws IOException {
        while (true) {
            System.out.print("Modo de numeracao [A]utomatico / [M]anual: ");
            String op = teclado.readLine();
            if (op == null) return true;
            op = op.trim();
            if (op.equalsIgnoreCase("A")) {
                System.out.println("Modo automatico.");
                return true;
            }
            if (op.equalsIgnoreCase("M")) {
                System.out.println("Modo manual.");
                return false;
            }
            System.out.println("Opcao invalida.");
        }
    }

    private static int lerNumero(BufferedReader teclado) throws IOException {
        System.out.print("  numero de sequencia N: ");
        String s = teclado.readLine();
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return Integer.MIN_VALUE;
        }
    }
}