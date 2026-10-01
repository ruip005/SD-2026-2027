import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDPServer {
    private static final List<String> mensagensEntregues = new ArrayList<>();
    private static final Map<Integer, String> mensagensTemporarias = new HashMap<>();

    /**
     * @return número da última mensagem entregue em ordem
     */
    public static int processDeliveredMessages(
            int nLastMessageInOrder,
            int nCurrentMessage,
            String currentMessage) {

        if (nCurrentMessage <= nLastMessageInOrder) {
            return nLastMessageInOrder;
        }

        if (nCurrentMessage == nLastMessageInOrder + 1) {
            mensagensEntregues.add(currentMessage);
            int ultimo = nCurrentMessage;

            while (mensagensTemporarias.containsKey(ultimo + 1)) {
                ultimo++;
                mensagensEntregues.add(mensagensTemporarias.remove(ultimo));
            }

            return ultimo;
        }

        mensagensTemporarias.put(nCurrentMessage, currentMessage);
        return nLastMessageInOrder;
    }

    public static void main(String args[]) {
        DatagramSocket aSocket = null;

        int L = 0;

        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];

            System.out.println("Servidor UDP a escutar no porto 6789.");
            System.out.println("Estado inicial: L = " + L);

            while (true) {
                DatagramPacket request =
                        new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);

                String texto = new String(request.getData(),
                        0,
                        request.getLength());

                int N = -1;
                int virgula = texto.indexOf(',');
                if (virgula > 0) {
                    try {
                        N = Integer.parseInt(texto.substring(0, virgula).trim());
                    } catch (NumberFormatException e) {
                        N = -1;
                    }
                }

                int LAnterior = L;
                int totalAnterior = mensagensEntregues.size();

                if (N > 0 && virgula < texto.length() - 1) {
                    L = processDeliveredMessages(L, N, texto);
                }

                String resposta;
                if (L > LAnterior) {
                    resposta = texto;
                } else {
                    resposta = "waitingfor," + (L + 1);
                }

                byte[] r = resposta.getBytes();
                DatagramPacket reply = new DatagramPacket(
                        r, r.length, request.getAddress(), request.getPort());
                aSocket.send(reply);

                System.out.println("recebido: \"" + texto + "\""
                        + " -> enviado: \"" + resposta + "\"");
                System.out.println("L = " + L);
                System.out.println("temporarias = " + mensagensTemporarias);
                System.out.println("entregues neste passo = "
                        + mensagensEntregues.subList(
                        totalAnterior, mensagensEntregues.size()));
                System.out.println("lista de rececao = " + mensagensEntregues);
            }

        } catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (aSocket != null) aSocket.close();
        }
    }
}