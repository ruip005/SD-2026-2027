import java.net.*;
import java.io.*;

public class UDPServer {

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

                String resposta;
                if (N == L + 1) {
                    resposta = texto;
                    L = N;
                } else {
                    resposta = "waitingfor," + (L + 1);
                }

                byte[] r = resposta.getBytes();
                DatagramPacket reply = new DatagramPacket(
                        r, r.length, request.getAddress(), request.getPort());
                aSocket.send(reply);

                System.out.println("recebido: \"" + texto + "\""
                        + "  ->  enviado: \"" + resposta + "\""
                        + "   (L = " + L + ")");
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