package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * {@link Recuperador} sobre TCP: abre uma ligação ao líder, envia {@code PEDIDO <de> <até>} e lê linhas até
 * {@code FIM}. A ligação e cada leitura têm o limite {@link Wire#T_TCP_MS}.
 */
public class TcpRecuperador implements Recuperador {
    private final int timeoutMs;

    public TcpRecuperador() {
        this(Wire.T_TCP_MS);
    }

    public TcpRecuperador(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void pedir(String host, int porto, long de, long ate, Consumer<String> linha) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, porto), timeoutMs);
            s.setSoTimeout(timeoutMs);
            PrintWriter out = new PrintWriter(s.getOutputStream(), true, StandardCharsets.UTF_8);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            out.println("PEDIDO " + de + " " + ate);
            String l;
            while ((l = in.readLine()) != null) {
                if (l.equals("FIM")) {
                    return;
                }
                if (l.startsWith("ERRO")) {
                    throw new IOException("líder respondeu: " + l);
                }
                linha.accept(l);
            }
            throw new IOException("ligação terminada antes de FIM");
        }
    }
}
