package rep03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * {@link Recuperador} sobre TCP: abre uma ligação ao líder, envia {@code PEDIDO <de> <até>} e lê
 * linhas até {@code FIM}. A ligação e cada leitura têm o limite {@link Wire#T_TCP_MS}.
 *
 * <p><strong>Ações bloqueantes</strong>: {@code connect} (espera pelo líder) e {@code readLine}
 * (espera por cada linha). Ambas têm tempo limite: sem ele, um líder desligado de forma abrupta ou
 * "mudo" deixava a thread da partição presa para sempre e a réplica deixava de receber.</p>
 *
 * <p><strong>O que pode correr mal</strong>: {@code ConnectException} (líder em baixo ou endereço
 * errado, por exemplo {@code 127.0.0.1} anunciado por um líder noutra máquina);
 * {@code SocketTimeoutException} (líder mudo ou rede lenta); ligação cortada antes de {@code FIM}.
 * Em todos os casos lança {@code IOException} e a réplica tenta de novo no registo seguinte; as
 * linhas já entregues não se perdem.</p>
 */
public class TcpRecuperador implements Recuperador {
    /** Tempo máximo de ligação e de cada leitura, em milissegundos. */
    private final int timeoutMs;

    /** Usa o tempo limite da ficha, {@link Wire#T_TCP_MS}. */
    public TcpRecuperador() {
        this(Wire.T_TCP_MS);
    }

    /**
     * Usa um tempo limite à escolha (útil nos testes).
     *
     * @param timeoutMs tempo máximo de ligação e de cada leitura, em milissegundos
     */
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
