package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Test;

/** Testes do cliente TCP de recuperação contra um servidor falso em localhost. */
public class TcpRecuperadorTest {

    /** Servidor de uma só ligação: lê o pedido e responde com as linhas dadas. */
    private Thread servidor(ServerSocket ss, List<String> pedidoRecebido, String... resposta) {
        Thread t = new Thread(() -> {
            try (Socket s = ss.accept()) {
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true, StandardCharsets.UTF_8);
                pedidoRecebido.add(in.readLine());
                for (String l : resposta) {
                    out.println(l);
                }
            } catch (IOException ignored) {
                // o teste falha pelas asserções do cliente
            }
        });
        t.start();
        return t;
    }

    @Test
    public void enviaPedidoELeAteFim() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            List<String> pedido = new ArrayList<>();
            servidor(ss, pedido, "5;S01;20.0;t", "6;S01;21.0;t", "FIM");
            List<String> linhas = new ArrayList<>();
            new TcpRecuperador(2000).pedir("127.0.0.1", ss.getLocalPort(), 5, 6, linhas::add);
            assertEquals("PEDIDO 5 6", pedido.get(0));
            assertEquals(2, linhas.size());
            assertEquals("5;S01;20.0;t", linhas.get(0));
        }
    }

    @Test
    public void fimSemRegistosEValido() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            servidor(ss, new ArrayList<>(), "FIM");
            List<String> linhas = new ArrayList<>();
            new TcpRecuperador(2000).pedir("127.0.0.1", ss.getLocalPort(), 5, 6, linhas::add);
            assertTrue(linhas.isEmpty());
        }
    }

    @Test
    public void erroDoLiderEIOException() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            servidor(ss, new ArrayList<>(), "ERRO intervalo inválido");
            try {
                new TcpRecuperador(2000).pedir("127.0.0.1", ss.getLocalPort(), 9, 2, l -> { });
                fail("esperava IOException");
            } catch (IOException esperado) {
                assertTrue(esperado.getMessage().contains("ERRO"));
            }
        }
    }

    @Test
    public void ligacaoTerminadaAntesDeFimEIOExceptionMasEntregaOQueChegou() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            servidor(ss, new ArrayList<>(), "5;S01;20.0;t");
            List<String> linhas = new ArrayList<>();
            try {
                new TcpRecuperador(2000).pedir("127.0.0.1", ss.getLocalPort(), 5, 9, linhas::add);
                fail("esperava IOException");
            } catch (IOException esperado) {
                assertEquals(1, linhas.size());
            }
        }
    }

    @Test(expected = IOException.class)
    public void ligacaoRecusadaEIOException() throws Exception {
        int portoLivre;
        try (ServerSocket ss = new ServerSocket(0)) {
            portoLivre = ss.getLocalPort();
        }
        Consumer<String> nada = l -> { };
        new TcpRecuperador(1000).pedir("127.0.0.1", portoLivre, 1, 2, nada);
    }

    @Test
    public void tempoEsgotadoSemRespostaEIOException() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            Thread mudo = new Thread(() -> {
                try {
                    Socket s = ss.accept();
                    Thread.sleep(1500);
                    s.close();
                } catch (Exception ignored) {
                    // servidor que aceita e não responde
                }
            });
            mudo.start();
            long t0 = System.currentTimeMillis();
            try {
                new TcpRecuperador(300).pedir("127.0.0.1", ss.getLocalPort(), 1, 2, l -> { });
                fail("esperava IOException por tempo esgotado");
            } catch (IOException esperado) {
                assertTrue("deve falhar perto do limite, não esperar pelo servidor",
                        System.currentTimeMillis() - t0 < 1400);
            }
        }
    }
}
