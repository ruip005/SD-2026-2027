package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testes de {@link IpfsPubSub} contra um "Kubo falso" (um servidor HTTP do próprio JDK que imita
 * a API {@code /api/v0/pubsub}). Permitem provar o cliente sem instalar o IPFS.
 */
public class IpfsPubSubTest {
    private HttpServer kubo;
    private final List<String> pedidos = Collections.synchronizedList(new ArrayList<>());
    private volatile String corpoRecebido;
    private volatile int estadoPub = 200;
    private final List<String> linhasSub = new ArrayList<>();

    @Before
    public void arranque() throws Exception {
        kubo = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        kubo.createContext("/api/v0/pubsub/pub", ex -> {
            pedidos.add(ex.getRequestMethod() + " " + ex.getRequestURI());
            corpoRecebido = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            byte[] r = (estadoPub == 200 ? "" : "{\"Message\":\"pubsub not enabled\"}").getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(estadoPub, r.length == 0 ? -1 : r.length);
            if (r.length > 0) {
                ex.getResponseBody().write(r);
            }
            ex.close();
        });
        kubo.createContext("/api/v0/pubsub/sub", ex -> {
            pedidos.add(ex.getRequestMethod() + " " + ex.getRequestURI());
            ex.sendResponseHeaders(200, 0);
            try (OutputStream os = ex.getResponseBody()) {
                for (String l : linhasSub) {
                    os.write((l + "\n").getBytes(StandardCharsets.UTF_8));
                    os.flush();
                }
            }
        });
        kubo.start();
    }

    @After
    public void fim() {
        kubo.stop(0);
    }

    private String api() {
        return "http://127.0.0.1:" + kubo.getAddress().getPort() + "/api/v0";
    }

    /** Uma linha JSON como a que o Kubo envia por cada mensagem de uma subscrição. */
    private static String jsonKubo(String topico, String msg) {
        return "{\"from\":\"12D3KooWTeste\",\"data\":\"" + IpfsPubSub.toMultibase(msg)
                + "\",\"seqno\":\"uAAE\",\"topicIDs\":[\"" + IpfsPubSub.toMultibase(topico) + "\"]}";
    }

    @Test
    public void multibaseIdaEVoltaComAcentosEPontoEVirgula() {
        String s = "REG;127.0.0.1;7002;42;S07;21.4;2026-10-20T10:00:00 · partição ç";
        String m = IpfsPubSub.toMultibase(s);
        assertTrue(m.startsWith("u"));
        assertTrue("base64url sem '+', '/' nem '='", m.matches("u[A-Za-z0-9_-]*"));
        assertEquals(s, IpfsPubSub.fromMultibase(m));
    }

    @Test(expected = IllegalArgumentException.class)
    public void multibaseComOutroPrefixoERejeitado() {
        IpfsPubSub.fromMultibase("mSGVsbG8");
    }

    @Test
    public void publishUsaPostComOTopicoEmMultibaseEOCorpoMultipart() throws Exception {
        new IpfsPubSub(api()).publish("rep02-in-p1", "IN;S07;21.4");
        assertEquals(1, pedidos.size());
        String p = pedidos.get(0);
        assertTrue(p, p.startsWith("POST /api/v0/pubsub/pub?arg=u"));
        String arg = p.substring(p.indexOf("arg=") + 4);
        assertEquals("rep02-in-p1", IpfsPubSub.fromMultibase(arg));
        assertTrue(corpoRecebido.contains("IN;S07;21.4"));
        assertTrue(corpoRecebido.contains("form-data; name=\"file\""));
    }

    @Test
    public void erroDoKuboNaPublicacaoEIOException() throws Exception {
        estadoPub = 500;
        try {
            new IpfsPubSub(api()).publish("rep02-in-p0", "IN;S01;20");
            fail("devia lançar IOException");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("500"));
        }
    }

    @Test
    public void subscribeEntregaAsMensagensDescodificadasEPorOrdem() throws Exception {
        linhasSub.add(jsonKubo("rep02-p2", "REG;h;7002;1;S07;21.4;t"));
        linhasSub.add("{}"); // linha sem "data" (o Kubo pode enviar): é ignorada
        linhasSub.add(jsonKubo("rep02-p2", "REG;h;7002;2;S07;22.0;t"));
        List<String> recebidas = new ArrayList<>();
        // o Kubo falso fecha a resposta no fim, por isso aqui o subscribe termina (o real não termina)
        new IpfsPubSub(api()).subscribe("rep02-p2", recebidas::add);
        assertEquals(List.of("REG;h;7002;1;S07;21.4;t", "REG;h;7002;2;S07;22.0;t"), recebidas);
        assertTrue(pedidos.get(0).startsWith("POST /api/v0/pubsub/sub?arg=" + IpfsPubSub.toMultibase("rep02-p2")));
    }

    @Test
    public void kuboDesligadoEIOException() throws Exception {
        int porto;
        try (ServerSocket s = new ServerSocket(0)) {
            porto = s.getLocalPort();
        }
        try {
            new IpfsPubSub("http://127.0.0.1:" + porto + "/api/v0").publish("t", "m");
            fail("devia lançar IOException");
        } catch (IOException e) {
            // ConnectException: ninguém a escutar no porto (equivale a "ipfs daemon" não arrancado)
        }
    }

    @Test
    public void extractFieldLeOCampoEFalhaSeNaoExistir() {
        assertEquals("abc", IpfsPubSub.extractField("{\"a\":\"x\",\"data\":\"abc\"}", "data"));
        try {
            IpfsPubSub.extractField("{\"a\":\"x\"}", "data");
            fail();
        } catch (IllegalArgumentException e) {
            // esperado
        }
    }
}
