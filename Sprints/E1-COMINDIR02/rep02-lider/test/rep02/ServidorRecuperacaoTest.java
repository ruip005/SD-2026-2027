package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Testes do servidor de recuperação do líder com ligações TCP reais em localhost.
 *
 * <p>Mostram o comportamento de {@link PartitionLeader#servir(ServerSocket)}: as ligações são
 * atendidas <strong>uma de cada vez</strong>, e uma réplica que liga e não envia nada só prende o
 * servidor até {@link Wire#T_TCP_MS} ms. O ficheiro do líder é preparado à mão, por isso só
 * {@code processarPedido} (núcleo) é exercitado.</p>
 */
public class ServidorRecuperacaoTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private ServerSocket ss;
    private Thread srv;

    @Before
    public void arranque() throws Exception {
        RecordFile file = new RecordFile(new File(tmp.getRoot(), "lider-p0.txt").getPath());
        for (int s = 1; s <= 5; s++) {
            file.append(new SensorRecord(s, "S01", 20.0 + s, "2026-10-20T10:00:0" + s));
        }
        // N = 1: todas as leituras pertencem à partição 0 (aqui só interessa o servidor)
        PartitionLeader leader = new PartitionLeader(0, new Partitioner(1), file, "127.0.0.1", 0,
                (t, m) -> { });
        ss = new ServerSocket(0);
        srv = new Thread(() -> {
            try {
                leader.servir(ss);
            } catch (Exception ignored) {
                // terminou porque o teste fechou o ServerSocket
            }
        });
        srv.setDaemon(true);
        srv.start();
    }

    @After
    public void fim() throws Exception {
        ss.close();
        srv.join(2000);
    }

    /** Lê uma linha e falha com uma mensagem clara se o servidor fechou sem responder. */
    private static String linha(BufferedReader in) throws Exception {
        String l = in.readLine();
        org.junit.Assert.assertNotNull("o servidor fechou sem responder (processarPedido já está implementado?)", l);
        return l;
    }

    private Socket liga() throws Exception {
        Socket c = new Socket("127.0.0.1", ss.getLocalPort());
        c.setSoTimeout(Wire.T_TCP_MS + 4000);
        return c;
    }

    @Test
    public void variosPedidosNaMesmaLigacaoSaoTodosRespondidos() throws Exception {
        try (Socket c = liga()) {
            PrintWriter out = new PrintWriter(c.getOutputStream(), true, StandardCharsets.UTF_8);
            BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8));
            out.println("PEDIDO 1 1");
            assertEquals(1, SensorRecord.fromLine(linha(in)).getSeq());
            assertEquals("FIM", linha(in));
            out.println("PEDIDO 4 9");
            assertEquals(4, SensorRecord.fromLine(linha(in)).getSeq());
            assertEquals(5, SensorRecord.fromLine(linha(in)).getSeq());
            assertEquals("FIM", linha(in));
            out.println("lixo");
            assertTrue(linha(in).startsWith("ERRO"));
        }
    }

    /** Uma ligação que fecha sem pedir nada não pode derrubar o servidor. */
    @Test
    public void ligacaoQueFechaSemPedirNaoDerrubaOServidor() throws Exception {
        liga().close();
        try (Socket c = liga()) {
            PrintWriter out = new PrintWriter(c.getOutputStream(), true, StandardCharsets.UTF_8);
            BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8));
            out.println("PEDIDO 2 2");
            assertEquals(2, SensorRecord.fromLine(linha(in)).getSeq());
            assertEquals("FIM", linha(in));
        }
    }

    /**
     * Servidor sequencial: uma réplica "muda" à frente atrasa a seguinte, mas no máximo
     * {@link Wire#T_TCP_MS} ms (o {@code readLine} do líder tem tempo limite). Sem esse limite o
     * servidor ficava preso para sempre. Demora cerca de 5 s.
     */
    @Test
    public void clienteMudoSoAtrasaOsSeguintesAteAoTempoLimite() throws Exception {
        try (Socket mudo = liga(); Socket c = liga()) {
            long t0 = System.currentTimeMillis();
            PrintWriter out = new PrintWriter(c.getOutputStream(), true, StandardCharsets.UTF_8);
            BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8));
            out.println("PEDIDO 3 3");
            assertEquals(3, SensorRecord.fromLine(linha(in)).getSeq());
            assertEquals("FIM", linha(in));
            long espera = System.currentTimeMillis() - t0;
            assertTrue("devia esperar pelo tempo limite do mudo (" + espera + " ms)", espera >= Wire.T_TCP_MS - 1000);
            assertTrue("esperou demais (" + espera + " ms)", espera <= Wire.T_TCP_MS + 3000);
        }
    }
}
