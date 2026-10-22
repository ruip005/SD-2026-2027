package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Testes do líder da partição {@code K = 2} (com {@code N = 3}). Definem o contrato do núcleo:
 * {@code onIn} e {@code processarPedido}.
 */
public class LeaderTest {
    private static final int K = 2;
    private static final int N = 3;
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private final Partitioner part = new Partitioner(N);
    private final List<String[]> publicadas = Collections.synchronizedList(new ArrayList<>());
    private RecordFile file;
    private PartitionLeader leader;

    /** Sensor que pertence à partição pedida. */
    private String sensorDa(int k) {
        for (int i = 1; i < 1000; i++) {
            String s = String.format("S%02d", i);
            if (part.of(s) == k) {
                return s;
            }
        }
        throw new AssertionError();
    }

    @Before
    public void arranque() throws Exception {
        file = new RecordFile(new File(tmp.getRoot(), "lider-p2.txt").getPath());
        leader = new PartitionLeader(K, part, file, "127.0.0.1", 7002,
                (topic, msg) -> publicadas.add(new String[]{topic, msg}));
    }

    // ---------------- onIn ----------------

    @Test
    public void primeiroRegistoTemSeqUmEPublicaReg() throws Exception {
        String s = sensorDa(K);
        assertTrue(leader.onIn(Wire.in(s, 21.4)));
        assertEquals(1, leader.getUltimoSeq());
        assertEquals(1, file.lastSeq());
        assertEquals(1, publicadas.size());
        assertEquals("rep02-p2", publicadas.get(0)[0]);
        String[] p = Wire.regParts(publicadas.get(0)[1]);
        assertEquals("127.0.0.1", p[0]);
        assertEquals("7002", p[1]);
        SensorRecord r = SensorRecord.fromLine(p[2]);
        assertEquals(1, r.getSeq());
        assertEquals(s, r.getSensorId());
        assertEquals(21.4, r.getTemperature(), 0.0);
    }

    @Test
    public void numeraEmSequenciaSemLacunas() throws Exception {
        String s = sensorDa(K);
        for (int i = 0; i < 5; i++) {
            leader.onIn(Wire.in(s, 20 + i));
        }
        assertEquals(5, leader.getUltimoSeq());
        List<SensorRecord> todos = file.range(1, 5);
        for (int i = 0; i < 5; i++) {
            assertEquals(i + 1, todos.get(i).getSeq());
        }
    }

    /** O registo tem de estar no ficheiro ANTES de ser propagado (o ficheiro é a fonte da recuperação). */
    @Test
    public void escreveEmFicheiroAntesDePublicar() throws Exception {
        AtomicReference<Long> ultimoNoFicheiroAoPublicar = new AtomicReference<>();
        PartitionLeader l = new PartitionLeader(K, part, new RecordFile(new File(tmp.getRoot(), "x.txt").getPath()),
                "h", 7002, (topic, msg) -> {
                    long seqPublicado = SensorRecord.fromLine(Wire.regParts(msg)[2]).getSeq();
                    ultimoNoFicheiroAoPublicar.set(seqPublicado);
                    assertEquals("ficheiro tem de ter o registo antes da publicação", seqPublicado,
                            new RecordFile(new File(tmp.getRoot(), "x.txt").getPath()).lastSeq());
                });
        assertTrue(l.onIn(Wire.in(sensorDa(K), 20)));
        assertEquals(Long.valueOf(1), ultimoNoFicheiroAoPublicar.get());
    }

    @Test
    public void sensorDeOutraParticaoERejeitadoENaoConsomeSeq() throws Exception {
        assertFalse(leader.onIn(Wire.in(sensorDa(0), 20)));
        assertEquals(0, leader.getUltimoSeq());
        assertTrue(publicadas.isEmpty());
        assertTrue(leader.onIn(Wire.in(sensorDa(K), 20)));
        assertEquals(1, leader.getUltimoSeq());
    }

    @Test
    public void mensagensMalFormadasSaoRejeitadas() throws Exception {
        String s = sensorDa(K);
        assertFalse(leader.onIn("LIXO"));
        assertFalse(leader.onIn("REG;h;1;1;" + s + ";20;t"));
        assertFalse(leader.onIn("IN;" + s));
        assertFalse(leader.onIn("IN;" + s + ";abc"));
        assertFalse(leader.onIn("IN;" + s + ";20;extra"));
        assertEquals(0, leader.getUltimoSeq());
        assertTrue(publicadas.isEmpty());
    }

    @Test
    public void retomaDoUltimoSeqDoFicheiroAposReinicio() throws Exception {
        String s = sensorDa(K);
        leader.onIn(Wire.in(s, 20));
        leader.onIn(Wire.in(s, 21));
        PartitionLeader reiniciado = new PartitionLeader(K, part, file, "127.0.0.1", 7002, (t, m) -> { });
        assertEquals(2, reiniciado.getUltimoSeq());
        reiniciado.onIn(Wire.in(s, 22));
        assertEquals(3, file.lastSeq());
    }

    @Test
    public void falhaDePublicacaoNaoPerdeORegistoNemOSeq() throws Exception {
        PartitionLeader l = new PartitionLeader(K, part, new RecordFile(new File(tmp.getRoot(), "y.txt").getPath()),
                "h", 7002, (t, m) -> {
                    throw new java.io.IOException("kubo em baixo");
                });
        assertTrue(l.onIn(Wire.in(sensorDa(K), 20)));
        assertEquals(1, l.getUltimoSeq());
    }

    /** Atribuição de seq + append têm de ser uma só secção crítica: sem repetições nem buracos. */
    @Test
    public void concorrenciaNaoRepeteNemSaltaSeq() throws Exception {
        final int threads = 8;
        final int porThread = 50;
        final String s = sensorDa(K);
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread th = new Thread(() -> {
                try {
                    go.await();
                    for (int i = 0; i < porThread; i++) {
                        leader.onIn(Wire.in(s, 20));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            ts.add(th);
            th.start();
        }
        go.countDown();
        for (Thread th : ts) {
            th.join();
        }
        assertEquals(threads * porThread, leader.getUltimoSeq());
        List<SensorRecord> todos = file.range(1, threads * porThread);
        assertEquals(threads * porThread, todos.size());
        for (int i = 0; i < todos.size(); i++) {
            assertEquals("ordem do ficheiro", i + 1, todos.get(i).getSeq());
        }
    }

    // ---------------- pedido de recuperação ----------------

    private void comRegistos(int n) {
        String s = sensorDa(K);
        for (int i = 0; i < n; i++) {
            leader.onIn(Wire.in(s, 20 + i));
        }
    }

    @Test
    public void pedidoValidoDevolveRegistosEFim() {
        comRegistos(10);
        List<String> r = leader.processarPedido("PEDIDO 3 5");
        assertEquals(4, r.size());
        assertEquals(3, SensorRecord.fromLine(r.get(0)).getSeq());
        assertEquals(5, SensorRecord.fromLine(r.get(2)).getSeq());
        assertEquals("FIM", r.get(3));
    }

    @Test
    public void pedidoDeUmSoRegisto() {
        comRegistos(5);
        List<String> r = leader.processarPedido("PEDIDO 4 4");
        assertEquals(2, r.size());
        assertEquals(4, SensorRecord.fromLine(r.get(0)).getSeq());
        assertEquals("FIM", r.get(1));
    }

    /** O extremo superior é limitado ao último seq já escrito. */
    @Test
    public void extremoSuperiorELimitadoAoUltimoSeq() {
        comRegistos(5);
        List<String> r = leader.processarPedido("PEDIDO 4 1000");
        assertEquals(3, r.size());
        assertEquals(5, SensorRecord.fromLine(r.get(1)).getSeq());
        assertEquals("FIM", r.get(2));
    }

    @Test
    public void pedidoTodoAlemDoUltimoDevolveSoFim() {
        comRegistos(3);
        assertEquals(List.of("FIM"), leader.processarPedido("PEDIDO 10 20"));
    }

    @Test
    public void deMaiorQueAteEErro() {
        comRegistos(5);
        List<String> r = leader.processarPedido("PEDIDO 5 2");
        assertEquals(1, r.size());
        assertTrue(r.get(0).startsWith("ERRO"));
    }

    @Test
    public void pedidosMalFormadosSaoErro() {
        comRegistos(5);
        for (String p : new String[]{"", "PEDIDO", "PEDIDO 1", "PEDIDO a b", "PEDIDO 1 2 3", "GET 1 2", "PEDIDO 0 3", "PEDIDO -1 3"}) {
            List<String> r = leader.processarPedido(p);
            assertEquals("pedido: '" + p + "'", 1, r.size());
            assertTrue("pedido: '" + p + "'", r.get(0).startsWith("ERRO"));
        }
    }

    /** As linhas da resposta usam o formato do ficheiro e são interpretáveis por fromLine. */
    @Test
    public void linhasDaRespostaSaoIguaisAsDoFicheiro() throws Exception {
        comRegistos(3);
        List<String> r = leader.processarPedido("PEDIDO 1 3");
        List<SensorRecord> doFicheiro = file.range(1, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(doFicheiro.get(i).toLine(), r.get(i));
        }
    }

    // ---------------- servidor TCP (ligação real em localhost) ----------------

    @Test
    public void atendePedidoPorTcp() throws Exception {
        comRegistos(6);
        try (ServerSocket ss = new ServerSocket(0)) {
            Thread srv = new Thread(() -> {
                try {
                    leader.atender(ss.accept());
                } catch (Exception ignored) {
                    // o teste falha pelas asserções do cliente
                }
            });
            srv.start();
            try (Socket c = new Socket("127.0.0.1", ss.getLocalPort())) {
                c.setSoTimeout(3000);
                PrintWriter out = new PrintWriter(c.getOutputStream(), true, StandardCharsets.UTF_8);
                BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8));
                out.println("PEDIDO 2 3");
                assertEquals(2, SensorRecord.fromLine(in.readLine()).getSeq());
                assertEquals(3, SensorRecord.fromLine(in.readLine()).getSeq());
                assertEquals("FIM", in.readLine());
            }
            srv.join(3000);
        }
    }
}
