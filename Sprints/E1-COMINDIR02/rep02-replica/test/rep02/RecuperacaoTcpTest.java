package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Cenários de falha com <strong>TCP real</strong> entre a réplica ({@link TcpRecuperador}) e um
 * líder falso que fala o mesmo protocolo ({@code PEDIDO <de> <até>} &rarr; linhas &rarr; {@code FIM}).
 *
 * <p>São os cenários que costumam ser perguntados na defesa: "e se o líder for abaixo?", "e se
 * chegar uma réplica nova depois de já haver mensagens trocadas?", "e se o líder não responder?",
 * "as duas réplicas ficam iguais mesmo com perdas diferentes?".</p>
 */
public class RecuperacaoTcpTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /** Líder falso: serve os registos 1..ultimo e regista cada pedido recebido. */
    static class LiderTcpFalso implements AutoCloseable {
        final ServerSocket ss;
        final List<String> pedidos = Collections.synchronizedList(new ArrayList<>());
        volatile long ultimo;
        volatile boolean mudo;
        final Thread t;

        LiderTcpFalso(int porto, long ultimo) throws Exception {
            ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new java.net.InetSocketAddress("127.0.0.1", porto));
            this.ultimo = ultimo;
            t = new Thread(this::servir);
            t.setDaemon(true);
            t.start();
        }

        int porto() {
            return ss.getLocalPort();
        }

        private void servir() {
            while (!ss.isClosed()) {
                try (Socket s = ss.accept()) {
                    s.setSoTimeout(3000);
                    BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                    PrintWriter out = new PrintWriter(s.getOutputStream(), true, StandardCharsets.UTF_8);
                    String l = in.readLine();
                    if (l == null) {
                        continue;
                    }
                    pedidos.add(l);
                    if (mudo) {
                        Thread.sleep(2000); // não responde: a réplica tem de desistir sozinha
                        continue;
                    }
                    String[] p = l.split(" ");
                    long de = Long.parseLong(p[1]);
                    long ate = Math.min(Long.parseLong(p[2]), ultimo);
                    for (long seq = de; seq <= ate; seq++) {
                        out.println(rec(seq).toLine());
                    }
                    out.println("FIM");
                } catch (SocketTimeoutException | InterruptedException ignored) {
                    // próxima ligação
                } catch (Exception e) {
                    if (ss.isClosed()) {
                        return;
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            ss.close();
        }
    }

    static SensorRecord rec(long seq) {
        return new SensorRecord(seq, "S07", 20.0 + (seq % 10), "2026-10-20T10:00:00");
    }

    private final List<AutoCloseable> fechar = new ArrayList<>();

    @After
    public void fim() throws Exception {
        for (AutoCloseable c : fechar) {
            c.close();
        }
    }

    private static int portoLivre() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private PartitionReplica.Particao particao(String nome, int timeoutMs) throws Exception {
        RecordFile f = new RecordFile(new File(tmp.getRoot(), nome).getPath());
        return new PartitionReplica.Particao(2, f, new TcpRecuperador(timeoutMs));
    }

    private static String reg(int porto, long seq) {
        return Wire.reg("127.0.0.1", porto, rec(seq));
    }

    private List<Long> seqsNoFicheiro(String nome) throws Exception {
        List<Long> out = new ArrayList<>();
        for (String l : Files.readAllLines(new File(tmp.getRoot(), nome).toPath(), StandardCharsets.UTF_8)) {
            out.add(SensorRecord.fromLine(l).getSeq());
        }
        return out;
    }

    private static List<Long> de1a(long n) {
        List<Long> out = new ArrayList<>();
        for (long s = 1; s <= n; s++) {
            out.add(s);
        }
        return out;
    }

    /**
     * Líder em baixo: o registo 1 perde-se, chegam 2..12 (11 retidos) e o pedido falha com ligação
     * recusada. Nada se perde: o estado fica igual. O líder volta, chega o 13 e a recuperação, de
     * novo tentada, completa o ficheiro de 1 a 13.
     */
    @Test
    public void liderEmBaixoEDepoisVoltaRecuperaNoRegistoSeguinte() throws Exception {
        int porto = portoLivre(); // ninguém a escutar: equivale ao líder desligado
        PartitionReplica.Particao p = particao("replica-1-p2.txt", 1000);
        for (long s = 2; s <= 12; s++) {
            p.onReg(reg(porto, s));
        }
        assertEquals("nada escrito enquanto falta o 1", 0, p.getUltimo());
        assertEquals("os 11 retidos mantêm-se", 11, p.getRetidos());

        LiderTcpFalso lider = new LiderTcpFalso(porto, 13);
        fechar.add(lider);
        p.onReg(reg(porto, 13));
        assertEquals(13, p.getUltimo());
        assertEquals(0, p.getRetidos());
        assertEquals(de1a(13), seqsNoFicheiro("replica-1-p2.txt"));
        assertEquals(List.of("PEDIDO 1 1"), lider.pedidos);
    }

    /**
     * Réplica nova: o líder e a outra réplica já trocaram 30 registos. A réplica nova arranca com o
     * ficheiro vazio, recebe 31..41 e, à 11.ª retida, obtém numa única ligação todo o histórico.
     */
    @Test
    public void replicaNovaSincronizaTodoOHistoricoNumaSoLigacao() throws Exception {
        LiderTcpFalso lider = new LiderTcpFalso(0, 41);
        fechar.add(lider);
        PartitionReplica.Particao nova = particao("replica-3-p2.txt", 2000);
        for (long s = 31; s <= 41; s++) {
            nova.onReg(reg(lider.porto(), s));
        }
        assertEquals(List.of("PEDIDO 1 30"), lider.pedidos);
        assertEquals(41, nova.getUltimo());
        assertEquals(de1a(41), seqsNoFicheiro("replica-3-p2.txt"));
    }

    /** Líder que aceita a ligação mas não responde: a réplica desiste no tempo limite e não perde estado. */
    @Test
    public void liderMudoEsgotaOTempoLimiteEMantemOEstado() throws Exception {
        LiderTcpFalso lider = new LiderTcpFalso(0, 20);
        lider.mudo = true;
        fechar.add(lider);
        PartitionReplica.Particao p = particao("replica-1-p2.txt", 300);
        long t0 = System.currentTimeMillis();
        for (long s = 2; s <= 12; s++) {
            p.onReg(reg(lider.porto(), s));
        }
        long dur = System.currentTimeMillis() - t0;
        assertTrue("não pode ficar bloqueada (" + dur + " ms)", dur < 1500);
        assertEquals(0, p.getUltimo());
        assertEquals(11, p.getRetidos());
    }

    /**
     * Duas réplicas da mesma partição perdem mensagens diferentes (20 % cada, ao acaso) nos registos
     * 1 a 60. Depois chegam 30 registos sem perdas: cada um que chega com mais de 10 retidos dispara
     * a recuperação do buraco seguinte, por isso todos os buracos são fechados. No fim os dois
     * ficheiros têm de ser iguais ao do líder (a perda na cauda é outro teste, em ReplicaTest).
     */
    @Test
    public void duasReplicasComPerdasDiferentesFicamIguaisAoLider() throws Exception {
        LiderTcpFalso lider = new LiderTcpFalso(0, 90);
        fechar.add(lider);
        PartitionReplica.Particao a = particao("replica-2-p2.txt", 2000);
        PartitionReplica.Particao b = particao("replica-3-p2.txt", 2000);
        Random ra = new Random(1);
        Random rb = new Random(2);
        for (long s = 1; s <= 90; s++) {
            boolean cauda = s > 60;
            if (cauda || ra.nextDouble() >= 0.2) {
                a.onReg(reg(lider.porto(), s));
            }
            if (cauda || rb.nextDouble() >= 0.2) {
                b.onReg(reg(lider.porto(), s));
            }
        }
        assertEquals(de1a(90), seqsNoFicheiro("replica-2-p2.txt"));
        assertEquals(de1a(90), seqsNoFicheiro("replica-3-p2.txt"));
        assertEquals(0, a.getRetidos());
        assertEquals(0, b.getRetidos());
    }

    /** Mesmo REG entregue duas vezes pelo PubSub (reentrega): o ficheiro não repete linhas. */
    @Test
    public void reentregaDoMesmoRegNaoDuplicaOFicheiro() throws Exception {
        PartitionReplica.Particao p = particao("replica-1-p2.txt", 1000);
        int porto = portoLivre();
        p.onReg(reg(porto, 1));
        p.onReg(reg(porto, 1));
        p.onReg(reg(porto, 2));
        p.onReg(reg(porto, 2));
        assertEquals(List.of(1L, 2L), seqsNoFicheiro("replica-1-p2.txt"));
    }
}
