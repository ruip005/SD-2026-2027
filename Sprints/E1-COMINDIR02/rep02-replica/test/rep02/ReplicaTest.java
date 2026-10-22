package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Testes da regra de aplicação de uma partição na réplica (secção 7 da arquitetura) e da
 * recuperação. Definem o contrato do núcleo: {@code aplicar}, {@code escreverEmSequencia} e
 * {@code recuperar}.
 */
public class ReplicaTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /** Recuperador de teste: serve registos de um "líder" falso e regista os pedidos recebidos. */
    static class LiderFalso implements Recuperador {
        final List<long[]> pedidos = new ArrayList<>();
        final List<String> hosts = new ArrayList<>();
        long maxSeq = 1000;
        boolean falha = false;
        /** Se >= 0, a ligação cai depois de entregar este número de registos (sem FIM). */
        int cairApos = -1;

        @Override
        public void pedir(String host, int porto, long de, long ate, Consumer<String> linha) throws IOException {
            pedidos.add(new long[]{de, ate});
            hosts.add(host + ":" + porto);
            if (falha) {
                throw new IOException("ligação recusada");
            }
            int enviados = 0;
            for (long s = de; s <= Math.min(ate, maxSeq); s++) {
                if (cairApos >= 0 && enviados == cairApos) {
                    throw new IOException("ligação caiu antes de FIM");
                }
                linha.accept(rec(s).toLine());
                enviados++;
            }
        }
    }

    static SensorRecord rec(long seq) {
        return new SensorRecord(seq, "S01", 20.0, "2026-10-20T10:00:00");
    }

    private final LiderFalso lider = new LiderFalso();
    private RecordFile file;
    private PartitionReplica.Particao p;

    @Before
    public void arranque() throws Exception {
        file = new RecordFile(new File(tmp.getRoot(), "replica-1-p0.txt").getPath());
        p = new PartitionReplica.Particao(0, file, lider);
        p.onReg(Wire.reg("127.0.0.1", 7000, rec(1))); // dá a conhecer o líder; escreve o 1
    }

    private void reg(long seq) {
        p.onReg(Wire.reg("127.0.0.1", 7000, rec(seq)));
    }

    private List<Long> seqsNoFicheiro() throws Exception {
        List<Long> out = new ArrayList<>();
        for (SensorRecord r : file.range(0, Long.MAX_VALUE)) {
            out.add(r.getSeq());
        }
        return out;
    }

    // ---------------- regra de aplicação ----------------

    @Test
    public void registoEmSequenciaEEscrito() throws Exception {
        reg(2);
        reg(3);
        assertEquals(List.of(1L, 2L, 3L), seqsNoFicheiro());
        assertEquals(3, p.getUltimo());
        assertEquals(0, p.getRetidos());
    }

    @Test
    public void duplicadoEIgnorado() throws Exception {
        reg(2);
        reg(2);
        reg(1);
        assertEquals(List.of(1L, 2L), seqsNoFicheiro());
    }

    @Test
    public void registoAdiantadoFicaRetidoEOFicheiroNaoMuda() throws Exception {
        reg(3);
        assertEquals(List.of(1L), seqsNoFicheiro());
        assertEquals(1, p.getUltimo());
        assertEquals(1, p.getRetidos());
    }

    @Test
    public void retidoRepetidoEDuplicado() throws Exception {
        reg(3);
        reg(3);
        assertEquals(1, p.getRetidos());
    }

    /** Fora de ordem: 3 e 4 chegam antes do 2; quando o 2 chega, saem 2, 3, 4 por ordem. */
    @Test
    public void escreveEmCascataQuandoOEmFaltaChega() throws Exception {
        reg(4);
        reg(3);
        reg(2);
        assertEquals(List.of(1L, 2L, 3L, 4L), seqsNoFicheiro());
        assertEquals(4, p.getUltimo());
        assertEquals(0, p.getRetidos());
    }

    @Test
    public void cascataParaNoProximoBuraco() throws Exception {
        reg(3);
        reg(5);
        reg(2);
        assertEquals(List.of(1L, 2L, 3L), seqsNoFicheiro());
        assertEquals(1, p.getRetidos()); // o 5 continua à espera do 4
    }

    @Test
    public void retomaDoUltimoSeqDoFicheiroAposReinicio() throws Exception {
        reg(2);
        PartitionReplica.Particao reiniciada = new PartitionReplica.Particao(0, file, lider);
        assertEquals(2, reiniciada.getUltimo());
        reiniciada.onReg(Wire.reg("127.0.0.1", 7000, rec(3)));
        assertEquals(List.of(1L, 2L, 3L), seqsNoFicheiro());
    }

    @Test
    public void mensagensMalFormadasSaoRejeitadasSemAlterarEstado() throws Exception {
        p.onReg("LIXO");
        p.onReg("REG;so;dois");
        p.onReg("REG;h;porto-invalido;2;S01;20;t");
        p.onReg("IN;S01;20");
        assertEquals(List.of(1L), seqsNoFicheiro());
    }

    // ---------------- recuperação ----------------

    /** Exatamente 10 retidos não chegam: é preciso EXCEDER o limite. */
    @Test
    public void dezRetidosNaoDesencadeiamRecuperacao() throws Exception {
        for (long s = 3; s <= 12; s++) {
            reg(s); // 10 retidos (3..12), falta o 2
        }
        assertEquals(10, p.getRetidos());
        assertTrue(lider.pedidos.isEmpty());
    }

    @Test
    public void onzeRetidosDesencadeiamPedidoDoIntervaloEmFalta() throws Exception {
        for (long s = 3; s <= 13; s++) {
            reg(s); // 11 retidos: 3..13
        }
        assertEquals(1, lider.pedidos.size());
        assertEquals(2, lider.pedidos.get(0)[0]); // ultimo + 1
        assertEquals(2, lider.pedidos.get(0)[1]); // primeiro retido - 1
        assertEquals("127.0.0.1:7000", lider.hosts.get(0));
        assertEquals(13, p.getUltimo());
        assertEquals(0, p.getRetidos());
        assertEquals(13, seqsNoFicheiro().size());
    }

    @Test
    public void intervaloPedidoCobreTodoOBuraco() throws Exception {
        // ultimo = 1; retidos 6..16 (11 registos): falta 2..5
        for (long s = 6; s <= 16; s++) {
            reg(s);
        }
        assertEquals(2, lider.pedidos.get(0)[0]);
        assertEquals(5, lider.pedidos.get(0)[1]);
        assertEquals(16, p.getUltimo());
        List<Long> esperado = new ArrayList<>();
        for (long s = 1; s <= 16; s++) {
            esperado.add(s);
        }
        assertEquals(esperado, seqsNoFicheiro());
    }

    /** O pedido usa o endereço do REG mais recente. */
    @Test
    public void usaOEnderecoDoUltimoReg() throws Exception {
        p.onReg(Wire.reg("10.1.1.1", 7777, rec(3)));
        for (long s = 4; s <= 13; s++) {
            p.onReg(Wire.reg("10.1.1.1", 7777, rec(s)));
        }
        assertEquals("10.1.1.1:7777", lider.hosts.get(0));
    }

    @Test
    public void falhaDeRecuperacaoMantemEstadoETentaDeNovoNoRegistoSeguinte() throws Exception {
        lider.falha = true;
        for (long s = 3; s <= 13; s++) {
            reg(s);
        }
        assertEquals(1, lider.pedidos.size());
        assertEquals(List.of(1L), seqsNoFicheiro());
        assertEquals(11, p.getRetidos());
        lider.falha = false;
        reg(14); // a condição persiste: tenta outra vez
        assertEquals(2, lider.pedidos.size());
        assertEquals(14, p.getUltimo());
        assertEquals(0, p.getRetidos());
    }

    @Test
    public void interrupcaoAntesDeFimMantemOsRegistosJaRecebidos() throws Exception {
        // ultimo = 1; retidos 6..16; falta 2..5; o líder entrega 2 e 3 e cai
        lider.cairApos = 2;
        for (long s = 6; s <= 16; s++) {
            reg(s);
        }
        assertEquals(List.of(1L, 2L, 3L), seqsNoFicheiro());
        assertEquals(11, p.getRetidos());
        lider.cairApos = -1;
        reg(17); // nova tentativa: pede 4..5
        assertEquals(4, lider.pedidos.get(1)[0]);
        assertEquals(5, lider.pedidos.get(1)[1]);
        assertEquals(17, p.getUltimo());
    }

    /** O mesmo registo pode chegar por PubSub e por TCP: só pode ser escrito uma vez. */
    @Test
    public void registoRecebidoPorPubSubEPorTcpNaoEDuplicadoNoFicheiro() throws Exception {
        for (long s = 3; s <= 13; s++) {
            reg(s);
        }
        reg(2); // chega tarde por PubSub, mas já foi recuperado por TCP
        List<Long> seqs = seqsNoFicheiro();
        assertEquals(13, seqs.size());
        assertEquals(13, seqs.stream().distinct().count());
    }

    /** Ficheiro vazio: retém tudo e, à 11.ª retida, pede todo o histórico numa só ligação. */
    @Test
    public void replicaNovaPedeTodoOHistoricoNumaSoLigacao() throws Exception {
        RecordFile vazio = new RecordFile(new File(tmp.getRoot(), "replica-2-p0.txt").getPath());
        PartitionReplica.Particao nova = new PartitionReplica.Particao(0, vazio, lider);
        for (long s = 100; s <= 110; s++) {
            nova.onReg(Wire.reg("127.0.0.1", 7000, rec(s)));
        }
        assertEquals(1, lider.pedidos.size());
        assertEquals(1, lider.pedidos.get(0)[0]);
        assertEquals(99, lider.pedidos.get(0)[1]);
        assertEquals(110, nova.getUltimo());
        assertEquals(110, vazio.range(0, 1000).size());
    }

    /** A perda do último registo de uma série não se recupera: nunca chegam 10 posteriores. */
    @Test
    public void perdaDoUltimoRegistoNaoDesencadeiaRecuperacao() throws Exception {
        for (long s = 2; s <= 5; s++) {
            reg(s);
        }
        // o 6 perde-se; nada mais chega
        assertTrue(lider.pedidos.isEmpty());
        assertEquals(5, p.getUltimo());
    }

    @Test
    public void semEnderecoDeLiderNaoRecupera() throws Exception {
        RecordFile f2 = new RecordFile(new File(tmp.getRoot(), "r2.txt").getPath());
        PartitionReplica.Particao q = new PartitionReplica.Particao(0, f2, lider);
        for (long s = 5; s <= 20; s++) {
            q.aplicar(rec(s)); // aplicar direto: nenhum REG anunciou o líder
        }
        assertTrue(lider.pedidos.isEmpty());
        assertEquals(16, q.getRetidos());
    }
}
