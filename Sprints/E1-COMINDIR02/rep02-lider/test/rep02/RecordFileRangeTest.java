package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Testes de {@link RecordFile}: append, lastSeq e o novo range. */
public class RecordFileRangeTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private RecordFile comRegistos(int n) throws Exception {
        File f = new File(tmp.getRoot(), "f.txt");
        RecordFile rf = new RecordFile(f.getPath());
        for (int i = 1; i <= n; i++) {
            rf.append(new SensorRecord(i, "S01", 20.0 + i, "2026-10-20T10:00:00"));
        }
        return rf;
    }

    @Test
    public void ficheiroInexistenteTemUltimoZeroERangeVazio() throws Exception {
        RecordFile rf = new RecordFile(new File(tmp.getRoot(), "nao-existe.txt").getPath());
        assertEquals(0, rf.lastSeq());
        assertTrue(rf.range(1, 10).isEmpty());
    }

    @Test
    public void lastSeqDevolveOUltimo() throws Exception {
        assertEquals(5, comRegistos(5).lastSeq());
    }

    @Test
    public void rangeEInclusivoNosDoisExtremos() throws Exception {
        List<SensorRecord> r = comRegistos(10).range(3, 6);
        assertEquals(4, r.size());
        assertEquals(3, r.get(0).getSeq());
        assertEquals(6, r.get(3).getSeq());
    }

    @Test
    public void rangeDeUmSoRegisto() throws Exception {
        List<SensorRecord> r = comRegistos(10).range(7, 7);
        assertEquals(1, r.size());
        assertEquals(7, r.get(0).getSeq());
    }

    @Test
    public void rangeAlemDoFimDevolveSoOQueExiste() throws Exception {
        assertEquals(3, comRegistos(5).range(3, 100).size());
    }

    @Test
    public void rangeForaDoFicheiroEVazio() throws Exception {
        assertTrue(comRegistos(5).range(6, 9).isEmpty());
    }

    @Test
    public void rangeInvertidoEVazio() throws Exception {
        assertTrue(comRegistos(5).range(4, 2).isEmpty());
    }
}
