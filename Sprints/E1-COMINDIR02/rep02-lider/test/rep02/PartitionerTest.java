package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Testes da função de partição. */
public class PartitionerTest {

    @Test
    public void resultadoEstaSempreEmZeroAteNMenosUm() {
        Partitioner p = new Partitioner(3);
        for (int i = 0; i < 1000; i++) {
            int k = p.of("S" + i);
            assertTrue("k=" + k, k >= 0 && k < 3);
        }
    }

    @Test
    public void edeterministica() {
        assertEquals(new Partitioner(3).of("S07"), new Partitioner(3).of("S07"));
    }

    /** "polygenelubricants".hashCode() é Integer.MIN_VALUE: com o operador % daria negativo. */
    @Test
    public void hashCodeNegativoNaoDaParticaoNegativa() {
        assertEquals(Integer.MIN_VALUE, "polygenelubricants".hashCode());
        assertEquals(Math.floorMod(Integer.MIN_VALUE, 3), new Partitioner(3).of("polygenelubricants"));
        assertTrue(new Partitioner(3).of("polygenelubricants") >= 0);
    }

    @Test
    public void comUmaParticaoTudoVaiParaZero() {
        assertEquals(0, new Partitioner(1).of("qualquer"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void nZeroERejeitado() {
        new Partitioner(0);
    }
}
