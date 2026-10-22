package rep02;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Testes do formato das mensagens e dos nomes dos tópicos. */
public class WireTest {

    @Test
    public void nomesDosTopicos() {
        assertEquals("rep02-in-p2", Wire.topicIn(2));
        assertEquals("rep02-p0", Wire.topicData(0));
    }

    @Test
    public void constantesDaFicha() {
        assertEquals(10, Wire.LIMITE_RETIDOS);
        assertEquals(5000, Wire.T_TCP_MS);
        assertEquals(7000, Wire.PORTO_BASE);
    }

    @Test
    public void mensagemIn() {
        assertEquals("IN;S07;21.4", Wire.in("S07", 21.4));
    }

    @Test
    public void mensagemRegTemOFormatoDaFicha() {
        SensorRecord r = new SensorRecord(42, "S07", 21.4, "2026-10-20T10:00:00");
        assertEquals("REG;127.0.0.1;7002;42;S07;21.4;2026-10-20T10:00:00", Wire.reg("127.0.0.1", 7002, r));
    }

    @Test
    public void tipoECorpo() {
        assertEquals("IN", Wire.type("IN;S07;21.4"));
        assertEquals("S07;21.4", Wire.body("IN;S07;21.4"));
        assertEquals("SEMPONTOEVIRGULA", Wire.type("SEMPONTOEVIRGULA"));
        assertEquals("", Wire.body("SEMPONTOEVIRGULA"));
    }

    @Test
    public void regPartsDevolveHostPortoELinha() {
        String msg = "REG;10.0.0.5;7001;5;S02;18.0;2026-10-20T10:00:00";
        assertArrayEquals(new String[]{"10.0.0.5", "7001", "5;S02;18.0;2026-10-20T10:00:00"}, Wire.regParts(msg));
    }

    @Test
    public void regPartsEInterpretavelPorFromLine() {
        SensorRecord r = new SensorRecord(9, "S01", 20.5, "2026-10-20T10:00:00");
        String[] p = Wire.regParts(Wire.reg("h", 7000, r));
        assertEquals(r.toLine(), SensorRecord.fromLine(p[2]).toLine());
    }

    @Test(expected = IllegalArgumentException.class)
    public void regMalFormadoLancaExcecao() {
        Wire.regParts("REG;so-um-campo");
    }
}
