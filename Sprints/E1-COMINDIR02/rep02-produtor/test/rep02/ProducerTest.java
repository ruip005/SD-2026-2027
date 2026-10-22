package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;

/** Testes do produtor com um publicador em memória (sem IPFS). */
public class ProducerTest {
    private final List<String[]> pub = new ArrayList<>();
    private final Producer p = new Producer(new Partitioner(3), (t, m) -> pub.add(new String[]{t, m}));

    @Test
    public void publicaNoTopicoDeEntradaDaParticaoDoSensor() throws Exception {
        int k = p.publicar("S07", 21.4);
        assertEquals(new Partitioner(3).of("S07"), k);
        assertEquals("rep02-in-p" + k, pub.get(0)[0]);
        assertEquals("IN;S07;21.4", pub.get(0)[1]);
    }

    @Test
    public void mesmoSensorVaiSempreParaAMesmaParticao() throws Exception {
        int a = p.publicar("S03", 20);
        int b = p.publicar("S03", 25);
        assertEquals(a, b);
    }

    @Test
    public void rajadaPublicaNLeiturasEContaPorParticao() throws Exception {
        int[] c = p.rajada(300, 20, false, new Random(1));
        assertEquals(300, pub.size());
        assertEquals(300, c[0] + c[1] + c[2]);
    }

    @Test
    public void rajadaUsaTemperaturasEntre15e30() throws Exception {
        p.rajada(200, 5, false, new Random(2));
        for (String[] m : pub) {
            double t = Double.parseDouble(Wire.body(m[1]).split(";")[1]);
            assertTrue("t=" + t, t >= 15 && t <= 30);
        }
    }

    @Test
    public void rajadaQuenteConcentraAbout80PorCentoEmS01() throws Exception {
        p.rajada(1000, 20, true, new Random(3));
        long s01 = pub.stream().filter(m -> m[1].startsWith("IN;S01;")).count();
        assertTrue("S01=" + s01, s01 >= 750 && s01 <= 880);
    }

    @Test
    public void rajadaCarregaUmaParticaoDesproporcionada() throws Exception {
        int[] c = p.rajada(1000, 20, true, new Random(4));
        int kQuente = new Partitioner(3).of("S01");
        assertTrue(c[kQuente] > 750);
    }
    /** Um ';' no nome partiria o formato IN;sensor;temp e as linhas dos ficheiros. */
    @Test
    public void sensorComPontoEVirgulaERejeitadoENadaEPublicado() throws Exception {
        try {
            p.publicar("S0;1", 20);
            org.junit.Assert.fail("devia rejeitar");
        } catch (IllegalArgumentException e) {
            assertTrue(pub.isEmpty());
        }
    }

    @Test
    public void sensorVazioOuComEspacosERejeitado() {
        for (String s : new String[]{"", "  ", "S 01", " S01", null}) {
            try {
                Producer.validar(s, 20);
                org.junit.Assert.fail("devia rejeitar '" + s + "'");
            } catch (IllegalArgumentException e) {
                // esperado
            }
        }
    }

    @Test
    public void temperaturaNaoFinitaERejeitada() {
        for (double d : new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            try {
                Producer.validar("S01", d);
                org.junit.Assert.fail("devia rejeitar " + d);
            } catch (IllegalArgumentException e) {
                // esperado
            }
        }
    }

    @Test
    public void temperaturasNegativasSaoValidas() throws Exception {
        p.publicar("S01", -5.5);
        assertEquals("IN;S01;-5.5", pub.get(0)[1]);
    }
}
