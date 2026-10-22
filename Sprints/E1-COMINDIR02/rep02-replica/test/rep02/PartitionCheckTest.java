package rep02;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Testes do verificador a posteriori, com ficheiros de ensaio construídos à mão (N = 2). */
public class PartitionCheckTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();
    private final Partitioner part = new Partitioner(2);

    private String sensorDa(int k) {
        for (int i = 1; i < 1000; i++) {
            String s = String.format("S%02d", i);
            if (part.of(s) == k) {
                return s;
            }
        }
        throw new AssertionError();
    }

    private List<String> linhas(int k, int de, int ate) {
        List<String> out = new ArrayList<>();
        for (int s = de; s <= ate; s++) {
            out.add(new SensorRecord(s, sensorDa(k), 20, "2026-10-20T10:00:00").toLine());
        }
        return out;
    }

    private void escreve(String nome, List<String> ls) throws Exception {
        Files.write(tmp.getRoot().toPath().resolve(nome), ls, StandardCharsets.UTF_8);
    }

    /** Ensaio correto: 2 réplicas por partição, uma igual e outra prefixo. */
    private void ensaioCorreto() throws Exception {
        for (int k = 0; k < 2; k++) {
            escreve("lider-p" + k + ".txt", linhas(k, 1, 5));
            escreve("replica-1-p" + k + ".txt", linhas(k, 1, 5));
            escreve("replica-2-p" + k + ".txt", linhas(k, 1, 3));
        }
    }

    private PartitionCheck.Resultado verificar() throws Exception {
        return PartitionCheck.verificar(2, tmp.getRoot().toPath());
    }

    @Test
    public void ensaioCorretoPassaEIndicaOsRegistosEmFalta() throws Exception {
        ensaioCorreto();
        PartitionCheck.Resultado r = verificar();
        assertTrue(String.join("\n", r.linhas), r.ok);
        assertTrue(r.linhas.stream().anyMatch(l -> l.contains("igual ao líder")));
        assertTrue(r.linhas.stream().anyMatch(l -> l.contains("faltam 2")));
        assertTrue(r.linhas.stream().anyMatch(l -> l.contains("5 registos")));
    }

    @Test
    public void sensorNaParticaoErradaFalha() throws Exception {
        ensaioCorreto();
        List<String> l = linhas(0, 1, 4);
        l.add(new SensorRecord(5, sensorDa(1), 20, "t").toLine());
        escreve("lider-p0.txt", l);
        assertFalse(verificar().ok);
    }

    @Test
    public void lacunaNaSequenciaFalha() throws Exception {
        ensaioCorreto();
        List<String> l = linhas(0, 1, 5);
        l.remove(2);
        escreve("lider-p0.txt", l);
        assertFalse(verificar().ok);
    }

    @Test
    public void sequenciaQueNaoComecaEmUmFalha() throws Exception {
        ensaioCorreto();
        escreve("lider-p1.txt", linhas(1, 2, 6));
        assertFalse(verificar().ok);
    }

    @Test
    public void sequenciaRepetidaFalha() throws Exception {
        ensaioCorreto();
        List<String> l = linhas(0, 1, 3);
        l.add(l.get(2));
        escreve("lider-p0.txt", l);
        assertFalse(verificar().ok);
    }

    @Test
    public void numeroDeReplicasDiferenteDeDoisFalha() throws Exception {
        ensaioCorreto();
        Files.delete(tmp.getRoot().toPath().resolve("replica-2-p0.txt"));
        assertFalse(verificar().ok);
        escreve("replica-2-p0.txt", linhas(0, 1, 5));
        escreve("replica-3-p0.txt", linhas(0, 1, 5));
        assertFalse(verificar().ok);
    }

    @Test
    public void replicaQueDivergeDoLiderFalha() throws Exception {
        ensaioCorreto();
        List<String> l = linhas(1, 1, 5);
        l.set(1, new SensorRecord(2, sensorDa(1), 99.9, "t").toLine());
        escreve("replica-1-p1.txt", l);
        assertFalse(verificar().ok);
    }

    @Test
    public void replicaMaiorQueOLiderFalha() throws Exception {
        ensaioCorreto();
        escreve("replica-1-p0.txt", linhas(0, 1, 6));
        assertFalse(verificar().ok);
    }

    @Test
    public void ficheiroDeLiderForaDeZeroAteNMenosUmFazOTotalNaoBater() throws Exception {
        ensaioCorreto();
        escreve("lider-p2.txt", linhas(0, 1, 3));
        PartitionCheck.Resultado r = verificar();
        assertFalse(r.ok);
        assertEquals("RESULTADO: FALHOU", r.linhas.get(r.linhas.size() - 1));
    }
    /** Um ficheiro corrompido tem de dar FALHA no relatório e não rebentar o verificador. */
    @Test
    public void linhaMalFormadaNoLiderFalhaSemRebentar() throws Exception {
        ensaioCorreto();
        List<String> l = linhas(0, 1, 5);
        l.add(2, "isto-nao-e-um-registo");
        escreve("lider-p0.txt", l);
        PartitionCheck.Resultado r = verificar();
        assertFalse(r.ok);
        assertTrue(r.linhas.stream().anyMatch(x -> x.contains("mal formada")));
    }
}
