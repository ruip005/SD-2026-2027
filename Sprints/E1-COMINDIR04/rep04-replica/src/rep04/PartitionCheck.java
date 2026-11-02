package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * Ferramenta de verificação a posteriori: lê os ficheiros {@code lider-p*.txt} e {@code replica-*-p*.txt} de
 * uma pasta e verifica se o sistema ficou correto. Não comunica com ninguém; opera apenas sobre ficheiros.
 * Executar no fim de cada ensaio.
 */
public class PartitionCheck {
    public static final int REPLICAS_ESPERADAS = 2;

    public static class Resultado {
        public final List<String> linhas = new ArrayList<>();
        public boolean ok = true;

        public Resultado() {
        }

        void falha(String msg) {
            ok = false;
            linhas.add("  FALHA: " + msg);
        }
    }

    private PartitionCheck() {
    }

    private static List<String> linhasDe(Path f) throws IOException {
        List<String> out = new ArrayList<>();
        for (String l : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            if (!l.isBlank()) {
                out.add(l.trim());
            }
        }
        return out;
    }

    // Verifica partição e seq dos registos, quantidade de réplicas e se cada ficheiro é igual ou prefixo do líder.
    public static Resultado verificar(int n, Path pasta) throws IOException {
        Resultado res = new Resultado();
        Partitioner part = new Partitioner(n);
        long somaParticoes = 0;
        for (int k = 0; k < n; k++) {
            res.linhas.add("Partição " + k);
            Path lf = pasta.resolve("lider-p" + k + ".txt");
            List<String> lider = Files.exists(lf) ? linhasDe(lf) : new ArrayList<>();
            Set<String> sensores = new HashSet<>();
            long esperado = 1;
            for (String l : lider) {
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(l);
                } catch (IllegalArgumentException e) {
                    // um ficheiro corrompido deve dar FALHA no relatório, não rebentar o verificador
                    res.falha("linha mal formada no líder: " + l);
                    continue;
                }
                sensores.add(r.getSensorId());
                if (part.of(r.getSensorId()) != k) {
                    res.falha("sensor " + r.getSensorId() + " (seq " + r.getSeq() + ") não pertence à partição " + k);
                }
                if (r.getSeq() != esperado) {
                    res.falha("sequência quebrada: esperado " + esperado + ", lido " + r.getSeq());
                    esperado = r.getSeq();
                }
                esperado++;
            }
            res.linhas.add("  líder: " + lider.size() + " registos, " + sensores.size() + " sensores distintos");
            somaParticoes += lider.size();

            TreeMap<String, List<String>> reps = new TreeMap<>();
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta, "replica-*-p" + k + ".txt")) {
                for (Path p : ds) {
                    reps.put(p.getFileName().toString(), linhasDe(p));
                }
            }
            if (reps.size() != REPLICAS_ESPERADAS) {
                res.falha("réplicas: esperadas " + REPLICAS_ESPERADAS + ", encontradas " + reps.size());
            }
            for (var e : reps.entrySet()) {
                List<String> rep = e.getValue();
                boolean prefixo = rep.size() <= lider.size() && lider.subList(0, rep.size()).equals(rep);
                if (!prefixo) {
                    res.falha(e.getKey() + " diverge do líder");
                } else if (rep.size() == lider.size()) {
                    res.linhas.add("  " + e.getKey() + ": igual ao líder");
                } else {
                    res.linhas.add("  " + e.getKey() + ": prefixo do líder, faltam " + (lider.size() - rep.size()) + " registos");
                }
            }
        }
        long total = 0;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta, "lider-p*.txt")) {
            for (Path p : ds) {
                total += linhasDe(p).size();
            }
        }
        res.linhas.add("Total do sistema: " + total + " registos; soma das partições 0.." + (n - 1) + ": " + somaParticoes);
        if (total != somaParticoes) {
            res.falha("total do sistema não coincide com a soma das partições (há ficheiros de líder fora de 0.." + (n - 1) + ")");
        }
        res.linhas.add(res.ok ? "RESULTADO: OK" : "RESULTADO: FALHOU");
        return res;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Uso: PartitionCheck <N> <pasta>");
            return;
        }
        Resultado r = verificar(Integer.parseInt(args[0]), Paths.get(args[1]));
        r.linhas.forEach(System.out::println);
        System.exit(r.ok ? 0 : 1);
    }
}
