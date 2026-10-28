package rep03;

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
 * Ferramenta de verificação a posteriori: lê os ficheiros {@code lider-p*.txt} e
 * {@code replica-*-p*.txt} de uma pasta e verifica se o sistema ficou correto. Não comunica com
 * ninguém; opera apenas sobre ficheiros. Executar no fim de cada ensaio.
 *
 * <p>Invocação: {@code PartitionCheck <N> <pasta>}.</p>
 *
 * <p>Para cada partição verifica: (1) todos os registos do líder satisfazem {@code of(sensor) == k};
 * (2) a sequência começa em 1, sem lacunas nem repetições; (3) existem exatamente
 * {@value #REPLICAS_ESPERADAS} réplicas, cada uma igual ao líder ou um prefixo dele (indicando os
 * registos em falta); (4) número de registos e de sensores distintos. No fim, o total do sistema
 * (todos os ficheiros {@code lider-p*.txt} da pasta) tem de coincidir com a soma das partições
 * {@code 0..N-1}.</p>
 */
public class PartitionCheck {
    /** Número de réplicas que cada partição deve ter (fator de replicação R). */
    public static final int REPLICAS_ESPERADAS = 2;

    /** Resultado global de uma verificação. */
    public static class Resultado {
        /** Linhas do relatório, prontas a imprimir. */
        public final List<String> linhas = new ArrayList<>();
        /** Verdadeiro se nenhuma verificação falhou. */
        public boolean ok = true;

        /** Cria um resultado vazio e conforme. */
        // Cria o relatório inicialmente sem falhas e com uma lista vazia de mensagens.
        public Resultado() {
        }

        /**
         * Regista uma falha e marca o resultado como não conforme.
         *
         * @param msg descrição da falha
         */
        // Acrescenta o diagnóstico ao relatório e marca a verificação como falhada.
        void falha(String msg) {
            ok = false;
            linhas.add("  FALHA: " + msg);
        }
    }

    /** Classe utilitária: não se instancia. */
    // Impede criar instâncias desta ferramenta, que é utilizada através de métodos estáticos.
    private PartitionCheck() {
    }

    /**
     * Lê as linhas não vazias de um ficheiro.
     *
     * @param f ficheiro a ler
     * @return as linhas, sem as vazias
     * @throws IOException se o ficheiro não puder ser lido
     */
    // Lê um ficheiro UTF-8, retirando linhas vazias e espaços exteriores.
    private static List<String> linhasDe(Path f) throws IOException {
        List<String> out = new ArrayList<>();
        for (String l : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            if (!l.isBlank()) {
                out.add(l.trim());
            }
        }
        return out;
    }

    /**
     * Verifica todas as partições.
     *
     * @param n     número de partições
     * @param pasta pasta com os ficheiros de um ensaio
     * @return o relatório e o veredicto global
     * @throws IOException se algum ficheiro não puder ser lido
     */
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

    /**
     * Imprime o relatório.
     *
     * @param args {@code <N> <pasta>}
     * @throws IOException se algum ficheiro não puder ser lido
     */
    // Recebe N e a pasta de ensaio, imprime o relatório e termina com código 1 se houver falhas.
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
