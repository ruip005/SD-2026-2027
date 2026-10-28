package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/** Análise a posteriori; não impõe entrega causal nas réplicas. */
public final class CausalCheck {
    private CausalCheck() { }

    private static final class Event {
        final int k;
        final SensorRecord r;
        final VectorClock v;
        Event(int k, SensorRecord r) { this.k = k; this.r = r; this.v = r.getVector(); }
        String id() { return "p" + k + "#" + r.getSeq(); }
    }

    public static final class Result {
        public final List<String> lines = new ArrayList<>();
        public long before, after, concurrent, distinctLamport, violations, invalid;
        public double proportion() { return concurrent == 0 ? 0 : (double) distinctLamport / concurrent; }
        public boolean ok() { return invalid == 0 && violations == 0; }
        private void fail(String message) { invalid++; lines.add("FALHA: " + message); }
    }

    private static void checkLamport(Result result, Event first, Event second) {
        if (first.r.getLamport() >= second.r.getLamport()) {
            result.violations++;
            result.lines.add("VIOLAÇÃO L: " + first.id() + " → " + second.id());
        }
    }

    public static Result verificar(int n, Path folder) throws IOException {
        Partitioner partitioner = new Partitioner(n);
        if (!Files.isDirectory(folder)) throw new IllegalArgumentException("Pasta inexistente: " + folder);
        Result result = new Result();
        List<Event> events = new ArrayList<>();
        // A ausência de um ficheiro não deve produzir um relatório vazio com sucesso.
        for (int k = 0; k < n; k++) {
            Path file = folder.resolve("lider-p" + k + ".txt");
            if (!Files.exists(file)) {
                result.fail("ficheiro em falta: " + file.getFileName());
                continue;
            }
            long previous = 0;
            int lineNumber = 0, count = 0;
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                lineNumber++;
                if (line.isBlank()) continue;
                try {
                    SensorRecord r = SensorRecord.fromLine(line, n);
                    if (r.getSeq() != previous + 1) result.fail(file.getFileName() + ": sequência na linha " + lineNumber);
                    previous = r.getSeq();
                    if (r.getVector().get(k) != r.getSeq()) result.fail("V[" + k + "] != seq em p" + k + "#" + r.getSeq());
                    if (partitioner.of(r.getSensorId()) != k) result.fail("sensor na partição errada: " + r.getSensorId());
                    events.add(new Event(k, r));
                    count++;
                } catch (IllegalArgumentException e) {
                    result.fail(file.getFileName() + ":" + lineNumber + ": " + e.getMessage());
                }
            }
            result.lines.add("p" + k + ": " + count + " registos");
        }
        // i < j conta cada par uma única vez; BEFORE/AFTER refere-se a essa orientação.
        for (int i = 0; i < events.size(); i++) {
            Event a = events.get(i);
            for (int j = i + 1; j < events.size(); j++) {
                Event b = events.get(j);
                if (a.k == b.k) {
                    // A sequência estabelece a ordem causal local, mesmo com carimbos inválidos.
                    boolean before = a.r.getSeq() < b.r.getSeq();
                    checkLamport(result, before ? a : b, before ? b : a);
                    continue; // As estatísticas de pares continuam a abranger só partições diferentes.
                }
                VectorClock.Order order = a.v.compare(b.v);
                if (order == VectorClock.Order.CONCURRENT) {
                    result.concurrent++;
                    if (a.r.getLamport() != b.r.getLamport()) result.distinctLamport++;
                } else if (order == VectorClock.Order.BEFORE || order == VectorClock.Order.AFTER) {
                    boolean before = order == VectorClock.Order.BEFORE;
                    if (before) result.before++; else result.after++;
                    checkLamport(result, before ? a : b, before ? b : a);
                } else {
                    result.fail("vetores iguais em eventos distintos: " + a.id() + ", " + b.id());
                }
            }
        }
        events.sort(Comparator.comparingLong((Event e) -> e.r.getLamport())
                .thenComparingInt(e -> e.k).thenComparingLong(e -> e.r.getSeq()));
        List<String> view = new ArrayList<>();
        for (Event e : events) view.add(e.k + ";" + e.r.toLine());
        Files.write(folder.resolve("vista-lamport.txt"), view, StandardCharsets.UTF_8);
        result.lines.add("Pares entre partições (a em partição menor que b):");
        result.lines.add("a → b: " + result.before);
        result.lines.add("b → a: " + result.after);
        result.lines.add("Pares causais: " + (result.before + result.after));
        result.lines.add("Pares concorrentes: " + result.concurrent);
        result.lines.add(String.format(Locale.ROOT, "Concorrentes com L distinto: %d (%.4f%%)",
                result.distinctLamport, 100 * result.proportion()));
        result.lines.add("Violações da condição de relógio: " + result.violations);
        result.lines.add("RESULTADO: " + (result.ok() ? "OK" : "FALHA"));
        return result;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Uso: CausalCheck <N> <pasta>");
            return;
        }
        Result r = verificar(Integer.parseInt(args[0]), Path.of(args[1]));
        for (String line : r.lines) System.out.println(line);
        if (!r.ok()) System.exit(1);
    }
}
