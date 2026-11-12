package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Verificação a posteriori da eleição de líder. Lê, numa pasta, os ficheiros dos nós
 * (no-p&lt;k&gt;-n&lt;i&gt;, eventos-, hb-, estado-, truncados-), o produtor.txt, os monitor-*.txt e,
 * opcionalmente, um ficheiro de falhas (linhas {@code p<k>;n<i>;instante}).
 * Uso: RaftCheck &lt;N&gt; &lt;pasta&gt; [falhas.txt]
 */
public final class RaftCheck {
    private static final int G = Wire.GRUPO;

    /** Uma linha de eventos-p&lt;k&gt;-n&lt;i&gt;.txt: instante;TIPO;p&lt;k&gt;;detalhe. */
    private static final class Ev {
        final long t; final String tipo; final String det; final int node;
        Ev(long t, String tipo, String det, int node) { this.t = t; this.tipo = tipo; this.det = det; this.node = node; }
        String get(String key) {
            for (String f : det.split(";")) if (f.startsWith(key + "=")) return f.substring(key.length() + 1);
            return null;
        }
        long termo() { String v = get("termo"); return v == null ? -1 : Long.parseLong(v); }
    }

    private static final class Falha {
        final int k, node; final long t;
        Falha(int k, int node, long t) { this.k = k; this.node = node; this.t = t; }
    }

    private final int n;
    private final Path dir;
    private final List<String> out = new ArrayList<>();
    private boolean ok = true;

    private RaftCheck(int n, Path dir) { this.n = n; this.dir = dir; }

    private void p(String s) { out.add(s); }
    private void fail(String s) { ok = false; p("  FALHA: " + s); }

    private List<String> lines(Path f) throws IOException {
        List<String> r = new ArrayList<>();
        if (!Files.exists(f)) return r;
        for (String l : Files.readAllLines(f, StandardCharsets.UTF_8)) if (!l.isBlank()) r.add(l.trim());
        return r;
    }

    private List<Ev> eventos(int k, int i) throws IOException {
        List<Ev> r = new ArrayList<>();
        for (String l : lines(dir.resolve("eventos-p" + k + "-n" + i + ".txt"))) {
            String[] f = l.split(";", 4);
            if (f.length < 3) continue;
            try { r.add(new Ev(Long.parseLong(f[0]), f[1], f.length > 3 ? f[3] : "", i)); }
            catch (NumberFormatException e) { /* linha estranha: ignora */ }
        }
        return r;
    }

    public static RaftCheck verificar(int n, Path dir, Path falhasFile) throws IOException {
        if (!Files.isDirectory(dir)) throw new IllegalArgumentException("Pasta inexistente: " + dir);
        RaftCheck c = new RaftCheck(n, dir);
        List<Falha> falhas = c.lerFalhas(falhasFile);
        Map<Integer, List<Ev>> todos = new LinkedHashMap<>();
        for (int k = 0; k < n; k++) {
            List<Ev> l = new ArrayList<>();
            for (int i = 1; i <= G; i++) l.addAll(c.eventos(k, i));
            l.sort((a, b) -> Long.compare(a.t, b.t));
            todos.put(k, l);
        }
        c.eleicoes(todos);
        c.seguranca(todos);
        c.indisponibilidade(todos, falhas);
        c.perdas(todos, falhas);
        c.truncagens(todos);
        c.coincidencia();
        c.monitores();
        c.p("RESULTADO: " + (c.ok ? "OK" : "FALHA"));
        return c;
    }

    private List<Falha> lerFalhas(Path f) throws IOException {
        List<Falha> r = new ArrayList<>();
        if (f == null) return r;
        if (!Files.exists(f) && Files.exists(dir.resolve(f.toString()))) f = dir.resolve(f.toString());
        for (String l : lines(f)) {
            String[] t = l.split(";");
            if (t.length < 3 || l.startsWith("#")) continue;
            r.add(new Falha(Integer.parseInt(t[0].substring(1)), Integer.parseInt(t[1].substring(1)), Long.parseLong(t[2])));
        }
        return r;
    }

    // 1) candidatos, votos, líder e duração de cada eleição; tempo até ao primeiro líder
    private void eleicoes(Map<Integer, List<Ev>> todos) {
        p("== Eleições");
        for (int k = 0; k < n; k++) {
            List<Ev> evs = todos.get(k);
            p("p" + k + ":");
            long arranque = Long.MAX_VALUE, primeiroLider = Long.MAX_VALUE;
            boolean temArranque = false;
            Map<Long, Map<Integer, Long>> candidatos = new TreeMap<>();   // termo -> candidato -> instante
            Map<Long, Map<Integer, Set<Integer>>> votos = new HashMap<>(); // termo -> candidato -> votantes
            Map<Long, Map<Integer, Long>> lideres = new HashMap<>();       // termo -> líder -> instante
            for (Ev e : evs) {
                switch (e.tipo) {
                    case "ARRANQUE": temArranque = true; arranque = Math.min(arranque, e.t); break;
                    case "CANDIDATO":
                        candidatos.computeIfAbsent(e.termo(), x -> new TreeMap<>()).put(e.node, e.t);
                        votos.computeIfAbsent(e.termo(), x -> new HashMap<>())
                                .computeIfAbsent(e.node, x -> new TreeSet<>()).add(e.node);
                        break;
                    case "VOTO":
                        if (e.det.endsWith("concedido") && e.get("candidato") != null) {
                            int c = Integer.parseInt(e.get("candidato").substring(1));
                            votos.computeIfAbsent(e.termo(), x -> new HashMap<>())
                                    .computeIfAbsent(c, x -> new TreeSet<>()).add(e.node);
                        }
                        break;
                    case "LIDER":
                        lideres.computeIfAbsent(e.termo(), x -> new TreeMap<>()).put(e.node, e.t);
                        primeiroLider = Math.min(primeiroLider, e.t);
                        break;
                    default: break;
                }
            }
            if (!temArranque && !evs.isEmpty()) arranque = evs.get(0).t;
            for (Map.Entry<Long, Map<Integer, Long>> en : candidatos.entrySet()) {
                long termo = en.getKey();
                StringBuilder sb = new StringBuilder("  termo " + termo + ": candidatos ");
                long primeiro = Long.MAX_VALUE;
                boolean first = true;
                for (Map.Entry<Integer, Long> c : en.getValue().entrySet()) {
                    if (!first) sb.append(", ");
                    first = false;
                    sb.append("n").append(c.getKey()).append(" (votos ")
                            .append(votos.get(termo).get(c.getKey()).size()).append(")");
                    primeiro = Math.min(primeiro, c.getValue());
                }
                Map<Integer, Long> l = lideres.get(termo);
                if (l == null) sb.append(" → sem líder (voto dividido)");
                else {
                    long tl = Long.MAX_VALUE;
                    for (long t : l.values()) tl = Math.min(tl, t);
                    sb.append(" → líder ").append(l.keySet()).append(" em ").append(tl - primeiro).append(" ms");
                }
                p(sb.toString());
            }
            if (primeiroLider == Long.MAX_VALUE) p("  nenhum líder eleito" + (evs.isEmpty() ? " (sem eventos)" : ""));
            else p("  primeiro líder " + (primeiroLider - arranque) + " ms após o "
                    + (temArranque ? "arranque do primeiro nó" : "primeiro evento (sem ARRANQUE)"));
        }
    }

    // 2) no máximo um líder por termo; termo nunca decresce em cada nó; estado final coerente
    private void seguranca(Map<Integer, List<Ev>> todos) throws IOException {
        p("== Segurança");
        int multiplos = 0, descidas = 0;
        for (int k = 0; k < n; k++) {
            Map<Long, Set<Integer>> lid = new TreeMap<>();
            long maxVisto = 0;
            for (int i = 1; i <= G; i++) {
                long atual = 0;
                for (Ev e : eventos(k, i)) {
                    if (e.tipo.equals("LIDER")) lid.computeIfAbsent(e.termo(), x -> new TreeSet<>()).add(i);
                    long t = e.termo();
                    if (e.tipo.equals("TERMO")) t = Long.parseLong(e.det.substring(e.det.indexOf("->") + 2).trim());
                    if (t < 0 || e.tipo.equals("RECEBIDO") || e.tipo.equals("IGNORADO")) continue;
                    if (t < atual) { descidas++; fail("p" + k + " n" + i + ": termo desceu de " + atual + " para " + t); }
                    atual = Math.max(atual, t);
                    maxVisto = Math.max(maxVisto, t);
                }
                for (String l : lines(dir.resolve("estado-p" + k + "-n" + i + ".txt"))) {
                    long ft = Long.parseLong(l.split(";")[0]);
                    if (ft < atual) fail("p" + k + " n" + i + ": estado em disco (termo " + ft + ") atrás do visto (" + atual + ")");
                }
            }
            for (Map.Entry<Long, Set<Integer>> e : lid.entrySet())
                if (e.getValue().size() > 1) { multiplos++; fail("p" + k + " termo " + e.getKey() + " com vários líderes " + e.getValue()); }
        }
        p("  termos com mais de um líder: " + multiplos);
        p("  descidas de termo: " + descidas);
    }

    // 3) indisponibilidade: da falha do líder ao primeiro LOTE de um líder de termo superior
    private void indisponibilidade(Map<Integer, List<Ev>> todos, List<Falha> falhas) throws IOException {
        if (falhas.isEmpty()) return;
        p("== Indisponibilidade (falhas)");
        for (Falha f : falhas) {
            Ev lider = null;
            for (Ev e : todos.get(f.k)) if (e.tipo.equals("LIDER") && e.t <= f.t) lider = e;
            if (lider == null || lider.node != f.node) {
                p("  p" + f.k + " n" + f.node + " @" + f.t + ": não era líder; sem indisponibilidade");
                continue;
            }
            long termo = lider.termo(), primeiro = Long.MAX_VALUE;
            for (int i = 1; i <= G; i++)
                for (String l : lines(dir.resolve("hb-p" + f.k + "-n" + i + ".txt"))) {
                    String[] t = l.split(";");
                    long tt = Long.parseLong(t[0]), ins = Long.parseLong(t[2]);
                    if (tt > termo && ins >= f.t) primeiro = Math.min(primeiro, ins);
                }
            if (primeiro == Long.MAX_VALUE) { p("  p" + f.k + " n" + f.node + ": sem lote de novo líder (partição ficou sem líder)"); continue; }
            int perdas = 0;
            for (String l : lines(dir.resolve("produtor.txt"))) {
                String[] t = l.split(";");
                if (t[1].equals("p" + f.k) && Long.parseLong(t[0]) > f.t && Long.parseLong(t[0]) <= primeiro) perdas++;
            }
            p("  p" + f.k + " líder n" + f.node + " (termo " + termo + ") falhou em " + f.t + ": primeiro lote do novo líder em "
                    + primeiro + " → indisponível " + (primeiro - f.t) + " ms; leituras enviadas nesse intervalo: " + perdas);
        }
    }

    private static String chave(String sensor, String temp) {
        return sensor + ";" + Double.parseDouble(temp);
    }

    // Leituras (não alarmes) de cada nó, em multiconjunto
    private Map<String, Integer> contagem(Path f) throws IOException {
        Map<String, Integer> m = new HashMap<>();
        for (String l : lines(f)) {
            String[] t = l.split(";");
            if (t.length < 4 || t[2].startsWith("ALARM-")) continue;
            m.merge(chave(t[2], t[3]), 1, Integer::sum);
        }
        return m;
    }

    // 4) leituras enviadas pelo produtor que não constam do registo final de nenhum nó
    private void perdas(Map<Integer, List<Ev>> todos, List<Falha> falhas) throws IOException {
        Path pf = dir.resolve("produtor.txt");
        if (!Files.exists(pf)) { p("== Leituras perdidas: produtor.txt em falta"); return; }
        p("== Leituras perdidas (produtor.txt vs registo final dos nós)");
        long totalEnviadas = 0, totalPerdidas = 0, naJanela = 0;
        for (int k = 0; k < n; k++) {
            Map<String, Integer> disponivel = new HashMap<>();
            for (int i = 1; i <= G; i++)
                for (Map.Entry<String, Integer> e : contagem(dir.resolve("no-p" + k + "-n" + i + ".txt")).entrySet())
                    disponivel.merge(e.getKey(), e.getValue(), Math::max);   // cópias do mesmo registo: máximo, não soma
            int enviadas = 0, perdidas = 0, janela = 0;
            for (String l : lines(pf)) {
                String[] t = l.split(";");
                if (!t[1].equals("p" + k)) continue;
                enviadas++;
                String ch = chave(t[2], t[3]);
                Integer d = disponivel.get(ch);
                if (d != null && d > 0) { disponivel.put(ch, d - 1); continue; }
                perdidas++;
                long ins = Long.parseLong(t[0]);
                for (Falha f : falhas) if (f.k == k && ins >= f.t && ins <= f.t + 60_000) janela++;
            }
            p("  p" + k + ": enviadas " + enviadas + ", perdidas " + perdidas
                    + (falhas.isEmpty() ? "" : " (próximas de uma falha: " + janela + ")"));
            totalEnviadas += enviadas; totalPerdidas += perdidas; naJanela += janela;
        }
        p("  total: enviadas " + totalEnviadas + ", perdidas " + totalPerdidas);
    }

    // 5) registos truncados e quantos reaparecem com outro seq no registo final do líder
    private void truncagens(Map<Integer, List<Ev>> todos) throws IOException {
        p("== Truncagens");
        int total = 0;
        for (int k = 0; k < n; k++) {
            Ev ultimo = null;
            for (Ev e : todos.get(k)) if (e.tipo.equals("LIDER")) ultimo = e;
            Map<String, Integer> finalLider = ultimo == null ? new HashMap<>()
                    : contagem(dir.resolve("no-p" + k + "-n" + ultimo.node + ".txt"));
            for (int i = 1; i <= G; i++) {
                for (Ev e : todos.get(k)) {
                    if (!e.tipo.equals("TRUNCADO") || e.node != i) continue;
                    int desc = Integer.parseInt(e.get("descartados"));
                    total += desc;
                    int reaparecem = 0;
                    Map<String, Integer> pool = new HashMap<>(finalLider);
                    boolean temFicheiro = Files.exists(dir.resolve("truncados-p" + k + "-n" + i + ".txt"));
                    for (String l : lines(dir.resolve("truncados-p" + k + "-n" + i + ".txt"))) {
                        String[] t = l.split(";");
                        Integer d = pool.get(chave(t[2], t[3]));
                        if (d != null && d > 0 && !t[2].startsWith("ALARM-")) { pool.put(chave(t[2], t[3]), d - 1); reaparecem++; }
                    }
                    p("  p" + k + " n" + i + ": TRUNCADO de=" + e.get("de") + " descartados=" + desc + " termos=" + e.get("termos")
                            + (temFicheiro ? "; reaparecem no registo final do líder (com outro seq): " + reaparecem
                                           : "; (sem truncados-p" + k + "-n" + i + ".txt)"));
                }
            }
        }
        p("  total de registos truncados: " + total);
    }

    // 6) os registos finais dos nós de cada partição devem coincidir
    private void coincidencia() throws IOException {
        p("== Coincidência dos registos finais");
        for (int k = 0; k < n; k++) {
            List<List<String>> ficheiros = new ArrayList<>();
            List<Integer> ids = new ArrayList<>();
            for (int i = 1; i <= G; i++) {
                Path f = dir.resolve("no-p" + k + "-n" + i + ".txt");
                if (Files.exists(f)) { ficheiros.add(lines(f)); ids.add(i); }
            }
            if (ficheiros.size() < 2) { p("  p" + k + ": menos de dois ficheiros de nós"); continue; }
            int max = 0, min = Integer.MAX_VALUE;
            for (List<String> l : ficheiros) { max = Math.max(max, l.size()); min = Math.min(min, l.size()); }
            Set<Integer> difere = new LinkedHashSet<>();
            for (int s = 0; s < min; s++) {
                String ref = ficheiros.get(0).get(s);
                for (List<String> l : ficheiros) if (!l.get(s).equals(ref)) difere.add(s + 1);
            }
            if (difere.isEmpty() && min == max) p("  p" + k + ": os " + ficheiros.size() + " nós coincidem (" + max + " registos)");
            else {
                if (!difere.isEmpty()) fail("p" + k + ": seq diferentes entre nós: " + difere);
                if (min != max) p("  p" + k + ": tamanhos diferentes (" + min + ".." + max + ") — réplica ainda atrasada?");
            }
        }
    }

    // 7) resumo dos monitores do produtor e do processador
    private void monitores() throws IOException {
        for (String nome : new String[] {"monitor-produtor.txt", "monitor-processador.txt"}) {
            Path f = dir.resolve(nome);
            if (!Files.exists(f)) continue;
            Map<String, Integer> c = new TreeMap<>();
            for (String l : lines(f)) { String[] t = l.split(";"); if (t.length > 1) c.merge(t[1], 1, Integer::sum); }
            p("== " + nome + ": " + c);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: RaftCheck <N> <pasta> [falhas.txt]");
            return;
        }
        RaftCheck c = verificar(Integer.parseInt(args[0]), Path.of(args[1]), args.length == 3 ? Path.of(args[2]) : null);
        for (String l : c.out) System.out.println(l);
        if (!c.ok) System.exit(1);
    }
}
