package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Análise a posteriori do detetor de falhas e do envio em lote, a partir dos ficheiros
 * {@code hb-p<k>.txt}, {@code lider-p<k>.txt} e {@code monitor-<id>.txt} de uma pasta.
 *
 * <p>Invocação: {@code HeartbeatCheck <N> <pasta> [falhas.txt]}. O relatório é impresso e gravado em
 * {@code relatorio-heartbeat.txt} na pasta. Compara instantes escritos por processos diferentes, por
 * isso só é válido com todos os processos na mesma máquina (o mesmo relógio físico).</p>
 */
public final class HeartbeatCheck {
    // Impede instanciar a ferramenta de análise, que usa métodos estáticos.
    private HeartbeatCheck() { }

    /** Uma linha {@code inc;hb;instante;ultimoSeq;n;bytes} de {@code hb-p<k>.txt}. */
    private static final class Lote {
        final long inc, hb, instante, ultimoSeq;
        final int n, bytes;
        // Interpreta os seis campos de uma linha do ficheiro de heartbeats do líder.
        Lote(String line) {
            String[] f = line.trim().split(";", -1);
            if (f.length != 6) throw new IllegalArgumentException("linha com " + f.length + " campos");
            inc = Long.parseLong(f[0]);
            hb = Long.parseLong(f[1]);
            instante = Long.parseLong(f[2]);
            ultimoSeq = Long.parseLong(f[3]);
            n = Integer.parseInt(f[4]);
            bytes = Integer.parseInt(f[5]);
        }
    }

    /** Uma linha {@code instante;evento;p<k>;detalhe} de {@code monitor-<id>.txt}. */
    private static final class Evento {
        final long instante;
        final String tipo;
        final int k;
        final Map<String, String> campos = new HashMap<>();
        // Interpreta o instante, o tipo, a partição e os campos chave=valor do detalhe.
        Evento(String line) {
            String[] f = line.trim().split(";", -1);
            if (f.length < 3 || !f[2].startsWith("p")) throw new IllegalArgumentException("evento mal formado");
            instante = Long.parseLong(f[0]);
            tipo = f[1];
            k = Integer.parseInt(f[2].substring(1));
            for (int i = 3; i < f.length; i++) {
                int eq = f[i].indexOf('=');
                if (eq > 0) campos.put(f[i].substring(0, eq), f[i].substring(eq + 1));
            }
        }
        // Devolve o inc deste evento; no REINICIO é a incarnação nova (a seguir a "->"); -1 se não tiver.
        long inc() {
            String v = campos.get("inc");
            if (v == null) return -1;
            int arrow = v.indexOf("->");
            return Long.parseLong(arrow >= 0 ? v.substring(arrow + 2) : v);
        }
    }

    public static final class Result {
        public final List<String> lines = new ArrayList<>();
        public long invalid;
        // Indica se todos os ficheiros esperados existiam e todas as linhas foram interpretadas.
        public boolean ok() { return invalid == 0; }
        // Conta um erro nos dados e acrescenta a respetiva mensagem ao relatório.
        private void fail(String message) { invalid++; lines.add("FALHA: " + message); }
        // Acrescenta uma linha formatada ao relatório, com ponto decimal independente da configuração regional.
        private void add(String format, Object... args) { lines.add(String.format(Locale.ROOT, format, args)); }
    }

    // Lê os ficheiros da pasta e produz as secções do relatório pela ordem do enunciado.
    public static Result analisar(int n, Path folder, Path falhasFile) throws IOException {
        if (!Files.isDirectory(folder)) throw new IllegalArgumentException("Pasta inexistente: " + folder);
        Result res = new Result();
        List<List<Lote>> lotes = new ArrayList<>();
        List<Map<Long, Long>> instanteRegisto = new ArrayList<>();
        List<List<String>> linhasLider = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            lotes.add(lerLotes(res, folder.resolve("hb-p" + k + ".txt")));
            List<String> ls = new ArrayList<>();
            instanteRegisto.add(lerLider(res, folder.resolve("lider-p" + k + ".txt"), n, ls));
            linhasLider.add(ls);
        }
        Map<String, List<Evento>> monitores = lerMonitores(res, folder);

        seccaoLotes(res, lotes);
        seccaoSuspeitas(res, monitores, lotes);
        if (falhasFile != null) seccaoDetecao(res, falhasFile, monitores);
        seccaoLatencia(res, n, monitores, instanteRegisto);
        seccaoContagens(res, monitores);
        seccaoBytes(res, lotes, linhasLider);
        return res;
    }

    // Lê hb-p<k>.txt; um ficheiro em falta ou uma linha inválida contam como falha nos dados.
    private static List<Lote> lerLotes(Result res, Path file) throws IOException {
        List<Lote> out = new ArrayList<>();
        if (!Files.exists(file)) {
            res.fail("ficheiro em falta: " + file.getFileName());
            return out;
        }
        int lineNumber = 0;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            lineNumber++;
            if (line.isBlank()) continue;
            try {
                out.add(new Lote(line));
            } catch (IllegalArgumentException e) {
                res.fail(file.getFileName() + ":" + lineNumber + ": " + e.getMessage());
            }
        }
        return out;
    }

    // Lê lider-p<k>.txt e devolve, por seq, o instante do registo em ms (o ficheiro guarda-o ao segundo, em hora local).
    private static Map<Long, Long> lerLider(Result res, Path file, int n, List<String> linhas) throws IOException {
        Map<Long, Long> out = new HashMap<>();
        if (!Files.exists(file)) {
            res.fail("ficheiro em falta: " + file.getFileName());
            return out;
        }
        int lineNumber = 0;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            lineNumber++;
            if (line.isBlank()) continue;
            try {
                SensorRecord r = SensorRecord.fromLine(line, n);
                long ms = LocalDateTime.parse(r.getTimestamp()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                out.put(r.getSeq(), ms);
                linhas.add(line.trim());
            } catch (IllegalArgumentException e) {
                res.fail(file.getFileName() + ":" + lineNumber + ": " + e.getMessage());
            }
        }
        return out;
    }

    // Lê todos os monitor-<id>.txt da pasta, indexados pelo id do participante e por ordem de nome.
    private static Map<String, List<Evento>> lerMonitores(Result res, Path folder) throws IOException {
        Map<String, List<Evento>> out = new TreeMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(folder, "monitor-*.txt")) {
            for (Path file : ds) {
                String nome = file.getFileName().toString();
                String id = nome.substring("monitor-".length(), nome.length() - ".txt".length());
                List<Evento> evs = new ArrayList<>();
                int lineNumber = 0;
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    lineNumber++;
                    if (line.isBlank()) continue;
                    try {
                        evs.add(new Evento(line));
                    } catch (IllegalArgumentException e) {
                        res.fail(nome + ":" + lineNumber + ": " + e.getMessage());
                    }
                }
                out.put(id, evs);
            }
        }
        if (out.isEmpty()) res.fail("nenhum ficheiro monitor-*.txt na pasta");
        return out;
    }

    // Por partição e incarnação: lotes, proporção de vazios, registos por lote e intervalo entre heartbeats consecutivos.
    private static void seccaoLotes(Result res, List<List<Lote>> lotes) {
        res.lines.add("== Lotes por partição e incarnação (hb-p<k>.txt)");
        for (int k = 0; k < lotes.size(); k++) {
            Map<Long, List<Lote>> porInc = new LinkedHashMap<>();
            for (Lote l : lotes.get(k)) porInc.computeIfAbsent(l.inc, x -> new ArrayList<>()).add(l);
            long total = 0, vaziosTotal = 0;
            for (Map.Entry<Long, List<Lote>> e : porInc.entrySet()) {
                List<Lote> ls = e.getValue();
                long vazios = 0, registos = 0, maxRegistos = 0, somaIntervalos = 0, maxIntervalo = 0;
                for (int i = 0; i < ls.size(); i++) {
                    Lote l = ls.get(i);
                    if (l.n == 0) vazios++;
                    registos += l.n;
                    maxRegistos = Math.max(maxRegistos, l.n);
                    if (i > 0) {
                        long d = l.instante - ls.get(i - 1).instante;
                        somaIntervalos += d;
                        maxIntervalo = Math.max(maxIntervalo, d);
                    }
                }
                total += ls.size();
                vaziosTotal += vazios;
                long duracao = ls.get(ls.size() - 1).instante - ls.get(0).instante;
                String porMinuto = duracao > 0 ? String.format(Locale.ROOT, "%.1f", (ls.size() - 1) * 60000.0 / duracao) : "-";
                String intervalo = ls.size() > 1
                        ? String.format(Locale.ROOT, "média %.0f ms, máx %d ms", (double) somaIntervalos / (ls.size() - 1), maxIntervalo)
                        : "-";
                res.add("p%d inc=%d: %d lotes (%s/min), vazios %d (%.1f%%), registos/lote média %.2f máx %d, intervalo hb %s",
                        k, e.getKey(), ls.size(), porMinuto, vazios, 100.0 * vazios / ls.size(),
                        (double) registos / ls.size(), maxRegistos, intervalo);
            }
            res.add("p%d total: %d lotes, vazios %.1f%%", k, total, total == 0 ? 0 : 100.0 * vaziosTotal / total);
        }
    }

    // Classifica cada SUSPEITA: falsa se a RETOMA seguinte tiver o mesmo inc de antes; real se não houver RETOMA dessa incarnação.
    private static void seccaoSuspeitas(Result res, Map<String, List<Evento>> monitores, List<List<Lote>> lotes) {
        res.lines.add("== Suspeitas por participante e partição");
        long reais = 0, falsas = 0, semInc = 0;
        for (Map.Entry<String, List<Evento>> e : monitores.entrySet()) {
            List<Evento> evs = e.getValue();
            Map<Integer, Long> incAtual = new HashMap<>();
            for (int i = 0; i < evs.size(); i++) {
                Evento ev = evs.get(i);
                if (!ev.tipo.equals("SUSPEITA")) {
                    if (ev.inc() > 0) incAtual.merge(ev.k, ev.inc(), Math::max);
                    continue;
                }
                long incAntes = incAtual.getOrDefault(ev.k, -1L);
                Evento retoma = null;
                for (int j = i + 1; j < evs.size(); j++) {
                    Evento x = evs.get(j);
                    if (x.k == ev.k && x.tipo.equals("RETOMA")) {
                        retoma = x;
                        break;
                    }
                }
                String classe;
                if (incAntes < 0) {
                    classe = "líder nunca ouvido (sem incarnação anterior)";
                    semInc++;
                } else if (retoma != null && retoma.inc() == incAntes) {
                    classe = String.format(Locale.ROOT, "FALSA SUSPEITA (retoma inc=%d após %d ms)", incAntes,
                            retoma.instante - ev.instante);
                    falsas++;
                } else {
                    classe = retoma == null ? "FALHA REAL (sem retoma)"
                            : String.format(Locale.ROOT, "FALHA REAL (retoma com inc=%d)", retoma.inc());
                    reais++;
                }
                Lote ultimo = ultimoAntes(lotes.size() > ev.k ? lotes.get(ev.k) : List.of(), ev.instante);
                String silencio = ultimo == null ? "sem heartbeat publicado antes"
                        : (ev.instante - ultimo.instante) + " ms desde hb=" + ultimo.hb + " inc=" + ultimo.inc;
                res.add("%s p%d em %d: %s; silêncio %s (detetor: %s ms)", e.getKey(), ev.k, ev.instante, classe,
                        silencio, ev.campos.getOrDefault("silencio_ms", "?"));
            }
        }
        res.add("total: %d falhas reais, %d falsas suspeitas, %d sem incarnação anterior", reais, falsas, semInc);
    }

    // Devolve o último lote publicado com instante menor ou igual a t, ou null se não houver.
    private static Lote ultimoAntes(List<Lote> ls, long t) {
        Lote out = null;
        for (Lote l : ls) {
            if (l.instante <= t && (out == null || l.instante >= out.instante)) out = l;
        }
        return out;
    }

    // Para cada falha anotada em falhas.txt, mede em cada participante o tempo até à primeira SUSPEITA dessa partição.
    private static void seccaoDetecao(Result res, Path falhasFile, Map<String, List<Evento>> monitores) throws IOException {
        res.lines.add("== Tempo de deteção (" + falhasFile.getFileName() + ")");
        if (!Files.exists(falhasFile)) {
            res.fail("ficheiro em falta: " + falhasFile);
            return;
        }
        List<Long> tempos = new ArrayList<>();
        int lineNumber = 0;
        for (String line : Files.readAllLines(falhasFile, StandardCharsets.UTF_8)) {
            lineNumber++;
            if (line.isBlank()) continue;
            int k;
            long tFalha;
            try {
                String[] f = line.trim().split(";", -1);
                if (f.length != 2 || !f[0].startsWith("p")) throw new IllegalArgumentException("esperado p<k>;instante");
                k = Integer.parseInt(f[0].substring(1));
                tFalha = Long.parseLong(f[1]);
            } catch (IllegalArgumentException e) {
                res.fail(falhasFile.getFileName() + ":" + lineNumber + ": " + e.getMessage());
                continue;
            }
            for (Map.Entry<String, List<Evento>> e : monitores.entrySet()) {
                boolean acompanha = false;
                boolean ouviu = false; // recebeu algum heartbeat de p<k> antes da suspeita
                Evento suspeita = null;
                for (Evento ev : e.getValue()) {
                    if (ev.k != k) continue;
                    acompanha = true;
                    if (ev.tipo.equals("SUSPEITA") && ev.instante >= tFalha) {
                        suspeita = ev;
                        break;
                    }
                    if (ev.inc() > 0) ouviu = true;
                }
                if (!acompanha) continue;
                if (suspeita == null) {
                    res.add("falha p%d em %d: %s não suspeitou", k, tFalha, e.getKey());
                } else if (!ouviu) {
                    // o temporizador contou desde o arranque do participante, não desde um heartbeat
                    res.add("falha p%d em %d: %s suspeitou após %.3f s, mas nunca tinha recebido heartbeat de p%d (não conta)",
                            k, tFalha, e.getKey(), (suspeita.instante - tFalha) / 1000.0, k);
                } else {
                    long d = suspeita.instante - tFalha;
                    tempos.add(d);
                    res.add("falha p%d em %d: %s suspeitou após %.3f s", k, tFalha, e.getKey(), d / 1000.0);
                }
            }
        }
        if (!tempos.isEmpty()) {
            long min = Long.MAX_VALUE, max = Long.MIN_VALUE, soma = 0;
            for (long t : tempos) {
                min = Math.min(min, t);
                max = Math.max(max, t);
                soma += t;
            }
            res.add("tempo de deteção: mínimo %.3f s, médio %.3f s, máximo %.3f s (%d medições)",
                    min / 1000.0, soma / 1000.0 / tempos.size(), max / 1000.0, tempos.size());
        }
    }

    // Para cada registo de cada RECEBIDO com intervalo de seq conhecido: instante do RECEBIDO menos o instante do registo.
    private static void seccaoLatencia(Result res, int n, Map<String, List<Evento>> monitores,
                                       List<Map<Long, Long>> instanteRegisto) {
        res.lines.add("== Latência de lote (RECEBIDO − instante do registo)");
        res.lines.add("(o instante do registo está gravado ao segundo, por isso cada valor tem um excesso entre 0 e 1 s)");
        long[] soma = new long[n], max = new long[n], conta = new long[n];
        for (int k = 0; k < n; k++) max[k] = Long.MIN_VALUE;
        for (List<Evento> evs : monitores.values()) {
            for (Evento ev : evs) {
                if (!ev.tipo.equals("RECEBIDO") || ev.k < 0 || ev.k >= n) continue;
                String seq = ev.campos.get("seq");
                if (seq == null || !seq.contains("..")) continue; // vazio ("-") ou desconhecido ("?", produtor)
                String[] ab = seq.split("\\.\\.");
                long a = Long.parseLong(ab[0]), b = Long.parseLong(ab[1]);
                for (long s = a; s <= b; s++) {
                    Long t = instanteRegisto.get(ev.k).get(s);
                    if (t == null) continue;
                    long d = ev.instante - t;
                    soma[ev.k] += d;
                    conta[ev.k]++;
                    max[ev.k] = Math.max(max[ev.k], d);
                }
            }
        }
        for (int k = 0; k < n; k++) {
            if (conta[k] == 0) res.add("p%d: sem registos recebidos em lote", k);
            else res.add("p%d: média %.0f ms, máx %d ms (%d receções)", k, (double) soma[k] / conta[k], max[k], conta[k]);
        }
    }

    // Conta os eventos HB_PERDIDO e RECUPERACAO por participante e no total.
    private static void seccaoContagens(Result res, Map<String, List<Evento>> monitores) {
        res.lines.add("== HB_PERDIDO e RECUPERACAO");
        long perdidos = 0, recuperacoes = 0;
        for (Map.Entry<String, List<Evento>> e : monitores.entrySet()) {
            long p = 0, r = 0;
            for (Evento ev : e.getValue()) {
                if (ev.tipo.equals("HB_PERDIDO")) p++;
                if (ev.tipo.equals("RECUPERACAO")) r++;
            }
            perdidos += p;
            recuperacoes += r;
            res.add("%s: HB_PERDIDO %d, RECUPERACAO %d", e.getKey(), p, r);
        }
        res.add("total: HB_PERDIDO %d, RECUPERACAO %d", perdidos, recuperacoes);
    }

    // Bytes por registo em LOTE (bytes dos lotes com registos / registos) contra um REG da COMINDIR03 com o mesmo registo.
    private static void seccaoBytes(Result res, List<List<Lote>> lotes, List<List<String>> linhasLider) {
        res.lines.add("== Mensagens e bytes por registo no tópico de dados");
        long bytesLote = 0, registosLote = 0, mensagensLote = 0;
        for (List<Lote> ls : lotes) {
            for (Lote l : ls) {
                mensagensLote++;
                if (l.n > 0) {
                    bytesLote += l.bytes;
                    registosLote += l.n;
                }
            }
        }
        long bytesReg = 0, registos = 0;
        for (int k = 0; k < linhasLider.size(); k++) {
            // REG;<host>;<porto>;<linha>, com o endereço por omissão do líder (todos os processos na mesma máquina).
            String prefixo = "REG;127.0.0.1;" + (Wire.PORTO_BASE + k) + ";";
            for (String line : linhasLider.get(k)) {
                bytesReg += (prefixo + line).getBytes(StandardCharsets.UTF_8).length;
                registos++;
            }
        }
        res.add("mensagens: COMINDIR03 (um REG por registo) %d / COMINDIR04 (LOTE) %d", registos, mensagensLote);
        res.add("bytes por registo: REG individual %s / em LOTE %s",
                registos == 0 ? "-" : String.format(Locale.ROOT, "%.1f", (double) bytesReg / registos),
                registosLote == 0 ? "-" : String.format(Locale.ROOT, "%.1f", (double) bytesLote / registosLote));
    }

    // Executa a análise, imprime o relatório, grava-o na pasta e termina com 1 se faltarem ficheiros ou houver linhas inválidas.
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
            System.exit(2);
        }
        Path folder = Path.of(args[1]);
        Result res = analisar(Integer.parseInt(args[0]), folder, args.length > 2 ? Path.of(args[2]) : null);
        res.lines.forEach(System.out::println);
        Files.write(folder.resolve("relatorio-heartbeat.txt"), res.lines, StandardCharsets.UTF_8);
        if (!res.ok()) System.exit(1);
    }
}
