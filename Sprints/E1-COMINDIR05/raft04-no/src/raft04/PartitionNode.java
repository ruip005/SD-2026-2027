package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Nó Raft: seguidor/candidato/líder de uma partição. */
public final class PartitionNode {
    private final int k, self;
    private final Partitioner partitioner;
    private final RecordFile file;
    private final RaftState raft;
    private final Publisher publisher;
    private final String host;
    private final int port;
    private final Path hbFile, eventFile;
    private final Object lock = new Object();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private long lastSeq, lastTerm, lamport, hb;
    private VectorClock vector;
    private List<SensorRecord> pending = new ArrayList<>();
    private ScheduledFuture<?> leaderTask;

    public PartitionNode(int k, int self, int n, String host, Publisher publisher) throws IOException {
        if (k < 0 || k >= n || self < 1 || self > Wire.GRUPO) throw new IllegalArgumentException("Nó inválido");
        this.k = k; this.self = self; this.partitioner = new Partitioner(n);
        this.host = host; this.port = Wire.nodePort(k, self); this.publisher = publisher;
        this.file = new RecordFile("no-p" + k + "-n" + self + ".txt");
        this.hbFile = Path.of("hb-p" + k + "-n" + self + ".txt");
        this.eventFile = Path.of("eventos-p" + k + "-n" + self + ".txt");
        SensorRecord last = file.last();
        lastSeq = last == null ? 0 : last.getSeq();
        lastTerm = last == null ? 0 : last.getTerm();
        lamport = last == null ? 0 : last.getLamport();
        vector = last == null ? new VectorClock(n) : last.getVector();
        if (vector.get(k) != lastSeq) throw new IllegalArgumentException("V[k] incompatível com seq");
        raft = new RaftState(k, self, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS,
                Path.of("estado-p" + k + "-n" + self + ".txt"), this::event);
        event("ARRANQUE;p" + k + ";termo=" + raft.term() + ";ultimoSeq=" + lastSeq + ";ultimoTermo=" + lastTerm);
    }

    private void event(String detail) {
        String line = System.currentTimeMillis() + ";" + detail;
        System.out.println(line);
        try {
            Files.writeString(eventFile, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) { System.err.println("ERRO evento: " + e.getMessage()); }
    }

    private boolean isolated() { return Wire.isolated(k, self); }

    public void onIn(String msg) {
        String[] f = "IN".equals(Wire.type(msg)) ? Wire.body(msg).split(";", -1) : new String[0];
        if (f.length != 4) { event("REJEITADO;p" + k + ";IN"); return; }
        try {
            double temp = Double.parseDouble(f[1]);
            long receivedL = Long.parseLong(f[2]);
            VectorClock receivedV = VectorClock.parse(f[3], partitioner.count());
            if (!Double.isFinite(temp) || receivedL < 0 || partitioner.of(f[0]) != k) throw new IllegalArgumentException();
            synchronized (lock) {
                long term = raft.termIfLeader();
                if (term < 0) { event("IGNORADO;p" + k + ";IN sem líder"); return; }
                long nextL = Math.incrementExact(Math.max(lamport, receivedL));
                VectorClock nextV = vector.copy(); nextV.merge(receivedV); nextV.tick(k);
                long nextSeq = Math.incrementExact(lastSeq);
                if (nextV.get(k) != nextSeq) throw new IllegalStateException("V[k] incompatível com seq");
                SensorRecord r = SensorRecord.now(nextSeq, term, f[0], temp, nextL, nextV);
                file.append(r);                 // antes de anunciar o registo num lote
                lastSeq = nextSeq; lastTerm = term; lamport = nextL; vector = nextV; pending.add(r);
            }
        } catch (IOException | IllegalArgumentException | ArithmeticException e) {
            event("REJEITADO;p" + k + ";IN");
        }
    }

    public void onElection(String msg) {
        if (isolated()) return;
        try {
            if (VoteRequest.TIPO.equals(Wire.type(msg))) {
                VoteRequest r = VoteRequest.decode(msg);
                if (r.getCandidate() == self) return;
                RaftState.Vote decision;
                synchronized (lock) {
                    decision = raft.onVoteRequest(r.getTerm(), r.getCandidate(), r.getLastSeq(), r.getLastTerm(),
                            lastSeq, lastTerm);
                }
                if (!isolated()) publisher.publish(Wire.topicElection(k),
                        new VoteReply(decision.term, self, r.getCandidate(), decision.granted).encode());
            } else if (VoteReply.TIPO.equals(Wire.type(msg))) {
                VoteReply r = VoteReply.decode(msg);
                if (r.getVoter() == self || r.getCandidate() != self) return;
                synchronized (lock) {
                    if (raft.onVote(r.getTerm(), r.getVoter(), r.getCandidate(), r.isGranted())) becomeLeaderLocked();
                }
            }
        } catch (Exception e) { event("ERRO;p" + k + ";eleição=" + e.getMessage()); }
    }

    private void becomeLeaderLocked() {
        hb = 0; pending = new ArrayList<>();
        if (leaderTask != null) leaderTask.cancel(false);
        leaderTask = scheduler.scheduleAtFixedRate(this::sendBatch, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    private void sendBatch() {
        Batch batch;
        synchronized (lock) {
            long term = raft.termIfLeader();
            if (term < 0) { if (leaderTask != null) leaderTask.cancel(false); return; }
            List<SensorRecord> out = pending; pending = new ArrayList<>();
            batch = new Batch(host, port, term, self, ++hb, lastSeq, lastTerm, out);
        }
        if (isolated()) return;
        try {
            String msg = batch.encode();
            publisher.publish(Wire.topicData(k), msg);
            Files.writeString(hbFile, batch.getTerm() + ";" + batch.getHb() + ";" + System.currentTimeMillis()
                            + ";" + batch.getLastSeq() + ";" + batch.getLastTerm() + ";" + batch.getRecords().size()
                            + ";" + msg.getBytes(StandardCharsets.UTF_8).length + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) { event("ERRO;p" + k + ";LOTE=" + e.getMessage()); }
    }

    public void onBatch(String msg) {
        if (isolated()) return;
        try {
            Batch b = Batch.decode(msg, partitioner.count());
            if (b.getLeader() == self) return;
            long recoverFrom = 0;
            synchronized (lock) {
                if (!raft.onBatch(b.getTerm(), b.getLeader())) { event("IGNORADO;p" + k + ";termo=" + b.getTerm()); return; }
                pending = new ArrayList<>();   // somos seguidores: descarta lixo de uma liderança antiga
                event("RECEBIDO;p" + k + ";termo=" + b.getTerm() + ";lider=n" + b.getLeader() + ";hb=" + b.getHb()
                        + ";ultimoSeq=" + b.getLastSeq() + ";ultimoTermo=" + b.getLastTerm() + ";n=" + b.getRecords().size());

                // 1) Registos do lote (ordem crescente de seq: o primeiro conflito é o de menor seq)
                for (SensorRecord r : b.getRecords()) {
                    if (r.getSeq() == lastSeq + 1) {
                        file.append(r); acceptLocked(r);
                    } else if (r.getSeq() <= lastSeq) {
                        if (!sameRecordLocked(r)) { recoverFrom = blockStartLocked(r.getSeq()); break; }
                    } else {
                        recoverFrom = lastSeq + 1; break;   // buraco
                    }
                }

                // 2) Sem conflito nos registos: compara os metadados do lote
                if (recoverFrom == 0) {
                    long L = b.getLastSeq();
                    if (lastSeq < L) recoverFrom = lastSeq + 1;                              // falta cauda
                    else if (L == 0) { if (lastSeq > 0) recoverFrom = 1; }                   // líder vazio: trunca tudo
                    else if (termAtLocked(L) != b.getLastTerm()) recoverFrom = blockStartLocked(L); // conflito em L
                    else if (lastSeq > L) recoverFrom = L + 1;                               // cauda a mais
                }
            }
            if (recoverFrom > 0) recover(b, recoverFrom);
        } catch (Exception e) { event("ERRO;p" + k + ";LOTE=" + e.getMessage()); }
    }

    private boolean sameRecordLocked(SensorRecord incoming) throws IOException {
        for (SensorRecord local : file.range(incoming.getSeq(), incoming.getSeq()))
            return local.getTerm() == incoming.getTerm() && local.toLine().equals(incoming.toLine());
        return false;
    }

    /** Termo do registo local em seq, ou -1 se não existir. */
    private long termAtLocked(long seq) throws IOException {
        List<SensorRecord> at = file.range(seq, seq);
        return at.isEmpty() ? -1 : at.get(0).getTerm();
    }

    /** Início do bloco local (mesmo termo) que contém conflictSeq. */
    private long blockStartLocked(long conflictSeq) throws IOException {
        List<SensorRecord> at = file.range(conflictSeq, conflictSeq);
        if (at.isEmpty()) return Math.max(1, Math.min(conflictSeq, lastSeq + 1));
        long t = at.get(0).getTerm();
        long start = conflictSeq;
        while (start > 1) {
            List<SensorRecord> previous = file.range(start - 1, start - 1);
            if (previous.isEmpty() || previous.get(0).getTerm() != t) break;
            start--;
        }
        return start;
    }

    private void acceptLocked(SensorRecord r) { lastSeq = r.getSeq(); lastTerm = r.getTerm(); lamport = r.getLamport(); vector = r.getVector(); }

    /** Pede ao líder, por TCP, os registos de from até ao seu último seq. */
    private List<SensorRecord> fetch(Batch b, long from) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        try (Socket s = new Socket(b.getAddress(), b.getPort());
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter w = new PrintWriter(s.getOutputStream(), true, StandardCharsets.UTF_8)) {
            s.setSoTimeout(Wire.T_TCP_MS);
            w.println("PEDIDO " + from + " " + b.getLastSeq());
            for (String line; (line = in.readLine()) != null && !"FIM".equals(line); )
                out.add(SensorRecord.fromLine(line, partitioner.count()));
        }
        return out;
    }

    /** O primeiro registo recebido coincide com o local em from (ou não há nada a comparar)? */
    private boolean firstMatches(long from, List<SensorRecord> recovered) throws IOException {
        synchronized (lock) {
            List<SensorRecord> mine = file.range(from, from);
            if (mine.isEmpty() || recovered.isEmpty()) return true;
            return recovered.get(0).getSeq() == from && mine.get(0).toLine().equals(recovered.get(0).toLine());
        }
    }

    private void recover(Batch b, long from) {
        if (isolated()) return;
        try {
            // Recua um bloco de termo de cada vez enquanto o primeiro registo recebido não coincidir com o local.
            List<SensorRecord> recovered = fetch(b, from);
            while (from > 1 && !firstMatches(from, recovered)) {
                synchronized (lock) { from = blockStartLocked(from - 1); }
                recovered = fetch(b, from);
            }
            synchronized (lock) {
                List<SensorRecord> mine = file.range(from, lastSeq);
                int same = 0;                                   // prefixo comum a partir de from
                while (same < mine.size() && same < recovered.size()
                        && mine.get(same).toLine().equals(recovered.get(same).toLine())) same++;
                long cut = from + same;                         // primeiro seq em que há diferença
                List<SensorRecord> discarded = mine.subList(same, mine.size());
                List<SensorRecord> toApply = recovered.subList(same, recovered.size());

                if (!discarded.isEmpty() || !toApply.isEmpty()) {
                    if (!discarded.isEmpty()) saveDiscardedLocked(discarded);
                    file.replaceFrom(cut, toApply);
                    SensorRecord last = file.last();
                    lastSeq = last == null ? 0 : last.getSeq(); lastTerm = last == null ? 0 : last.getTerm();
                    lamport = last == null ? 0 : last.getLamport();
                    vector = last == null ? new VectorClock(partitioner.count()) : last.getVector();
                }
                if (!discarded.isEmpty()) {
                    java.util.LinkedHashSet<Long> terms = new java.util.LinkedHashSet<>();
                    for (SensorRecord r : discarded) terms.add(r.getTerm());
                    event("TRUNCADO;p" + k + ";de=" + cut + ";descartados=" + discarded.size() + ";termos=" + terms);
                } else {
                    event("RECUPERACAO;p" + k + ";de=" + cut + ";n=" + toApply.size());
                }
            }
        } catch (Exception e) { event("ERRO;p" + k + ";recuperação=" + e.getMessage()); }
    }

    /** Guarda os registos descartados (para o RaftCheck saber que leituras foram truncadas). */
    private void saveDiscardedLocked(List<SensorRecord> discarded) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (SensorRecord r : discarded) sb.append(r.toLine()).append(System.lineSeparator());
        Files.writeString(Path.of("truncados-p" + k + "-n" + self + ".txt"), sb.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public void tick() {
        VoteRequest request = null;
        synchronized (lock) {                       // fase 1: lógica local
            long term = raft.tick();
            if (term >= 0) request = new VoteRequest(term, self, lastSeq, lastTerm);
        }
        if (request == null || isolated()) return;  // fase 2: só rede
        try { publisher.publish(Wire.topicElection(k), request.encode()); }
        catch (Exception e) { event("ERRO;p" + k + ";PEDIDO_VOTO=" + e.getMessage()); }
    }

    public void serve(ServerSocket ss) throws IOException {
        while (!ss.isClosed()) { Socket s = ss.accept(); if (isolated()) s.close(); else scheduler.execute(() -> serveOne(s)); }
    }
    private void serveOne(Socket s) {
        try (Socket socket = s; BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8)) {
            socket.setSoTimeout(Wire.T_TCP_MS);
            String request = in.readLine(); String[] f = request == null ? new String[0] : request.trim().split("\\s+");
            if (f.length != 3 || !"PEDIDO".equals(f[0])) { out.println("ERRO pedido mal formado"); return; }
            long from = Long.parseLong(f[1]), to = Long.parseLong(f[2]);
            synchronized (lock) {
                long end = Math.min(to, lastSeq);
                if (from <= end) for (SensorRecord r : file.range(from, end)) out.println(r.toLine());
            }
            out.println("FIM");
        } catch (Exception e) { event("ERRO;p" + k + ";TCP=" + e.getMessage()); }
    }

    private static void subscribe(IpfsPubSub ipfs, String topic, Consumer<String> handler) {
        Thread t = new Thread(() -> { while (!Thread.currentThread().isInterrupted()) try { ipfs.subscribe(topic, handler); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); } catch (Exception e) { try { Thread.sleep(2000); } catch (InterruptedException x) { Thread.currentThread().interrupt(); } } }, "sub-" + topic);
        t.setDaemon(true); t.start();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) { System.err.println("Uso: PartitionNode <k> <i> [N]"); return; }
        int k = Integer.parseInt(args[0]), i = Integer.parseInt(args[1]), n = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        IpfsPubSub ipfs = new IpfsPubSub("http://127.0.0.1:5001/api/v0");
        PartitionNode node = new PartitionNode(k, i, n, "127.0.0.1", ipfs);
        subscribe(ipfs, Wire.topicIn(k), node::onIn); subscribe(ipfs, Wire.topicData(k), node::onBatch); subscribe(ipfs, Wire.topicElection(k), node::onElection);
        node.scheduler.scheduleAtFixedRate(node::tick, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        try (ServerSocket ss = new ServerSocket(Wire.nodePort(k, i))) { node.serve(ss); }
    }
}
