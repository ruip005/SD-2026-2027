package rep03;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Testes sem dependências externas; executados por verificar.ps1. */
public final class ClockIntegrationTest {
    private static int checks;
    private static final String TIME = "2026-10-25T12:00:00";
    private static void check(boolean condition, String message) {
        checks++;
        if (!condition) throw new AssertionError(message);
    }
    private static VectorClock v(String s) { return VectorClock.parse(s, 3); }
    private static SensorRecord record(long seq, String sensor, double temp, long l, String vector) {
        return new SensorRecord(seq, sensor, temp, TIME, l, v(vector));
    }
    private static void rejected(Runnable action, String message) {
        boolean rejected = false;
        try { action.run(); } catch (IllegalArgumentException e) { rejected = true; }
        check(rejected, message);
    }
    private static RecordFile file(Path dir, int k) { return new RecordFile(dir.resolve("lider-p" + k + ".txt").toString()); }

    private static void vectors() {
        check(v("0,1,0").compare(v("0,1,1")) == VectorClock.Order.BEFORE, "before");
        check(v("0,1,1").compare(v("0,1,0")) == VectorClock.Order.AFTER, "after");
        check(v("1,0,0").compare(v("0,1,1")) == VectorClock.Order.CONCURRENT, "concurrent");
        check(v("0,1,1").compare(v("0,1,1")) == VectorClock.Order.EQUAL, "equal");
        VectorClock source = v("0,1,0");
        SensorRecord r = new SensorRecord(1, "S02", 29.3, TIME, 1, source);
        source.tick(1);
        r.getVector().tick(0);
        check(r.getVector().equals(v("0,1,0")), "registo imutável");
        check(SensorRecord.fromLine(r.toLine(), 3).toLine().equals(r.toLine()), "round trip");
        rejected(() -> VectorClock.parse("1,2,3,4", 3), "dimensão");
        rejected(() -> v("0,-1,0"), "negativo");
        rejected(() -> v("0,1,"), "componente vazia");
        rejected(() -> v("0,1,0").merge(new VectorClock(4)), "merge dimensão");
    }

    private static void figureAndRecovery(Path root) throws Exception {
        Path dir = Files.createDirectory(root.resolve("figura"));
        Partitioner part = new Partitioner(3);
        List<String> data = new ArrayList<>();
        PartitionLeader[] leaders = new PartitionLeader[3];
        for (int k = 0; k < 3; k++) leaders[k] = new PartitionLeader(k, part, file(dir, k), "127.0.0.1", 7000+k,
                (topic, msg) -> data.add(msg));
        List<String> inputs = new ArrayList<>();
        Producer producer = new Producer(part, (topic, msg) -> inputs.add(msg));
        producer.publicar("S02", 29.3);
        check(inputs.get(0).equals("IN;S02;29.3;0;0,0,0"), "produtor sem conhecimento");
        check(leaders[1].onIn(inputs.get(0)), "evento a");
        AlarmProcessor processor = new AlarmProcessor(3, (topic, msg) -> {
            check(topic.equals("rep03-in-p2"), "destino alarme");
            check(leaders[2].onIn(msg), "evento c");
        });
        check(processor.onReg(1, data.get(0)), "processar a");
        check(!processor.onReg(1, data.get(0)), "duplicado sem alarme");
        check(!processor.onReg(2, data.get(1)), "sem alarmes sobre alarmes");
        check(leaders[0].onIn(Wire.in("S01", 21, 0, new VectorClock(3))), "evento b");
        check(leaders[0].onIn(Wire.in("S04", 22.5, 0, new VectorClock(3))), "evento d");
        check(file(dir, 1).last().getVector().equals(v("0,1,0")), "vetor a");
        check(file(dir, 0).last().getVector().equals(v("2,0,0")), "vetor d");
        SensorRecord c = file(dir, 2).last();
        check(c.getLamport() == 2 && c.getVector().equals(v("0,1,1")), "carimbos c");
        CausalCheck.Result report = CausalCheck.verificar(3, dir);
        check(report.ok() && report.before == 1 && report.after == 0 && report.concurrent == 4
                && report.distinctLamport == 2, "relatório figura: 1 causal, 4 concorrentes, 2 L distinto");
        List<String> view = Files.readAllLines(dir.resolve("vista-lamport.txt"));
        check(view.get(0).startsWith("0;1;") && view.get(3).startsWith("2;1;"), "ordem por L,k");
        PartitionLeader restarted = new PartitionLeader(2, part, file(dir,2), "127.0.0.1",7002, (t,m) -> {});
        check(restarted.onIn(Wire.in("ALARM-S02", 30, 0, new VectorClock(3))), "reinício");
        SensorRecord next = file(dir,2).last();
        check(next.getSeq() == 2 && next.getLamport() == 3 && next.getVector().equals(v("0,1,2")), "retoma L e V");
        check(!restarted.onIn("IN;ALARM-S02;30;0;0,0,0,0"), "rejeita N diferente");
        check(!restarted.onIn("IN;ALARM-S02;NaN;0;0,0,0"), "rejeita NaN");
        check(!restarted.onIn("IN;ALARM-S02;30;0;0,0,99"), "rejeita conhecimento próprio impossível");
        List<String> recovered = restarted.processarPedido("PEDIDO 1 2");
        check(recovered.size() == 3 && recovered.get(0).equals(c.toLine()) && recovered.get(2).equals("FIM"), "recuperação conserva carimbos");
        PartitionReplica.Particao replica = new PartitionReplica.Particao(2,
                new RecordFile(dir.resolve("replica-1-p2.txt").toString()), (host,port,from,to,consumer) -> {
                    for (String line : restarted.processarPedido("PEDIDO " + from + " " + to))
                        if (!line.equals("FIM")) consumer.accept(line);
                });
        replica.onReg(Wire.reg("127.0.0.1",7002,next));
        replica.recuperar();
        check(replica.getUltimo() == 2 && replica.getRetidos() == 0, "réplica recupera lacuna");
        check(Files.readAllLines(dir.resolve("replica-1-p2.txt")).equals(recovered.subList(0,2)), "ficheiros idênticos");
    }

    private static void accumulatedKnowledge() {
        List<String> alarms = new ArrayList<>();
        AlarmProcessor p = new AlarmProcessor(3, (t,m) -> alarms.add(m));
        p.onReg(0,Wire.reg("host",7000,record(2,"S01",20,2,"2,0,0")));
        p.onReg(0,Wire.reg("host",7000,record(1,"S01",20,1,"1,0,0")));
        p.onReg(1,Wire.reg("host",7001,record(1,"S02",28,1,"0,1,0")));
        check(alarms.size() == 1 && alarms.get(0).equals("IN;ALARM-S02;28.0;2;2,1,0"), "acumula todos os observados; limiar inclusivo");
        check(!p.onReg(0,Wire.reg("host",7000,record(1,"S01",30,1,"1,0,0"))), "duplicado reordenado");
        check(alarms.size() == 1, "duplicado não republica");
    }

    private static void concurrentLeader(Path root) throws Exception {
        Path dir = Files.createDirectory(root.resolve("concorrencia"));
        Partitioner part = new Partitioner(3);
        List<String> messages = Collections.synchronizedList(new ArrayList<>());
        PartitionLeader leader = new PartitionLeader(0,part,file(dir,0),"host",7000,(t,m) -> messages.add(m));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < 80; i++) futures.add(pool.submit(() -> leader.onIn("IN;S01;20;0;0,0,0")));
            for (Future<Boolean> f : futures) check(f.get(), "entrada concorrente aceite");
        } finally { pool.shutdownNow(); }
        List<SensorRecord> records = file(dir,0).range(1,100);
        check(records.size() == 80 && messages.size() == 80, "sem perdas na secção sincronizada");
        for (int i = 0; i < records.size(); i++) {
            SensorRecord r = records.get(i);
            check(r.getSeq() == i+1 && r.getLamport() == i+1 && r.getVector().get(0) == i+1, "seq/L/V atómicos");
        }
        RecordFile broken = new RecordFile(dir.resolve("missing/file.txt").toString());
        PartitionLeader failed = new PartitionLeader(0,part,broken,"host",7000,(t,m) -> { throw new AssertionError("publicou sem gravar"); });
        check(!failed.onIn("IN;S01;20;0;0,0,0") && failed.getUltimoSeq() == 0, "falha de append não confirma estado");
    }

    private static void tcpRecovery(Path root) throws Exception {
        Path dir = Files.createDirectory(root.resolve("tcp"));
        PartitionLeader leader = new PartitionLeader(1,new Partitioner(3),file(dir,1),"127.0.0.1",7001,(t,m) -> {});
        leader.onIn("IN;S02;29.3;7;2,0,1");
        try (java.net.ServerSocket server = new java.net.ServerSocket(0, 10, java.net.InetAddress.getLoopbackAddress())) {
            Thread service = new Thread(() -> {
                try { leader.atender(server.accept()); }
                catch (IOException e) { throw new java.io.UncheckedIOException(e); }
            });
            service.setDaemon(true);
            service.start();
            List<String> recovered = new ArrayList<>();
            new TcpRecuperador().pedir("127.0.0.1",server.getLocalPort(),1,1,recovered::add);
            service.join(6000);
            check(!service.isAlive(), "ligação TCP terminou");
            check(recovered.size() == 1, "recebe linha TCP");
            SensorRecord r = SensorRecord.fromLine(recovered.get(0),3);
            check(r.getLamport() == 8 && r.getVector().equals(v("2,1,1")), "TCP real conserva L e V");
        }
    }

    private static void checkerFailures(Path root) throws Exception {
        Path dir = Files.createDirectory(root.resolve("falhas"));
        Files.writeString(dir.resolve("lider-p0.txt"),record(1,"S01",20,5,"1,0,0").toLine());
        Files.writeString(dir.resolve("lider-p1.txt"),record(1,"S02",29.3,1,"0,1,0").toLine());
        Files.writeString(dir.resolve("lider-p2.txt"),record(1,"ALARM-S02",29.3,1,"0,1,1").toLine());
        CausalCheck.Result r = CausalCheck.verificar(3,dir);
        check(!r.ok() && r.violations == 1, "deteta violação Lamport");
        Files.writeString(dir.resolve("lider-p2.txt"),"linha corrompida");
        check(!CausalCheck.verificar(3,dir).ok(), "deteta linha inválida");
        Files.delete(dir.resolve("lider-p1.txt"));
        check(!CausalCheck.verificar(3,dir).ok(), "deteta ficheiro em falta");
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        Files.createDirectories(root);
        vectors();
        figureAndRecovery(root);
        accumulatedKnowledge();
        concurrentLeader(root);
        tcpRecovery(root);
        checkerFailures(root);
        System.out.println("OK: " + checks + " verificações");
    }
}
