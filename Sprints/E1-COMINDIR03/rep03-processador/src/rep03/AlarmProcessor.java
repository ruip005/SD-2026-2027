package rep03;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Acumula conhecimento causal sem gerar eventos locais de relógio. */
public final class AlarmProcessor {
    private final Partitioner partitioner;
    private final Publisher publisher;
    private final Object lock = new Object();
    private final List<Set<Long>> vistos = new ArrayList<>();
    private long lamport;
    private final VectorClock vector;

    public AlarmProcessor(int n, Publisher publisher) {
        partitioner = new Partitioner(n);
        this.publisher = publisher;
        vector = new VectorClock(n);
        for (int k = 0; k < n; k++) vistos.add(new HashSet<>());
    }

    /** Devolve true apenas quando uma leitura nova foi aceite. */
    public boolean onReg(int k, String msg) {
        SensorRecord r;
        try {
            if (k < 0 || k >= partitioner.count() || !"REG".equals(Wire.type(msg)))
                throw new IllegalArgumentException("Partição/tipo inválido");
            r = SensorRecord.fromLine(Wire.regParts(msg)[2], partitioner.count());
            if (partitioner.of(r.getSensorId()) != k || r.getVector().get(k) != r.getSeq())
                throw new IllegalArgumentException("Partição/seq incompatível com vetor");
        } catch (IllegalArgumentException e) {
            System.err.println("REJEITADO no processador: " + e.getMessage());
            return false;
        }
        if (r.getSensorId().startsWith("ALARM-")) return false;

        String alarm = null;
        int target = -1;
        synchronized (lock) {
            // Um conjunto por partição também trata duplicados após reordenação.
            if (!vistos.get(k).add(r.getSeq())) return false;
            lamport = Math.max(lamport, r.getLamport());
            vector.merge(r.getVector());
            if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
                String sensor = "ALARM-" + r.getSensorId();
                target = partitioner.of(sensor);
                // Captura um snapshot coerente; a publicação não retém o lock durante HTTP.
                alarm = Wire.in(sensor, r.getTemperature(), lamport, vector.copy());
            }
        }
        if (alarm != null) {
            try {
                publisher.publish(Wire.topicIn(target), alarm);
                System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " → p" + target);
            } catch (Exception e) {
                // PubSub best-effort: não existe recuperação no troço processador → líder.
                System.err.println("PUBLICAÇÃO DE ALARME FALHOU: " + e.getMessage());
            }
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: AlarmProcessor <N> [api]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        IpfsPubSub ipfs = new IpfsPubSub(args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0");
        AlarmProcessor processor = new AlarmProcessor(n, ipfs);
        List<Thread> threads = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            final int partition = k;
            Thread t = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        ipfs.subscribe(Wire.topicData(partition), msg -> processor.onReg(partition, msg));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        System.err.println("Subscrição p" + partition + " falhou: " + e.getMessage());
                    }
                    try { Thread.sleep(2000); }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "alarmes-p" + k);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) t.join();
    }
}
