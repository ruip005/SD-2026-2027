package raft04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Mensagem {@code LOTE}: heartbeat do líder e lote com os registos do intervalo. */
public final class Batch {
    public static final String TIPO = "LOTE";

    private final String address;
    private final int port;
    private final long term;
    private final int leader;
    private final long hb;
    private final long lastSeq;
    private final long lastTerm;
    private final List<SensorRecord> records;

    public Batch(String address, int port, long term, int leader, long hb,
                 long lastSeq, long lastTerm, List<SensorRecord> records) {
        if (address == null || address.isBlank() || address.contains(";") || address.contains("\n")) {
            throw new IllegalArgumentException("Endereço inválido");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Porto inválido: " + port);
        }
        if (term < 1) {
            throw new IllegalArgumentException("Termo deve ser positivo");
        }
        if (leader < 1) {
            throw new IllegalArgumentException("Líder inválido: " + leader);
        }
        if (hb < 1) {
            throw new IllegalArgumentException("hb deve ser positivo");
        }
        if (lastSeq < 0) {
            throw new IllegalArgumentException("ultimoSeq negativo");
        }
        if (lastTerm < 0) {
            throw new IllegalArgumentException("ultimoTermo negativo");
        }

        this.address = address;
        this.port = port;
        this.term = term;
        this.leader = leader;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append(TIPO).append(';')
                .append(term).append(';')
                .append(leader).append(';')
                .append(address).append(';')
                .append(port).append(';')
                .append(hb).append(';')
                .append(lastSeq).append(';')
                .append(lastTerm).append(';')
                .append(records.size());

        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    // Lê o cabeçalho, confirma que há exatamente n linhas e reconstrói cada registo.
    public static Batch decode(String msg, int n) {
        String[] lines = splitLines(msg);
        String[] h = header(lines[0]);
        int count = parseCount(h[8]);

        if (lines.length - 1 != count) {
            throw new IllegalArgumentException("LOTE com " + (lines.length - 1)
                    + " linhas de registo, mas o cabeçalho indica n=" + count);
        }

        List<SensorRecord> records = new ArrayList<>(count);
        for (int i = 1; i < lines.length; i++) {
            records.add(n > 0
                    ? SensorRecord.fromLine(lines[i], n)
                    : SensorRecord.fromLine(lines[i]));
        }

        return new Batch(
                h[3],
                Integer.parseInt(h[4]),
                Long.parseLong(h[1]),
                Integer.parseInt(h[2]),
                Long.parseLong(h[5]),
                Long.parseLong(h[6]),
                Long.parseLong(h[7]),
                records
        );
    }

    // Lê apenas o cabeçalho; as linhas de registo, se existirem, são ignoradas.
    public static Batch decodeHeader(String msg) {
        String[] h = header(splitLines(msg)[0]);
        parseCount(h[8]);

        return new Batch(
                h[3],
                Integer.parseInt(h[4]),
                Long.parseLong(h[1]),
                Integer.parseInt(h[2]),
                Long.parseLong(h[5]),
                Long.parseLong(h[6]),
                Long.parseLong(h[7]),
                List.of()
        );
    }

    private static String[] splitLines(String msg) {
        if (msg == null || msg.isBlank()) {
            throw new IllegalArgumentException("LOTE vazio");
        }
        return msg.strip().split("\\r?\\n");
    }

    private static String[] header(String line) {
        String[] h = line.trim().split(";", -1);
        if (h.length != 9 || !TIPO.equals(h[0])) {
            throw new IllegalArgumentException("Cabeçalho de LOTE mal formado: " + line);
        }
        return h;
    }

    private static int parseCount(String s) {
        int count = Integer.parseInt(s);
        if (count < 0) {
            throw new IllegalArgumentException("n negativo no LOTE: " + count);
        }
        return count;
    }

    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getTerm() { return term; }
    public int getLeader() { return leader; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public long getLastTerm() { return lastTerm; }
    public List<SensorRecord> getRecords() { return records; }
}