package rep04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Mensagem {@code LOTE}: heartbeat do líder e, ao mesmo tempo, lote com os registos do intervalo. */
public final class Batch {
    public static final String TIPO = "LOTE";

    private final String address;
    private final int port;
    private final long inc;
    private final long hb;
    private final long lastSeq;
    private final List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        if (address == null || address.isBlank() || address.contains(";") || address.contains("\n"))
            throw new IllegalArgumentException("Endereço inválido");
        if (port < 1 || port > 65535) throw new IllegalArgumentException("Porto inválido: " + port);
        if (hb < 1) throw new IllegalArgumentException("hb deve ser positivo");
        if (lastSeq < 0) throw new IllegalArgumentException("ultimoSeq negativo");
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append(TIPO).append(';').append(address).append(';').append(port).append(';')
                .append(inc).append(';').append(hb).append(';').append(lastSeq).append(';')
                .append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    // Lê o cabeçalho, confirma que há exatamente n linhas de registo e reconstrói cada registo.
    public static Batch decode(String msg, int n) {
        String[] lines = splitLines(msg);
        String[] h = header(lines[0]);
        int count = parseCount(h[6]);
        if (lines.length - 1 != count) {
            throw new IllegalArgumentException("LOTE com " + (lines.length - 1)
                    + " linhas de registo, mas o cabeçalho indica n=" + count);
        }
        List<SensorRecord> records = new ArrayList<>(count);
        for (int i = 1; i < lines.length; i++) {
            records.add(n > 0 ? SensorRecord.fromLine(lines[i], n) : SensorRecord.fromLine(lines[i]));
        }
        return new Batch(h[1], Integer.parseInt(h[2]), Long.parseLong(h[3]), Long.parseLong(h[4]),
                Long.parseLong(h[5]), records);
    }

    // Lê só a primeira linha; as linhas de registo, se existirem, são ignoradas.
    public static Batch decodeHeader(String msg) {
        String[] h = header(splitLines(msg)[0]);
        parseCount(h[6]);
        return new Batch(h[1], Integer.parseInt(h[2]), Long.parseLong(h[3]), Long.parseLong(h[4]),
                Long.parseLong(h[5]), List.of());
    }

    private static String[] splitLines(String msg) {
        if (msg == null || msg.isBlank()) throw new IllegalArgumentException("LOTE vazio");
        return msg.strip().split("\r?\n");
    }

    private static String[] header(String line) {
        String[] h = line.trim().split(";", -1);
        if (h.length != 7 || !TIPO.equals(h[0])) {
            throw new IllegalArgumentException("Cabeçalho de LOTE mal formado: " + line);
        }
        return h;
    }

    private static int parseCount(String s) {
        int count = Integer.parseInt(s);
        if (count < 0) throw new IllegalArgumentException("n negativo no LOTE: " + count);
        return count;
    }

    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getInc() { return inc; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public List<SensorRecord> getRecords() { return records; }
}
