package raft04;

import java.time.LocalDateTime;

/** Registo imutável, incluindo uma cópia do conhecimento causal do líder. */
public final class SensorRecord {
    private final long seq;
    private final long term;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    // Valida os dados e guarda o registo com uma cópia do vetor.
    public SensorRecord(long seq, long term, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        if (seq < 1 || term < 1 || lamport < 1) {
            throw new IllegalArgumentException("seq, termo e L devem ser positivos");
        }
        if (sensorId == null || sensorId.isBlank() || sensorId.contains(";")
                || sensorId.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("Sensor inválido");
        }
        if (!Double.isFinite(temperature)) {
            throw new IllegalArgumentException("Temperatura inválida");
        }
        try {
            LocalDateTime.parse(timestamp);
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("Instante inválido", e);
        }
        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector.copy();
    }

    public static SensorRecord now(long seq, long term, String sensor, double temp,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, term, sensor, temp,
                LocalDateTime.now().withNano(0).toString(), lamport, vector);
    }

    // seq;termo;sensor;temperatura;instante;L;V
    public String toLine() {
        return seq + ";" + term + ";" + sensorId + ";" + temperature + ";"
                + timestamp + ";" + lamport + ";" + vector;
    }

    public static SensorRecord fromLine(String line) {
        String[] f = line.trim().split(";", -1);
        if (f.length != 7) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(
                Long.parseLong(f[0]),
                Long.parseLong(f[1]),
                f[2],
                Double.parseDouble(f[3]),
                f[4],
                Long.parseLong(f[5]),
                VectorClock.parse(f[6], f[6].split(",", -1).length)
        );
    }

    public static SensorRecord fromLine(String line, int n) {
        SensorRecord r = fromLine(line);
        if (r.vector.size() != n) {
            throw new IllegalArgumentException("Dimensão do vetor diferente de N=" + n);
        }
        return r;
    }

    public long getSeq() { return seq; }
    public long getTerm() { return term; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector.copy(); }
}