package rep03;

import java.time.LocalDateTime;

/** Registo imutável, incluindo uma cópia do conhecimento causal do líder. */
public final class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;
    public SensorRecord(long seq, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        if (seq < 1 || lamport < 1) throw new IllegalArgumentException("seq e L devem ser positivos");
        if (sensorId == null || sensorId.isBlank() || sensorId.contains(";")
                || sensorId.chars().anyMatch(Character::isWhitespace))
            throw new IllegalArgumentException("Sensor inválido");
        if (!Double.isFinite(temperature)) throw new IllegalArgumentException("Temperatura inválida");
        try { LocalDateTime.parse(timestamp); }
        catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("Instante inválido", e);
        }
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector.copy();
    }
    public static SensorRecord now(long seq, String sensor, double temp, long l, VectorClock v) {
        return new SensorRecord(seq, sensor, temp, LocalDateTime.now().withNano(0).toString(), l, v);
    }
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }
    /** Interpreta a dimensão transportada; o líder e o analisador validam-na contra N. */
    public static SensorRecord fromLine(String line) {
        String[] f = line.trim().split(";", -1);
        if (f.length != 6) throw new IllegalArgumentException("Registo mal formado: " + line);
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), f[3],
                Long.parseLong(f[4]), VectorClock.parse(f[5], f[5].split(",", -1).length));
    }
    public static SensorRecord fromLine(String line, int n) {
        SensorRecord r = fromLine(line);
        if (r.vector.size() != n) throw new IllegalArgumentException("Dimensão do vetor diferente de N=" + n);
        return r;
    }
    public long getSeq() { return seq; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector.copy(); }
}
