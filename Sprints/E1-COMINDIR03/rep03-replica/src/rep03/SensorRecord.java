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
    // Valida os dados e guarda o registo com uma cópia do vetor, preservando os carimbos recebidos.
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
    // Cria um registo com o instante físico atual e com seq, L e V fornecidos pelo líder.
    public static SensorRecord now(long seq, String sensor, double temp, long l, VectorClock v) {
        return new SensorRecord(seq, sensor, temp, LocalDateTime.now().withNano(0).toString(), l, v);
    }
    // Serializa o registo como seq;sensor;temperatura;instante;L;V, para persistência e recuperação TCP.
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }
    /** Interpreta a dimensão transportada; o líder e o analisador validam-na contra N. */
    // Lê uma linha com seis campos e reconstrói o registo; a versão com n também verifica a dimensão do vetor.
    public static SensorRecord fromLine(String line) {
        String[] f = line.trim().split(";", -1);
        if (f.length != 6) throw new IllegalArgumentException("Registo mal formado: " + line);
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), f[3],
                Long.parseLong(f[4]), VectorClock.parse(f[5], f[5].split(",", -1).length));
    }
    // Lê uma linha com seis campos e reconstrói o registo; a versão com n também verifica a dimensão do vetor.
    public static SensorRecord fromLine(String line, int n) {
        SensorRecord r = fromLine(line);
        if (r.vector.size() != n) throw new IllegalArgumentException("Dimensão do vetor diferente de N=" + n);
        return r;
    }
    // Devolve o número de sequência atribuído pelo líder da partição.
    public long getSeq() { return seq; }
    // Devolve o identificador da leitura ou do sensor de alarme.
    public String getSensorId() { return sensorId; }
    // Devolve a temperatura guardada no registo.
    public double getTemperature() { return temperature; }
    // Devolve o instante físico guardado na criação do registo.
    public String getTimestamp() { return timestamp; }
    // Devolve o carimbo de Lamport atribuído a este registo.
    public long getLamport() { return lamport; }
    // Devolve uma cópia defensiva do vetor; quem consulta não consegue alterar o registo guardado.
    public VectorClock getVector() { return vector.copy(); }
}
