package rep02;

import java.time.LocalDateTime;

/**
 * Registo imutável de uma leitura de sensor, no formato textual
 * {@code seq;sensor;temperatura;instante}.
 *
 * <p>Esta classe é herdada da COMINDIR01 sem alterações de comportamento. O mesmo formato é
 * usado no ficheiro do líder, nos ficheiros das réplicas, na mensagem {@code REG} e nas linhas
 * da resposta TCP de recuperação, o que permite interpretar todas com {@link #fromLine(String)}.</p>
 */
public class SensorRecord {
    /** Número de sequência atribuído pelo líder. */
    private final long seq;
    /** Identificador do sensor (chave de partição). */
    private final String sensorId;
    /** Temperatura lida. */
    private final double temperature;
    /** Instante da leitura, em texto. */
    private final String timestamp;

    /**
     * Cria um registo com todos os campos explícitos.
     *
     * @param seq         número de sequência atribuído pelo líder da partição
     * @param sensorId    identificador do sensor (a chave de partição)
     * @param temperature temperatura lida, em graus Celsius
     * @param timestamp   instante da leitura em texto (ISO-8601, sem nanosegundos)
     */
    public SensorRecord(long seq, String sensorId, double temperature, String timestamp) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    /**
     * Cria um registo com o instante atual. Só o líder deve usar este método.
     *
     * @param seq         número de sequência do novo registo
     * @param sensorId    identificador do sensor
     * @param temperature temperatura lida
     * @return o novo registo, carimbado com a hora do sistema
     */
    public static SensorRecord now(long seq, String sensorId, double temperature) {
        return new SensorRecord(seq, sensorId, temperature, LocalDateTime.now().withNano(0).toString());
    }

    /**
     * Serializa o registo para texto. É o que se guarda em ficheiro e o que viaja pela rede.
     *
     * @return a linha {@code seq;sensor;temperatura;instante}
     */
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp;
    }

    /**
     * Interpreta uma linha produzida por {@link #toLine()}.
     *
     * @param line linha com exatamente quatro campos separados por {@code ;}
     * @return o registo correspondente
     * @throws IllegalArgumentException se a linha não tiver quatro campos
     * @throws NumberFormatException    se o número de sequência ou a temperatura não forem numéricos
     */
    public static SensorRecord fromLine(String line) {
        String[] f = line.trim().split(";");
        if (f.length != 4) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), f[3]);
    }

    /**
     * Devolve o número de sequência.
     *
     * @return o número de sequência do registo
     */
    public long getSeq() { return seq; }

    /**
     * Devolve o identificador do sensor.
     *
     * @return o identificador do sensor
     */
    public String getSensorId() { return sensorId; }

    /**
     * Devolve a temperatura.
     *
     * @return a temperatura lida
     */
    public double getTemperature() { return temperature; }

    /**
     * Devolve o instante da leitura.
     *
     * @return o instante da leitura em texto
     */
    public String getTimestamp() { return timestamp; }
}
