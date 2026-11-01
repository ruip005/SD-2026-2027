package rep04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Mensagem {@code LOTE}: heartbeat do líder e, ao mesmo tempo, lote com os registos do intervalo.
 *
 * <p>Formato (várias linhas, separadas por {@code \n}):</p>
 * <pre>
 * LOTE;&lt;endereço&gt;;&lt;porto&gt;;&lt;inc&gt;;&lt;hb&gt;;&lt;ultimoSeq&gt;;&lt;n&gt;
 * &lt;seq&gt;;&lt;sensor&gt;;&lt;temperatura&gt;;&lt;instante&gt;;&lt;L&gt;;&lt;V&gt;
 * ...
 * </pre>
 *
 * <p>O cabeçalho tem {@code n} = número de linhas de registo que se seguem; um lote vazio
 * ({@code n} = 0) é só cabeçalho e funciona como heartbeat puro.</p>
 */
public final class Batch {
    public static final String TIPO = "LOTE";

    private final String address;
    private final int port;
    private final long inc;
    private final long hb;
    private final long lastSeq;
    private final List<SensorRecord> records;

    // Valida os campos do cabeçalho e guarda uma cópia imutável da lista de registos.
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

    /**
     * Produz a mensagem {@code LOTE}.
     *
     * @return cabeçalho seguido de uma linha por registo, no formato de {@link SensorRecord#toLine()}
     */
    // Escreve o cabeçalho com n = número de registos e depois cada registo numa linha própria.
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

    /**
     * Interpreta uma mensagem {@code LOTE} completa.
     *
     * @param msg mensagem recebida
     * @param n   número de partições N, para validar a dimensão do vetor de cada registo; com
     *            {@code n <= 0} a dimensão não é validada (a réplica não conhece N)
     * @return o lote
     * @throws IllegalArgumentException se o cabeçalho for inválido ou se o número de linhas de
     *                                  registo não coincidir com o {@code n} do cabeçalho
     */
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

    /**
     * Interpreta apenas o cabeçalho de uma mensagem {@code LOTE} (usado pelo produtor).
     *
     * @param msg mensagem recebida
     * @return lote com os campos do cabeçalho e lista de registos vazia
     * @throws IllegalArgumentException se o cabeçalho for inválido
     */
    // Lê só a primeira linha; as linhas de registo, se existirem, são ignoradas.
    public static Batch decodeHeader(String msg) {
        String[] h = header(splitLines(msg)[0]);
        parseCount(h[6]);
        return new Batch(h[1], Integer.parseInt(h[2]), Long.parseLong(h[3]), Long.parseLong(h[4]),
                Long.parseLong(h[5]), List.of());
    }

    // Divide a mensagem em linhas, aceitando \r\n e ignorando linhas vazias no fim.
    private static String[] splitLines(String msg) {
        if (msg == null || msg.isBlank()) throw new IllegalArgumentException("LOTE vazio");
        return msg.strip().split("\r?\n");
    }

    // Valida o tipo e o número de campos da linha de cabeçalho.
    private static String[] header(String line) {
        String[] h = line.trim().split(";", -1);
        if (h.length != 7 || !TIPO.equals(h[0])) {
            throw new IllegalArgumentException("Cabeçalho de LOTE mal formado: " + line);
        }
        return h;
    }

    // Lê o campo n do cabeçalho, que não pode ser negativo.
    private static int parseCount(String s) {
        int count = Integer.parseInt(s);
        if (count < 0) throw new IllegalArgumentException("n negativo no LOTE: " + count);
        return count;
    }

    // Devolve o endereço onde o líder atende pedidos de recuperação por TCP.
    public String getAddress() { return address; }
    // Devolve o porto onde o líder atende pedidos de recuperação por TCP.
    public int getPort() { return port; }
    // Devolve a incarnação do líder (instante de arranque em milissegundos).
    public long getInc() { return inc; }
    // Devolve o número do heartbeat dentro da incarnação, a partir de 1.
    public long getHb() { return hb; }
    // Devolve o seq mais recente atribuído pelo líder quando o lote foi fechado.
    public long getLastSeq() { return lastSeq; }
    // Devolve os registos do lote, pela ordem em que foram acumulados; a lista não é alterável.
    public List<SensorRecord> getRecords() { return records; }
}
