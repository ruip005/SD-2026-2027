package rep02;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Ficheiro de texto com um {@link SensorRecord} por linha.
 *
 * <p>Todos os métodos são {@code synchronized} no próprio objeto, pelo que uma leitura feita por
 * {@link #range(long, long)} nunca acontece a meio de um {@link #append(SensorRecord)} da mesma
 * instância. Herdada da COMINDIR01 e estendida com {@code range}.</p>
 *
 * <p><strong>Porquê ficheiro de texto e {@code List}</strong>: o ficheiro é a memória persistente do
 * processo (sobrevive a reinícios e serve a recuperação). {@link #range(long, long)} devolve uma
 * {@code List} porque o resultado é percorrido uma vez, por ordem, para ser enviado; não é preciso
 * procurar por chave (para isso seria um mapa).</p>
 *
 * <p><strong>O que pode correr mal</strong>: {@code range} e {@code lastSeq} leem o ficheiro todo,
 * o custo cresce com o histórico (linear); a escrita não faz {@code fsync}, por isso uma falha de
 * energia pode perder as últimas linhas; ficheiros de um ensaio anterior fazem o líder continuar a
 * numeração antiga (apagar entre ensaios).</p>
 */
public class RecordFile {
    /** Caminho do ficheiro de registos. */
    private final Path path;

    /**
     * Associa a instância a um ficheiro (que não precisa de existir ainda).
     *
     * @param fileName caminho do ficheiro
     */
    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);
    }

    /**
     * Acrescenta um registo ao fim do ficheiro, criando-o se for preciso.
     *
     * @param r registo a escrever
     * @throws IOException se a escrita falhar
     */
    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * Devolve o número de sequência do último registo guardado.
     *
     * @return o último {@code seq}, ou 0 se o ficheiro não existir ou estiver vazio
     * @throws IOException se a leitura falhar
     */
    public synchronized long lastSeq() throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        long last = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line).getSeq();
            }
        }
        return last;
    }

    /**
     * Devolve os registos cujo número de sequência está no intervalo fechado {@code [from, to]},
     * pela ordem do ficheiro. Percorre o ficheiro todo, pelo que o custo é linear no histórico.
     *
     * @param from primeiro número de sequência pedido (inclusivo)
     * @param to   último número de sequência pedido (inclusivo)
     * @return lista possivelmente vazia (ficheiro inexistente, intervalo vazio ou fora do ficheiro)
     * @throws IOException se a leitura falhar
     */
    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(path)) {
            return out;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            SensorRecord r = SensorRecord.fromLine(line);
            if (r.getSeq() >= from && r.getSeq() <= to) {
                out.add(r);
            }
        }
        return out;
    }
}
