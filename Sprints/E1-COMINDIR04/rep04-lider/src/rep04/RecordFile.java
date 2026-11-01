package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Ficheiro de texto com um {@link SensorRecord} por linha. */
public class RecordFile {
    private final Path path;

    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);
    }

    // Acrescenta o registo completo ao ficheiro; pode também registar intervalos de escrita para os ensaios.
    public synchronized void append(SensorRecord r) throws IOException {
        String trace = System.getProperty("rep04.traceWrites");
        long start = trace == null ? 0 : System.nanoTime();
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        long end = trace == null ? 0 : System.nanoTime();
        if (trace != null) {
            // Intervalos do mesmo JVM: end(A) < start(B) prova que A foi escrito antes de B.
            // A sincronização abaixo protege apenas o ficheiro de diagnóstico, após o append.
            synchronized (RecordFile.class) {
                try {
                    Files.writeString(Paths.get(trace), java.time.Instant.now() + ";" + start + ";" + end
                            + ";" + path.getFileName() + ";" + r.toLine() + System.lineSeparator(),
                            StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    System.err.println("Falha no diagnóstico de escrita: " + e.getMessage());
                }
            }
        }
    }

    public synchronized long lastSeq() throws IOException {
        SensorRecord r = last();
        return r == null ? 0 : r.getSeq();
    }

    // Lê o último registo persistido, incluindo seq, L e V, para retomar o estado depois de um reinício.
    public synchronized SensorRecord last() throws IOException {
        if (!Files.exists(path)) return null;
        SensorRecord last = null;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) last = SensorRecord.fromLine(line);
        }
        return last;
    }

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
