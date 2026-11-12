package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Ficheiro de texto com um {@link SensorRecord} por linha. */
public class RecordFile {
    private final Path path;

    public RecordFile(String fileName) { path = Paths.get(fileName); }

    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized long lastSeq() throws IOException {
        SensorRecord r = last();
        return r == null ? 0 : r.getSeq();
    }

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
        if (!Files.exists(path)) return out;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            SensorRecord r = SensorRecord.fromLine(line);
            if (r.getSeq() >= from && r.getSeq() <= to) out.add(r);
        }
        return out;
    }

    /** Substitui atomically os registos a partir de {@code from}, inclusive. */
    public synchronized void replaceFrom(long from, List<SensorRecord> replacement) throws IOException {
        if (from < 1) throw new IllegalArgumentException("seq inicial inválido: " + from);
        StringBuilder text = new StringBuilder();
        for (SensorRecord r : range(1, from - 1)) {
            text.append(r.toLine()).append(System.lineSeparator());
        }
        for (SensorRecord r : replacement) {
            if (r.getSeq() < from) throw new IllegalArgumentException("Registo antes do corte");
            text.append(r.toLine()).append(System.lineSeparator());
        }
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(tmp, text.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
