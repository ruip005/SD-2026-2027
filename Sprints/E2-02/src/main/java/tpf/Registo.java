package tpf;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

public record Registo(long lsn, Instant instante, String autor, String canal, String texto) {
    String linha() {
        return lsn + ";" + instante + ";" + autor + ";" + canal + ";"
                + Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
    }

    static Registo daLinha(String linha) {
        String[] campos = linha.split(";", 5);
        if (campos.length != 5) {
            throw new IllegalArgumentException("Linha de registo inválida");
        }
        return new Registo(Long.parseLong(campos[0]), Instant.parse(campos[1]), campos[2], campos[3],
                new String(Base64.getDecoder().decode(campos[4]), StandardCharsets.UTF_8));
    }
}
