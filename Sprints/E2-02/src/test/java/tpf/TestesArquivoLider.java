package tpf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class TestesArquivoLider {
    public static void main(String[] args) throws Exception {
        Path pasta = Files.createTempDirectory("arquivo-e2-02-");
        Path diario = pasta.resolve("registos.log");
        Path selados = pasta.resolve("selados");
        Path chave = pasta.resolve("master.key");

        try (ArquivoLider arquivo = new ArquivoLider(pasta)) {
            verifica(arquivo.registar("rui", "arquivo", "Olá;\ntexto") == 1, "Primeiro LSN");
            Registo lido = Registo.daLinha(Files.readAllLines(diario).get(0));
            verifica(lido.lsn() == 1 && lido.texto().equals("Olá;\ntexto"), "Registo no diário antes da resposta");
            esperaChunks(selados, 1, Wire.SELAGEM_MS + 3000L);
            verifica(arquivo.registar("rui", "arquivo", "registo por selar") == 2, "Segundo LSN");
        }

        verifica(Files.size(chave) == 32, "Tamanho da chave");
        if (Files.getFileAttributeView(chave, PosixFileAttributeView.class) != null) {
            verifica(Files.getPosixFilePermissions(chave).equals(PosixFilePermissions.fromString("rw-------")),
                    "Permissões POSIX da chave");
        } else {
            var acl = Files.getFileAttributeView(chave, AclFileAttributeView.class);
            verifica(acl.getAcl().size() == 1 && acl.getAcl().get(0).principal().equals(acl.getOwner()),
                    "ACL da chave apenas para o dono");
        }

        Files.writeString(diario, "3;linha cortada", StandardOpenOption.APPEND);
        try (ArquivoLider arquivo = new ArquivoLider(pasta)) {
            for (int i = 3; i <= 502; i++) {
                verifica(arquivo.registar("rui", "arquivo", "Registo de rajada: ".repeat(12)) == i,
                        "LSN após reinício: " + i);
            }
            esperaChunks(selados, 3, 0);
            // Dá ao último chunk parcial tempo para selar antes de comparar todos os LSN.
            long limite = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS
                    .toNanos(Wire.SELAGEM_MS + 3000L);
            while (System.nanoTime() < limite && !lerChunks(selados, chave).containsKey(502L)) {
                Thread.sleep(50);
            }
        }

        Map<Long, Registo> reconstruidos = lerChunks(selados, chave);
        List<Registo> esperados = Files.readAllLines(diario).stream().map(Registo::daLinha).toList();
        verifica(esperados.size() == 502 && esperados.equals(List.copyOf(reconstruidos.values())),
                "Chunks reconstruídos iguais ao diário");
        List<String> eventos = Files.readAllLines(pasta.resolve("eventos-lider.txt"));
        verifica(eventos.stream().filter(linha -> linha.contains(";REGISTO;")).count() == 502,
                "Eventos REGISTO");
        verifica(eventos.stream().filter(linha -> linha.contains(";SELADO;")).count() == contarChunks(selados),
                "Eventos SELADO");

        Files.write(chave, new byte[31]);
        try (ArquivoLider ignorado = new ArquivoLider(pasta)) {
            throw new AssertionError("Foi aceite uma chave de tamanho inválido");
        } catch (IOException erro) {
            verifica(erro.getMessage().contains("32 bytes"), "Erro ao abrir a chave inválida");
        }
        System.out.println("Testes do arquivo passaram. Dados de teste em " + pasta);
    }

    private static Map<Long, Registo> lerChunks(Path pasta, Path chave) throws Exception {
        ChunkCrypto crypto = new ChunkCrypto(Files.readAllBytes(chave));
        Map<Long, Registo> resultado = new TreeMap<>();
        try (Stream<Path> ficheiros = Files.list(pasta)) {
            for (Path bin : ficheiros.filter(p -> p.toString().endsWith(".bin")).toList()) {
                String nome = bin.getFileName().toString();
                String id = nome.substring(0, nome.length() - 4);
                byte[] cifrado = Files.readAllBytes(bin);
                verifica(!new String(cifrado, StandardCharsets.ISO_8859_1).contains("Registo de rajada"),
                        "Cifrado sem texto de teste visível");
                byte[] dados = crypto.open(Sealed.dosFicheiros(id,
                        Files.readString(pasta.resolve(id + ".hdr")), cifrado));
                verifica(dados.length % Wire.PAD_BYTES == 0, "Padding");
                for (Registo registo : Chunk.ler(dados)) {
                    verifica(resultado.putIfAbsent(registo.lsn(), registo) == null, "LSN duplicado no chunk");
                }
            }
        }
        return resultado;
    }

    private static void esperaChunks(Path pasta, int minimo, long esperaMs) throws Exception {
        long limite = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(esperaMs);
        long total = contarChunks(pasta);
        while (total < minimo && System.nanoTime() < limite) {
            Thread.sleep(50);
            total = contarChunks(pasta);
        }
        verifica(total >= minimo, "Número de chunks esperado: " + minimo);
    }

    private static long contarChunks(Path pasta) throws IOException {
        try (Stream<Path> ficheiros = Files.list(pasta)) {
            return ficheiros.filter(p -> p.toString().endsWith(".bin")).count();
        }
    }

    private static void verifica(boolean condicao, String descricao) {
        if (!condicao) {
            throw new AssertionError(descricao);
        }
    }
}
