package tpf;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

final class ArquivoLider implements AutoCloseable {
    private final Path pastaSelados;
    private final Path eventos;
    private final FileChannel diario;
    private final FileChannel bloqueio;
    private final ChunkCrypto crypto;
    private final ScheduledExecutorService agendador = Executors.newSingleThreadScheduledExecutor(tarefa -> {
        Thread thread = new Thread(tarefa, "selagem-lider");
        thread.setDaemon(true);
        return thread;
    });
    private long ultimoLsn;
    private Chunk aberto;
    private Sealed pendente;
    private ScheduledFuture<?> prazo;
    private boolean selagemIniciada;
    private boolean fechado;
    private boolean diarioInutilizavel;

    ArquivoLider(Path pasta) throws IOException, GeneralSecurityException {
        Path registos = pasta.resolve("registos.log");
        pastaSelados = pasta.resolve("selados");
        eventos = pasta.resolve("eventos-lider.txt");
        bloqueio = FileChannel.open(pasta.resolve(".lider.lock"), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        FileChannel canalDiario = null;
        try {
            if (bloqueio.tryLock() == null) {
                throw new IOException("Já existe um líder a usar " + registos);
            }
            canalDiario = FileChannel.open(registos, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            diario = canalDiario;
            crypto = new ChunkCrypto(ChaveMestra.carregar(pasta));
            Files.createDirectories(pastaSelados);
            reaplicar(registos);
        } catch (IOException | GeneralSecurityException | RuntimeException erro) {
            agendador.shutdownNow();
            fecharAposErro(canalDiario, erro);
            fecharAposErro(bloqueio, erro);
            throw erro;
        }
    }

    private static void fecharAposErro(FileChannel canal, Exception erro) {
        if (canal != null) {
            try {
                canal.close();
            } catch (IOException erroFecho) {
                erro.addSuppressed(erroFecho);
            }
        }
    }

    synchronized long registar(String autor, String canal, String texto) throws IOException {
        if (fechado || diarioInutilizavel) {
            throw new IOException("Diário indisponível");
        }
        if (selagemIniciada) {
            tentarSelar();
            if (aberto != null) {
                throw new IOException("Selagem pendente");
            }
        }
        long lsn = Math.incrementExact(ultimoLsn);
        Registo registo = new Registo(lsn, Instant.now(), autor, canal, texto);
        long inicio = diario.size();
        try {
            acrescentar(diario, registo.linha());
        } catch (IOException erro) {
            try {
                diario.truncate(inicio);
                diario.force(true);
            } catch (IOException erroRollback) {
                diarioInutilizavel = true;
                erro.addSuppressed(erroRollback);
            }
            throw erro;
        }
        ultimoLsn = lsn;
        tentarRegistarEvento(registo);
        juntar(registo);
        return lsn;
    }

    private void juntar(Registo registo) throws IOException {
        if (aberto == null) {
            aberto = new Chunk();
            agendar(aberto, Wire.SELAGEM_MS);
        }
        aberto.adicionar(registo);
        if (aberto.tamanho() >= Wire.CHUNK_MAX_BYTES) {
            tentarSelar();
        }
    }

    private void agendar(Chunk chunk, long milissegundos) {
        prazo = agendador.schedule(() -> {
            synchronized (this) {
                if (!fechado && aberto == chunk) {
                    tentarSelar();
                }
            }
        }, milissegundos, TimeUnit.MILLISECONDS);
    }

    private void tentarSelar() {
        selagemIniciada = true;
        prazo.cancel(false);
        try {
            if (pendente == null) {
                pendente = crypto.seal(aberto.fechar(Wire.PAD_BYTES));
            }
            String id = pendente.chunkId();
            publicar(pastaSelados.resolve(id + ".hdr"), (pendente.cabecalho() + "\n")
                    .getBytes(StandardCharsets.US_ASCII));
            byte[] cifrado = pendente.cifrado();
            publicar(pastaSelados.resolve(id + ".bin"), cifrado);
            evento("SELADO", "chunkId=" + id + ",lsn=" + aberto.primeiroLsn() + "-"
                    + aberto.ultimoLsn() + ",bytes=" + cifrado.length);
            pendente = null;
            aberto = null;
            selagemIniciada = false;
        } catch (IOException | GeneralSecurityException erro) {
            System.err.println("Falha a selar chunk: " + erro.getMessage());
            agendar(aberto, Wire.SELAGEM_MS);
        }
    }

    private static void publicar(Path destino, byte[] dados) throws IOException {
        if (Files.exists(destino)) {
            return;
        }
        Path temporario = Files.createTempFile(destino.getParent(), ".selagem-", ".tmp");
        try {
            try (FileChannel canal = FileChannel.open(temporario, StandardOpenOption.WRITE)) {
                escrever(canal, ByteBuffer.wrap(dados));
                canal.force(true);
            }
            Files.move(temporario, destino, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(temporario);
        }
    }

    private synchronized void reaplicar(Path registos) throws IOException, GeneralSecurityException {
        long seladoAte = ultimoLsnSelado();
        descartarLinhaIncompleta();
        try (BufferedReader leitor = Files.newBufferedReader(registos, StandardCharsets.UTF_8)) {
            String linha;
            while ((linha = leitor.readLine()) != null) {
                Registo registo;
                try {
                    registo = Registo.daLinha(linha);
                } catch (RuntimeException erro) {
                    throw new IOException("Linha inválida após LSN " + ultimoLsn, erro);
                }
                if (registo.lsn() != Math.incrementExact(ultimoLsn)) {
                    throw new IOException("LSN fora de sequência em registos.log: " + registo.lsn());
                }
                ultimoLsn = registo.lsn();
                if (ultimoLsn > seladoAte) {
                    if (selagemIniciada) {
                        throw new IOException("Falha a selar durante a recuperação do diário");
                    }
                    juntar(registo);
                }
            }
        }
        if (ultimoLsn < seladoAte) {
            throw new IOException("registos.log termina antes do último chunk selado");
        }
    }

    private long ultimoLsnSelado() throws IOException, GeneralSecurityException {
        TreeMap<Long, Long> intervalos = new TreeMap<>();
        try (Stream<Path> ficheiros = Files.list(pastaSelados)) {
            for (Path ficheiro : (Iterable<Path>) ficheiros.filter(p -> p.toString().endsWith(".bin"))::iterator) {
                String nome = ficheiro.getFileName().toString();
                String id = nome.substring(0, nome.length() - 4);
                Sealed selado = Sealed.dosFicheiros(id,
                        Files.readString(pastaSelados.resolve(id + ".hdr"), StandardCharsets.US_ASCII),
                        Files.readAllBytes(ficheiro));
                List<Registo> registos = Chunk.ler(crypto.open(selado));
                long primeiro = registos.get(0).lsn();
                long ultimo = primeiro;
                for (Registo registo : registos) {
                    if (registo.lsn() != ultimo++) {
                        throw new IOException("LSN fora de sequência no chunk " + id);
                    }
                }
                if (intervalos.putIfAbsent(primeiro, ultimo - 1) != null) {
                    throw new IOException("Chunks com LSN inicial repetido: " + primeiro);
                }
            }
        }
        long ultimo = 0;
        for (var intervalo : intervalos.entrySet()) {
            if (intervalo.getKey() != Math.incrementExact(ultimo)) {
                throw new IOException("Intervalo de LSN fora de sequência nos chunks selados");
            }
            ultimo = intervalo.getValue();
        }
        return ultimo;
    }

    private void descartarLinhaIncompleta() throws IOException {
        long fim = diario.size();
        long posicao = fim;
        ByteBuffer byteLido = ByteBuffer.allocate(1);
        while (posicao > 0) {
            byteLido.clear();
            diario.read(byteLido, --posicao);
            if (byteLido.get(0) == '\n') {
                posicao++;
                break;
            }
        }
        if (posicao != fim) {
            diario.truncate(posicao);
            diario.force(true);
        }
    }

    private void tentarRegistarEvento(Registo registo) {
        try {
            evento("REGISTO", "lsn=" + registo.lsn());
        } catch (IOException erro) {
            System.err.println("Falha a gravar evento REGISTO: " + erro.getMessage());
        }
    }

    private void evento(String nome, String detalhe) throws IOException {
        try (FileChannel canal = FileChannel.open(eventos, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            acrescentar(canal, Instant.now() + ";" + nome + ";" + detalhe);
        }
    }

    private static void acrescentar(FileChannel canal, String linha) throws IOException {
        canal.position(canal.size());
        escrever(canal, StandardCharsets.UTF_8.encode(linha + "\n"));
        canal.force(true);
    }

    private static void escrever(FileChannel canal, ByteBuffer dados) throws IOException {
        while (dados.hasRemaining()) {
            canal.write(dados);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        fechado = true;
        agendador.shutdownNow();
        try {
            diario.close();
        } finally {
            bloqueio.close();
        }
    }
}
