package tpf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class Chunk {
    private final ByteArrayOutputStream dados = new ByteArrayOutputStream();
    private int quantidade;
    private long primeiroLsn;
    private long ultimoLsn;
    private boolean fechado;

    void adicionar(Registo registo) throws IOException {
        if (fechado) {
            throw new IllegalStateException("Chunk já fechado");
        }
        byte[] linha = registo.linha().getBytes(StandardCharsets.UTF_8);
        DataOutputStream fluxo = new DataOutputStream(dados);
        fluxo.writeInt(linha.length);
        fluxo.write(linha);
        if (quantidade++ == 0) {
            primeiroLsn = registo.lsn();
        }
        ultimoLsn = registo.lsn();
    }

    int tamanho() {
        return Integer.BYTES + dados.size();
    }

    long primeiroLsn() {
        return primeiroLsn;
    }

    long ultimoLsn() {
        return ultimoLsn;
    }

    byte[] fechar(int bloco) throws IOException {
        fechado = true;
        ByteArrayOutputStream serializado = new ByteArrayOutputStream(tamanho());
        new DataOutputStream(serializado).writeInt(quantidade);
        dados.writeTo(serializado);
        int padding = (bloco - tamanho() % bloco) % bloco;
        return Arrays.copyOf(serializado.toByteArray(), Math.addExact(tamanho(), padding));
    }

    public static List<Registo> ler(byte[] dados) throws IOException {
        DataInputStream fluxo = new DataInputStream(new ByteArrayInputStream(dados));
        int quantidade = fluxo.readInt();
        if (quantidade <= 0 || quantidade > fluxo.available() / Integer.BYTES) {
            throw new IOException("Quantidade de registos inválida no chunk");
        }
        List<Registo> registos = new ArrayList<>();
        for (int i = 0; i < quantidade; i++) {
            int tamanho = fluxo.readInt();
            if (tamanho <= 0 || tamanho > fluxo.available()) {
                throw new IOException("Tamanho de registo inválido no chunk");
            }
            try {
                registos.add(Registo.daLinha(new String(fluxo.readNBytes(tamanho), StandardCharsets.UTF_8)));
            } catch (RuntimeException erro) {
                throw new IOException("Registo inválido no chunk", erro);
            }
        }
        return List.copyOf(registos);
    }
}
