package tpf;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;

public class TestesChunkCrypto {
    public static void main(String[] args) throws GeneralSecurityException {
        ChunkCrypto crypto = new ChunkCrypto(new byte[32]);
        byte[] dados = Arrays.copyOf("Registo de teste com acentuação".getBytes(StandardCharsets.UTF_8), 4096);
        Sealed selado = crypto.seal(dados);
        if (!Arrays.equals(dados, crypto.open(selado)) || selado.cifrado().length != dados.length + 16) {
            throw new AssertionError("O round-trip ou o tamanho da tag GCM está incorreto");
        }
        byte[] adulterado = alterar(selado.cifrado());
        falha(crypto, new Sealed(selado.chunkId(), 2, selado.salt(), selado.nonce(), adulterado), "SHA-256");
        String hashAdulterado = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(adulterado));
        falha(crypto, new Sealed(hashAdulterado, 1, selado.salt(), selado.nonce(), adulterado), null);
        falha(crypto, new Sealed(selado.chunkId(), 1, alterar(selado.salt()), selado.nonce(), selado.cifrado()), null);
        falha(crypto, new Sealed(selado.chunkId(), 1, selado.salt(), alterar(selado.nonce()), selado.cifrado()), null);
        falha(crypto, new Sealed(selado.chunkId(), 2, selado.salt(), selado.nonce(), selado.cifrado()), "Versão");
        falha(new ChunkCrypto(alterar(new byte[32])), selado, null);
        System.out.println("Testes da cifra passaram.");
    }

    private static byte[] alterar(byte[] bytes) {
        bytes[0] ^= 1;
        return bytes;
    }

    private static void falha(ChunkCrypto crypto, Sealed selado, String mensagem) throws GeneralSecurityException {
        try {
            crypto.open(selado);
            throw new AssertionError("O chunk alterado foi aceite");
        } catch (GeneralSecurityException erro) {
            if (mensagem != null && !erro.getMessage().startsWith(mensagem)) {
                throw erro;
            }
            if (mensagem == null && !(erro instanceof javax.crypto.AEADBadTagException)) {
                throw erro;
            }
        }
    }
}
