package tpf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public final class ChunkCrypto {
    private static final byte[] INFO = "tpf-chunk-v1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PREFIXO_AAD = "tpf-v1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSAO = 1;
    private static final int SALT_BYTES = 32;
    private static final int NONCE_BYTES = 12;
    private static final SecureRandom ALEATORIO = new SecureRandom();
    private final byte[] chaveMestra;

    public ChunkCrypto(byte[] chaveMestra) {
        if (chaveMestra.length != 32) {
            throw new IllegalArgumentException("A chave mestra tem de ter 32 bytes");
        }
        this.chaveMestra = chaveMestra.clone();
    }

    public Sealed seal(byte[] dados) throws GeneralSecurityException {
        byte[] salt = new byte[SALT_BYTES];
        byte[] nonce = new byte[NONCE_BYTES];
        ALEATORIO.nextBytes(salt);
        ALEATORIO.nextBytes(nonce);
        byte[] cifrado = cipher(Cipher.ENCRYPT_MODE, VERSAO, salt, nonce).doFinal(dados);
        return new Sealed(id(cifrado), VERSAO, salt, nonce, cifrado);
    }

    public byte[] open(Sealed selado) throws GeneralSecurityException {
        byte[] cifrado = selado.cifrado();
        if (!id(cifrado).equals(selado.chunkId())) {
            throw new GeneralSecurityException("SHA-256 do chunk inválido");
        }
        if (selado.keyVersion() != VERSAO) {
            throw new GeneralSecurityException("Versão da chave desconhecida");
        }
        byte[] salt = selado.salt();
        byte[] nonce = selado.nonce();
        if (salt.length != SALT_BYTES || nonce.length != NONCE_BYTES) {
            throw new GeneralSecurityException("Tamanho de salt ou nonce inválido");
        }
        return cipher(Cipher.DECRYPT_MODE, selado.keyVersion(), salt, nonce).doFinal(cifrado);
    }

    private Cipher cipher(int modo, int versao, byte[] salt, byte[] nonce) throws GeneralSecurityException {
        Mac hmac = Mac.getInstance("HmacSHA256");
        hmac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = hmac.doFinal(chaveMestra);
        hmac.init(new SecretKeySpec(prk, "HmacSHA256"));
        hmac.update(INFO);
        byte[] dek = hmac.doFinal(new byte[]{1});

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(modo, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, nonce));
        cipher.updateAAD(ByteBuffer.allocate(PREFIXO_AAD.length + Integer.BYTES + salt.length + nonce.length)
                .put(PREFIXO_AAD).putInt(versao).put(salt).put(nonce).array());
        return cipher;
    }

    private static String id(byte[] cifrado) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(cifrado));
    }
}
