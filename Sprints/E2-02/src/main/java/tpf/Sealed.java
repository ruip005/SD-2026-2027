package tpf;

import java.util.Base64;

public record Sealed(String chunkId, int keyVersion, byte[] salt, byte[] nonce, byte[] cifrado) {
    public Sealed {
        salt = salt.clone();
        nonce = nonce.clone();
        cifrado = cifrado.clone();
    }

    @Override
    public byte[] salt() {
        return salt.clone();
    }

    @Override
    public byte[] nonce() {
        return nonce.clone();
    }

    @Override
    public byte[] cifrado() {
        return cifrado.clone();
    }

    String cabecalho() {
        Base64.Encoder base64 = Base64.getEncoder();
        return keyVersion + ";" + base64.encodeToString(salt) + ";" + base64.encodeToString(nonce);
    }

    static Sealed dosFicheiros(String id, String cabecalho, byte[] cifrado) {
        String[] campos = cabecalho.strip().split(";", -1);
        if (campos.length != 3) {
            throw new IllegalArgumentException("Cabeçalho inválido no chunk " + id);
        }
        Base64.Decoder base64 = Base64.getDecoder();
        return new Sealed(id, Integer.parseInt(campos[0]), base64.decode(campos[1]),
                base64.decode(campos[2]), cifrado);
    }
}
