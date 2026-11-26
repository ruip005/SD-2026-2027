package tpf;

public final class Wire {
    public static final int CHUNK_MAX_BYTES = positivo("tpf.chunkMaxBytes", 65536);
    public static final int PAD_BYTES = positivo("tpf.padBytes", 4096);
    public static final int SELAGEM_MS = positivo("tpf.selagemMs", 5000);

    private Wire() {
    }

    private static int positivo(String propriedade, int omissao) {
        int valor = Integer.parseInt(System.getProperty(propriedade, Integer.toString(omissao)));
        if (valor <= 0) {
            throw new IllegalArgumentException(propriedade + " tem de ser positiva");
        }
        return valor;
    }
}
