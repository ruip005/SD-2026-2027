package raft04;

/** Pedido de voto enviado por um candidato durante uma eleição. */
public final class VoteRequest {
    public static final String TIPO = "PEDIDO_VOTO";

    private final long term;
    private final int candidate;
    private final long lastSeq;
    private final long lastTerm;

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        if (term < 1) throw new IllegalArgumentException("Termo deve ser positivo");
        if (candidate < 1) throw new IllegalArgumentException("Candidato inválido: " + candidate);
        if (lastSeq < 0) throw new IllegalArgumentException("ultimoSeq negativo");
        if (lastTerm < 0) throw new IllegalArgumentException("ultimoTermo negativo");
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return TIPO + ";" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static VoteRequest decode(String msg) {
        if (msg == null || msg.isBlank()) throw new IllegalArgumentException("PEDIDO_VOTO vazio");
        String[] f = msg.trim().split(";", -1);
        if (f.length != 5 || !TIPO.equals(f[0])) {
            throw new IllegalArgumentException("PEDIDO_VOTO mal formado: " + msg);
        }
        return new VoteRequest(
                Long.parseLong(f[1]),
                Integer.parseInt(f[2]),
                Long.parseLong(f[3]),
                Long.parseLong(f[4])
        );
    }

    public long getTerm() { return term; }
    public int getCandidate() { return candidate; }
    public long getLastSeq() { return lastSeq; }
    public long getLastTerm() { return lastTerm; }
}
