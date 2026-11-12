package raft04;

/** Resposta de um nó a um {@code PEDIDO_VOTO}. */
public final class VoteReply {
    public static final String TIPO = "VOTO";

    private final long term;
    private final int voter;
    private final int candidate;
    private final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        if (term < 1) throw new IllegalArgumentException("Termo deve ser positivo");
        if (voter < 1) throw new IllegalArgumentException("Eleitor inválido: " + voter);
        if (candidate < 1) throw new IllegalArgumentException("Candidato inválido: " + candidate);
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return TIPO + ";" + term + ";" + voter + ";" + candidate + ";"
                + (granted ? "S" : "N");
    }

    public static VoteReply decode(String msg) {
        if (msg == null || msg.isBlank()) throw new IllegalArgumentException("VOTO vazio");
        String[] f = msg.trim().split(";", -1);
        if (f.length != 5 || !TIPO.equals(f[0])) {
            throw new IllegalArgumentException("VOTO mal formado: " + msg);
        }
        if (!"S".equals(f[4]) && !"N".equals(f[4])) {
            throw new IllegalArgumentException("Voto inválido: " + f[4]);
        }
        return new VoteReply(
                Long.parseLong(f[1]),
                Integer.parseInt(f[2]),
                Integer.parseInt(f[3]),
                "S".equals(f[4])
        );
    }

    public long getTerm() { return term; }
    public int getVoter() { return voter; }
    public int getCandidate() { return candidate; }
    public boolean isGranted() { return granted; }
}
