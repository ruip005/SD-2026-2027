package rep04;

/** Função de partição: decide a que partição pertence um sensor. */
public class Partitioner {
    private final int n;

    // Valida e guarda o número de partições usado por este processo.
    public Partitioner(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("N tem de ser >= 1: " + n);
        }
        this.n = n;
    }

    // Calcula a partição do identificador com floorMod(hashCode, N), obtendo sempre um índice não negativo.
    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), n);
    }

    public int count() {
        return n;
    }
}
