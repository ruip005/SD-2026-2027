package rep02;

/**
 * Função de partição: decide a que partição pertence um sensor.
 *
 * <p>Usa {@code floorMod(sensorId.hashCode(), n)}. O resultado de {@link String#hashCode()} está
 * fixado pela especificação da linguagem, logo a função é determinística em qualquer JVM. Usa-se
 * {@link Math#floorMod(int, int)} e não {@code %} porque este último devolve negativos quando o
 * {@code hashCode} é negativo.</p>
 *
 * <p><strong>Porquê particionar por hash do sensor</strong>: o mesmo sensor vai sempre para a mesma
 * partição, logo as leituras de um sensor ficam ordenadas por um único líder; e ninguém precisa de
 * uma tabela central "sensor &rarr; partição" (cada processo calcula sozinho).</p>
 *
 * <p><strong>O que pode correr mal</strong>: processos com {@code N} diferentes discordam da
 * partição (o líder responde {@code PARTIÇÃO ERRADA}); mudar {@code N} reatribui a maioria dos
 * sensores e invalida os ficheiros existentes; uma chave muito frequente (S01 em
 * {@code rajada-quente}) sobrecarrega uma só partição, porque o hash reparte <em>chaves</em>, não
 * <em>carga</em>.</p>
 */
public class Partitioner {
    /** Número de partições. */
    private final int n;

    /**
     * Cria a função de partição.
     *
     * @param n número de partições (maior que zero)
     * @throws IllegalArgumentException se {@code n < 1}
     */
    public Partitioner(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("N tem de ser >= 1: " + n);
        }
        this.n = n;
    }

    /**
     * Calcula a partição de um sensor.
     *
     * @param sensorId identificador do sensor (chave de partição)
     * @return a partição, em {@code [0, n-1]}
     */
    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), n);
    }

    /**
     * Indica quantas partições existem.
     *
     * @return o número de partições
     */
    public int count() {
        return n;
    }
}
