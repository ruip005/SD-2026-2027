package rep03;

import java.util.Arrays;

/** Relógio mutável; o chamador sincroniza o acesso a instâncias partilhadas. */
public final class VectorClock {
    public enum Order { BEFORE, AFTER, EQUAL, CONCURRENT }
    private final long[] v;
    // Cria um relógio de dimensão N, inicialmente com todas as componentes a zero.
    public VectorClock(int n) {
        if (n < 1) throw new IllegalArgumentException("Dimensão deve ser positiva");
        v = new long[n];
    }
    // Encapsula o array fornecido internamente, usado ao copiar ou interpretar um vetor.
    private VectorClock(long[] v) { this.v = v; }
    // Devolve o número de componentes do vetor, correspondente ao número de partições.
    public int size() { return v.length; }
    // Rejeita a operação se os vetores tiverem dimensões diferentes.
    private void sameSize(VectorClock other) {
        if (v.length != other.v.length) throw new IllegalArgumentException("Dimensões diferentes");
    }
    // Acumula o conhecimento do outro vetor, guardando o máximo em cada componente; não incrementa o relógio.
    public void merge(VectorClock other) {
        sameSize(other);
        for (int i = 0; i < v.length; i++) v[i] = Math.max(v[i], other.v[i]);
    }
    // Incrementa apenas a componente k e sinaliza erro se o contador exceder o limite de long.
    public void tick(int k) { v[k] = Math.incrementExact(v[k]); }
    // Consulta o contador da componente k sem alterar o vetor.
    public long get(int k) { return v[k]; }
    // Cria um vetor com um array independente, para alterações futuras não modificarem a cópia.
    public VectorClock copy() { return new VectorClock(v.clone()); }
    // Compara todas as componentes e devolve BEFORE, AFTER, EQUAL ou CONCURRENT.
    public Order compare(VectorClock other) {
        sameSize(other);
        boolean menor = false, maior = false;
        for (int i = 0; i < v.length; i++) {
            if (v[i] < other.v[i]) menor = true;
            if (v[i] > other.v[i]) maior = true;
        }
        if (menor && maior) return Order.CONCURRENT;
        if (menor) return Order.BEFORE;
        if (maior) return Order.AFTER;
        return Order.EQUAL;
    }
    // Converte o vetor para texto com componentes separadas por vírgulas, usado nas mensagens e ficheiros.
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(v[i]);
        }
        return sb.toString();
    }
    // Reconstrói o vetor a partir de texto, verificando a dimensão N e rejeitando componentes inválidas.
    public static VectorClock parse(String s, int n) {
        if (n < 1) throw new IllegalArgumentException("Dimensão deve ser positiva");
        String[] f = s.trim().split(",", -1);
        if (f.length != n) throw new IllegalArgumentException("Vetor com dimensão " + f.length + ", esperado " + n);
        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = Long.parseLong(f[i]);
            if (v[i] < 0) throw new IllegalArgumentException("Componente negativa");
        }
        return new VectorClock(v);
    }
    // Considera iguais os vetores que têm os mesmos valores em todas as componentes.
    @Override public boolean equals(Object o) {
        return o instanceof VectorClock && Arrays.equals(v, ((VectorClock) o).v);
    }
    // Calcula um hash a partir das componentes, coerente com a comparação feita por equals.
    @Override public int hashCode() { return Arrays.hashCode(v); }
}
