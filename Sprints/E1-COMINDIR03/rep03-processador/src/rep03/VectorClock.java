package rep03;

import java.util.Arrays;

/** Relógio mutável; o chamador sincroniza o acesso a instâncias partilhadas. */
public final class VectorClock {
    public enum Order { BEFORE, AFTER, EQUAL, CONCURRENT }
    private final long[] v;
    public VectorClock(int n) {
        if (n < 1) throw new IllegalArgumentException("Dimensão deve ser positiva");
        v = new long[n];
    }
    private VectorClock(long[] v) { this.v = v; }
    public int size() { return v.length; }
    private void sameSize(VectorClock other) {
        if (v.length != other.v.length) throw new IllegalArgumentException("Dimensões diferentes");
    }
    public void merge(VectorClock other) {
        sameSize(other);
        for (int i = 0; i < v.length; i++) v[i] = Math.max(v[i], other.v[i]);
    }
    public void tick(int k) { v[k] = Math.incrementExact(v[k]); }
    public long get(int k) { return v[k]; }
    public VectorClock copy() { return new VectorClock(v.clone()); }
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
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(v[i]);
        }
        return sb.toString();
    }
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
    @Override public boolean equals(Object o) {
        return o instanceof VectorClock && Arrays.equals(v, ((VectorClock) o).v);
    }
    @Override public int hashCode() { return Arrays.hashCode(v); }
}
