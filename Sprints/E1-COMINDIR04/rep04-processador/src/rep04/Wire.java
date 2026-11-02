package rep04;

/** Formato das mensagens e constantes partilhadas pelos três projetos, para garantir coerência. */
public final class Wire {
    /** Número de registos retidos acima do qual a réplica considera que houve perda. */
    public static final int LIMITE_RETIDOS = 10;
    public static final double LIMIAR_ALARME = 28.0;
    /** Tempo máximo, em milissegundos, de ligação e de leitura no pedido TCP de recuperação. */
    public static final int T_TCP_MS = 5000;
    /** O líder da partição k escuta recuperações no porto PORTO_BASE + k. */
    public static final int PORTO_BASE = 7000;
    /** Período Δ entre heartbeats (e lotes), em ms; propriedade de sistema rep04.periodo. */
    public static final long PERIODO_HB_MS = Long.getLong("rep04.periodo", 5000L);
    /** TIMEOUT = FATOR_TIMEOUT × Δ; propriedade de sistema rep04.fator. */
    public static final int FATOR_TIMEOUT = Integer.getInteger("rep04.fator", 3);
    /** Período, em ms, com que cada participante verifica os seus detetores. */
    public static final int VERIFICACAO_MS = 1000;

    private Wire() {
    }

    // Constrói o nome rep04-in-p<k>, onde o líder recebe leituras originais e alarmes.
    public static String topicIn(int k) {
        return "rep04-in-p" + k;
    }

    // Constrói o nome rep04-p<k>, onde o líder publica os registos para réplicas e processador.
    public static String topicData(int k) {
        return "rep04-p" + k;
    }

    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }

    public static String type(String msg) {
        int i = msg.indexOf(';');
        return i < 0 ? msg : msg.substring(0, i);
    }

    public static String body(String msg) {
        int i = msg.indexOf(';');
        return i < 0 ? "" : msg.substring(i + 1);
    }
}
