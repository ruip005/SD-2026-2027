package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

/** Formato das mensagens e constantes partilhadas pelos três projetos. */
public final class Wire {
    /**
     * Número de registos retidos acima do qual a réplica considera que houve perda.
     */
    public static final int LIMITE_RETIDOS = 10;
    public static final double LIMIAR_ALARME = 28.0;
    /**
     * Tempo máximo, em milissegundos, de ligação e leitura na recuperação TCP.
     */
    public static final int T_TCP_MS = 5000;
    /**
     * Base dos portos TCP de recuperação.
     */
    public static final int PORTO_BASE = 7000;

    /**
     * Período Δ entre heartbeats e lotes; propriedade raft04.periodo.
     */
    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000L);
    /**
     * Fator do timeout dos monitores; propriedade raft04.fator.
     */
    public static final int FATOR_TIMEOUT = Integer.getInteger("raft04.fator", 3);
    /**
     * Frequência com que cada nó verifica o seu prazo eleitoral.
     */
    public static final int VERIFICACAO_MS = 200;
    /**
     * Limite inferior do prazo eleitoral; propriedade raft04.eleicaoMin.
     */
    public static final long ELEICAO_MIN_MS =
            Long.getLong("raft04.eleicaoMin", 3 * PERIODO_HB_MS);
    /**
     * Limite superior do prazo eleitoral; propriedade raft04.eleicaoMax.
     */
    public static final long ELEICAO_MAX_MS =
            Long.getLong("raft04.eleicaoMax", 6 * PERIODO_HB_MS);
    /**
     * Número de nós em cada grupo de uma partição.
     */
    public static final int GRUPO = 3;

    private Wire() {
    }

    // Constrói o nome raft04-in-p<k>, onde os nós recebem leituras originais.
    public static String topicIn(int k) {
        return "raft04-in-p" + k;
    }

    // Constrói o nome raft04-p<k>, onde o líder publica lotes e heartbeats.
    public static String topicData(int k) {
        return "raft04-p" + k;
    }

    // Constrói o nome raft04-eleicao-p<k>, para PEDIDO_VOTO e VOTO.
    public static String topicElection(int k) {
        return "raft04-eleicao-p" + k;
    }

    // Cada nó recebe um porto próprio para o servidor TCP de recuperação.
    public static int nodePort(int k, int i) {
        return PORTO_BASE + k * GRUPO + i;
    }

    // Simula isolamento de rede sem afetar o tópico de entradas do produtor.
    public static boolean isolated(int k, int i) {
        return Files.exists(Path.of("isolar-p" + k + "-n" + i));
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