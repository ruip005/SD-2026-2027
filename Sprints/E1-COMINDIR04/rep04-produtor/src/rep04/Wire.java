package rep04;

/**
 * Formato das mensagens e constantes partilhadas pelos três projetos, para garantir coerência.
 *
 * <p>Mensagens por PubSub:</p>
 * <ul>
 *   <li>{@code IN;<sensor>;<temp>;<L>;<V>}: produtor para o líder, no tópico {@code rep04-in-p<k>};</li>
 *   <li>{@code LOTE;<host>;<porto>;<inc>;<hb>;<ultimoSeq>;<n>} seguido de {@code n} linhas
 *       {@code <seq>;<sensor>;<temp>;<instante>;<L>;<V>}: líder para os participantes, no tópico
 *       {@code rep04-p<k>}, a cada {@link #PERIODO_HB_MS} ms (ver {@link Batch}). O {@code host} e o
 *       {@code porto} dizem aos participantes onde pedir recuperação.</li>
 * </ul>
 *
 * <p>Por TCP (porto {@code 7000 + k}): a réplica envia {@code PEDIDO <de> <até>}; o líder responde
 * com 0 a n linhas {@code seq;sensor;temp;instante;L;V} e termina com {@code FIM}, ou com uma única
 * linha {@code ERRO <motivo>}. O {@code FIM} é necessário porque o TCP é um fluxo de bytes sem
 * fronteiras entre mensagens: sem um marcador, a réplica não saberia quando parar de ler.</p>
 *
 * <p>Tudo é texto separado por {@code ;} (e não objetos serializados como no TCP01) para os três
 * projetos e o Kubo trocarem mensagens sem partilharem classes Java.</p>
 */
public final class Wire {
    /** Número de registos retidos acima do qual a réplica considera que houve perda. */
    public static final int LIMITE_RETIDOS = 10;
    public static final double LIMIAR_ALARME = 28.0;
    /** Tempo máximo, em milissegundos, de ligação e de leitura no pedido TCP de recuperação. */
    public static final int T_TCP_MS = 5000;
    /** O líder da partição {@code k} escuta recuperações no porto {@code PORTO_BASE + k}. */
    public static final int PORTO_BASE = 7000;
    /** Período Δ entre heartbeats (e lotes), em ms; propriedade de sistema {@code rep04.periodo}. */
    public static final long PERIODO_HB_MS = Long.getLong("rep04.periodo", 5000L);
    /** TIMEOUT = FATOR_TIMEOUT × Δ; propriedade de sistema {@code rep04.fator}. */
    public static final int FATOR_TIMEOUT = Integer.getInteger("rep04.fator", 3);
    /** Período, em ms, com que cada participante verifica os seus detetores. */
    public static final int VERIFICACAO_MS = 1000;

    /** Classe utilitária: não se instancia. */
    // Impede criar instâncias desta classe, que reúne apenas constantes e métodos estáticos.
    private Wire() {
    }

    /**
     * Nome do tópico onde o produtor publica as leituras da partição.
     *
     * @param k partição
     * @return nome do tópico de entrada da partição ({@code rep04-in-p<k>})
     */
    // Constrói o nome rep04-in-p<k>, onde o líder recebe leituras originais e alarmes.
    public static String topicIn(int k) {
        return "rep04-in-p" + k;
    }

    /**
     * Nome do tópico onde o líder publica os registos numerados da partição.
     *
     * @param k partição
     * @return nome do tópico de dados da partição ({@code rep04-p<k>})
     */
    // Constrói o nome rep04-p<k>, onde o líder publica os registos para réplicas e processador.
    public static String topicData(int k) {
        return "rep04-p" + k;
    }

    /**
     * Constrói uma mensagem {@code IN}.
     *
     * @param sensor identificador do sensor
     * @param temp   temperatura
     * @return {@code IN;<sensor>;<temp>;<L>;<V>}
     */
    // Constrói a mensagem IN com sensor, temperatura e conhecimento causal L e V do emissor.
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }

    /**
     * Extrai o tipo da mensagem.
     *
     * @param msg mensagem completa
     * @return o tipo (texto antes do primeiro {@code ;}); a mensagem inteira se não houver {@code ;}
     */
    // Obtém o tipo da mensagem, lendo o texto anterior ao primeiro ponto e vírgula.
    public static String type(String msg) {
        int i = msg.indexOf(';');
        return i < 0 ? msg : msg.substring(0, i);
    }

    /**
     * Extrai o corpo da mensagem.
     *
     * @param msg mensagem completa
     * @return o texto depois do primeiro {@code ;}; vazio se não houver {@code ;}
     */
    // Obtém os campos posteriores ao tipo da mensagem, retirando o primeiro separador.
    public static String body(String msg) {
        int i = msg.indexOf(';');
        return i < 0 ? "" : msg.substring(i + 1);
    }
}
