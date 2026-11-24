package tpf;

/**
 * Regra de quais mensagens o bot deve considerar: so as de pessoas (nao bots)
 * escritas no canal #arquivo de um servidor. Tudo o resto e ignorado.
 */
public final class CanalArquivo {

    public static final String NOME_CANAL = "arquivo";

    private CanalArquivo() {
    }

    /**
     * @param autorEBot    true se o autor e um bot (inclui o proprio LeaderBot e webhooks)
     * @param vemDeServidor false para mensagens privadas (DM)
     * @param nomeDoCanal  nome do canal sem o '#'
     */
    public static boolean deveProcessar(boolean autorEBot, boolean vemDeServidor, String nomeDoCanal) {
        if (autorEBot || !vemDeServidor) {
            return false;
        }
        return NOME_CANAL.equalsIgnoreCase(nomeDoCanal);
    }

    /** Confirma a identidade do canal por ID, evitando aceitar outro canal com nome igual. */
    public static boolean eCanalArquivo(long idDoCanal, long idDoArquivo) {
        return idDoCanal > 0 && idDoCanal == idDoArquivo;
    }
}
