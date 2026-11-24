package tpf;

/**
 * Resultado de validar um comando: ou esta tudo bem (e ha uma mensagem provisoria
 * para mostrar), ou ha um erro (e a mensagem explica o que correu mal).
 *
 * Esta classe NAO depende do JDA, por isso pode ser testada sem Discord.
 */
public final class Resultado {

    private final boolean ok;
    private final String mensagem;

    private Resultado(boolean ok, String mensagem) {
        this.ok = ok;
        this.mensagem = mensagem;
    }

    public static Resultado ok(String mensagem) {
        return new Resultado(true, mensagem);
    }

    public static Resultado erro(String mensagem) {
        return new Resultado(false, mensagem);
    }

    public boolean isOk() {
        return ok;
    }

    public String getMensagem() {
        return mensagem;
    }

    @Override
    public String toString() {
        return (ok ? "OK: " : "ERRO: ") + mensagem;
    }
}
