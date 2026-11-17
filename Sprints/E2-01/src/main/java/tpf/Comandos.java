package tpf;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Definicao dos 7 comandos do LeaderBot e validacao dos seus argumentos.
 *
 * Nenhum comando faz trabalho real neste sprint: se os argumentos forem validos
 * respondemos com uma mensagem provisoria que ecoa o que recebemos.
 *
 * Os argumentos chegam como um Map<String,String> (nome da opcao -> valor em texto).
 * Assim a logica de validacao nao depende do JDA e testa-se com um simples main().
 *
 * Nomes de opcoes no Discord: so minusculas, numeros, '-' e '_' (max. 32).
 * Por isso o parametro e "intervalo_ms" e nao "intervaloMs".
 */
public final class Comandos {

    // ---- nomes dos comandos ----
    public static final String REGISTAR = "registar";
    public static final String ESTADO = "estado";
    public static final String CONFIG = "config";
    public static final String RECUPERAR = "recuperar";
    public static final String REPLICA_TOKEN = "replica-token";
    public static final String REPLICA_REMOVER = "replica-remover";
    public static final String RAJADA = "rajada";

    /** Todos os comandos, pela ordem em que aparecem no autocomplete. */
    public static final List<String> TODOS = List.of(
            REGISTAR, ESTADO, CONFIG, RECUPERAR, REPLICA_TOKEN, REPLICA_REMOVER, RAJADA);

    /** Comandos de administracao: so quem tem o papel admin-arquivo os pode usar. */
    public static final Set<String> ADMIN = Set.of(CONFIG, REPLICA_TOKEN, REPLICA_REMOVER, RAJADA);

    // ---- chaves dos argumentos (iguais aos nomes das opcoes no Discord) ----
    public static final String ARG_SUBCOMANDO = "subcomando";
    public static final String ARG_TEXTO = "texto";
    public static final String ARG_N = "n";
    public static final String ARG_DE = "de";
    public static final String ARG_ATE = "ate";
    public static final String ARG_ID = "id";
    public static final String ARG_INTERVALO_MS = "intervalo_ms";

    // ---- limites (ASSUMIDOS: a issue #20 nao os fixa; ajustar se o enunciado completo disser outra coisa) ----
    public static final int TEXTO_MAX = 1000;
    public static final int R_MIN = 1;
    public static final int R_MAX = 10;
    public static final int RAJADA_N_MAX = 10_000;
    public static final long RAJADA_INTERVALO_MAX_MS = 60_000L;
    public static final int ID_MAX = 64;

    private Comandos() {
    }

    public static boolean existe(String comando) {
        return TODOS.contains(comando);
    }

    public static boolean eAdmin(String comando) {
        return ADMIN.contains(comando);
    }

    /**
     * Valida os argumentos de um comando (a permissao e verificada antes, em {@link Decisor}).
     */
    public static Resultado validar(String comando, Map<String, String> args) {
        if (comando == null || !existe(comando)) {
            return Resultado.erro("Comando desconhecido: `/" + comando + "`.");
        }
        switch (comando) {
            case REGISTAR:
                return validarRegistar(args);
            case ESTADO:
                return Resultado.ok("`/estado` recebido — ainda não implementado.");
            case CONFIG:
                return validarConfig(args);
            case RECUPERAR:
                return validarRecuperar(args);
            case REPLICA_TOKEN:
                return Resultado.ok("`/replica-token` recebido — ainda não implementado.");
            case REPLICA_REMOVER:
                return validarReplicaRemover(args);
            case RAJADA:
                return validarRajada(args);
            default:
                return Resultado.erro("Comando desconhecido: `/" + comando + "`.");
        }
    }

    // ------------------------------------------------------------------ comandos

    private static Resultado validarRegistar(Map<String, String> args) {
        String texto = args.get(ARG_TEXTO);
        if (texto == null || texto.trim().isEmpty()) {
            return Resultado.erro("`/registar` precisa de um texto não vazio.");
        }
        if (texto.length() > TEXTO_MAX) {
            return Resultado.erro("`/registar`: o texto tem " + texto.length()
                    + " caracteres; o máximo é " + TEXTO_MAX + ".");
        }
        return Resultado.ok("`/registar` recebido: texto=\"" + texto + "\" — ainda não implementado.");
    }

    private static Resultado validarConfig(Map<String, String> args) {
        String sub = args.get(ARG_SUBCOMANDO);
        if (!"r".equals(sub)) {
            return Resultado.erro("`/config`: só existe o subcomando `r` (ex.: `/config r 2`).");
        }
        Long n = lerInteiro(args.get(ARG_N));
        if (n == null) {
            return Resultado.erro("`/config r`: `n` tem de ser um número inteiro (recebi \""
                    + args.get(ARG_N) + "\").");
        }
        if (n < R_MIN || n > R_MAX) {
            return Resultado.erro("`/config r`: `n` tem de estar entre " + R_MIN + " e " + R_MAX
                    + " (recebi " + n + ").");
        }
        return Resultado.ok("`/config r` recebido: n=" + n + " — ainda não implementado.");
    }

    private static Resultado validarRecuperar(Map<String, String> args) {
        Long de = lerInteiro(args.get(ARG_DE));
        Long ate = lerInteiro(args.get(ARG_ATE));
        if (de == null || ate == null) {
            return Resultado.erro("`/recuperar`: `de` e `ate` têm de ser números inteiros.");
        }
        if (de < 1 || ate < 1) {
            return Resultado.erro("`/recuperar`: `de` e `ate` têm de ser pelo menos 1.");
        }
        if (de > ate) {
            return Resultado.erro("`/recuperar`: `de` (" + de + ") não pode ser maior que `ate` (" + ate + ").");
        }
        return Resultado.ok("`/recuperar` recebido: de=" + de + ", ate=" + ate + " — ainda não implementado.");
    }

    private static Resultado validarReplicaRemover(Map<String, String> args) {
        String id = args.get(ARG_ID);
        if (id == null || id.trim().isEmpty()) {
            return Resultado.erro("`/replica-remover` precisa do `id` da réplica.");
        }
        if (id.length() > ID_MAX || !id.matches("[A-Za-z0-9_-]+")) {
            return Resultado.erro("`/replica-remover`: `id` inválido (só letras, números, '-' e '_'; máx. "
                    + ID_MAX + " caracteres).");
        }
        return Resultado.ok("`/replica-remover` recebido: id=" + id + " — ainda não implementado.");
    }

    private static Resultado validarRajada(Map<String, String> args) {
        Long n = lerInteiro(args.get(ARG_N));
        Long intervalo = lerInteiro(args.get(ARG_INTERVALO_MS));
        if (n == null || intervalo == null) {
            return Resultado.erro("`/rajada`: `n` e `intervalo_ms` têm de ser números inteiros.");
        }
        if (n < 1 || n > RAJADA_N_MAX) {
            return Resultado.erro("`/rajada`: `n` tem de estar entre 1 e " + RAJADA_N_MAX + " (recebi " + n + ").");
        }
        if (intervalo < 0 || intervalo > RAJADA_INTERVALO_MAX_MS) {
            return Resultado.erro("`/rajada`: `intervalo_ms` tem de estar entre 0 e "
                    + RAJADA_INTERVALO_MAX_MS + " (recebi " + intervalo + ").");
        }
        return Resultado.ok("`/rajada` recebido: n=" + n + ", intervalo_ms=" + intervalo + " — ainda não implementado.");
    }

    // ------------------------------------------------------------------ auxiliares

    /**
     * Converte texto em inteiro de forma estrita: so aceita algarismos com um '-' opcional.
     * Devolve null se nao for um inteiro (ex.: "abc", "", "2.5", numero grande demais).
     * Evita as surpresas do Long.parseLong com "+5" ou espacos.
     */
    static Long lerInteiro(String texto) {
        if (texto == null || !texto.matches("-?[0-9]{1,18}")) {
            return null;
        }
        return Long.parseLong(texto);
    }
}
