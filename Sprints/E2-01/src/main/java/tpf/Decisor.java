package tpf;

import java.util.Collection;
import java.util.Map;

/**
 * Junta as duas verificacoes pela ordem certa:
 * 1) o comando existe?  2) o membro tem permissao?  3) os argumentos sao validos?
 *
 * A permissao vem ANTES da validacao: assim quem nao e admin nao descobre, pelos
 * erros de validacao, como funcionam os comandos que nao pode usar.
 */
public final class Decisor {

    private Decisor() {
    }

    public static Resultado decidir(String comando, Collection<String> papeisDoMembro, Map<String, String> args) {
        if (comando == null || !Comandos.existe(comando)) {
            return Resultado.erro("Comando desconhecido: `/" + comando + "`.");
        }
        if (Comandos.eAdmin(comando) && !Permissoes.eAdmin(papeisDoMembro)) {
            return Resultado.erro("Sem permissão: `/" + comando + "` só pode ser usado por quem tem o papel `"
                    + Permissoes.PAPEL_ADMIN + "`.");
        }
        return Comandos.validar(comando, args);
    }
}
