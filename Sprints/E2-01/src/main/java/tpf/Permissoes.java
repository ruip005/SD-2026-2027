package tpf;

import java.util.Collection;

/**
 * Decide se um membro pode usar os comandos de administracao.
 * Recebe so os NOMES dos papeis do membro, para nao depender do JDA.
 */
public final class Permissoes {

    /** Nome do papel que o servidor de teste tem de ter. */
    public static final String PAPEL_ADMIN = "admin-arquivo";

    private Permissoes() {
    }

    /** True se algum dos papeis for admin-arquivo (sem distinguir maiusculas). */
    public static boolean eAdmin(Collection<String> nomesDosPapeis) {
        if (nomesDosPapeis == null) {
            return false;
        }
        for (String nome : nomesDosPapeis) {
            if (PAPEL_ADMIN.equalsIgnoreCase(nome)) {
                return true;
            }
        }
        return false;
    }
}
