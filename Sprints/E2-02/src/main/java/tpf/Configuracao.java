package tpf;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

/**
 * Le o token do bot e o id do servidor SEM os pôr no codigo nem no repositorio.
 *
 * Ordem de procura (a primeira que existir ganha):
 *   1) variaveis de ambiente DISCORD_TOKEN e GUILD_ID
 *   2) ficheiro leader.properties na pasta de trabalho (esta no .gitignore)
 *
 * Nao depende do JDA.
 */
public final class Configuracao {

    public static final String ENV_TOKEN = "DISCORD_TOKEN";
    public static final String ENV_GUILD = "GUILD_ID";
    public static final String FICHEIRO = "leader.properties";

    private final String token;
    private final String guildId;

    private Configuracao(String token, String guildId) {
        this.token = token;
        this.guildId = guildId;
    }

    public String getToken() {
        return token;
    }

    public String getGuildId() {
        return guildId;
    }

    /** Nunca mostra o token (para nao ir parar a logs ou capturas de ecra). */
    @Override
    public String toString() {
        return "Configuracao{token=***, guildId=" + guildId + "}";
    }

    public static Configuracao carregar() throws IOException {
        return carregar(System.getenv(), Path.of(FICHEIRO));
    }

    /** Versao testavel: recebe o "ambiente" e o caminho do ficheiro. */
    public static Configuracao carregar(Map<String, String> ambiente, Path ficheiro) throws IOException {
        Properties p = new Properties();
        if (Files.isRegularFile(ficheiro)) {
            try (Reader r = Files.newBufferedReader(ficheiro, StandardCharsets.UTF_8)) {
                p.load(r);
            }
        }
        String token = primeiroNaoVazio(ambiente.get(ENV_TOKEN), p.getProperty("token"));
        String guild = primeiroNaoVazio(ambiente.get(ENV_GUILD), p.getProperty("guild_id"));

        if (token == null) {
            throw new IllegalStateException("Falta o token do bot. Define a variavel " + ENV_TOKEN
                    + " ou cria " + FICHEIRO + " (copia leader.properties.example) com token=...");
        }
        if (guild == null || !guild.matches("[0-9]{5,25}")) {
            throw new IllegalStateException("Falta o id do servidor (GUILD_ID / guild_id em " + FICHEIRO
                    + "), ou nao e numerico. No Discord: Definicoes > Avancadas > Modo de programador, "
                    + "depois clica com o botao direito no servidor > Copiar ID.");
        }
        return new Configuracao(token, guild);
    }

    private static String primeiroNaoVazio(String a, String b) {
        if (a != null && !a.trim().isEmpty()) {
            return a.trim();
        }
        if (b != null && !b.trim().isEmpty()) {
            return b.trim();
        }
        return null;
    }
}
