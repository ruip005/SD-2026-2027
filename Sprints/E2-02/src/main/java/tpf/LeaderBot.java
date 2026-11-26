package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Role;
import net.dv8tion.jda.api.entities.channel.concrete.TextChannel;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.DefaultMemberPermissions;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Liga os eventos do Discord à validação de comandos e ao arquivo local.
 */
public class LeaderBot extends ListenerAdapter {

    private final long canalArquivoId;
    private final ArquivoLider arquivo;
    private final ExecutorService rajadas = Executors.newSingleThreadExecutor(tarefa -> {
        Thread thread = new Thread(tarefa, "rajada-lider");
        thread.setDaemon(true);
        return thread;
    });

    private LeaderBot(long canalArquivoId, ArquivoLider arquivo) {
        this.canalArquivoId = canalArquivoId;
        this.arquivo = arquivo;
    }

    public static void main(String[] args) throws Exception {
        Configuracao cfg = Configuracao.carregar();
        ArquivoLider arquivo = new ArquivoLider(Path.of("."));
        try {
            iniciar(cfg, arquivo);
        } catch (Exception erro) {
            try {
                arquivo.close();
            } catch (IOException erroFecho) {
                erro.addSuppressed(erroFecho);
            }
            throw erro;
        }
    }

    private static void iniciar(Configuracao cfg, ArquivoLider arquivo) throws Exception {
        // MESSAGE_CONTENT e um intent "privilegiado": tem de estar ligado no Developer Portal
        // (Bot > Privileged Gateway Intents > Message Content Intent), senao o login falha.
        JDA jda = JDABuilder.createDefault(cfg.getToken())
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .build();

        try {
            configurarServidor(jda.awaitReady(), cfg, arquivo);
        } catch (Exception erro) {
            jda.shutdownNow();
            throw erro;
        }
    }

    private static void configurarServidor(JDA jda, Configuracao cfg, ArquivoLider arquivo) {
        Guild guild = jda.getGuildById(cfg.getGuildId());
        if (guild == null) {
            throw new IllegalStateException("O bot nao esta no servidor " + cfg.getGuildId()
                    + ". Convida-o com o URL OAuth2 (scopes bot + applications.commands).");
        }

        Role papelAdmin = guild.getRolesByName(Permissoes.PAPEL_ADMIN, true).stream()
                .findFirst()
                .orElseGet(() -> guild.createRole().setName(Permissoes.PAPEL_ADMIN).complete());
        TextChannel canalArquivo = guild.getTextChannelsByName(CanalArquivo.NOME_CANAL, true).stream()
                .findFirst()
                .orElseGet(() -> guild.createTextChannel(CanalArquivo.NOME_CANAL).complete());
        System.out.println("Recursos Discord prontos: papel " + papelAdmin.getName()
                + " e canal #" + canalArquivo.getName() + ".");

        LeaderBot bot = new LeaderBot(canalArquivo.getIdLong(), arquivo);
        jda.addEventListener(bot);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            jda.shutdownNow();
            bot.rajadas.shutdownNow();
            try {
                arquivo.close();
            } catch (IOException erro) {
                System.err.println("Falha a fechar registos.log: " + erro.getMessage());
            }
        }, "fecho-lider"));

        // Guild commands aparecem logo (os globais podem demorar ate 1 hora).
        // updateCommands() substitui a lista inteira: comandos antigos desaparecem.
        guild.updateCommands().addCommands(
                Commands.slash(Comandos.REGISTAR, "Regista um texto no arquivo")
                        .addOption(OptionType.STRING, Comandos.ARG_TEXTO, "Texto a registar", true),
                Commands.slash(Comandos.ESTADO, "Mostra o estado do líder"),
                Commands.slash(Comandos.CONFIG, "Configura o líder (só admin-arquivo)")
                        .addSubcommands(new SubcommandData("r", "Define o fator de replicação R")
                                .addOption(OptionType.INTEGER, Comandos.ARG_N, "Novo valor de R", true))
                        .setDefaultPermissions(DefaultMemberPermissions.DISABLED),
                Commands.slash(Comandos.RECUPERAR, "Recupera registos entre dois números de sequência")
                        .addOption(OptionType.INTEGER, Comandos.ARG_DE, "Primeiro número de sequência", true)
                        .addOption(OptionType.INTEGER, Comandos.ARG_ATE, "Último número de sequência", true),
                Commands.slash(Comandos.REPLICA_TOKEN, "Gera um token para uma nova réplica (só admin-arquivo)")
                        .setDefaultPermissions(DefaultMemberPermissions.DISABLED),
                Commands.slash(Comandos.REPLICA_REMOVER, "Remove uma réplica (só admin-arquivo)")
                        .addOption(OptionType.STRING, Comandos.ARG_ID, "Id da réplica", true)
                        .setDefaultPermissions(DefaultMemberPermissions.DISABLED),
                Commands.slash(Comandos.RAJADA, "Envia uma rajada de registos de teste (só admin-arquivo)")
                        .addOption(OptionType.INTEGER, Comandos.ARG_N, "Número de registos", true)
                        .addOption(OptionType.INTEGER, Comandos.ARG_INTERVALO_MS, "Intervalo entre registos (ms)", true)
                        .setDefaultPermissions(DefaultMemberPermissions.DISABLED)
        ).queue(
                ok -> System.out.println("LeaderBot online em \"" + guild.getName() + "\" com "
                        + Comandos.TODOS.size() + " comandos registados."),
                erro -> System.err.println("Falha ao registar comandos: " + erro.getMessage()));
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        if (event.getGuild() == null
                || !CanalArquivo.eCanalArquivo(event.getChannel().getIdLong(), canalArquivoId)) {
            event.reply("Os comandos só podem ser usados em #" + CanalArquivo.NOME_CANAL + ".")
                    .setEphemeral(true)
                    .queue();
            return;
        }

        // Papeis do membro que escreveu o comando (vem no proprio pedido, nao precisa de intent extra).
        List<String> papeis = new ArrayList<>();
        if (event.getMember() != null) {
            for (Role r : event.getMember().getRoles()) {
                papeis.add(r.getName());
            }
        }

        // Converte as opcoes do JDA num Map simples: nome -> valor em texto.
        Map<String, String> args = new LinkedHashMap<>();
        if (event.getSubcommandName() != null) {
            args.put(Comandos.ARG_SUBCOMANDO, event.getSubcommandName());
        }
        for (OptionMapping o : event.getOptions()) {
            args.put(o.getName(), o.getAsString());
        }

        Resultado r = Decisor.decidir(event.getName(), papeis, args);

        if (r.isOk() && Comandos.REGISTAR.equals(event.getName())) {
            try {
                long lsn = arquivo.registar(event.getUser().getName(), CanalArquivo.NOME_CANAL,
                        args.get(Comandos.ARG_TEXTO));
                event.reply("Registado com LSN " + lsn + ".").queue();
            } catch (IOException erro) {
                System.err.println("Falha no registo: " + erro.getMessage());
                event.reply("Falha ao registar o texto.").setEphemeral(true).queue();
            }
            return;
        }
        if (r.isOk() && Comandos.RAJADA.equals(event.getName())) {
            iniciarRajada(event, Integer.parseInt(args.get(Comandos.ARG_N)),
                    Long.parseLong(args.get(Comandos.ARG_INTERVALO_MS)));
            return;
        }

        // Erros so aparecem a quem escreveu o comando (ephemeral); respostas ok ficam visiveis no canal.
        event.reply(r.getMensagem()).setEphemeral(!r.isOk()).queue();
        System.out.println("[comando] " + event.getUser().getName() + " -> " + event.getCommandString()
                + " => " + (r.isOk() ? "aceite" : "recusado"));
    }

    private void iniciarRajada(SlashCommandInteractionEvent event, int n, long intervalo) {
        String autor = event.getUser().getName();
        String pedido = event.getId();
        event.deferReply(true).queue(resposta -> rajadas.execute(() -> {
            int gravados = 0;
            try {
                for (int i = 1; i <= n; i++) {
                    String texto = "Rajada " + pedido + " [" + i + "/" + n + "] "
                            + "Registo sintético de teste do arquivo do líder. ".repeat(4);
                    arquivo.registar(autor, CanalArquivo.NOME_CANAL, texto);
                    gravados++;
                    if (intervalo > 0 && i < n) {
                        Thread.sleep(intervalo);
                    }
                }
                resposta.editOriginal("Rajada concluída. Registos gravados: " + gravados + ".").queue();
            } catch (IOException erro) {
                System.err.println("Falha na rajada: " + erro.getMessage());
                resposta.editOriginal("Rajada interrompida. Registos gravados: " + gravados + ".").queue();
            } catch (InterruptedException erro) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        boolean autorEBot = event.getAuthor().isBot() || event.isWebhookMessage();
        if (!CanalArquivo.deveProcessar(autorEBot, event.isFromGuild(), event.getChannel().getName())
                || !CanalArquivo.eCanalArquivo(event.getChannel().getIdLong(), canalArquivoId)) {
            return;
        }
        try {
            arquivo.registar(event.getAuthor().getName(), CanalArquivo.NOME_CANAL,
                    event.getMessage().getContentRaw());
        } catch (IOException erro) {
            System.err.println("Falha ao registar mensagem de #arquivo: " + erro.getMessage());
        }
    }
}
