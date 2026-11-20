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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sprint E2-01: base do lider como bot do Discord.
 *
 * Esta classe e so a "cola" com o JDA. As decisoes (comando valido? tem permissao?
 * a mensagem interessa?) estao em Decisor / Comandos / CanalArquivo, que se testam sem Discord.
 *
 * Nada do arquivo (registo, cifra, replicas, recuperacao) e implementado aqui.
 */
public class LeaderBot extends ListenerAdapter {

    private final long canalArquivoId;

    private LeaderBot(long canalArquivoId) {
        this.canalArquivoId = canalArquivoId;
    }

    public static void main(String[] args) throws Exception {
        tpf.Configuracao cfg = Configuracao.carregar();

        // MESSAGE_CONTENT e um intent "privilegiado": tem de estar ligado no Developer Portal
        // (Bot > Privileged Gateway Intents > Message Content Intent), senao o login falha.
        JDA jda = JDABuilder.createDefault(cfg.getToken())
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .build()
                .awaitReady();

        Guild guild = jda.getGuildById(cfg.getGuildId());
        if (guild == null) {
            System.err.println("O bot nao esta no servidor " + cfg.getGuildId()
                    + ". Convida-o com o URL OAuth2 (scopes bot + applications.commands).");
            jda.shutdown();
            return;
        }

        Role papelAdmin = guild.getRolesByName(Permissoes.PAPEL_ADMIN, true).stream()
                .findFirst()
                .orElseGet(() -> guild.createRole().setName(Permissoes.PAPEL_ADMIN).complete());
        TextChannel canalArquivo = guild.getTextChannelsByName(CanalArquivo.NOME_CANAL, true).stream()
                .findFirst()
                .orElseGet(() -> guild.createTextChannel(CanalArquivo.NOME_CANAL).complete());
        System.out.println("Recursos Discord prontos: papel " + papelAdmin.getName()
                + " e canal #" + canalArquivo.getName() + ".");

        jda.addEventListener(new LeaderBot(canalArquivo.getIdLong()));

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

        // Erros so aparecem a quem escreveu o comando (ephemeral); respostas ok ficam visiveis no canal.
        event.reply(r.getMensagem()).setEphemeral(!r.isOk()).queue();
        System.out.println("[comando] " + event.getUser().getName() + " -> " + event.getCommandString()
                + " => " + (r.isOk() ? "aceite" : "recusado"));
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        boolean autorEBot = event.getAuthor().isBot() || event.isWebhookMessage();
        if (!CanalArquivo.deveProcessar(autorEBot, event.isFromGuild(), event.getChannel().getName())
                || !CanalArquivo.eCanalArquivo(event.getChannel().getIdLong(), canalArquivoId)) {
            return;
        }
        // Neste sprint so detetamos e registamos na consola; o arquivo vem em sprints seguintes.
        System.out.println("[#arquivo] " + event.getAuthor().getName() + ": "
                + event.getMessage().getContentDisplay());
    }
}
