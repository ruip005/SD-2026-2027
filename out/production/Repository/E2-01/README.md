# tpf-lider (Sprint E2-01)

LeaderBot em Java 17 + JDA 5. Recebe os 7 comandos no Discord e responde com uma mensagem provisória. Nenhuma funcionalidade do arquivo está implementada.

## Configurar o token

1. Copia `leader.properties.example` para `leader.properties` (está no `.gitignore`).
2. Preenche `token` (Developer Portal > Bot > Reset Token) e `guild_id` (id do servidor de teste).
3. Em alternativa, define as variáveis de ambiente `DISCORD_TOKEN` e `GUILD_ID`.

Nunca coloques o token num commit. Se isso acontecer, faz Reset Token.

## Pré-requisitos no Discord

- Message Content Intent ligado (Bot > Privileged Gateway Intents).
- Bot convidado com os scopes `bot` e `applications.commands`.
- Canal `#arquivo` e papel `admin-arquivo` no servidor.

## Arrancar

```
mvn package
java -jar target/tpf-lider-0.1.0.jar
```

No IntelliJ: correr `tpf.LeaderBot` com o Working directory na pasta deste projeto.

Sucesso: a consola mostra «LeaderBot online» com 7 comandos registados.
