# LeaderBot — E2-01

## Configurar e iniciar

1. Copia `leader.properties.example` para `leader.properties` e define `token` e `guild_id` com os dados do bot e do servidor. O ficheiro real é ignorado pelo Git. Também podes usar `DISCORD_TOKEN` e `GUILD_ID` como variáveis de ambiente.
2. No portal de desenvolvimento do Discord, ativa **Message Content Intent**. Dá ao bot as permissões **Manage Roles** e **Manage Channels**, e mantém o cargo do bot acima de `admin-arquivo` na hierarquia.
3. Na pasta `Sprints/E2-01`, executa `mvn package` e depois `java -jar target/tpf-lider-0.1.0.jar`.

Na inicialização, o bot cria `admin-arquivo` e `#arquivo` se ainda não existirem. Os slash commands só respondem em `#arquivo`; as mensagens de texto desse canal são detetadas na consola.

## Acesso aos comandos de administração

Os quatro comandos administrativos são registados com acesso desativado por omissão. Depois do primeiro arranque, um administrador do servidor deve abrir **Definições do servidor → Integrações → LeaderBot → Comandos** e permitir esses comandos ao cargo `admin-arquivo`. O bot também valida o cargo ao executar cada comando. O Discord pode sempre mostrar comandos a administradores do servidor; a validação do bot continua a exigir `admin-arquivo`.
