# LeaderBot — E2-02

## Arranque

Reutiliza o bot da E2-01. Nesta pasta, configura `DISCORD_TOKEN` e `GUILD_ID` ou copia
`leader.properties.example` para `leader.properties` e preenche `token` e `guild_id`.
No Discord, confirma que **Message Content Intent** está ativo e que o bot consegue
ler e escrever em `#arquivo`. Se o cargo ou o canal estiver em falta, o bot usa
**Manage Roles** ou **Manage Channels** para o criar, respetivamente.

Com Java 17 ou posterior, executa nesta pasta:

```text
mvn package
java -jar target/tpf-lider-0.1.0.jar
```

As propriedades abaixo podem ser passadas antes de `-jar`. O diretório de trabalho
contém a chave, os registos e os chunks deste líder.

| Propriedade | Valor por omissão | Unidade |
| --- | ---: | --- |
| `-Dtpf.chunkMaxBytes` | 65536 | bytes antes do padding |
| `-Dtpf.padBytes` | 4096 | bytes por bloco de padding |
| `-Dtpf.selagemMs` | 5000 | milissegundos desde a abertura do chunk |

## Registo e chunks

`master.key` contém 32 bytes aleatórios e é criada com acesso apenas para o dono.
Guarda a mesma chave entre arranques para continuar a abrir os chunks existentes.

`registos.log` é UTF-8, com um LF por linha e os campos
`lsn;instante;autor;canal;texto`. O LSN começa em 1, o instante está em ISO 8601
e o texto UTF-8 é codificado em Base64. O líder força a escrita antes de confirmar
o LSN. No arranque lê o diário, verifica os chunks e volta a carregar os registos
que ainda não foram selados.

O formato binário antes da cifra começa com a quantidade de registos (inteiro de
32 bits em big-endian). Para cada registo escreve um inteiro de 32 bits em
big-endian com o comprimento, seguido dos bytes UTF-8 da linha do diário sem o LF.
O registo que atinge ou ultrapassa o limite em bytes fica no chunk que se sela.
O padding acrescenta zeros até um múltiplo de `tpf.padBytes`. A leitura usa a
quantidade e os comprimentos para saber onde acabam os registos.

A DEK de cada chunk vem de HKDF-SHA256 com a chave mestra, um salt aleatório de
32 bytes e o contexto ASCII `tpf-chunk-v1`. AES-256-GCM cifra o chunk com nonce
aleatório de 12 bytes e tag de 16 bytes. O AAD concatena `tpf-v1` em ASCII, a
versão da chave em 4 bytes big-endian, o salt e o nonce.

- `selados/<chunkId>.bin` contém o texto cifrado seguido da tag GCM. O nome-base
  é o SHA-256 hexadecimal de todos esses bytes. Com o padding por omissão o
  tamanho do binário cifrado é um múltiplo de 4096 acrescido de 16 bytes.
- `selados/<chunkId>.hdr` contém uma linha ASCII `keyVersion;salt;nonce` com
  versão decimal `1` e salt e nonce em Base64. Usa o identificador do binário para
  associar os dois ficheiros.
- `eventos-lider.txt` contém linhas `instante;evento;detalhe`. `REGISTO`
  identifica o LSN e `SELADO` guarda o identificador do chunk, o intervalo de LSN
  e o comprimento do binário em bytes.

## Testes

Depois de `mvn package`, corre os testes sem ligação ao Discord. Em PowerShell:

```powershell
java -cp "target/classes;target/test-classes" tpf.TestesLogica
java -cp "target/classes;target/test-classes" tpf.TestesChunkCrypto
java -cp "target/classes;target/test-classes" tpf.TestesArquivoLider
```

No Discord, usa um canal `#arquivo` de testes. Envia `/registar texto:teste`, uma
mensagem normal em `#arquivo` e uma mensagem noutro canal. Confirma no diário
quais foram gravadas e verifica a continuação do LSN após reiniciar o bot.
`/rajada n:500 intervalo_ms:0` testa a selagem por tamanho. Para o executar,
um responsável do servidor deve atribuir-te `admin-arquivo` e autorizar esse cargo
em **Definições do servidor → Integrações → LeaderBot → Comandos**.
