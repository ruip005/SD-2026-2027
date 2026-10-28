package rep03;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Cliente mínimo da API HTTP do Kubo (IPFS) para PubSub, feito com {@link java.net.http.HttpClient}.
 *
 * <p>Fornece comunicação indireta: quem publica não conhece quem subscreve. A entrega é
 * <em>best-effort</em> (pode haver perdas e reordenação) e o Kubo não guarda mensagens: quem
 * não tiver a subscrição aberta no momento da publicação não as recebe. Herdada da COMINDIR01.</p>
 *
 * <p><strong>Como funciona por baixo</strong>: o Java não fala o protocolo IPFS. Faz pedidos HTTP
 * {@code POST} à API do nó local ({@code 127.0.0.1:5001}). É o daemon Kubo que espalha a mensagem
 * pelos outros nós ligados (protocolo gossipsub sobre libp2p, porta 4001) e a entrega aos
 * subscritores. Os nomes dos tópicos e os dados vão em multibase porque a API o exige.</p>
 *
 * <p><strong>O que pode correr mal</strong>: daemon desligado ({@code ConnectException}); PubSub não
 * ativado ({@code Pubsub.Enabled}) e a API responde com erro; a subscrição cai se o daemon reiniciar
 * (os {@code main} voltam a subscrever); o pedido de publicação não tem tempo limite, por isso um
 * daemon pendurado bloqueia quem publica.</p>
 */
public class IpfsPubSub implements Publisher {
    /** Texto da resposta do Kubo quando rejeita uma publicação numa rajada (repete-se). */
    private static final String VALIDACAO_IGNORADA = "validation ignored";
    /** Número máximo de envios de uma mensagem. */
    private static final int MAX_TENTATIVAS = 5;
    /** Pausa base entre tentativas (cresce linearmente). */
    private static final long PAUSA_MS = 50;
    /** URL base da API do Kubo, por exemplo {@code http://127.0.0.1:5001/api/v0}. */
    private final String api;
    /** Cliente HTTP reutilizado em todos os pedidos. */
    private final HttpClient http = HttpClient.newHttpClient();

    /**
     * Cria o cliente.
     *
     * @param api URL base da API do Kubo, por exemplo {@code http://127.0.0.1:5001/api/v0}
     */
    // Guarda o endereço da API HTTP do Kubo que será usada para publicar e subscrever.
    public IpfsPubSub(String api) {
        this.api = api;
    }

    /**
     * Codifica em multibase (base64url sem padding, prefixo {@code u}), como o Kubo exige.
     *
     * @param s texto a codificar
     * @return o texto codificado, com prefixo {@code u}
     */
    // o Kubo exige o tópico e os dados em multibase: base64url sem padding, com prefixo 'u'
    // Codifica texto UTF-8 em base64url sem padding, acrescentando o prefixo multibase u exigido pelo Kubo.
    static String toMultibase(String s) {
        return "u" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Inverso de {@link #toMultibase(String)}.
     *
     * @param s texto em multibase
     * @return o texto original
     * @throws IllegalArgumentException se o prefixo não for {@code u}
     */
    // Valida o prefixo u e descodifica o conteúdo base64url para texto UTF-8.
    static String fromMultibase(String s) {
        if (!s.startsWith("u")) {
            throw new IllegalArgumentException("Codificação multibase não suportada: " + s);
        }
        return new String(Base64.getUrlDecoder().decode(s.substring(1)), StandardCharsets.UTF_8);
    }

    /**
     * Publica uma mensagem no tópico (o corpo segue como ficheiro multipart).
     *
     * @param topic   nome do tópico em texto simples (é convertido para multibase)
     * @param message conteúdo da mensagem
     * @throws IOException          se o Kubo responder com erro ou não estiver acessível
     * @throws InterruptedException se a thread for interrompida durante o pedido
     */
    @Override
    // Publica pelo Kubo e repete apenas a rejeição validation ignored, com pausa e limite de tentativas.
    public void publish(String topic, String message) throws IOException, InterruptedException {
        for (int tentativa = 1; ; tentativa++) {
            HttpResponse<String> resp = enviar(topic, message);
            if (resp.statusCode() == 200) {
                return;
            }
            // Em rajadas o Kubo rejeita a mensagem localmente ("validation ignored"): nada saiu,
            // por isso repete com uma pausa curta. Outros erros não se repetem.
            if (!resp.body().contains(VALIDACAO_IGNORADA) || tentativa == MAX_TENTATIVAS) {
                throw new IOException("pub falhou (" + resp.statusCode() + "): " + resp.body());
            }
            Thread.sleep(PAUSA_MS * tentativa);
        }
    }

    /**
     * Envia um pedido de publicação ao Kubo, sem tratar a resposta.
     *
     * @param topic   nome do tópico em texto simples
     * @param message conteúdo da mensagem
     * @return a resposta HTTP do Kubo
     * @throws IOException          se a ligação ao Kubo falhar
     * @throws InterruptedException se a thread for interrompida durante o pedido
     */
    // Monta o corpo multipart e executa o pedido HTTP de publicação, devolvendo a resposta do Kubo.
    private HttpResponse<String> enviar(String topic, String message) throws IOException, InterruptedException {
        String boundary = "rep03-" + System.nanoTime();
        String body = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"file\"; filename=\"msg\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\n"
                + message + "\r\n"
                + "--" + boundary + "--\r\n";
        HttpRequest req = HttpRequest.newBuilder(URI.create(api + "/pubsub/pub?arg=" + toMultibase(topic)))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Subscreve o tópico. <strong>Bloqueia indefinidamente</strong>: cada linha da resposta HTTP é
     * uma mensagem JSON, descodificada e entregue a {@code onMessage}. Por isso cada subscrição
     * precisa de uma thread dedicada.
     *
     * @param topic     nome do tópico em texto simples
     * @param onMessage tratador chamado com o conteúdo de cada mensagem recebida
     * @throws IOException          se o Kubo responder com erro ou a ligação cair
     * @throws InterruptedException se a thread for interrompida
     */
    // Mantém a subscrição HTTP aberta, descodifica cada mensagem recebida e chama onMessage; bloqueia a thread.
    public void subscribe(String topic, Consumer<String> onMessage) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(api + "/pubsub/sub?arg=" + toMultibase(topic)))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<Stream<String>> resp = http.send(req, HttpResponse.BodyHandlers.ofLines());
        if (resp.statusCode() != 200) {
            throw new IOException("sub falhou (" + resp.statusCode() + ")");
        }
        resp.body() // bloqueia à espera de cada linha
                .filter(json -> json.contains("\"data\""))
                .forEach(json -> onMessage.accept(fromMultibase(extractField(json, "data"))));
    }

    /**
     * Extrai o valor de um campo de texto de um objeto JSON simples.
     *
     * @param json  objeto JSON numa linha
     * @param field nome do campo
     * @return o valor do campo
     * @throws IllegalArgumentException se o campo não existir
     */
    // extração mínima de um campo de texto de um objeto JSON simples (evita bibliotecas externas)
    // Extrai um campo de texto do JSON simples recebido do Kubo, sem recorrer a bibliotecas externas.
    static String extractField(String json, String field) {
        String key = "\"" + field + "\":\"";
        int start = json.indexOf(key);
        if (start < 0) {
            throw new IllegalArgumentException("Campo '" + field + "' ausente: " + json);
        }
        start += key.length();
        return json.substring(start, json.indexOf('"', start));
    }
}