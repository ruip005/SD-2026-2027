package raft04;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** Cliente mínimo da API HTTP do Kubo (IPFS) para PubSub, feito com {@link HttpClient}. */
public class IpfsPubSub implements Publisher {
    /** Texto da resposta do Kubo quando rejeita uma publicação numa rajada (repete-se). */
    private static final String VALIDACAO_IGNORADA = "validation ignored";
    private static final int MAX_TENTATIVAS = 5;
    private static final long PAUSA_MS = 50;
    private final String api;
    private final HttpClient http = HttpClient.newHttpClient();

    public IpfsPubSub(String api) {
        this.api = api;
    }

    // o Kubo exige o tópico e os dados em multibase: base64url sem padding, com prefixo 'u'
    static String toMultibase(String s) {
        return "u" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    static String fromMultibase(String s) {
        if (!s.startsWith("u")) {
            throw new IllegalArgumentException("Codificação multibase não suportada: " + s);
        }
        return new String(Base64.getUrlDecoder().decode(s.substring(1)), StandardCharsets.UTF_8);
    }

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

    private HttpResponse<String> enviar(String topic, String message) throws IOException, InterruptedException {
        String boundary = "rep04-" + System.nanoTime();
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

    // extração mínima de um campo de texto de um objeto JSON simples (evita bibliotecas externas)
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