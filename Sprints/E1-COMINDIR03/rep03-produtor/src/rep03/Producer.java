package rep03;

import java.util.Random;
import java.util.Scanner;

/**
 * Ponto de entrada das leituras. Lê comandos da consola e publica mensagens {@code IN} no tópico
 * de entrada da partição do sensor.
 *
 * <p>Invocação: {@code Producer <N> [api]}, com {@code N} o número de partições e {@code api} a
 * URL base do Kubo (por omissão {@code http://127.0.0.1:5001/api/v0}).</p>
 *
 * <p>Comandos: {@code <sensor> <temperatura>}; {@code rajada <n> <s>} (n leituras repartidas pelos
 * sensores S01..S&lt;s&gt;, 15 a 30 °C); {@code rajada-quente <n> <s>} (80 % das leituras do sensor
 * S01); {@code sair}.</p>
 *
 * <p>O produtor não numera nem guarda o que envia: uma perda neste troço não é detetável nem
 * recuperável.</p>
 *
 * <h2>Fluxo</h2>
 * <ol>
 *   <li>Lê uma linha da consola (<strong>bloqueia</strong> em {@code Scanner.nextLine()}).</li>
 *   <li>Valida o sensor e a temperatura ({@link #validar(String, double)}).</li>
 *   <li>Calcula a partição {@code k = floorMod(hashCode(sensor), N)}.</li>
 *   <li>Publica {@code IN;<sensor>;<temp>;0;<vetor-zero>} no tópico {@code rep03-in-p<k>} (pedido HTTP ao Kubo,
 *       <strong>bloqueia</strong> até o Kubo responder).</li>
 * </ol>
 *
 * <h2>O que pode correr mal</h2>
 * <ul>
 *   <li>Kubo desligado ou sem PubSub ativo: a publicação falha; o produtor avisa e continua.</li>
 *   <li>Nenhum líder subscrito no momento: o Kubo aceita a mensagem e ela perde-se em silêncio
 *       (o PubSub não guarda mensagens).</li>
 *   <li>Sensor com {@code ;} partiria o formato da mensagem: é rejeitado antes de publicar.</li>
 *   <li>N diferente do usado pelos líderes: as leituras vão para o tópico errado e o líder
 *       responde {@code PARTIÇÃO ERRADA}.</li>
 * </ul>
 */
public class Producer {
    /** Função de partição. */
    private final Partitioner partitioner;
    /** Destino das mensagens {@code IN}. */
    private final Publisher publisher;

    /**
     * Cria o produtor.
     *
     * @param partitioner função de partição
     * @param publisher   destino das mensagens {@code IN}
     */
    // Recebe a função de partição e o publicador, permitindo usar transporte real ou simulado nos testes.
    public Producer(Partitioner partitioner, Publisher publisher) {
        this.partitioner = partitioner;
        this.publisher = publisher;
    }

    /**
     * Publica uma leitura no tópico de entrada da partição do sensor.
     *
     * @param sensor identificador do sensor
     * @param temp   temperatura
     * @return a partição de destino
     * @throws IllegalArgumentException se o sensor ou a temperatura forem inválidos (nada é publicado)
     * @throws Exception se a publicação falhar
     */
    // Valida a leitura, calcula o destino e envia IN com L=0 e vetor zero, pois o produtor não observa registos.
    public int publicar(String sensor, double temp) throws Exception {
        validar(sensor, temp);
        int k = partitioner.of(sensor);
        publisher.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0, new VectorClock(partitioner.count())));
        return k;
    }

    /**
     * Valida uma leitura antes de a publicar.
     *
     * <p>O identificador do sensor viaja dentro de mensagens separadas por {@code ;} e é gravado em
     * linhas {@code seq;sensor;temp;instante}; um {@code ;} ou espaços no nome partiriam esse
     * formato em todos os processos a jusante. Temperaturas {@code NaN} ou infinitas também não
     * fazem sentido para um sensor.</p>
     *
     * @param sensor identificador do sensor
     * @param temp   temperatura
     * @throws IllegalArgumentException se o sensor for nulo, vazio, tiver {@code ;} ou espaços, ou
     *                                  se a temperatura não for um número finito
     */
    // Rejeita sensores incompatíveis com o formato textual e temperaturas que não sejam números finitos.
    public static void validar(String sensor, double temp) {
        if (sensor == null || sensor.isBlank() || sensor.contains(";") || !sensor.equals(sensor.trim())
                || sensor.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("sensor inválido: '" + sensor + "'");
        }
        if (!Double.isFinite(temp)) {
            throw new IllegalArgumentException("temperatura inválida: " + temp);
        }
    }

    /**
     * Publica uma rajada de leituras à máxima cadência.
     *
     * @param n      número de leituras
     * @param s      número de sensores (S01..S&lt;s&gt;)
     * @param quente se verdadeiro, 80 % das leituras são do sensor S01 (chave muito frequente)
     * @param rnd    gerador aleatório (injetado para os testes serem reprodutíveis)
     * @return número de leituras publicadas por partição
     * @throws Exception se a publicação falhar
     */
    // Gera n leituras aleatórias para s sensores e conta os envios por partição; o modo quente concentra leituras em S01.
    public int[] rajada(int n, int s, boolean quente, Random rnd) throws Exception {
        int[] porParticao = new int[partitioner.count()];
        for (int i = 0; i < n; i++) {
            int idx = (quente && rnd.nextDouble() < 0.8) ? 1 : 1 + rnd.nextInt(s);
            String sensor = String.format("S%02d", idx);
            double temp = Math.round((15 + rnd.nextDouble() * 15) * 10) / 10.0;
            porParticao[publicar(sensor, temp)]++;
        }
        return porParticao;
    }

    /**
     * Arranca o produtor interativo.
     *
     * @param args {@code <N> [api]}
     * @throws Exception se o Kubo não estiver acessível
     */
    // Arranca a consola do produtor e interpreta comandos de leitura, rajada e saída.
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: Producer <N> [api]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        Producer p = new Producer(new Partitioner(n), new IpfsPubSub(api));
        Scanner sc = new Scanner(System.in);
        System.out.println("Comandos: <sensor> <temp> | rajada <n> <s> | rajada-quente <n> <s> | sair");
        while (sc.hasNextLine()) {
            String[] t = sc.nextLine().trim().split("\\s+");
            try {
                if (t[0].equals("sair")) {
                    break;
                } else if (t[0].startsWith("rajada") && t.length == 3) {
                    long t0 = System.currentTimeMillis();
                    int[] c = p.rajada(Integer.parseInt(t[1]), Integer.parseInt(t[2]),
                            t[0].equals("rajada-quente"), new Random());
                    System.out.println("primeira..última publicação: " + t0 + ".." + System.currentTimeMillis());
                    for (int k = 0; k < c.length; k++) {
                        System.out.println("partição " + k + ": " + c[k] + " leituras");
                    }
                } else if (t.length == 2) {
                    System.out.println("publicado na partição " + p.publicar(t[0], Double.parseDouble(t[1])));
                } else {
                    System.out.println("Comando inválido");
                }
            } catch (NumberFormatException e) {
                System.out.println("Número inválido: " + e.getMessage());
            } catch (IllegalArgumentException e) {
                System.out.println("REJEITADO: " + e.getMessage());
            } catch (java.io.IOException e) {
                // Kubo desligado, sem PubSub ou a responder com erro: avisa e continua a aceitar comandos
                System.out.println("PUBLICAÇÃO FALHOU (o Kubo está a correr com PubSub ativo?): " + e.getMessage());
            }
        }
    }
}
