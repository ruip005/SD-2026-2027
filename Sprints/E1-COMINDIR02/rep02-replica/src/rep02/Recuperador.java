package rep02;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Canal de recuperação de registos perdidos, a partir do líder da partição.
 *
 * <p>Existe como interface para a lógica da réplica poder ser testada sem rede: em produção usa-se
 * {@link TcpRecuperador}; nos testes, uma implementação que devolve registos fixos ou falha.</p>
 */
public interface Recuperador {
    /**
     * Pede ao líder os registos de {@code de} a {@code ate} (inclusivos).
     *
     * <p>Cada linha de registo é entregue a {@code linha} <strong>à medida que chega</strong>, pelo
     * que, se a ligação falhar a meio, os registos já entregues não se perdem.</p>
     *
     * @param host  endereço do líder (anunciado no último {@code REG})
     * @param porto porto de recuperação do líder
     * @param de    primeiro {@code seq} pedido
     * @param ate   último {@code seq} pedido
     * @param linha tratador de cada linha de registo recebida
     * @throws IOException se a ligação for recusada, esgotar o tempo, o líder responder
     *                     {@code ERRO} ou a ligação terminar antes de {@code FIM}
     */
    void pedir(String host, int porto, long de, long ate, Consumer<String> linha) throws IOException;
}
