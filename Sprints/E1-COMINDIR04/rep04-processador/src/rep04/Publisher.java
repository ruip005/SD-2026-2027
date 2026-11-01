package rep04;

/**
 * Destino de mensagens publicadas num tópico. Existe para que o código do líder e do produtor possa ser
 * testado sem um daemon IPFS: em produção usa-se {@link IpfsPubSub}, nos testes uma implementação em memória.
 */
@FunctionalInterface
public interface Publisher {
    void publish(String topic, String message) throws Exception;
}
