package raft04;

import java.io.IOException;
import java.util.function.Consumer;

/** Canal de recuperação de registos perdidos, a partir do líder da partição. */
public interface Recuperador {
    void pedir(String host, int porto, long de, long ate, Consumer<String> linha) throws IOException;
}
