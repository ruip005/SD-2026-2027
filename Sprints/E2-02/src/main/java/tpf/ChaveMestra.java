package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.EnumSet;
import java.util.List;

final class ChaveMestra {
    private static final int TAMANHO = 32;

    private ChaveMestra() {
    }

    static byte[] carregar(Path pasta) throws IOException {
        Path ficheiro = pasta.resolve("master.key");
        if (Files.notExists(ficheiro, LinkOption.NOFOLLOW_LINKS)) {
            byte[] chave = new byte[TAMANHO];
            new SecureRandom().nextBytes(chave);
            boolean posix = Files.getFileAttributeView(pasta, PosixFileAttributeView.class) != null;
            Path temporario = posix
                    ? Files.createTempFile(pasta, "master-", ".key",
                            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
                    : Files.createTempFile(pasta, "master-", ".key");
            try {
                if (!posix) {
                    restringirAcl(temporario);
                }
                try (FileChannel canal = FileChannel.open(temporario, StandardOpenOption.WRITE)) {
                    ByteBuffer dados = ByteBuffer.wrap(chave);
                    while (dados.hasRemaining()) {
                        canal.write(dados);
                    }
                    canal.force(true);
                }
                Files.move(temporario, ficheiro, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(temporario);
            }
        }
        if (!Files.isRegularFile(ficheiro, LinkOption.NOFOLLOW_LINKS) || Files.size(ficheiro) != TAMANHO) {
            throw new IOException("master.key tem de ser um ficheiro normal de 32 bytes");
        }
        byte[] chave = Files.readAllBytes(ficheiro);
        if (chave.length != TAMANHO) {
            throw new IOException("master.key tem de ter 32 bytes");
        }
        return chave;
    }

    private static void restringirAcl(Path ficheiroVazio) throws IOException {
        AclFileAttributeView acl = Files.getFileAttributeView(ficheiroVazio, AclFileAttributeView.class);
        if (acl == null) {
            throw new IOException("O sistema de ficheiros não suporta permissões privadas para master.key");
        }
        var dono = acl.getOwner();
        List<AclEntry> privada = List.of(AclEntry.newBuilder().setType(AclEntryType.ALLOW)
                .setPrincipal(dono).setPermissions(EnumSet.allOf(AclEntryPermission.class)).build());
        acl.setAcl(privada);
        List<AclEntry> aplicada = acl.getAcl();
        if (aplicada.size() != 1 || !aplicada.get(0).principal().equals(dono)) {
            throw new IOException("Não foi possível tornar master.key privada");
        }
    }
}
