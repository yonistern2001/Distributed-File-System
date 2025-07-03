package org.example.filesystem.repository;

import lombok.Getter;
import org.example.filesystem.exception.InsufficientStorageException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

@Getter
@Repository
public class ChunkRepository {

    @Value("${storage.init}")
    private long remainingStorage;

    @Value("${node.id}")
    private String nodeId;

    public void storeChunk(String chunkId, byte[] chunk) throws IOException {
        Path path = getPath(chunkId);
        remainingStorage -= chunk.length;

        if(remainingStorage < 0) {
            throw new InsufficientStorageException("Not enough storage to store chunk");
        }
        Files.createDirectories(path.getParent());
        Files.write(path, chunk);
    }

    public byte[] getChunk(String chunkId) throws IOException {
        Path path = getPath(chunkId);
        return Files.readAllBytes(path);
    }

    public void deleteChunk(String chunkId) throws IOException {
        Path path = getPath(chunkId);
        File file = new File(path.toString());
        long size = file.length();
        boolean deleted = file.delete();
        if(!deleted) {
            throw new NoSuchFileException(path.toString());
        }
        remainingStorage += size;
    }

    private Path getPath(String chunkId) {
        return Path.of("chunks", nodeId, chunkId);
    }
}
