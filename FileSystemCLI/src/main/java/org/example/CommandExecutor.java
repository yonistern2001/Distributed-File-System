package org.example;

import lombok.RequiredArgsConstructor;
import org.example.model.ChunkMapping;
import org.example.model.UploadInitRequest;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.io.*;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

@RequiredArgsConstructor
public class CommandExecutor {

    private final static int RETRIES = 6;
    private final static int CHUNK_SIZE = 50000;
    private final NodeHttpClient httpClient;
    private final LeaderResolver leaderResolver;

    public void upload(String localPath, String targetPath) {
        preformUpload(localPath, targetPath);

        sendRequestToLeader(leaderAddr -> {
            httpClient.sendUploadCompleteRequest(leaderAddr, targetPath);
            return null;
        });
    }

    private void preformUpload(String localPath, String targetPath) {

        long fileSize = new File(localPath).length();
        for (int i = 0; i < RETRIES; i++) {
            UploadInitRequest uploadInitRequest = new UploadInitRequest(targetPath, fileSize, CHUNK_SIZE);
            ChunkMapping chunkMapping = sendRequestToLeader(leaderAddr -> httpClient.sendUploadInitRequest(leaderAddr, uploadInitRequest));

            try(FileInputStream fileInputStream = new FileInputStream(localPath)) {

                uploadChunks(chunkMapping, fileInputStream);
                return;

            } catch (WebClientRequestException ignored) {
                delete(targetPath);

            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            sleep(1000);
        }

        throw new RuntimeException("Uploading chunks failed");
    }

    private void uploadChunks(ChunkMapping chunkMapping, FileInputStream fileInputStream) throws IOException {
        for (ChunkMapping.ChunkEntry chunk : chunkMapping.getChunkMapping()) {
            byte[] data = fileInputStream.readNBytes(CHUNK_SIZE);

            for (String node : chunk.getNodeAddresses()) {
                this.httpClient.sendPutChunkRequest(node, chunk.getChunkId(), data);
            }
        }
    }

    public void download(String targetPath, String localPath) {

        ChunkMapping chunkMapping = sendRequestToLeader(leaderAddr -> httpClient.sendDownloadInitRequest(leaderAddr, targetPath));

        try (FileOutputStream fileOutputStream = new FileOutputStream(localPath)) {

            for (ChunkMapping.ChunkEntry chunk : chunkMapping.getChunkMapping()) {
                byte[] data = retrieveChunk(chunk);
                fileOutputStream.write(data);
            }

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[] retrieveChunk(ChunkMapping.ChunkEntry chunk) {
        for (String nodeAddr : chunk.getNodeAddresses()) {
            try {
                return httpClient.sendGetChunkRequest(nodeAddr, chunk.getChunkId());

            } catch (WebClientRequestException ignored) {}
        }

        throw new RuntimeException("Unable to retrieve chunk: " + chunk.getChunkId());
    }

    public void delete(String targetPath) {
        sendRequestToLeader(leaderAddr -> {
            httpClient.sendDeleteRequest(leaderAddr, targetPath);
            return null;
        });
    }

    public List<String> list(String prefix) {
        return sendRequestToLeader(leaderAddr -> httpClient.sendFileListRequest(leaderAddr, prefix));
    }

    private <T> T sendRequestToLeader(Function<String, T> requestSender) {
        for (int i = 0; i < RETRIES; i++) {
            try {
                String leaderAddress = leaderResolver.getLeader();
                if(leaderAddress != null) {
                    return requestSender.apply(leaderAddress);
                }

            } catch (WebClientRequestException | ExecutionException ignored) {

            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }

            sleep(2650);
        }

        throw new RuntimeException("Could not communicate with leader");
    }

    private static void sleep(long milis) {
        try {
            Thread.sleep(milis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void close() {
        this.leaderResolver.close();
    }
}
