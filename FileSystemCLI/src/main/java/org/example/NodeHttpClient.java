package org.example;

import org.example.model.ChunkMapping;
import org.example.model.UploadInitRequest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;

public class NodeHttpClient {

    public ChunkMapping sendUploadInitRequest(String leaderAddr, UploadInitRequest uploadInitRequest) {

        return WebClient.create("http://" + leaderAddr)
                .post()
                .uri("/upload-init")
                .bodyValue(uploadInitRequest)
                .retrieve()
                .bodyToMono(ChunkMapping.class)
                .block();
    }

    public void sendUploadCompleteRequest(String leaderAddr, String filePath) {
        WebClient.create("http://" + leaderAddr)
                .post()
                .uri(uriBuilder -> uriBuilder
                        .path("/upload-complete")
                        .queryParam("path", filePath)
                        .build())
                .retrieve()
                .toBodilessEntity()
                .block();
    }

    public ChunkMapping sendDownloadInitRequest(String leaderAddr, String filePath) {
        return WebClient.create("http://" + leaderAddr)
                .get()
                .uri(uriBuilder -> uriBuilder
                        .path("/download-init")
                        .queryParam("path", filePath)
                        .build())
                .retrieve()
                .bodyToMono(ChunkMapping.class)
                .block();
    }

    public List<String> sendFileListRequest(String leaderAddr, String prefix) {

        return WebClient.create("http://" + leaderAddr)
                .get()
                .uri(uriBuilder -> uriBuilder
                        .path("/list")
                        .queryParam("prefix", prefix)
                        .build())
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<List<String>>() {})
                .block();
    }

    public void sendDeleteRequest(String leaderAddr, String filePath) {
        WebClient.create("http://" + leaderAddr)
                .delete()
                .uri(uriBuilder -> uriBuilder
                        .path("/delete")
                        .queryParam("path", filePath)
                        .build())
                .retrieve()
                .toBodilessEntity()
                .block();
    }

    public byte[] sendGetChunkRequest(String nodeAddr, String chunkId) {
        return WebClient.create("http://" + nodeAddr)
                .get()
                .uri("/chunk/" + chunkId)
                .retrieve()
                .bodyToMono(byte[].class)
                .retry(3)
                .block();
    }

    public void sendPutChunkRequest(String nodeAddr, String chunkId, byte[] data) {
        WebClient.create("http://" + nodeAddr)
                .put()
                .uri("/chunk/" + chunkId)
                .bodyValue(data)
                .retrieve()
                .toBodilessEntity()
                .retry(3)
                .block();
    }
}
