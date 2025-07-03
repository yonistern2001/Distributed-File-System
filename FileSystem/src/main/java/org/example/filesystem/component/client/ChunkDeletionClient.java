package org.example.filesystem.component.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.filesystem.component.NodeStateStore;
import org.example.filesystem.service.NodeTracker;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Log4j2
@RequiredArgsConstructor
@Component
public class ChunkDeletionClient {

    private final NodeTracker nodeTracker;
    private final NodeStateStore nodeStateStore;

    public void sendDeleteToNode(String nodeId, String chunkId) {
        if(nodeTracker.isNodeDead(nodeId)) {
            return;
        }

        String nodeAddress = nodeStateStore.getNodeAddress(nodeId);
        String nodeUri = "http://" + nodeAddress;
        log.info("Sending delete request to {}: {}", nodeId,  nodeUri);
        WebClient.create(nodeUri)
                .delete()
                .uri("/chunk/" + chunkId)
                .retrieve()
                .toBodilessEntity()
                .subscribe();
    }
}
