package org.example.filesystem.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.filesystem.component.client.HeartbeatSenderClient;
import org.example.filesystem.model.Heartbeat;
import org.example.filesystem.repository.ChunkRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

@Log4j2
@Service
@RequiredArgsConstructor
public class HeartbeatSender {

    private final ChunkRepository chunkRepository;
    private final NodeTracker nodeTracker;
    private final HeartbeatSenderClient client;

    @Scheduled(fixedRateString = "${heartbeat.interval}")
    public void sendHeartbeat() throws ExecutionException, InterruptedException {

        String leaderAddress = nodeTracker.getLeaderAddress();

        if(nodeTracker.amLeader()) {
            return;
        }

        Heartbeat heartbeat = createHeartbeat();

        client.sendHeartbeat("http://" + leaderAddress, heartbeat, handleLeaderDown(leaderAddress));
    }

    private Consumer<Throwable> handleLeaderDown(String leaderAddress) {
        return error -> {
            try {
                nodeTracker.markLeaderDead(leaderAddress);

            } catch (ExecutionException | InterruptedException e) {
                log.error(e);
            }
        };
    }

    private Heartbeat createHeartbeat() {
        long remainingStorage = chunkRepository.getRemainingStorage();
        String nodeId = nodeTracker.getCurrentNodeId();
        String nodeAddress = nodeTracker.getCurrentNodeAddress();
        return new Heartbeat(nodeId, nodeAddress, remainingStorage);
    }
}
