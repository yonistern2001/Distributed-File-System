package org.example.filesystem.component.client;

import lombok.extern.log4j.Log4j2;
import org.example.filesystem.model.Heartbeat;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.function.Consumer;

@Log4j2
@Component
public class HeartbeatSenderClient {

    private static final int NUM_RETRIES = 3;

    public void sendHeartbeat(String leaderUri, Heartbeat heartbeat, Consumer<Throwable> onError) {
        log.info("Sending heartbeat to leader: {}\n{}", leaderUri, heartbeat);
        WebClient.create(leaderUri)
                .post()
                .uri("/heartbeat")
                .bodyValue(heartbeat)
                .retrieve()
                .toBodilessEntity()
                .retry(NUM_RETRIES)
                .doOnError(onError)
                .onErrorComplete()
                .subscribe();
    }
}
