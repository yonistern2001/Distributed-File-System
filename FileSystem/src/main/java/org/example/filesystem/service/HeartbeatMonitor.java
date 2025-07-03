package org.example.filesystem.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.filesystem.component.NodeStateStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Log4j2
@RequiredArgsConstructor
@Service
public class HeartbeatMonitor {

    @Value("${heartbeat.timeout.dead}")
    private long nodeTimeout;
    private final NodeTracker nodeTracker;
    private final NodeStateStore nodeStateStore;

    @Scheduled(fixedRateString = "${heartbeat.timeout.dead}")
    public void monitor() {
        if(!nodeTracker.amLeader()) {
            return;
        }

        log.info("Node states:\n{}", nodeStateStore);
        nodeStateStore.getNodeIds()
                .stream()
                .filter(this::isDead)
                .forEach(nodeTracker::markNodeDead);
    }
    
    private boolean isDead(String nodeId) {
        long currentTime = System.currentTimeMillis();
        long interval = currentTime - nodeStateStore.getTimeOfNodesLastHeartbeat(nodeId);
        return interval >= nodeTimeout;
    }
}
