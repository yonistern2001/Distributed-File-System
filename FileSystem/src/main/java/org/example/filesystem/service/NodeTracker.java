package org.example.filesystem.service;

import io.vertx.core.impl.ConcurrentHashSet;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.filesystem.repository.EtcdRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ExecutionException;

@Log4j2
@RequiredArgsConstructor
@Service
public class NodeTracker {

    private final EtcdRepository etcdRepository;
    private final Set<String> deadNodes = new ConcurrentHashSet<>();
    private volatile boolean amLeader = false;

    @Getter
    @Value("${node.id}")
    private String currentNodeId;

    @Getter
    @Value("${node.url}")
    private String currentNodeAddress;

    public boolean amLeader() {
        return this.amLeader;
    }

    public boolean isNodeDead(String nodeId) {
        return this.deadNodes.contains(nodeId);
    }

    public String getLeaderAddress() throws ExecutionException, InterruptedException {
        if(amLeader) {
            return currentNodeAddress;
        }

        String leader = etcdRepository.getOrSetLeader(this.currentNodeAddress);
        if(this.currentNodeAddress.equals(leader)) {
            this.amLeader = true;
            log.info("Current node elected leader: {}, address: {}", this.currentNodeId, this.currentNodeAddress);
        }
        return leader;
    }

    protected void markNodeDead(String nodeId) {
        if(this.deadNodes.contains(nodeId)) {
            return;
        }

        if(!deadNodes.isEmpty()) {
            throw new RuntimeException("More than one node has been marked as dead");
        }

        log.info("Marking node {} as dead", nodeId);
        this.deadNodes.add(nodeId);
    }

    protected void markLeaderDead(String currLeaderAddress) throws ExecutionException, InterruptedException {
        if(!deadNodes.isEmpty()) {
            throw new RuntimeException("More than one node has been marked as dead");
        }

        log.info("Leader dead: {}", currLeaderAddress);
        etcdRepository.deleteLeader(currLeaderAddress);
    }
}
