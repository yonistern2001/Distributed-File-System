package org.example.filesystem.service;

import lombok.RequiredArgsConstructor;
import org.example.filesystem.component.NodeStateStore;
import org.example.filesystem.exception.InsufficientStorageException;
import org.example.filesystem.model.ChunkInfo;
import org.example.filesystem.model.FileInfo;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Service
public class ChunkDistributionService {

    private static final int REPLICAS = 2;

    private final NodeStateStore nodeStateStore;
    private final NodeTracker nodeTracker;

    public FileInfo mapFileToChunks(String filePath, long fileSize, long chunkSize) {

        long numOfChunks = (fileSize + chunkSize - 1) / chunkSize;

        Map<String, Long> nodeToMemory = nodeStateStore.getNodeIds()
                .stream()
                .filter(Predicate.not(nodeTracker::isNodeDead))
                .collect(Collectors.toMap(Function.identity(), nodeStateStore::getNodeFreeSpaceRemaining));


        List<ChunkInfo> chunkInfos = new ArrayList<>();

        for(int i = 0; i < numOfChunks; i++) {
            UUID uuid = UUID.randomUUID();
            String chunkId = "chunk_" + uuid;
            Set<String> nodeIds = chooseNodesForChunk(chunkSize, nodeToMemory);
            chunkInfos.add(new ChunkInfo(chunkId, nodeIds));
        }

        return new FileInfo(filePath, false, chunkInfos);
    }

    private static Set<String> chooseNodesForChunk(long chunkSize, Map<String, Long> nodeToMemory) {
        if(REPLICAS > nodeToMemory.size()) {
            throw new RuntimeException();
        }

        Comparator<String> comparator = Comparator.comparing(nodeToMemory::get);
        PriorityQueue<String> priorityQueue = new PriorityQueue<>(comparator.reversed());

        priorityQueue.addAll(nodeToMemory.keySet());


        Set<String> chunkNodes = new HashSet<>();
        for(int i = 0; i < REPLICAS; i++) {
            chunkNodes.add(priorityQueue.remove());
        }

        for(String chunkNode : chunkNodes) {
            long value = nodeToMemory.compute(chunkNode, (k, v) -> v - chunkSize);
            if(value < 0) {
                throw new InsufficientStorageException();
            }
        }
        return chunkNodes;

    }
}
