package org.example.filesystem.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.example.filesystem.component.NodeStateStore;
import org.example.filesystem.component.client.ChunkDeletionClient;
import org.example.filesystem.model.*;
import org.example.filesystem.repository.EtcdRepository;
import org.example.filesystem.service.ChunkDistributionService;
import org.example.filesystem.service.NodeTracker;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@RequiredArgsConstructor
@RestController
public class ControlServiceController {

    private final NodeStateStore nodeStateStore;
    private final NodeTracker nodeTracker;
    private final EtcdRepository etcdRepository;
    private final ChunkDistributionService chunkDistributionService;
    private final ChunkDeletionClient client;

    @PostMapping("/upload-init")
    public ResponseEntity<ChunkMapping> uploadInit(@RequestBody UploadInitRequest request) throws ExecutionException, InterruptedException, JsonProcessingException {
        assertIsLeader();

        FileInfo fileInfo = chunkDistributionService
                .mapFileToChunks(request.getFilePath(), request.getFileSize(), request.getChunkSize());
        etcdRepository.saveFileInfo(fileInfo);
        ChunkMapping response = new ChunkMapping(fileInfo, nodeStateStore::getNodeAddress);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/upload-complete")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void uploadComplete(@RequestParam String path) throws IOException, ExecutionException, InterruptedException {
        assertIsLeader();

        FileInfo fileInfo = etcdRepository.getFileInfo(path);

        if(fileInfo == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "File info not found");
        }

        if(fileInfo.isActive()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "File info already marked as active");
        }

        fileInfo.setActive(true);
        etcdRepository.saveFileInfo(fileInfo);
    }

    @GetMapping("/download-init")
    public ResponseEntity<ChunkMapping> downloadInit(@RequestParam String path) throws ExecutionException, InterruptedException {
        assertIsLeader();

        FileInfo fileInfo = etcdRepository.getFileInfo(path);
        if(fileInfo == null || !fileInfo.isActive()) {
            return ResponseEntity.notFound().build();
        }

        ChunkMapping response = new ChunkMapping(fileInfo, nodeStateStore::getNodeAddress);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/list")
    public ResponseEntity<List<String>> listFiles(@RequestParam String prefix) throws ExecutionException, InterruptedException {
        assertIsLeader();

        List<FileInfo> fileInfos = etcdRepository.getFileInfosStartingWith(prefix);
        List<String> files = fileInfos
                .stream()
                .filter(FileInfo::isActive)
                .map(FileInfo::getFilePath)
                .toList();
        return ResponseEntity.ok(files);
    }

    @DeleteMapping("/delete")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deleteFile(@RequestParam String path) throws ExecutionException, InterruptedException {
        assertIsLeader();

        FileInfo deletedFileInfo = etcdRepository.deleteFileInfo(path);
        for(ChunkInfo chunk : deletedFileInfo.getChunks()) {
            Set<String> nodeIds = chunk.getNodeIds();
            nodeIds.forEach(nodeId -> client.sendDeleteToNode(nodeId, chunk.getChunkId()));
        }
    }

    @PostMapping("/heartbeat")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void receiveHeartbeat(@RequestBody Heartbeat heartbeat) {
        assertIsLeader();

        nodeStateStore.updateNodeState(heartbeat);
    }

    private void assertIsLeader() {
        if(!nodeTracker.amLeader()) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Node is not leader");
        }
    }

}
