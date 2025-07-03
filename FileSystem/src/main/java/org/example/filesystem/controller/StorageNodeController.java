package org.example.filesystem.controller;

import lombok.RequiredArgsConstructor;
import org.example.filesystem.repository.ChunkRepository;
import org.example.filesystem.service.NodeTracker;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

@RequiredArgsConstructor
@RestController
@RequestMapping("/chunk")
public class StorageNodeController {

    private final ChunkRepository chunkRepository;
    private final NodeTracker nodeTracker;

    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.CREATED)
    public void upload(@PathVariable String id, @RequestBody byte[] chunk) throws IOException {
        assertNotLeader();
        this.chunkRepository.storeChunk(id, chunk);
    }

    @GetMapping("/{id}")
    public ResponseEntity<byte[]> download(@PathVariable String id) throws IOException {
        assertNotLeader();
        return ResponseEntity.ok(this.chunkRepository.getChunk(id));
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String id) throws IOException {
        assertNotLeader();
        this.chunkRepository.deleteChunk(id);
    }

    @ExceptionHandler(NoSuchFileException.class)
    public ResponseEntity<Void> handleNoSuchFileException(NoSuchFileException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
    }

    private void assertNotLeader() {
        if(nodeTracker.amLeader()) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Node is not follower");
        }
    }
}
