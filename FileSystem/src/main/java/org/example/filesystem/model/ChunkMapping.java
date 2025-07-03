package org.example.filesystem.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
public class ChunkMapping {

    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("chunk_mapping")
    private List<ChunkEntry> chunkMapping;

    public ChunkMapping(FileInfo fileInfo, Function<String, String> nodeToAddressMapping) {
        this.filePath = fileInfo.getFilePath();
        this.chunkMapping = new ArrayList<>();

        for(ChunkInfo chunkInfo : fileInfo.getChunks()) {
            Set<String> nodeAddresses = chunkInfo.getNodeIds()
                    .stream()
                    .map(nodeToAddressMapping)
                    .collect(Collectors.toSet());

            ChunkEntry chunkEntry = new ChunkEntry(chunkInfo.getChunkId(), nodeAddresses);
            this.chunkMapping.add(chunkEntry);
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ChunkEntry {

        @JsonProperty("chunk_id")
        private String chunkId;

        @JsonProperty("node_address")
        private Set<String> nodeAddresses;
    }
}
