package org.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
public class ChunkMapping {

    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("chunk_mapping")
    private List<ChunkEntry> chunkMapping;

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

