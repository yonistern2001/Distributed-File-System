package org.example.filesystem.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class ChunkInfo {

    private String chunkId;

    private Set<String> nodeIds;
}