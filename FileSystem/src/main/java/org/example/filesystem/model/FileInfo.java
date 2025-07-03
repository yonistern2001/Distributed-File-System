package org.example.filesystem.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class FileInfo {

    private String filePath;

    private boolean isActive;

    private List<ChunkInfo> chunks;
}
