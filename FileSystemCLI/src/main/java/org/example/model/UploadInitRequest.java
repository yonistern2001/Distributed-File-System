package org.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UploadInitRequest {

    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("file_size")
    private long fileSize;

    @JsonProperty("chunk_size")
    private long chunkSize;
}
