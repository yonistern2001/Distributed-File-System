package org.example.filesystem.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Heartbeat {

    @JsonProperty("node_id")
    private String nodeId;

    private String address;

    @JsonProperty("free_space")
    private long freeSpace;
}
