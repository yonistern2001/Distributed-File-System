package org.example.filesystem.component;

import org.example.filesystem.model.Heartbeat;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class NodeStateStore {

    public final Map<String, State> nodeStates = new ConcurrentHashMap<>();

    public void updateNodeState(Heartbeat heartbeat) {
        State state = new State(heartbeat.getAddress(), heartbeat.getFreeSpace(), System.currentTimeMillis());
        nodeStates.put(heartbeat.getNodeId(), state);
    }

    public String getNodeAddress(String nodeId) {
        State nodeState = nodeStates.get(nodeId);
        return nodeState != null ? nodeState.nodeAddress() : null;
    }

    public Long getNodeFreeSpaceRemaining(String nodeId) {
        State nodeState = nodeStates.get(nodeId);
        return nodeState != null ? nodeState.storageRemaining() : null;
    }

    public Long getTimeOfNodesLastHeartbeat(String nodeId) {
        State nodeState = nodeStates.get(nodeId);
        return nodeState != null ? nodeState.timeStamp() : null;
    }

    public Set<String> getNodeIds() {
        return nodeStates.keySet();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        nodeStates.forEach((k, v) -> builder.append("node id: ").append(k).append(" ").append(v).append("\n"));
        return builder.toString();
    }

    public record State(String nodeAddress, long storageRemaining, long timeStamp) {}
}
