package org.example;
import java.io.Serializable;

public class GraphUpdate implements Serializable {
    public String opType;
    public Integer nodeId;
    public Integer targetNodeId;
    public String rawFeatures;
    public long timestamp;

    public GraphUpdate() {
    }

    public GraphUpdate(String opType, Integer nodeId, Integer targetNodeId, String rawFeatures, long timestamp) {
        this.opType = opType;
        this.nodeId = nodeId;
        this.targetNodeId = targetNodeId;
        this.rawFeatures = rawFeatures;
        this.timestamp = timestamp;
    }

    public static GraphUpdate fromCsv(String csv) {
        String[] parts = csv.split(",", 5);
        return new GraphUpdate(
                parts[0],
                Integer.parseInt(parts[1]),
                Integer.parseInt(parts[2]),
                parts[3],
                Long.parseLong(parts[4]));
    }

    @Override
    public String toString() {
        return String.format("GraphUpdate[op=%s, node=%d, target=%d, ts=%d]",
                opType, nodeId, targetNodeId, timestamp);
    }
}