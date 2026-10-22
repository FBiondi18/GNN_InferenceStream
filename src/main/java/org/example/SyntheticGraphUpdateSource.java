package org.example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class SyntheticGraphUpdateSource implements SourceFunction<String> {
    private volatile boolean running = true;
    private final AtomicLong baseTimestamp;
    private final Random random;
    private static final int MAX_NODE_ID = 232964;
    private static final String[] OP_TYPES = {
            "ADD_EDGE", "REMOVE_EDGE", "ADD_NODE", "REMOVE_NODE", "UPDATE_FEATURES"
    };

    public SyntheticGraphUpdateSource() {
        this.baseTimestamp = new AtomicLong(System.currentTimeMillis());
        this.random = new Random(42L);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (running) {
            String op = OP_TYPES[random.nextInt(OP_TYPES.length)];
            int nodeId = random.nextInt(MAX_NODE_ID + 1);
            int targetNodeId = (op.equals("ADD_EDGE") || op.equals("REMOVE_EDGE"))
                    ? random.nextInt(MAX_NODE_ID + 1)
                    : -1;

            // Per ora mettiamo un placeholder "dummy_feats" invece di 602 float
            String features = (op.equals("ADD_NODE") || op.equals("UPDATE_FEATURES"))
                    ? "0.5;0.5;0.5"
                    : "null";

            long eventTime = baseTimestamp.getAndAdd(200);

            // Formato: OP_TYPE,nodeId,targetNodeId,features,timestamp
            String message = op + "," + nodeId + "," + targetNodeId + "," + features + "," + eventTime;

            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(message);
            }

            // Emette 1 messaggio al secondo per facilitare il debug nei log
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}