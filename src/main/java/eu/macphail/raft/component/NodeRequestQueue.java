package eu.macphail.raft.component;

import eu.macphail.raft.entity.message.NodeMessage;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Component
public class NodeRequestQueue {

    private final BlockingQueue<NodeMessage> requestQueue;

    public NodeRequestQueue() {
        this.requestQueue = new LinkedBlockingDeque<>();
    }

    public void addToQueue(NodeMessage message) {
        this.requestQueue.add(message);
    }
}
