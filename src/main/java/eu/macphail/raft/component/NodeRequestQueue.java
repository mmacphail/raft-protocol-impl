package eu.macphail.raft.component;

import eu.macphail.raft.boundary.Node;
import eu.macphail.raft.entity.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class NodeRequestQueue {

    @Value("${node.id}")
    private int nodeId;

    private static final Logger LOG = LoggerFactory.getLogger(NodeRequestQueue.class);

    private final TaskExecutor taskExecutor;
    private final Node node;
    private final BlockingQueue<NodeMessage> requestQueue;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public NodeRequestQueue(TaskExecutor taskExecutor, Node node) {
        this.taskExecutor = taskExecutor;
        this.node = node;
        this.requestQueue = new LinkedBlockingDeque<>();
    }

    @PostConstruct
    public void init() {
        taskExecutor.execute(this::foreverDequeue);
    }

    @PreDestroy
    public void cleanup() {
        running.set(false);

        LOG.info("Shutting down NodeRequestQueue, there are {} messages awaiting in the queue", requestQueue.size());
    }

    public void addToQueue(NodeMessage message) {
        this.requestQueue.add(message);
    }

    private void foreverDequeue() {
        try {
            while (running.get()) {
                NodeMessage message = requestQueue.poll(100, TimeUnit.MILLISECONDS);
                if(message != null) {
                    LOG.debug("Handling message {} on node {}", message, nodeId);
                    if (message instanceof LogRequest) {
                        node.onLogRequest((LogRequest) message);
                    } else if (message instanceof LogResponse) {
                        node.onLogResponse((LogResponse) message);
                    } else if (message instanceof VoteRequest) {
                        node.onVoteRequest((VoteRequest) message);
                    } else if (message instanceof VoteResponse) {
                        node.onVoteResponse((VoteResponse) message);
                    }
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            LOG.info("Interrupted NodeRequestQueue, there are {} messages awaiting in the queue", requestQueue.size());
            running.set(false);
        }
    }
}
