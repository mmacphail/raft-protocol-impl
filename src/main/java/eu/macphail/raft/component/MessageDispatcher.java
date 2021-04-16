package eu.macphail.raft.component;

import eu.macphail.raft.entity.message.NodeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class MessageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(MessageDispatcher.class);

    public void dispatch(int nodeId, NodeMessage message) {
        LOG.info("Dispatched message {} to node {}", message, nodeId);
    }

    public void forwardToLeader(Integer leader, NodeMessage nodeMessage) {
        LOG.info("Forwarding message {} to leader {}", nodeMessage, leader);
    }

    public void deliver(byte[] data) {
        LOG.info("Delivering data");
    }
}
