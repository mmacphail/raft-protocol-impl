package eu.macphail.raft.component;

import eu.macphail.raft.boundary.Node;
import eu.macphail.raft.entity.Log;
import eu.macphail.raft.entity.NodeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Optional;

@Component
public class NodeBootstrap {

    @Value("${node.id}")
    private int nodeId;

    private static final Logger LOG = LoggerFactory.getLogger(NodeBootstrap.class);

    private final Node node;
    private final NodeDataRepository nodeDataRepository;
    private final LogRepository logRepository;

    public NodeBootstrap(Node node, NodeDataRepository nodeDataRepository, LogRepository logRepository) {
        this.node = node;
        this.nodeDataRepository = nodeDataRepository;
        this.logRepository = logRepository;
    }

    @PostConstruct
    public void init() {
        NodeData nodeData = null;
        Optional<NodeData> optTerm = nodeDataRepository.findById(nodeId);
        if(optTerm.isEmpty()) {
            NodeData data = initNodeData();
            nodeDataRepository.save(data);
            nodeData = data;
        } else {
            nodeData = optTerm.get();
        }

        List<Log> log = logRepository.findByNodeIdOrderByLogOffset(nodeId);

        node.init(nodeData.getCurrentTerm(),
                nodeData.getVotedFor(),
                log,
                nodeData.getCommitLength());
    }

    private NodeData initNodeData() {
        NodeData nodeData = new NodeData();
        nodeData.setNodeId(nodeId);
        nodeData.setCurrentTerm(0);
        nodeData.setVotedFor(null);
        nodeData.setCommitLength(0);
        return nodeData;
    }

}
